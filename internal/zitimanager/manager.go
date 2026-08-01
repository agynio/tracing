package zitimanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	zitimgmtv1 "github.com/agynio/tracing/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/tracing/internal/zitimgmtclient"
	"github.com/openziti/sdk-golang/ziti"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	retryInitialBackoff = 1 * time.Second
	retryMaxBackoff     = 30 * time.Second
	maxLeaseExtendRetry = 3
	retryUnlimited      = 0
)

// ErrIdentityLost reports that the service's OpenZiti identity no longer
// exists (garbage-collected while the pod was running). Recovery is a pod
// restart: a fresh pod enrolls a fresh identity.
var ErrIdentityLost = errors.New("ziti identity lost")

var (
	errIdentityNotFound = errors.New("ziti identity not found")
	newZitiContext      = ziti.NewContext
)

type ListenerFactory func(zitiCtx ziti.Context) (net.Listener, error)
type OnNewListener func(listener net.Listener)

type Manager struct {
	mu              sync.RWMutex
	zitiCtx         ziti.Context
	identityID      string
	mgmtClient      *zitimgmtclient.Client
	serviceType     zitimgmtv1.ServiceType
	renewalInterval time.Duration
	enrollTimeout   time.Duration

	listenerFactory ListenerFactory
	onNewListener   OnNewListener
}

func New(
	ctx context.Context,
	mgmtClient *zitimgmtclient.Client,
	serviceType zitimgmtv1.ServiceType,
	renewalInterval time.Duration,
	enrollTimeout time.Duration,
	listenerFactory ListenerFactory,
	onNewListener OnNewListener,
) (*Manager, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if mgmtClient == nil {
		return nil, fmt.Errorf("ziti management client is required")
	}
	if serviceType == zitimgmtv1.ServiceType_SERVICE_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("service type is required")
	}
	if renewalInterval <= 0 {
		return nil, fmt.Errorf("renewal interval must be positive")
	}
	if enrollTimeout <= 0 {
		return nil, fmt.Errorf("enrollment timeout must be positive")
	}
	if listenerFactory == nil {
		return nil, fmt.Errorf("listener factory is required")
	}
	if onNewListener == nil {
		return nil, fmt.Errorf("on new listener callback is required")
	}

	manager := &Manager{
		mgmtClient:      mgmtClient,
		serviceType:     serviceType,
		renewalInterval: renewalInterval,
		enrollTimeout:   enrollTimeout,
		listenerFactory: listenerFactory,
		onNewListener:   onNewListener,
	}

	if err := manager.initialEnroll(ctx); err != nil {
		return nil, err
	}

	return manager, nil
}

// RunLeaseRenewal extends the identity lease until ctx is cancelled (returns
// nil) or the identity is definitively gone (returns an error wrapping
// ErrIdentityLost). Identity loss is fatal by design: the caller must log the
// error and terminate so the pod restart path enrolls a fresh identity.
// Transient extension failures are retried with backoff and logged.
func (m *Manager) RunLeaseRenewal(ctx context.Context) error {
	ticker := time.NewTicker(m.renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		if ctx.Err() != nil {
			return nil
		}

		err := m.extendLease(ctx)
		switch {
		case err == nil:
			continue
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, errIdentityNotFound):
			return fmt.Errorf("%w: identity %s no longer exists", ErrIdentityLost, m.currentIdentityID())
		default:
			log.Printf("failed to extend ziti lease for identity %s: %v", m.currentIdentityID(), err)
		}
	}
}

func (m *Manager) Close() {
	m.mu.Lock()
	oldCtx := m.zitiCtx
	m.zitiCtx = nil
	m.identityID = ""
	m.mu.Unlock()

	if oldCtx != nil {
		oldCtx.Close()
	}
}

func (m *Manager) initialEnroll(ctx context.Context) error {
	zitiCtx, identityID, listener, err := m.enroll(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.zitiCtx = zitiCtx
	m.identityID = identityID
	m.mu.Unlock()

	m.onNewListener(listener)
	return nil
}

func (m *Manager) extendLease(ctx context.Context) error {
	identityID := m.currentIdentityID()
	if identityID == "" {
		return fmt.Errorf("ziti identity missing")
	}

	err := retryWithBackoff(ctx, "extend ziti lease", func(err error) bool {
		return !isNotFoundError(err)
	}, maxLeaseExtendRetry, func(attemptCtx context.Context) error {
		return m.mgmtClient.ExtendIdentityLease(attemptCtx, identityID)
	})
	if err == nil {
		return nil
	}
	if isNotFoundError(err) {
		return errIdentityNotFound
	}
	return err
}

func (m *Manager) enroll(ctx context.Context) (ziti.Context, string, net.Listener, error) {
	enrollCtx, cancel := context.WithTimeout(ctx, m.enrollTimeout)
	defer cancel()

	var identityID string
	var identityJSON []byte
	err := retryWithBackoff(enrollCtx, "ziti enrollment", nil, retryUnlimited, func(attemptCtx context.Context) error {
		var requestErr error
		identityID, identityJSON, requestErr = m.mgmtClient.RequestServiceIdentity(attemptCtx, m.serviceType)
		return requestErr
	})
	if err != nil {
		return nil, "", nil, err
	}

	zitiConfig := &ziti.Config{}
	if err := json.Unmarshal(identityJSON, zitiConfig); err != nil {
		return nil, "", nil, fmt.Errorf("parse ziti identity: %w", err)
	}

	zitiCtx, err := newZitiContext(zitiConfig)
	if err != nil {
		return nil, "", nil, fmt.Errorf("create ziti context: %w", err)
	}

	listener, err := m.listenerFactory(zitiCtx)
	if err != nil {
		zitiCtx.Close()
		return nil, "", nil, fmt.Errorf("listen on ziti service: %w", err)
	}

	return zitiCtx, identityID, listener, nil
}

func (m *Manager) currentIdentityID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityID
}

func retryWithBackoff(ctx context.Context, operationName string, isRetryable func(error) bool, maxAttempts int, fn func(context.Context) error) error {
	backoff := retryInitialBackoff
	attempt := 1
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if isRetryable != nil && !isRetryable(err) {
			return err
		}

		if maxAttempts > 0 && attempt >= maxAttempts {
			return err
		}

		delay := backoff
		if delay > retryMaxBackoff {
			delay = retryMaxBackoff
		}

		log.Printf("%s failed (attempt %d), retrying in %s: %v", operationName, attempt, delay, err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > retryMaxBackoff {
			backoff = retryMaxBackoff
		}
		attempt++
	}
}

func isNotFoundError(err error) bool {
	statusErr, ok := status.FromError(err)
	if !ok {
		return false
	}
	return statusErr.Code() == codes.NotFound
}
