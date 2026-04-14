package zitimanager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	zitimgmtv1 "github.com/agynio/tracing/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/tracing/internal/zitimgmtclient"
	"github.com/openziti/sdk-golang/ziti"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubListener struct {
	closed atomic.Bool
}

func (l *stubListener) Accept() (net.Conn, error) {
	return nil, errors.New("accept not implemented")
}

func (l *stubListener) Close() error {
	l.closed.Store(true)
	return nil
}

func (l *stubListener) Addr() net.Addr {
	return stubAddr{}
}

type stubAddr struct{}

func (stubAddr) Network() string { return "stub" }
func (stubAddr) String() string  { return "stub" }

type stubZitiContext struct {
	ziti.Context
	closed atomic.Bool
}

func (s *stubZitiContext) Close() {
	s.closed.Store(true)
}

type contextTracker struct {
	mu       sync.Mutex
	contexts []*stubZitiContext
}

func (c *contextTracker) add(ctx *stubZitiContext) {
	c.mu.Lock()
	c.contexts = append(c.contexts, ctx)
	c.mu.Unlock()
}

func (c *contextTracker) all() []*stubZitiContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*stubZitiContext(nil), c.contexts...)
}

func stubContexts(t *testing.T) *contextTracker {
	tracker := &contextTracker{}
	previous := newZitiContext
	newZitiContext = func(_ *ziti.Config) (ziti.Context, error) {
		ctx := &stubZitiContext{}
		tracker.add(ctx)
		return ctx, nil
	}
	t.Cleanup(func() {
		newZitiContext = previous
	})
	return tracker
}

type fakeZitiMgmtServer struct {
	zitimgmtv1.UnimplementedZitiManagementServiceServer
	requestIdentity     func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error)
	extendIdentityLease func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest) (*zitimgmtv1.ExtendIdentityLeaseResponse, error)
}

func (f *fakeZitiMgmtServer) RequestServiceIdentity(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
	if f.requestIdentity == nil {
		return nil, status.Error(codes.Unimplemented, "request identity not implemented")
	}
	return f.requestIdentity(ctx, req)
}

func (f *fakeZitiMgmtServer) ExtendIdentityLease(ctx context.Context, req *zitimgmtv1.ExtendIdentityLeaseRequest) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
	if f.extendIdentityLease == nil {
		return nil, status.Error(codes.Unimplemented, "extend lease not implemented")
	}
	return f.extendIdentityLease(ctx, req)
}

func startMgmtClient(t *testing.T, server *fakeZitiMgmtServer) (*zitimgmtclient.Client, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	zitimgmtv1.RegisterZitiManagementServiceServer(grpcServer, server)

	serveDone := make(chan struct{})
	go func() {
		_ = grpcServer.Serve(listener)
		close(serveDone)
	}()

	client, err := zitimgmtclient.NewClient(listener.Addr().String())
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("create client: %v", err)
	}

	cleanup := func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
		grpcServer.Stop()
		_ = listener.Close()
		<-serveDone
	}

	return client, cleanup
}

func waitForCondition(t *testing.T, timeout time.Duration, check func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", description)
}

func TestManagerInitialEnrollment(t *testing.T) {
	tracker := stubContexts(t)

	identityJSON := []byte(`{}`)
	var requestCalls atomic.Int32
	server := &fakeZitiMgmtServer{
		requestIdentity: func(_ context.Context, req *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
			requestCalls.Add(1)
			if req.GetServiceType() != zitimgmtv1.ServiceType_SERVICE_TYPE_LLM_PROXY {
				return nil, status.Error(codes.InvalidArgument, "unexpected service type")
			}
			return &zitimgmtv1.RequestServiceIdentityResponse{
				ZitiIdentityId: "identity-1",
				IdentityJson:   identityJSON,
			}, nil
		},
		extendIdentityLease: func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
			return &zitimgmtv1.ExtendIdentityLeaseResponse{}, nil
		},
	}

	client, cleanup := startMgmtClient(t, server)
	defer cleanup()

	var listenerCalls atomic.Int32
	var latestListener net.Listener
	listenerFactory := func(ziti.Context) (net.Listener, error) {
		return &stubListener{}, nil
	}
	onNewListener := func(listener net.Listener) {
		listenerCalls.Add(1)
		latestListener = listener
	}

	manager, err := New(
		context.Background(),
		client,
		zitimgmtv1.ServiceType_SERVICE_TYPE_LLM_PROXY,
		time.Minute,
		time.Second,
		listenerFactory,
		onNewListener,
	)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	if manager.currentIdentityID() != "identity-1" {
		t.Fatalf("expected identity id to be set, got %q", manager.currentIdentityID())
	}
	if requestCalls.Load() != 1 {
		t.Fatalf("expected 1 enrollment request, got %d", requestCalls.Load())
	}
	if listenerCalls.Load() != 1 {
		t.Fatalf("expected listener callback once, got %d", listenerCalls.Load())
	}
	if latestListener == nil {
		t.Fatal("expected listener to be created")
	}
	contexts := tracker.all()
	if len(contexts) != 1 {
		t.Fatalf("expected 1 ziti context, got %d", len(contexts))
	}
	if manager.zitiCtx != contexts[0] {
		t.Fatal("expected manager to store new ziti context")
	}
}

func TestManagerReEnrollsOnNotFound(t *testing.T) {
	tracker := stubContexts(t)

	identityJSON := []byte(`{}`)
	var requestCalls atomic.Int32
	var extendCalls atomic.Int32
	server := &fakeZitiMgmtServer{
		requestIdentity: func(_ context.Context, _ *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
			call := requestCalls.Add(1)
			return &zitimgmtv1.RequestServiceIdentityResponse{
				ZitiIdentityId: fmt.Sprintf("identity-%d", call),
				IdentityJson:   identityJSON,
			}, nil
		},
		extendIdentityLease: func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
			call := extendCalls.Add(1)
			if call == 1 {
				return nil, status.Error(codes.NotFound, "missing")
			}
			return &zitimgmtv1.ExtendIdentityLeaseResponse{}, nil
		},
	}

	client, cleanup := startMgmtClient(t, server)
	defer cleanup()

	var listenerCalls atomic.Int32
	listenerFactory := func(ziti.Context) (net.Listener, error) {
		return &stubListener{}, nil
	}
	onNewListener := func(net.Listener) {
		listenerCalls.Add(1)
	}

	manager, err := New(
		context.Background(),
		client,
		zitimgmtv1.ServiceType_SERVICE_TYPE_LLM_PROXY,
		10*time.Millisecond,
		time.Second,
		listenerFactory,
		onNewListener,
	)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go manager.RunLeaseRenewal(ctx)

	waitForCondition(t, 200*time.Millisecond, func() bool {
		return listenerCalls.Load() >= 2
	}, "listener rebinding")

	if requestCalls.Load() < 2 {
		t.Fatalf("expected re-enrollment request, got %d", requestCalls.Load())
	}
	if manager.currentIdentityID() != "identity-2" {
		t.Fatalf("expected identity to re-enroll, got %q", manager.currentIdentityID())
	}
	contexts := tracker.all()
	if len(contexts) < 2 {
		t.Fatalf("expected new context on re-enroll, got %d", len(contexts))
	}
	if !contexts[0].closed.Load() {
		t.Fatal("expected previous ziti context to be closed")
	}
}

func TestManagerReEnrollDebouncesConcurrentCalls(t *testing.T) {
	tracker := stubContexts(t)

	identityJSON := []byte(`{}`)
	var requestCalls atomic.Int32
	server := &fakeZitiMgmtServer{
		requestIdentity: func(_ context.Context, _ *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
			call := requestCalls.Add(1)
			return &zitimgmtv1.RequestServiceIdentityResponse{
				ZitiIdentityId: fmt.Sprintf("identity-%d", call),
				IdentityJson:   identityJSON,
			}, nil
		},
		extendIdentityLease: func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
			return &zitimgmtv1.ExtendIdentityLeaseResponse{}, nil
		},
	}

	client, cleanup := startMgmtClient(t, server)
	defer cleanup()

	var listenerCalls atomic.Int32
	listenerFactory := func(ziti.Context) (net.Listener, error) {
		return &stubListener{}, nil
	}
	onNewListener := func(net.Listener) {
		listenerCalls.Add(1)
	}

	manager, err := New(
		context.Background(),
		client,
		zitimgmtv1.ServiceType_SERVICE_TYPE_LLM_PROXY,
		time.Minute,
		time.Second,
		listenerFactory,
		onNewListener,
	)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 5)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- manager.reEnroll(ctx)
		}()
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil {
			t.Fatalf("unexpected re-enroll error: %v", err)
		}
	}

	if requestCalls.Load() != 2 {
		t.Fatalf("expected single re-enroll, got %d", requestCalls.Load())
	}
	if listenerCalls.Load() != 2 {
		t.Fatalf("expected listener rebinding once, got %d", listenerCalls.Load())
	}
	contexts := tracker.all()
	if len(contexts) != 2 {
		t.Fatalf("expected two contexts after re-enroll, got %d", len(contexts))
	}
}

func TestManagerReEnrollAfterFailure(t *testing.T) {
	tracker := stubContexts(t)

	identityJSON := []byte(`{}`)
	var requestCalls atomic.Int32
	var failNext atomic.Bool
	failNext.Store(true)
	server := &fakeZitiMgmtServer{
		requestIdentity: func(_ context.Context, _ *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
			call := requestCalls.Add(1)
			if call == 1 {
				return &zitimgmtv1.RequestServiceIdentityResponse{
					ZitiIdentityId: "identity-1",
					IdentityJson:   identityJSON,
				}, nil
			}
			if failNext.Load() {
				return nil, status.Error(codes.Unavailable, "boom")
			}
			return &zitimgmtv1.RequestServiceIdentityResponse{
				ZitiIdentityId: "identity-2",
				IdentityJson:   identityJSON,
			}, nil
		},
		extendIdentityLease: func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
			return &zitimgmtv1.ExtendIdentityLeaseResponse{}, nil
		},
	}

	client, cleanup := startMgmtClient(t, server)
	defer cleanup()

	var listenerCalls atomic.Int32
	listenerFactory := func(ziti.Context) (net.Listener, error) {
		return &stubListener{}, nil
	}
	onNewListener := func(net.Listener) {
		listenerCalls.Add(1)
	}

	manager, err := New(
		context.Background(),
		client,
		zitimgmtv1.ServiceType_SERVICE_TYPE_LLM_PROXY,
		time.Minute,
		time.Second,
		listenerFactory,
		onNewListener,
	)
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	failedCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if err := manager.reEnroll(failedCtx); err == nil {
		t.Fatal("expected re-enroll to fail")
	}
	if listenerCalls.Load() != 1 {
		t.Fatalf("expected no listener rebinding on failure, got %d", listenerCalls.Load())
	}

	failNext.Store(false)

	successCtx, cancelSuccess := context.WithTimeout(context.Background(), time.Second)
	defer cancelSuccess()

	if err := manager.reEnroll(successCtx); err != nil {
		t.Fatalf("expected re-enroll success, got %v", err)
	}
	if listenerCalls.Load() != 2 {
		t.Fatalf("expected listener rebinding after recovery, got %d", listenerCalls.Load())
	}
	if manager.currentIdentityID() != "identity-2" {
		t.Fatalf("expected identity-2 after recovery, got %q", manager.currentIdentityID())
	}
	contexts := tracker.all()
	if len(contexts) < 2 {
		t.Fatalf("expected new context after recovery, got %d", len(contexts))
	}
}
