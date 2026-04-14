package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	notificationsv1 "github.com/agynio/tracing/.gen/go/agynio/api/notifications/v1"
	tracingv1 "github.com/agynio/tracing/.gen/go/agynio/api/tracing/v1"
	zitimgmtv1 "github.com/agynio/tracing/.gen/go/agynio/api/ziti_management/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/openziti/sdk-golang/ziti"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agynio/tracing/internal/config"
	"github.com/agynio/tracing/internal/db"
	"github.com/agynio/tracing/internal/ingest"
	"github.com/agynio/tracing/internal/notifier"
	"github.com/agynio/tracing/internal/server"
	"github.com/agynio/tracing/internal/store"
	"github.com/agynio/tracing/internal/zitimanager"
	"github.com/agynio/tracing/internal/zitimgmtclient"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("tracing: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	notificationsConn, err := grpc.DialContext(ctx, cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications: %w", err)
	}
	defer notificationsConn.Close()

	st := store.NewStore(pool)
	n := notifier.New(notificationsv1.NewNotificationsServiceClient(notificationsConn))

	grpcServer := grpc.NewServer()
	tracingv1.RegisterTracingServiceServer(grpcServer, server.New(st))
	collectortracev1.RegisterTraceServiceServer(grpcServer, ingest.NewHandler(st, n))

	lis, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddress, err)
	}

	errCh := make(chan error, 2)

	go func() {
		log.Printf("TracingService listening on %s", cfg.GRPCAddress)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			errCh <- fmt.Errorf("grpc server stopped: %w", err)
		}
	}()

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	if cfg.ZitiEnabled {
		zitiMgmtClient, err := zitimgmtclient.NewClient(cfg.ZitiManagementAddress)
		if err != nil {
			return fmt.Errorf("create ziti management client: %w", err)
		}
		defer func() {
			if closeErr := zitiMgmtClient.Close(); closeErr != nil {
				log.Printf("failed to close ziti management client: %v", closeErr)
			}
		}()

		var listenerMu sync.Mutex
		var currentListener net.Listener
		listenerFactory := func(zitiCtx ziti.Context) (net.Listener, error) {
			return zitiCtx.ListenWithOptions(cfg.ZitiServiceName, ziti.DefaultListenOptions())
		}
		onNewListener := func(listener net.Listener) {
			listenerMu.Lock()
			previousListener := currentListener
			currentListener = listener
			listenerMu.Unlock()

			log.Printf("TracingService listening on ziti service %s", cfg.ZitiServiceName)
			go func(activeListener net.Listener) {
				if err := grpcServer.Serve(activeListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
					errCh <- fmt.Errorf("ziti server stopped: %w", err)
				}
			}(listener)

			if previousListener != nil {
				if err := previousListener.Close(); err != nil {
					log.Printf("failed to close previous ziti listener: %v", err)
				}
			}
		}

		zitiManager, err := zitimanager.New(
			ctx,
			zitiMgmtClient,
			zitimgmtv1.ServiceType_SERVICE_TYPE_TRACING,
			cfg.ZitiLeaseRenewalInterval,
			cfg.ZitiEnrollmentTimeout,
			listenerFactory,
			onNewListener,
		)
		if err != nil {
			return fmt.Errorf("setup ziti manager: %w", err)
		}
		defer zitiManager.Close()

		go zitiManager.RunLeaseRenewal(ctx)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}
	return nil
}
