package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	notificationsv1 "github.com/agynio/tracing/.gen/go/agynio/api/notifications/v1"
	tracingv1 "github.com/agynio/tracing/.gen/go/agynio/api/tracing/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agynio/tracing/internal/config"
	"github.com/agynio/tracing/internal/db"
	"github.com/agynio/tracing/internal/ingest"
	"github.com/agynio/tracing/internal/notifier"
	"github.com/agynio/tracing/internal/server"
	"github.com/agynio/tracing/internal/store"
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

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	log.Printf("TracingService listening on %s", cfg.GRPCAddress)
	if err := grpcServer.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
