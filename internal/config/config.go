package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultGRPCAddress        = ":50051"
	defaultZitiManagementAddr = "ziti-management:50051"
	defaultZitiServiceName    = "tracing"
	defaultZitiLeaseInterval  = 2 * time.Minute
	defaultZitiEnrollTimeout  = 5 * time.Minute
)

type Config struct {
	GRPCAddress              string
	DatabaseURL              string
	NotificationsAddress     string
	ZitiEnabled              bool
	ZitiManagementAddress    string
	ZitiServiceName          string
	ZitiLeaseRenewalInterval time.Duration
	ZitiEnrollmentTimeout    time.Duration
}

func FromEnv() (Config, error) {
	zitiEnabled, err := envBool("ZITI_ENABLED")
	if err != nil {
		return Config{}, err
	}

	zitiLeaseRenewalInterval, err := envDuration("ZITI_LEASE_RENEWAL_INTERVAL", defaultZitiLeaseInterval)
	if err != nil {
		return Config{}, err
	}
	if zitiLeaseRenewalInterval <= 0 {
		return Config{}, fmt.Errorf("ZITI_LEASE_RENEWAL_INTERVAL must be positive")
	}

	zitiEnrollmentTimeout, err := envDuration("ZITI_ENROLLMENT_TIMEOUT", defaultZitiEnrollTimeout)
	if err != nil {
		return Config{}, err
	}
	if zitiEnrollmentTimeout <= 0 {
		return Config{}, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be positive")
	}

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}

	notificationsAddress := strings.TrimSpace(os.Getenv("NOTIFICATIONS_ADDRESS"))
	if notificationsAddress == "" {
		return Config{}, fmt.Errorf("NOTIFICATIONS_ADDRESS must be set")
	}

	return Config{
		GRPCAddress:              envOrDefault("GRPC_ADDRESS", defaultGRPCAddress),
		DatabaseURL:              databaseURL,
		NotificationsAddress:     notificationsAddress,
		ZitiEnabled:              zitiEnabled,
		ZitiManagementAddress:    envOrDefault("ZITI_MANAGEMENT_ADDRESS", defaultZitiManagementAddr),
		ZitiServiceName:          envOrDefault("ZITI_SERVICE_NAME", defaultZitiServiceName),
		ZitiLeaseRenewalInterval: zitiLeaseRenewalInterval,
		ZitiEnrollmentTimeout:    zitiEnrollmentTimeout,
	}, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envBool(name string) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false, nil
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}

	return parsed, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", name, err)
	}

	return parsed, nil
}
