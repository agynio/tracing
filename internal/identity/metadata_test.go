package identity

import (
	"testing"

	identityv1 "github.com/agynio/tracing/.gen/go/agynio/api/identity/v1"
)

func TestIdentityTypeMetadataValue(t *testing.T) {
	tests := map[identityv1.IdentityType]string{
		identityv1.IdentityType_IDENTITY_TYPE_AGENT:          "agent",
		identityv1.IdentityType_IDENTITY_TYPE_AGENT_INSTANCE: "agent_instance",
		identityv1.IdentityType_IDENTITY_TYPE_RUNNER:         "runner",
		identityv1.IdentityType_IDENTITY_TYPE_USER:           "user",
		identityv1.IdentityType_IDENTITY_TYPE_APP:            "app",
	}
	for identityType, want := range tests {
		t.Run(identityType.String(), func(t *testing.T) {
			got, err := IdentityTypeMetadataValue(identityType)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != want {
				t.Fatalf("expected %q, got %q", want, got)
			}
		})
	}
}

func TestIdentityTypeMetadataValueRejectsUnsupported(t *testing.T) {
	for _, identityType := range []identityv1.IdentityType{
		identityv1.IdentityType_IDENTITY_TYPE_UNSPECIFIED,
		identityv1.IdentityType_IDENTITY_TYPE_SANDBOX,
	} {
		t.Run(identityType.String(), func(t *testing.T) {
			if _, err := IdentityTypeMetadataValue(identityType); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
