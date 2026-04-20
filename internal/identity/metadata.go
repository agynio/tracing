package identity

import (
	"fmt"

	identityv1 "github.com/agynio/tracing/.gen/go/agynio/api/identity/v1"
)

const (
	identityTypeAgent  = "agent"
	identityTypeRunner = "runner"
	identityTypeUser   = "user"
	identityTypeApp    = "app"
)

func IdentityTypeMetadataValue(identityType identityv1.IdentityType) (string, error) {
	switch identityType {
	case identityv1.IdentityType_IDENTITY_TYPE_AGENT:
		return identityTypeAgent, nil
	case identityv1.IdentityType_IDENTITY_TYPE_RUNNER:
		return identityTypeRunner, nil
	case identityv1.IdentityType_IDENTITY_TYPE_USER:
		return identityTypeUser, nil
	case identityv1.IdentityType_IDENTITY_TYPE_APP:
		return identityTypeApp, nil
	default:
		return "", fmt.Errorf("unsupported identity type: %s", identityType.String())
	}
}
