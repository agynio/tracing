package server

import (
	"context"
	"testing"

	tracingv1 "github.com/agynio/tracing/.gen/go/agynio/api/tracing/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListSpansRequiresOrganizationID(t *testing.T) {
	server := New(nil)
	_, err := server.ListSpans(context.Background(), &tracingv1.ListSpansRequest{})
	require.Error(t, err)

	statusErr, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, statusErr.Code())
	assert.Contains(t, statusErr.Message(), "organization_id")
}
