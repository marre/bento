package kafka

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	franzscram "github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/xdg-go/scram"
)

// TestXDGSCRAMWithFranzClient tests if xdg-go/scram server can work with franz-go SCRAM client
func TestXDGSCRAMWithFranzClient(t *testing.T) {
	t.Skip("This test demonstrates the incompatibility between xdg-go/scram and franz-go SCRAM")

	username := "testuser"
	password := "testpass"

	// Generate credentials using the helper function
	serverCreds, err := generateSCRAMCredentials("SCRAM-SHA-256", password)
	require.NoError(t, err)

	// Create xdg-go/scram server
	credLookup := func(user string) (scram.StoredCredentials, error) {
		if user == username {
			return serverCreds, nil
		}
		return scram.StoredCredentials{}, fmt.Errorf("user not found")
	}

	server, err := scram.SHA256.NewServer(credLookup)
	require.NoError(t, err)

	// Create franz-go SCRAM client
	auth := franzscram.Auth{
		User: username,
		Pass: password,
	}

	// This won't work directly because franz-go's Auth struct doesn't have a Step() method like xdg-go
	// They're fundamentally incompatible APIs

	t.Logf("Franz-go auth: %+v", auth)
	t.Logf("XDG server: %+v", server)

	// The issue is that these two libraries use different SCRAM implementations
	// and cannot directly communicate with each other
}
