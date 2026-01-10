package kafka

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xdg-go/scram"
)

// TestSCRAMCredentialGeneration verifies that our manual credential generation
// produces valid credentials that work with xdg-go/scram's server
func TestSCRAMCredentialGeneration(t *testing.T) {
	username := "testuser"
	password := "testpass"

	// Generate credentials using our function
	serverCreds, err := generateSCRAMCredentials("SCRAM-SHA-256", password)
	require.NoError(t, err)

	t.Logf("Generated credentials: Salt len=%d, Iters=%d, StoredKey len=%d, ServerKey len=%d",
		len(serverCreds.Salt), serverCreds.Iters, len(serverCreds.StoredKey), len(serverCreds.ServerKey))

	// Verify fields are populated
	assert.NotEmpty(t, serverCreds.Salt)
	assert.Equal(t, 4096, serverCreds.Iters)
	assert.NotEmpty(t, serverCreds.StoredKey)
	assert.NotEmpty(t, serverCreds.ServerKey)
	assert.Equal(t, 32, len(serverCreds.StoredKey), "SHA-256 should produce 32-byte StoredKey")
	assert.Equal(t, 32, len(serverCreds.ServerKey), "SHA-256 should produce 32-byte ServerKey")

	// Create a credential lookup that returns our credentials
	credLookup := func(user string) (scram.StoredCredentials, error) {
		if user == username {
			return serverCreds, nil
		}
		return scram.StoredCredentials{}, fmt.Errorf("user not found")
	}

	// Create a SCRAM server
	server, err := scram.SHA256.NewServer(credLookup)
	require.NoError(t, err)

	// Create a SCRAM client with the same password
	client, err := scram.SHA256.NewClient(username, password, "")
	require.NoError(t, err)

	// Perform SCRAM handshake
	serverConv := server.NewConversation()
	clientConv := client.NewConversation()

	// Step 1: Client sends first message
	clientFirst, err := clientConv.Step("")
	require.NoError(t, err)
	t.Logf("Client first: %s", clientFirst)

	// Step 2: Server responds with challenge
	serverFirst, err := serverConv.Step(clientFirst)
	require.NoError(t, err)
	t.Logf("Server first: %s", serverFirst)

	// Step 3: Client sends proof
	clientFinal, err := clientConv.Step(serverFirst)
	require.NoError(t, err)
	t.Logf("Client final: %s", clientFinal)

	// Step 4: Server verifies and responds
	serverFinal, err := serverConv.Step(clientFinal)
	require.NoError(t, err)
	t.Logf("Server final: %s", serverFinal)

	// Verify authentication succeeded
	assert.True(t, serverConv.Done(), "Server conversation should be done")
	assert.True(t, serverConv.Valid(), "Server conversation should be valid")

	// Client processes final message
	_, err = clientConv.Step(serverFinal)
	require.NoError(t, err)

	assert.True(t, clientConv.Done(), "Client conversation should be done")
	assert.True(t, clientConv.Valid(), "Client conversation should be valid")

	t.Log("SCRAM handshake completed successfully!")
}

// TestSCRAMCredentialGenerationWrongPassword verifies that wrong passwords are rejected
func TestSCRAMCredentialGenerationWrongPassword(t *testing.T) {
	username := "testuser"
	correctPassword := "testpass"
	wrongPassword := "wrongpass"

	// Generate server credentials with correct password
	serverCreds, err := generateSCRAMCredentials("SCRAM-SHA-256", correctPassword)
	require.NoError(t, err)

	credLookup := func(user string) (scram.StoredCredentials, error) {
		if user == username {
			return serverCreds, nil
		}
		return scram.StoredCredentials{}, fmt.Errorf("user not found")
	}

	server, err := scram.SHA256.NewServer(credLookup)
	require.NoError(t, err)

	// Create client with WRONG password
	client, err := scram.SHA256.NewClient(username, wrongPassword, "")
	require.NoError(t, err)

	serverConv := server.NewConversation()
	clientConv := client.NewConversation()

	// Exchange messages
	clientFirst, _ := clientConv.Step("")
	serverFirst, _ := serverConv.Step(clientFirst)
	clientFinal, _ := clientConv.Step(serverFirst)
	_, err = serverConv.Step(clientFinal)

	// Server should return an error for invalid credentials
	assert.Error(t, err, "Server should reject invalid credentials")
	assert.Contains(t, err.Error(), "proof invalid", "Error should indicate proof validation failure")

	t.Log("Successfully rejected wrong password")
}
