package kafka

import (
	"context"
	"fmt"
	"testing"

	franzscram "github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/xdg-go/scram"
)

// TestFranzGoSCRAMWithXDGServer tests franz-go SCRAM client with xdg-go/scram server
func TestFranzGoSCRAMWithXDGServer(t *testing.T) {
	username := "scramuser"
	password := "scrampass"

	// Generate server credentials
	serverCreds, err := generateSCRAMCredentials("SCRAM-SHA-256", password)
	if err != nil {
		t.Fatalf("Failed to generate credentials: %v", err)
	}

	// Create xdg-go/scram server
	credLookup := func(user string) (scram.StoredCredentials, error) {
		t.Logf("Server credential lookup for user: %s", user)
		if user == username {
			return serverCreds, nil
		}
		return scram.StoredCredentials{}, fmt.Errorf("user not found")
	}

	xdgServer, err := scram.SHA256.NewServer(credLookup)
	if err != nil {
		t.Fatalf("Failed to create xdg-go server: %v", err)
	}
	xdgServerConv := xdgServer.NewConversation()

	// Create franz-go SCRAM auth
	auth := franzscram.Auth{
		User: username,
		Pass: password,
	}

	// Create franz-go SCRAM mechanism
	mechanism := franzscram.Sha256(func(context.Context) (franzscram.Auth, error) {
		return auth, nil
	})

	// Start authentication
	ctx := context.Background()
	franzSession, clientFirst, err := mechanism.Authenticate(ctx, "test-host")
	if err != nil {
		t.Fatalf("Franz-go Authenticate failed: %v", err)
	}

	t.Logf("\n=== STEP 0: Client First Message ===")
	t.Logf("Franz-go client-first (len=%d):", len(clientFirst))
	t.Logf("  Text: %s", string(clientFirst))
	t.Logf("  Hex:  %x", clientFirst)

	// Server processes client-first
	serverFirst, err := xdgServerConv.Step(string(clientFirst))
	if err != nil {
		t.Fatalf("xdg-go server Step 0 failed: %v", err)
	}

	t.Logf("\n=== STEP 1: Server First Message ===")
	t.Logf("xdg-go server-first (len=%d):", len(serverFirst))
	t.Logf("  Text: %s", serverFirst)
	t.Logf("  Hex:  %x", []byte(serverFirst))

	// Analyze byte 44 in server-first
	if len(serverFirst) > 44 {
		t.Logf("\nBytes around position 44 in server-first:")
		start := 40
		if start < 0 {
			start = 0
		}
		end := 55
		if end > len(serverFirst) {
			end = len(serverFirst)
		}
		for i := start; i < end; i++ {
			ch := serverFirst[i]
			t.Logf("  [%d]: 0x%02x '%c' (printable: %v)", i, ch, ch, ch >= 32 && ch <= 126)
		}
	}

	// Franz-go client processes server-first
	done, clientFinal, err := franzSession.Challenge([]byte(serverFirst))
	if err != nil {
		t.Fatalf("Franz-go Challenge step 0 failed: %v (THIS IS THE ERROR!)", err)
	}
	if done {
		t.Fatal("Unexpected: franz-go says done after step 0")
	}

	t.Logf("\n=== STEP 2: Client Final Message ===")
	t.Logf("Franz-go client-final (len=%d):", len(clientFinal))
	t.Logf("  Text: %s", string(clientFinal))
	t.Logf("  Hex:  %x", clientFinal)

	// Server processes client-final
	serverFinal, err := xdgServerConv.Step(string(clientFinal))
	if err != nil {
		t.Fatalf("xdg-go server Step 1 failed: %v", err)
	}

	t.Logf("\n=== STEP 3: Server Final Message ===")
	t.Logf("xdg-go server-final (len=%d):", len(serverFinal))
	t.Logf("  Text: %s", serverFinal)
	t.Logf("  Hex:  %x", []byte(serverFinal))

	// Analyze byte 44 in server-final
	if len(serverFinal) > 44 {
		t.Logf("\nBytes around position 44 in server-final:")
		start := 40
		if start < 0 {
			start = 0
		}
		end := 55
		if end > len(serverFinal) {
			end = len(serverFinal)
		}
		for i := start; i < end; i++ {
			ch := serverFinal[i]
			t.Logf("  [%d]: 0x%02x '%c' (printable: %v)", i, ch, ch, ch >= 32 && ch <= 126)
		}
	}

	// Franz-go client verifies server
	done, _, err = franzSession.Challenge([]byte(serverFinal))
	if err != nil {
		t.Fatalf("Franz-go Challenge step 1 failed: %v (THIS IS THE ERROR!)", err)
	}
	if !done {
		t.Fatal("Unexpected: franz-go not done after step 1")
	}

	// Verify xdg-go server is also done
	if !xdgServerConv.Done() || !xdgServerConv.Valid() {
		t.Fatal("xdg-go server conversation not done or invalid")
	}

	t.Log("\n=== SUCCESS ===")
	t.Log("Franz-go SCRAM client successfully authenticated with xdg-go/scram server!")
}
