package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

func createCertificates() (certPem, keyPem []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	priv := x509.MarshalPKCS1PrivateKey(key)

	tml := x509.Certificate{
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(5, 0, 0),
		SerialNumber: big.NewInt(123123),
		Subject: pkix.Name{
			CommonName:   "Bento",
			Organization: []string{"Bento"},
		},
		BasicConstraintsValid: true,
	}

	cert, err := x509.CreateCertificate(rand.Reader, &tml, &tml, &key.PublicKey, key)
	if err != nil {
		log.Fatal("Certificate cannot be created.", err.Error())
	}

	certPem = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	})

	keyPem = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: priv})

	return certPem, keyPem
}

type keyPair struct {
	cert []byte
	key  []byte
}

func createCertificatesWithEncryptedPKCS1Key(t *testing.T, password string) keyPair {
	t.Helper()

	certPem, keyPem := createCertificates()
	decodedKey, _ := pem.Decode(keyPem)

	//nolint:staticcheck // SA1019 Disable linting for deprecated  x509.EncryptPEMBlock call
	block, err := x509.EncryptPEMBlock(rand.Reader, decodedKey.Type, decodedKey.Bytes, []byte(password), x509.PEMCipher3DES)
	require.NoError(t, err)

	keyPem = pem.EncodeToMemory(
		block,
	)
	return keyPair{cert: certPem, key: keyPem}
}

func createCertificatesWithEncryptedPKCS8Key(t *testing.T, password string) keyPair {
	t.Helper()

	certPem, keyPem := createCertificates()
	pemBlock, _ := pem.Decode(keyPem)
	decodedKey, err := x509.ParsePKCS1PrivateKey(pemBlock.Bytes)
	require.NoError(t, err)

	keyBytes, err := pkcs8.ConvertPrivateKeyToPKCS8(decodedKey, []byte(password))
	require.NoError(t, err)

	return keyPair{cert: certPem, key: pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: keyBytes})}
}

func TestCertificateFileWithEncryptedKey(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "bento"),
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "bento"),
		},
	}

	tmpDir := t.TempDir()
	for _, test := range tests {
		fCert, _ := os.CreateTemp(tmpDir, "cert.pem")
		_, _ = fCert.Write(test.kp.cert)
		fCert.Close()

		fKey, _ := os.CreateTemp(tmpDir, "key.pem")
		_, _ = fKey.Write(test.kp.key)
		fKey.Close()

		c := ClientCertConfig{
			KeyFile:  fKey.Name(),
			CertFile: fCert.Name(),
			Password: "bento",
		}

		_, err := c.Load(ifs.OS())
		if err != nil {
			t.Errorf("Failed to load %s certificate: %s", test.name, err)
		}
	}
}

func TestCertificateWithEncryptedKey(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "bento"),
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "bento"),
		},
	}

	for _, test := range tests {
		c := ClientCertConfig{
			Cert:     string(test.kp.cert),
			Key:      string(test.kp.key),
			Password: "bento",
		}

		_, err := c.Load(ifs.OS())
		if err != nil {
			t.Errorf("Failed to load %s certificate: %s", test.name, err)
		}
	}
}

func TestCertificateFileWithEncryptedKeyAndWrongPassword(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
		err  string
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "bento"),
			err:  "x509: decryption password incorrect",
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "bento"),
			err:  "pkcs8: incorrect password",
		},
	}

	tmpDir := t.TempDir()
	for _, test := range tests {
		fCert, _ := os.CreateTemp(tmpDir, "cert.pem")
		_, _ = fCert.Write(test.kp.cert)
		fCert.Close()

		fKey, _ := os.CreateTemp(tmpDir, "key.pem")
		_, _ = fKey.Write(test.kp.key)
		fKey.Close()

		c := ClientCertConfig{
			KeyFile:  fKey.Name(),
			CertFile: fCert.Name(),
			Password: "not_bentho",
		}

		_, err := c.Load(ifs.OS())
		require.ErrorContains(t, err, test.err, test.name)
	}
}

func TestEncryptedKeyWithWrongPassword(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
		err  string
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "bento"),
			err:  "x509: decryption password incorrect",
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "bento"),
			err:  "pkcs8: incorrect password",
		},
	}

	for _, test := range tests {
		c := ClientCertConfig{
			Cert:     string(test.kp.cert),
			Key:      string(test.kp.key),
			Password: "not_bentho",
		}

		_, err := c.Load(ifs.OS())
		require.ErrorContains(t, err, test.err, test.name)
	}
}

func TestCertificateFileWithNoEncryption(t *testing.T) {
	cert, key := createCertificates()

	tmpDir := t.TempDir()

	fCert, _ := os.CreateTemp(tmpDir, "cert.pem")
	_, _ = fCert.Write(cert)
	defer fCert.Close()

	fKey, _ := os.CreateTemp(tmpDir, "key.pem")
	_, _ = fKey.Write(key)
	defer fKey.Close()

	c := ClientCertConfig{
		KeyFile:  fKey.Name(),
		CertFile: fCert.Name(),
	}

	_, err := c.Load(ifs.OS())
	if err != nil {
		t.Errorf("Failed to load certificate %s", err)
	}
}

func TestCertificateWithNoEncryption(t *testing.T) {
	cert, key := createCertificates()

	c := ClientCertConfig{
		Key:  string(key),
		Cert: string(cert),
	}

	_, err := c.Load(ifs.OS())
	if err != nil {
		t.Errorf("Failed to load certificate %s", err)
	}
}

func TestMTLSClientAuth(t *testing.T) {
	tests := []struct {
		name          string
		clientAuth    string
		expectedAuth  uint8
		expectError   bool
		errorContains string
	}{
		{
			name:         "none",
			clientAuth:   "none",
			expectedAuth: 0, // tls.NoClientCert
		},
		{
			name:         "request",
			clientAuth:   "request",
			expectedAuth: 1, // tls.RequestClientCert
		},
		{
			name:         "require",
			clientAuth:   "require",
			expectedAuth: 2, // tls.RequireAnyClientCert
		},
		{
			name:         "verify_if_given",
			clientAuth:   "verify_if_given",
			expectedAuth: 3, // tls.VerifyClientCertIfGiven
		},
		{
			name:         "require_and_verify",
			clientAuth:   "require_and_verify",
			expectedAuth: 4, // tls.RequireAndVerifyClientCert
		},
		{
			name:          "invalid",
			clientAuth:    "invalid_option",
			expectError:   true,
			errorContains: "invalid client_auth_type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				ClientAuth: tt.clientAuth,
			}

			tlsConf, err := c.GetNonToggled(ifs.OS())

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.ErrorContains(t, err, tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, tlsConf)
			require.Equal(t, tt.expectedAuth, uint8(tlsConf.ClientAuth))
		})
	}
}

func TestMTLSClientCAs(t *testing.T) {
	// Create a CA certificate
	caCert, _ := createCertificates()

	t.Run("client_cas inline", func(t *testing.T) {
		c := Config{
			ClientCAs: string(caCert),
		}

		tlsConf, err := c.GetNonToggled(ifs.OS())
		require.NoError(t, err)
		require.NotNil(t, tlsConf)
		require.NotNil(t, tlsConf.ClientCAs)
	})

	t.Run("client_cas_file", func(t *testing.T) {
		tmpDir := t.TempDir()
		fCA, err := os.CreateTemp(tmpDir, "ca.pem")
		require.NoError(t, err)
		_, err = fCA.Write(caCert)
		require.NoError(t, err)
		fCA.Close()

		c := Config{
			ClientCAsFile: fCA.Name(),
		}

		tlsConf, err := c.GetNonToggled(ifs.OS())
		require.NoError(t, err)
		require.NotNil(t, tlsConf)
		require.NotNil(t, tlsConf.ClientCAs)
	})

	t.Run("both client_cas and client_cas_file", func(t *testing.T) {
		c := Config{
			ClientCAs:     string(caCert),
			ClientCAsFile: "/some/path",
		}

		_, err := c.GetNonToggled(ifs.OS())
		require.Error(t, err)
		require.ErrorContains(t, err, "only one field between client_cas and client_cas_file can be specified")
	})
}

func TestMTLSFullConfiguration(t *testing.T) {
	// Create server cert and key
	serverCert, serverKey := createCertificates()
	// Create client CA
	clientCACert, _ := createCertificates()

	tmpDir := t.TempDir()

	// Write server cert/key files
	fServerCert, err := os.CreateTemp(tmpDir, "server-cert.pem")
	require.NoError(t, err)
	_, err = fServerCert.Write(serverCert)
	require.NoError(t, err)
	err = fServerCert.Close()
	require.NoError(t, err)

	fServerKey, err := os.CreateTemp(tmpDir, "server-key.pem")
	require.NoError(t, err)
	_, err = fServerKey.Write(serverKey)
	require.NoError(t, err)
	err = fServerKey.Close()
	require.NoError(t, err)

	// Write client CA file
	fClientCA, err := os.CreateTemp(tmpDir, "client-ca.pem")
	require.NoError(t, err)
	_, err = fClientCA.Write(clientCACert)
	require.NoError(t, err)
	err = fClientCA.Close()
	require.NoError(t, err)

	c := Config{
		ClientCertificates: []ClientCertConfig{
			{
				CertFile: fServerCert.Name(),
				KeyFile:  fServerKey.Name(),
			},
		},
		ClientAuth:    "require_and_verify",
		ClientCAsFile: fClientCA.Name(),
	}

	tlsConf, err := c.GetNonToggled(ifs.OS())
	require.NoError(t, err)
	require.NotNil(t, tlsConf)
	require.Len(t, tlsConf.Certificates, 1)
	require.NotNil(t, tlsConf.ClientCAs)
	require.Equal(t, uint8(4), uint8(tlsConf.ClientAuth)) // tls.RequireAndVerifyClientCert
}
