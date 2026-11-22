package chtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pemCert returns the PEM encoding of a self-signed certificate.
func pemCert(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "runakefli-chtest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestOptionsTLS(t *testing.T) {
	// Unset-in-spirit (""), explicit "0": off. Anything else: on, bare config.
	for _, v := range []string{"", "0"} {
		t.Run("off", func(t *testing.T) {
			t.Setenv("RUNAKEFLI_TEST_CH_TLS", v)
			require.Nil(t, Options("db").TLS)
		})
	}
	t.Run("on", func(t *testing.T) {
		t.Setenv("RUNAKEFLI_TEST_CH_TLS", "1")
		o := Options("db")
		require.NotNil(t, o.TLS)
		require.Nil(t, o.TLS.RootCAs) // system roots unless a CA is given
	})

	// RUNAKEFLI_TEST_CH_TLS=1 plus a CA file: the CA becomes RootCAs.
	t.Run("on with CA", func(t *testing.T) {
		ca := filepath.Join(t.TempDir(), "ca.pem")
		require.NoError(t, os.WriteFile(ca, pemCert(t), 0o600))
		t.Setenv("RUNAKEFLI_TEST_CH_TLS", "1")
		t.Setenv("RUNAKEFLI_TEST_CH_TLS_CA", ca)
		o := Options("db")
		require.NotNil(t, o.TLS)
		require.NotNil(t, o.TLS.RootCAs)
	})
}
