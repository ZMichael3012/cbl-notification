package domain_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-notification/internal/domain"
)

// testMerchantID sets the canonical merchant UUID for the tests.
const testMerchantID = "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"

func TestEventCatalog(t *testing.T) {
	expected := []string{
		"invoice.detected", "invoice.confirming", "invoice.completed", "invoice.expired",
		"invoice.underpaid", "invoice.overpaid", "invoice.failed",
		"payout.completed", "payout.failed", "refund.completed", "refund.failed",
	}
	assert.Equal(t, expected, domain.EventCatalog(), "the exact CF-3 catalog")
	for _, event := range expected {
		assert.True(t, domain.KnownEvent(event), event)
	}
	assert.False(t, domain.KnownEvent("invoice.unknown"))
	assert.False(t, domain.KnownEvent(""))
	assert.False(t, domain.KnownEvent("payment.created"))
}

func TestNormalizeEvents(t *testing.T) {
	events, err := domain.NormalizeEvents([]string{"invoice.completed", "payout.failed", "invoice.completed"})
	require.NoError(t, err)
	assert.Equal(t, []string{"invoice.completed", "payout.failed"}, events)

	_, err = domain.NormalizeEvents(nil)
	assert.Error(t, err)
	_, err = domain.NormalizeEvents([]string{"unknown.type"})
	assert.Error(t, err)

	tooMany := make([]string, 65)
	for i := range tooMany {
		tooMany[i] = "invoice.completed"
	}
	_, err = domain.NormalizeEvents(tooMany)
	assert.Error(t, err, "the request list bound applies before deduplication")
}

func TestValidateURL(t *testing.T) {
	ok := []string{
		"https://example.com/hooks",
		"https://example.com:8443/x",
		"https://[2001:db8::1]/h",
		"https://hooks.example.com/a/b?c=d",
	}
	for _, raw := range ok {
		assert.NoError(t, domain.ValidateURL(raw), raw)
	}
	bad := []string{
		"",
		"http://example.com/hooks",
		"ftp://example.com/hooks",
		"https://user:pass@example.com/",
		"https://localhost/hooks",
		"https://localhost./hooks",
		"https://foo.localhost/hooks",
		"https://foo.local/hooks",
		"https://foo.local./hooks",
		"https://foo.internal/hooks",
		"https://127.0.0.1/hooks",
		"https://10.0.0.1/hooks",
		"https://192.168.1.5/hooks",
		"https://172.16.0.1/hooks",
		"https://169.254.1.1/hooks",
		"https://0.0.0.0/hooks",
		"https://0.1.2.3/hooks",
		"https://100.64.0.1/hooks",
		"https://[::1]/hooks",
		"https://[fe80::1%25eth0]/hooks",
		"https://example.com/" + strings.Repeat("a", domain.MaxURLLen),
	}
	for _, raw := range bad {
		assert.Error(t, domain.ValidateURL(raw), raw)
	}

	// The exact boundary length passes.
	prefix := "https://example.com/"
	exact := prefix + strings.Repeat("a", domain.MaxURLLen-len(prefix))
	assert.NoError(t, domain.ValidateURL(exact))
}

func TestSecretGenerationAndEncryption(t *testing.T) {
	secret, err := domain.GenerateSecret()
	require.NoError(t, err)
	assert.Len(t, secret, domain.SecretHexLen)
	assert.Regexp(t, "^[0-9a-f]{64}$", secret)
	other, err := domain.GenerateSecret()
	require.NoError(t, err)
	assert.NotEqual(t, secret, other)

	key := bytes.Repeat([]byte{7}, 32)
	ciphertext, nonce, err := domain.EncryptSecret(key, "01a154aa-0000-7000-8000-000000000001", secret)
	require.NoError(t, err)
	assert.NotContains(t, string(ciphertext), secret)

	decrypted, err := domain.DecryptSecret(key, "01a154aa-0000-7000-8000-000000000001", ciphertext, nonce)
	require.NoError(t, err)
	assert.Equal(t, secret, decrypted)

	_, err = domain.DecryptSecret(key, "01a154aa-0000-7000-8000-000000000002", ciphertext, nonce)
	assert.Error(t, err, "the endpoint id is the additional authenticated data")

	_, err = domain.DecryptSecret(bytes.Repeat([]byte{8}, 32), "01a154aa-0000-7000-8000-000000000001", ciphertext, nonce)
	assert.Error(t, err)

	tampered := append([]byte(nil), ciphertext...)
	tampered[0] ^= 0xff
	_, err = domain.DecryptSecret(key, "01a154aa-0000-7000-8000-000000000001", tampered, nonce)
	assert.Error(t, err, "a tampered ciphertext must fail authentication")

	_, _, err = domain.EncryptSecret([]byte("short"), "id", secret)
	assert.Error(t, err, "the key must be 32 bytes")
}

func TestRegisterRequestValidateAndHash(t *testing.T) {
	valid := domain.RegisterRequest{
		IdempotencyKey: "key-12345",
		MerchantID:     testMerchantID,
		URL:            "https://example.com/hooks",
		Events:         []string{"invoice.completed"},
	}
	require.NoError(t, valid.Validate())

	cases := map[string]domain.RegisterRequest{
		"short key":          {IdempotencyKey: "abc", MerchantID: testMerchantID, URL: "https://example.com/h", Events: []string{"invoice.completed"}},
		"long key":           {IdempotencyKey: strings.Repeat("k", 129), MerchantID: testMerchantID, URL: "https://example.com/h", Events: []string{"invoice.completed"}},
		"bad key chars":      {IdempotencyKey: "key with space", MerchantID: testMerchantID, URL: "https://example.com/h", Events: []string{"invoice.completed"}},
		"missing merchant":   {IdempotencyKey: "key-12345", URL: "https://example.com/h", Events: []string{"invoice.completed"}},
		"uppercase merchant": {IdempotencyKey: "key-12345", MerchantID: strings.ToUpper(testMerchantID), URL: "https://example.com/h", Events: []string{"invoice.completed"}},
		"bad url":            {IdempotencyKey: "key-12345", MerchantID: testMerchantID, URL: "http://example.com/h", Events: []string{"invoice.completed"}},
		"no events":          {IdempotencyKey: "key-12345", MerchantID: testMerchantID, URL: "https://example.com/h"},
		"unknown event":      {IdempotencyKey: "key-12345", MerchantID: testMerchantID, URL: "https://example.com/h", Events: []string{"invoice.unknown"}},
	}
	for name, request := range cases {
		assert.Error(t, request.Validate(), name)
	}

	sorted := valid
	sorted.Events = []string{"payout.failed", "invoice.completed"}
	reordered := valid
	reordered.Events = []string{"invoice.completed", "payout.failed"}
	assert.Equal(t, sorted.Hash(), reordered.Hash(), "the hash is order-insensitive")

	otherURL := valid
	otherURL.URL = "https://other.example.com/h"
	assert.NotEqual(t, valid.Hash(), otherURL.Hash())
}
