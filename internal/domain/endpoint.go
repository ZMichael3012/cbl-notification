package domain

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/ZMichael3012/cbl-kit/grpcmw"
)

// Delivery catalog and input bounds of the registration contract (CF-3).
const (
	// MaxURLLen bounds the delivery address.
	MaxURLLen = 2048
	// MaxEvents bounds the requested event list (the frozen REST contract).
	MaxEvents = 32
	// MinKeyLen and MaxKeyLen bound the idempotency key.
	MinKeyLen = 8
	MaxKeyLen = 128
	// SecretHexLen sets the delivered signing secret length (32 bytes hex).
	SecretHexLen = 64
	// SecretBytes sets the CSPRNG signing secret size.
	SecretBytes = 32
	// EncryptionKeyLen sets the AES-256 key size of the secret encryption.
	EncryptionKeyLen = 32
)

// eventCatalog lists the CF-3 merchant delivery types (CBL-003 section 4).
var eventCatalog = []string{
	"invoice.detected", "invoice.confirming", "invoice.completed", "invoice.expired",
	"invoice.underpaid", "invoice.overpaid", "invoice.failed",
	"payout.completed", "payout.failed", "refund.completed", "refund.failed",
}

// EventCatalog returns a copy of the delivery catalog.
func EventCatalog() []string {
	return append([]string(nil), eventCatalog...)
}

// KnownEvent reports whether the type belongs to the CF-3 catalog.
func KnownEvent(event string) bool {
	for _, known := range eventCatalog {
		if known == event {
			return true
		}
	}
	return false
}

// NormalizeEvents validates and deduplicates the requested delivery types,
// preserving the request order; the raw list is bounded (T-NOTIFY-04).
func NormalizeEvents(events []string) ([]string, error) {
	if len(events) == 0 {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "events_required", "at least one event type is required", nil)
	}
	if len(events) > MaxEvents {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "events_out_of_bounds", "too many event types", nil)
	}
	seen := make(map[string]struct{}, len(events))
	normalized := make([]string, 0, len(events))
	for _, event := range events {
		trimmed := strings.TrimSpace(event)
		if !KnownEvent(trimmed) {
			return nil, grpcmw.E(grpcmw.KindInvalidArgument, "unknown_event", "unknown event type", nil)
		}
		if _, duplicate := seen[trimmed]; duplicate {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	return normalized, nil
}

// ValidateURL checks the https delivery address with the registration-time
// SSRF guards (T-NOTIFY-03): no credentials, no internal names (a trailing dot
// is normalized), no IPv6 zones and no private, loopback, link-local,
// multicast, unspecified or CGNAT IP literals. A host name that resolves to an
// internal address is caught by the 8.2 delivery-time resolver guard.
func ValidateURL(raw string) error {
	if raw == "" || len(raw) > MaxURLLen {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_url", "the url is missing or too long", nil)
	}
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_url", "the url must be an absolute https address", nil)
	}
	if parsed.User != nil {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_url", "the url must not carry credentials", nil)
	}
	host := strings.ToLower(parsed.Hostname())
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_url", "the url has no host", nil)
	}
	if strings.Contains(host, "%") {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_url", "ipv6 zones are not allowed", nil)
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_url", "internal host names are not allowed", nil)
	}
	if ip := net.ParseIP(host); ip != nil && !IsPublicIP(ip) {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_url", "internal addresses are not allowed", nil)
	}
	return nil
}

// IsPublicIP reports whether the address is a globally routable unicast address:
// loopback, private, link-local, multicast, unspecified, CGNAT and "this
// network" ranges are internal. The delivery guard uses the same rule.
func IsPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || inRange(ip, cgnatRange) || inRange(ip, thisNetworkRange) {
		return false
	}
	return true
}

// cgnatRange sets 100.64.0.0/10 (carrier-grade NAT, not covered by IsPrivate).
var cgnatRange = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

// thisNetworkRange sets 0.0.0.0/8 ("this network", not covered by IsUnspecified).
var thisNetworkRange = &net.IPNet{IP: net.IPv4(0, 0, 0, 0), Mask: net.CIDRMask(8, 32)}

// inRange reports whether the address belongs to the network.
func inRange(ip net.IP, network *net.IPNet) bool {
	return network.Contains(ip)
}

// GenerateSecret returns a 32-byte CSPRNG signing secret as lowercase hex.
func GenerateSecret() (string, error) {
	buf := make([]byte, SecretBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("domain: generate secret: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// EncryptSecret encrypts the signing secret with AES-256-GCM; the endpoint id
// is the additional authenticated data, binding the ciphertext to the row
// (T-NOTIFY-01, FR-4).
func EncryptSecret(key []byte, endpointID, secret string) ([]byte, []byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("domain: generate nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(secret), []byte(endpointID))
	return ciphertext, nonce, nil
}

// DecryptSecret decrypts a stored signing secret.
func DecryptSecret(key []byte, endpointID string, ciphertext, nonce []byte) (string, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	if len(nonce) != gcm.NonceSize() {
		return "", fmt.Errorf("domain: invalid nonce size")
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, []byte(endpointID))
	if err != nil {
		return "", fmt.Errorf("domain: decrypt secret: %w", err)
	}
	return string(plaintext), nil
}

// newGCM builds the AES-256-GCM cipher of the secret encryption.
func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != EncryptionKeyLen {
		return nil, fmt.Errorf("domain: encryption key must be %d bytes", EncryptionKeyLen)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("domain: cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("domain: gcm: %w", err)
	}
	return gcm, nil
}

// RegisterRequest describes a validated endpoint registration.
type RegisterRequest struct {
	// IdempotencyKey sets the registration key.
	IdempotencyKey string
	// MerchantID sets the owning merchant (canonical UUID).
	MerchantID string
	// URL sets the delivery address.
	URL string
	// Events sets the requested CF-3 types.
	Events []string
}

// Validate checks the registration fields (FR-1).
func (r RegisterRequest) Validate() error {
	if err := validateIdempotencyKey(r.IdempotencyKey); err != nil {
		return err
	}
	if err := validateMerchantID(r.MerchantID); err != nil {
		return err
	}
	if err := ValidateURL(r.URL); err != nil {
		return err
	}
	if _, err := NormalizeEvents(r.Events); err != nil {
		return err
	}
	return nil
}

// Hash returns the order-insensitive request fingerprint of the registration
// (T-NOTIFY-02): the url plus the normalized (trimmed, deduplicated, sorted)
// event set, so a retry of the same logical request replays even when the
// client sends duplicates, whitespace or a different order.
func (r RegisterRequest) Hash() string {
	events, err := NormalizeEvents(r.Events)
	if err != nil {
		events = append([]string(nil), r.Events...)
	}
	sort.Strings(events)
	sum := sha256.Sum256([]byte(r.URL + "\n" + strings.Join(events, "\n")))
	return hex.EncodeToString(sum[:])
}

// validateIdempotencyKey checks the key shape: 8-128 characters of
// [A-Za-z0-9._~-] (the platform idempotency contract).
func validateIdempotencyKey(key string) error {
	if len(key) < MinKeyLen || len(key) > MaxKeyLen {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_idempotency_key", "the idempotency key length is out of bounds", nil)
	}
	for i := 0; i < len(key); i++ {
		b := key[i]
		switch {
		case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9', b == '.', b == '_', b == '~', b == '-':
		default:
			return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_idempotency_key", "the idempotency key has invalid characters", nil)
		}
	}
	return nil
}

// validateMerchantID requires the canonical lowercase UUID identity
// (T-NOTIFY-06).
func validateMerchantID(merchantID string) error {
	parsed, err := uuid.Parse(merchantID)
	if err != nil || parsed.String() != merchantID {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_merchant", "the merchant id must be a canonical uuid", nil)
	}
	return nil
}
