package usecase

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
)

// EndpointStore stores webhook endpoints (implemented by storage).
type EndpointStore interface {
	// Insert stores the record; a duplicate identity answers storage.ErrConflict.
	Insert(ctx context.Context, record storage.EndpointRecord) error
	// ByMerchantKey returns the endpoint of the merchant and key.
	ByMerchantKey(ctx context.Context, merchantID, key string) (storage.EndpointRecord, error)
}

// RegisterResult describes a successful registration.
type RegisterResult struct {
	// EndpointID sets the endpoint identifier.
	EndpointID string
	// URL sets the delivery address.
	URL string
	// Events sets the accepted CF-3 types.
	Events []string
	// SigningSecret sets the delivered signing secret (shown once per response).
	SigningSecret string
	// Replayed reports an idempotent replay of the registration.
	Replayed bool
}

// RegisterService registers webhook endpoints and issues signing secrets
// (CBL-8.1): the registration is idempotent per (merchant, key) and the secret
// is stored encrypted (T-NOTIFY-01/02).
type RegisterService struct {
	store EndpointStore
	key   []byte
}

// NewRegisterService creates the service on top of the endpoint store and the
// AES-256 secret encryption key.
func NewRegisterService(store EndpointStore, secretKey []byte) *RegisterService {
	return &RegisterService{store: store, key: secretKey}
}

// Register validates the request, replays an idempotent registration or
// creates a new endpoint with a fresh signing secret.
func (s *RegisterService) Register(ctx context.Context, request domain.RegisterRequest) (RegisterResult, error) {
	if err := request.Validate(); err != nil {
		return RegisterResult{}, err
	}
	events, err := domain.NormalizeEvents(request.Events)
	if err != nil {
		return RegisterResult{}, err
	}
	hash := request.Hash()

	existing, err := s.store.ByMerchantKey(ctx, request.MerchantID, request.IdempotencyKey)
	switch {
	case err == nil:
		return s.replay(existing, hash)
	case !errors.Is(err, storage.ErrNotFound):
		return RegisterResult{}, fmt.Errorf("usecase: load registration: %w", err)
	}

	endpointID, err := uuid.NewV7()
	if err != nil {
		return RegisterResult{}, fmt.Errorf("usecase: generate endpoint id: %w", err)
	}
	secret, err := domain.GenerateSecret()
	if err != nil {
		return RegisterResult{}, fmt.Errorf("usecase: generate signing secret: %w", err)
	}
	ciphertext, nonce, err := domain.EncryptSecret(s.key, endpointID.String(), secret)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("usecase: encrypt signing secret: %w", err)
	}
	record := storage.EndpointRecord{
		ID:               endpointID.String(),
		MerchantID:       request.MerchantID,
		URL:              request.URL,
		Events:           events,
		SecretCiphertext: ciphertext,
		SecretNonce:      nonce,
		SecretKeyVersion: 1,
		IdempotencyKey:   request.IdempotencyKey,
		RequestHash:      hash,
	}
	if err := s.store.Insert(ctx, record); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return s.resolveConflict(ctx, request, hash)
		}
		return RegisterResult{}, fmt.Errorf("usecase: store registration: %w", err)
	}
	return RegisterResult{
		EndpointID:    record.ID,
		URL:           record.URL,
		Events:        record.Events,
		SigningSecret: secret,
	}, nil
}

// resolveConflict maps an insert uniqueness violation: a racing registration
// of the same identity resolves to the idempotent replay, any other conflict
// is the frozen "one active endpoint per url" rule (409
// webhook_endpoint_exists in the REST contract).
func (s *RegisterService) resolveConflict(ctx context.Context, request domain.RegisterRequest, hash string) (RegisterResult, error) {
	existing, err := s.store.ByMerchantKey(ctx, request.MerchantID, request.IdempotencyKey)
	if errors.Is(err, storage.ErrNotFound) {
		return RegisterResult{}, grpcmw.E(grpcmw.KindAlreadyExists, "webhook_endpoint_exists",
			"the merchant already has an active endpoint with this url", nil)
	}
	if err != nil {
		return RegisterResult{}, fmt.Errorf("usecase: resolve registration conflict: %w", err)
	}
	return s.replay(existing, hash)
}

// replay returns the stored registration response or answers the reuse
// conflict when the request fingerprint changed (T-NOTIFY-02).
func (s *RegisterService) replay(existing storage.EndpointRecord, hash string) (RegisterResult, error) {
	if existing.RequestHash != hash {
		return RegisterResult{}, grpcmw.E(grpcmw.KindAlreadyExists, "idempotency_key_reuse",
			"the idempotency key was reused with a different request", nil)
	}
	secret, err := domain.DecryptSecret(s.key, existing.ID, existing.SecretCiphertext, existing.SecretNonce)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("usecase: decrypt stored secret: %w", err)
	}
	return RegisterResult{
		EndpointID:    existing.ID,
		URL:           existing.URL,
		Events:        existing.Events,
		SigningSecret: secret,
		Replayed:      true,
	}, nil
}
