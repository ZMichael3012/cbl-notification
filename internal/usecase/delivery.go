package usecase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/hmac"
	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
)

// HandlerKind selects the consumed event family of a handler.
type HandlerKind string

// Handler kinds of the delivery consumers.
const (
	// HandlerPayments consumes payment.events.
	HandlerPayments HandlerKind = "payments"
	// HandlerPayouts consumes payout.events.
	HandlerPayouts HandlerKind = "payouts"
	// HandlerRecon consumes recon.alerts (no merchant delivery type, T-NOTIFY-14).
	HandlerRecon HandlerKind = "recon"
)

// RetrySchedule sets the CF-3 delivery retry delays after the first attempt:
// 1s, 5s, 30s, 2m, 15m, 1h (7 attempts in total).
var RetrySchedule = []time.Duration{
	time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 15 * time.Minute, time.Hour,
}

// Store is the persistence port of the delivery pipeline (the endpoint and
// delivery repositories).
type Store interface {
	// ListEndpoints returns the merchant endpoints.
	ListEndpoints(ctx context.Context, merchantID string) ([]storage.EndpointRecord, error)
	// InsertDelivery stores a delivery row in the caller transaction.
	InsertDelivery(ctx context.Context, tx *gorm.DB, record storage.DeliveryRecord) error
	// ClaimDue claims the due pending deliveries with a lease.
	ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]storage.DeliveryRecord, error)
	// EndpointByID returns the target endpoint.
	EndpointByID(ctx context.Context, id string) (storage.EndpointRecord, error)
	// RecordAttempt journals one attempt.
	RecordAttempt(ctx context.Context, attempt storage.AttemptRecord) error
	// MarkDelivered marks the delivery delivered.
	MarkDelivered(ctx context.Context, deliveryID string, attempts int) error
	// ScheduleRetry schedules the next attempt.
	ScheduleRetry(ctx context.Context, deliveryID string, attempts int, next time.Time, lastError string) error
	// MarkFailed marks the delivery failed.
	MarkFailed(ctx context.Context, deliveryID string, attempts int, lastError string) error
}

// EventHandler consumes one platform event family through the kit inbox and
// creates one merchant delivery per matching endpoint (CBL-8.2).
type EventHandler struct {
	kind  HandlerKind
	store Store
	now   func() time.Time
	log   *zap.Logger
}

// WithEventHandlerClock sets the time source.
func WithEventHandlerClock(clock func() time.Time) func(*EventHandler) {
	return func(h *EventHandler) {
		if clock != nil {
			h.now = clock
		}
	}
}

// NewEventHandler creates the consumer handler of the family.
func NewEventHandler(kind HandlerKind, store Store, log *zap.Logger, opts ...func(*EventHandler)) *EventHandler {
	if log == nil {
		log = zap.NewNop()
	}
	handler := &EventHandler{kind: kind, store: store, now: time.Now, log: log}
	for _, opt := range opts {
		opt(handler)
	}
	return handler
}

// maxEventBytes bounds the consumed protobuf event (the topic is internal, the
// bound is defense in depth against a poison pill).
const maxEventBytes = 256 << 10

// Handle decodes the event in the inbox transaction, maps it onto a merchant
// event and inserts the deliveries (FR-1/FR-5).
func (h *EventHandler) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	if len(msg.Value) == 0 || len(msg.Value) > maxEventBytes {
		return fmt.Errorf("%w: event size out of bounds", domain.ErrInvalidEvent)
	}
	sourceEventID := strings.TrimSpace(msg.Headers[outbox.HeaderEventID])
	if parsed, err := uuid.Parse(sourceEventID); err != nil || parsed.String() != sourceEventID {
		return fmt.Errorf("%w: source event_id is not a canonical uuid", domain.ErrInvalidEvent)
	}
	var (
		merchantID string
		mapped     domain.MerchantEvent
		ok         bool
		err        error
	)
	switch h.kind {
	case HandlerPayments:
		var event eventsv1.PaymentEvent
		if decodeErr := proto.Unmarshal(msg.Value, &event); decodeErr != nil {
			return fmt.Errorf("%w: decode payment event: %w", domain.ErrInvalidEvent, decodeErr)
		}
		merchantID = event.GetMerchantId()
		mapped, ok, err = domain.FromPaymentEvent(&event)
	case HandlerPayouts:
		var event eventsv1.PayoutEvent
		if decodeErr := proto.Unmarshal(msg.Value, &event); decodeErr != nil {
			return fmt.Errorf("%w: decode payout event: %w", domain.ErrInvalidEvent, decodeErr)
		}
		merchantID = event.GetMerchantId()
		mapped, ok, err = domain.FromPayoutEvent(&event)
	case HandlerRecon:
		h.log.Warn("recon alert has no merchant delivery type; acknowledged (T-NOTIFY-14)")
		return nil
	default:
		return fmt.Errorf("%w: unknown handler kind %q", domain.ErrInvalidEvent, h.kind)
	}
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if parsed, err := uuid.Parse(merchantID); err != nil || parsed.String() != merchantID {
		return fmt.Errorf("%w: merchant_id", domain.ErrInvalidEvent)
	}
	endpoints, err := h.store.ListEndpoints(ctx, merchantID)
	if err != nil {
		return fmt.Errorf("usecase: list endpoints: %w", err)
	}
	now := h.now().UTC()
	for _, endpoint := range endpoints {
		if !slices.Contains(endpoint.Events, mapped.Type) {
			continue
		}
		deliveryID, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("usecase: generate delivery id: %w", err)
		}
		eventID, err := domain.NewEventID()
		if err != nil {
			return err
		}
		body, err := domain.Envelope{
			ID:        eventID,
			Type:      mapped.Type,
			CreatedAt: mapped.OccurredAt,
			Data:      mapped.Data,
		}.Marshal()
		if err != nil {
			return err
		}
		next := now
		record := storage.DeliveryRecord{
			ID:            deliveryID.String(),
			EventID:       eventID,
			SourceEventID: sourceEventID,
			MerchantID:    merchantID,
			EndpointID:    endpoint.ID,
			EventType:     mapped.Type,
			Body:          body,
			Status:        storage.DeliveryPending,
			NextAttemptAt: &next,
		}
		if err := h.store.InsertDelivery(ctx, tx, record); err != nil {
			return err
		}
	}
	return nil
}

// Sender performs one signed merchant delivery.
type Sender interface {
	// Send posts the body and returns the response status (0 on a transport error).
	Send(ctx context.Context, url string, body, secret []byte, eventID string, now time.Time) (int, error)
}

// IPResolver resolves a host name to addresses (the delivery guard port).
type IPResolver interface {
	// LookupIPAddr returns the addresses of the host.
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// DialFunc dials a network address.
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// ErrBlockedAddress reports a delivery target resolving to an internal address.
var ErrBlockedAddress = errors.New("usecase: delivery address is not allowed")

// GuardedDial resolves the host, rejects every non-public address (the
// delivery-time SSRF guard deferred from 8.1, T-NOTIFY-03/13) and dials the
// first validated address, pinning the resolution against DNS rebinding.
func GuardedDial(dial DialFunc, resolver IPResolver) DialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("%w: malformed address", ErrBlockedAddress)
		}
		addresses, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("usecase: resolve delivery host: %w", err)
		}
		if len(addresses) == 0 {
			return nil, fmt.Errorf("usecase: delivery host has no addresses")
		}
		for _, candidate := range addresses {
			if !domain.IsPublicIP(candidate.IP) {
				return nil, fmt.Errorf("%w: the host resolves to an internal address", ErrBlockedAddress)
			}
		}
		return dial(ctx, network, net.JoinHostPort(addresses[0].IP.String(), port))
	}
}

// HTTPSender delivers the signed webhooks: no redirects, no proxy environment,
// the guarded dialer, a bounded timeout and a discarded response body
// (T-NOTIFY-13).
type HTTPSender struct {
	client *http.Client
}

// HTTPSenderOption configures the HTTP sender.
type HTTPSenderOption func(*http.Transport)

// WithHTTPSenderDial replaces the transport dial (tests).
func WithHTTPSenderDial(dial DialFunc) HTTPSenderOption {
	return func(transport *http.Transport) {
		if dial != nil {
			transport.DialContext = dial
		}
	}
}

// NewHTTPSender creates the HTTP sender with the guarded dialer and the
// attempt timeout.
func NewHTTPSender(timeout time.Duration, opts ...HTTPSenderOption) *HTTPSender {
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           GuardedDial(dialer.DialContext, net.DefaultResolver),
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
	for _, opt := range opts {
		opt(transport)
	}
	return &HTTPSender{client: &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

// Send signs and delivers the body; the signature is the kit hmac t/v1 scheme
// over the stored raw body (T-NOTIFY-05/11). A transport failure is classified
// without the delivery address (the raw url error carries the URL and query
// tokens, T-NOTIFY-17).
func (s *HTTPSender) Send(ctx context.Context, url string, body, secret []byte, eventID string, now time.Time) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("usecase: invalid delivery request")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Billing-Event-Id", eventID)
	request.Header.Set("X-Billing-Signature",
		hmac.BuildHeader(hmac.Signature{Timestamp: now, Hex: hmac.Sign(secret, now, body)}))
	response, err := s.client.Do(request)
	if err != nil {
		return 0, classifyTransportError(err)
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return response.StatusCode, fmt.Errorf("usecase: merchant answered status %d", response.StatusCode)
	}
	return response.StatusCode, nil
}

// classifyTransportError reduces a transport failure to a bounded class; the
// delivery address and resolved addresses never reach the logs or the journal.
func classifyTransportError(err error) error {
	if errors.Is(err, ErrBlockedAddress) {
		return ErrBlockedAddress
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return errors.New("usecase: delivery timeout")
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "no such host"), strings.Contains(text, "server misbehaving"):
		return errors.New("usecase: delivery dns failure")
	case strings.Contains(text, "certificate"), strings.Contains(text, "tls"):
		return errors.New("usecase: delivery tls failure")
	case strings.Contains(text, "connection refused"), strings.Contains(text, "connection reset"),
		strings.Contains(text, "no route to host"), strings.Contains(text, "connection attempt failed"):
		return errors.New("usecase: delivery connection failure")
	default:
		return errors.New("usecase: delivery transport failure")
	}
}

// Dispatcher sends the due deliveries with the contract retry schedule and
// journals every attempt (FR-4).
type Dispatcher struct {
	store          Store
	key            []byte
	sender         Sender
	now            func() time.Time
	interval       time.Duration
	attemptTimeout time.Duration
	batch          int
	log            *zap.Logger
}

// DispatcherOption configures the dispatcher.
type DispatcherOption func(*Dispatcher)

// WithDispatcherClock sets the time source.
func WithDispatcherClock(clock func() time.Time) DispatcherOption {
	return func(d *Dispatcher) {
		if clock != nil {
			d.now = clock
		}
	}
}

// WithDispatcherSender replaces the HTTP sender.
func WithDispatcherSender(sender Sender) DispatcherOption {
	return func(d *Dispatcher) {
		if sender != nil {
			d.sender = sender
		}
	}
}

// WithDispatcherInterval sets the poll interval.
func WithDispatcherInterval(interval time.Duration) DispatcherOption {
	return func(d *Dispatcher) {
		if interval > 0 {
			d.interval = interval
		}
	}
}

// WithDispatcherAttemptTimeout sets the attempt timeout (it also sizes the
// claim lease; the env variable is NOTIFICATION_ATTEMPT_TIMEOUT).
func WithDispatcherAttemptTimeout(timeout time.Duration) DispatcherOption {
	return func(d *Dispatcher) {
		if timeout > 0 {
			d.attemptTimeout = timeout
			d.sender = NewHTTPSender(timeout)
		}
	}
}

// NewDispatcher creates the delivery worker; a nil logger is silent.
func NewDispatcher(store Store, key []byte, log *zap.Logger, opts ...DispatcherOption) *Dispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	dispatcher := &Dispatcher{
		store:          store,
		key:            key,
		now:            time.Now,
		interval:       time.Second,
		attemptTimeout: 10 * time.Second,
		batch:          20,
		log:            log,
	}
	dispatcher.sender = NewHTTPSender(dispatcher.attemptTimeout)
	for _, opt := range opts {
		opt(dispatcher)
	}
	return dispatcher
}

// Run polls the due deliveries until the context ends.
func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.ProcessDue(ctx); err != nil {
				d.log.Error("delivery dispatch failed", zap.Error(err))
			}
		}
	}
}

// ProcessDue claims and sends one batch of due deliveries. The claim lease
// covers the whole batch budget (one dispatcher instance on the stand,
// T-NOTIFY-15).
func (d *Dispatcher) ProcessDue(ctx context.Context) error {
	now := d.now().UTC()
	lease := time.Duration(d.batch)*d.attemptTimeout + 5*time.Second
	claimed, err := d.store.ClaimDue(ctx, now, lease, d.batch)
	if err != nil {
		return err
	}
	for _, delivery := range claimed {
		if err := d.deliver(ctx, delivery); err != nil {
			d.log.Error("delivery attempt failed",
				zap.String("delivery_id", delivery.ID),
				zap.String("event_id", delivery.EventID),
				zap.Error(err))
		}
	}
	return nil
}

// deliver performs one attempt of the delivery and applies the schedule; the
// attempt time is captured per delivery, not per batch.
func (d *Dispatcher) deliver(ctx context.Context, delivery storage.DeliveryRecord) error {
	attemptNow := d.now().UTC()
	attempt := delivery.Attempts + 1
	endpoint, err := d.store.EndpointByID(ctx, delivery.EndpointID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return d.store.MarkFailed(ctx, delivery.ID, attempt, "endpoint not found")
		}
		// A transient store failure: the lease expires and the delivery is
		// reclaimed (at-least-once, T-NOTIFY-15).
		return fmt.Errorf("usecase: load delivery endpoint: %w", err)
	}
	if endpoint.MerchantID != delivery.MerchantID {
		return d.store.MarkFailed(ctx, delivery.ID, attempt, "endpoint owner mismatch")
	}
	secret, err := domain.DecryptSecret(d.key, endpoint.ID, endpoint.SecretCiphertext, endpoint.SecretNonce)
	if err != nil {
		// A key fault is recoverable after the operator restores the key:
		// follow the retry schedule instead of failing immediately.
		return d.settle(ctx, delivery, attempt, attemptNow, 0, errors.New("signing secret is not decryptable"))
	}
	status, sendErr := d.sender.Send(ctx, endpoint.URL, delivery.Body, []byte(secret), delivery.EventID, attemptNow)
	return d.settle(ctx, delivery, attempt, attemptNow, status, sendErr)
}

// settle journals the attempt and applies delivered/retry/failed.
func (d *Dispatcher) settle(ctx context.Context, delivery storage.DeliveryRecord, attempt int, sentAt time.Time, status int, sendErr error) error {
	failure := errorText(sendErr)
	if recordErr := d.store.RecordAttempt(ctx, storage.AttemptRecord{
		DeliveryID: delivery.ID,
		Attempt:    attempt,
		SentAt:     sentAt,
		HTTPStatus: status,
		Error:      failure,
	}); recordErr != nil {
		d.log.Warn("delivery attempt journal failed", zap.String("delivery_id", delivery.ID), zap.Error(recordErr))
	}
	if sendErr == nil {
		return d.store.MarkDelivered(ctx, delivery.ID, attempt)
	}
	if attempt > len(RetrySchedule) {
		d.log.Warn("delivery retries exhausted",
			zap.String("delivery_id", delivery.ID),
			zap.Int("attempts", attempt))
		return d.store.MarkFailed(ctx, delivery.ID, attempt, failure)
	}
	return d.store.ScheduleRetry(ctx, delivery.ID, attempt, sentAt.Add(RetrySchedule[attempt-1]), failure)
}

// errorText renders a bounded failure text without control characters and
// guarantees valid UTF-8 (PostgreSQL rejects invalid text).
func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, err.Error())
	runes := []rune(text)
	if len(runes) > 256 {
		runes = runes[:256]
	}
	return strings.ToValidUTF8(string(runes), "")
}
