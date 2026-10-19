package domain_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-notification/internal/domain"
)

// TestEventCatalogMatchesFrozenContract pins the domain catalog against the
// frozen CF-3 delivery contract (openapi/webhooks.v1.yaml in cbl-protos), so a
// contract revision cannot silently drift from the service.
func TestEventCatalogMatchesFrozenContract(t *testing.T) {
	path := filepath.Join("..", "..", "..", "cbl-protos", "openapi", "webhooks.v1.yaml")
	raw, err := os.ReadFile(path)
	require.NoError(t, err, "the frozen contract is reachable through the replace")

	pattern := regexp.MustCompile(`(?m)^\s+- ((?:invoice|payout|refund)\.[a-z]+)\s*$`)
	var contract []string
	seen := map[string]bool{}
	for _, match := range pattern.FindAllStringSubmatch(string(raw), -1) {
		if !seen[match[1]] {
			seen[match[1]] = true
			contract = append(contract, match[1])
		}
	}
	require.NotEmpty(t, contract, "the enum extraction found the catalog")

	catalog := domain.EventCatalog()
	sort.Strings(catalog)
	sort.Strings(contract)
	assert.Equal(t, contract, catalog, "the domain catalog equals the frozen CF-3 enum")
}
