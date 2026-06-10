package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// mgmtClient is a tiny RabbitMQ HTTP management API client. Recovery tests use
// it to force-close a broker connection by its connection_name client property,
// which makes the library reconnect. It is created via managementClient, which
// skips the test when the management API is unavailable.
type mgmtClient struct {
	base string
	user string
	pass string
	http *http.Client
}

// managementClient returns a client for the broker's HTTP management API,
// skipping the test when no management endpoint is configured.
func managementClient(t *testing.T) *mgmtClient {
	t.Helper()
	if testManagementURL == "" {
		t.Skip(
			"RabbitMQ management API unavailable; use the testcontainers broker " +
				"or set RABBITMQ_TEST_MGMT_URL to enable connection-recovery tests",
		)
	}
	return &mgmtClient{
		base: strings.TrimRight(testManagementURL, "/"),
		user: testAdminUser,
		pass: testAdminPass,
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

// mgmtConnection is the subset of a management API connection record this client
// needs to identify and close a connection.
type mgmtConnection struct {
	Name             string         `json:"name"`
	ClientProperties map[string]any `json:"client_properties"`
}

func (m *mgmtClient) do(
	ctx context.Context,
	method, path string,
) (body []byte, status int, err error) {
	req, err := http.NewRequestWithContext(ctx, method, m.base+path, nil)
	if err != nil {
		return nil, 0, err
	}
	req.SetBasicAuth(m.user, m.pass)

	resp, err := m.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

// listConnections returns the broker's current connections. It returns an error
// rather than failing the test so callers can poll through transient
// unavailability (the management plugin may lag just after startup).
func (m *mgmtClient) listConnections(ctx context.Context) ([]mgmtConnection, error) {
	body, status, err := m.do(ctx, http.MethodGet, "/api/connections")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list connections: status %d: %s", status, body)
	}
	var conns []mgmtConnection
	if err := json.Unmarshal(body, &conns); err != nil {
		return nil, fmt.Errorf("decode connections: %w", err)
	}
	return conns, nil
}

// waitForNamedConnection polls until a connection advertising the given
// connection_name client property is visible to the management API.
func (m *mgmtClient) waitForNamedConnection(t *testing.T, name string) bool {
	t.Helper()
	return waitForCond(t, 15*time.Second, func() bool {
		conns, err := m.listConnections(context.Background())
		if err != nil {
			return false
		}
		for _, c := range conns {
			if connectionName(c) == name {
				return true
			}
		}
		return false
	})
}

// closeNamedConnections force-closes every connection advertising the given
// connection_name client property and returns how many it actually closed.
func (m *mgmtClient) closeNamedConnections(t *testing.T, name string) int {
	t.Helper()
	conns, err := m.listConnections(context.Background())
	if err != nil {
		t.Fatalf("list connections: %v", err)
	}

	closed := 0
	for _, c := range conns {
		if connectionName(c) != name {
			continue
		}
		_, status, err := m.do(
			context.Background(),
			http.MethodDelete,
			"/api/connections/"+url.PathEscape(c.Name),
		)
		if err != nil {
			t.Fatalf("close connection %q: %v", c.Name, err)
		}
		switch status {
		case http.StatusNoContent:
			closed++
		case http.StatusNotFound:
			// Already gone (stats lag); nothing to do.
		default:
			t.Fatalf("close connection %q: unexpected status %d", c.Name, status)
		}
	}
	return closed
}

// connectionName returns the connection_name client property, or "" when absent.
func connectionName(c mgmtConnection) string {
	if c.ClientProperties == nil {
		return ""
	}
	v, ok := c.ClientProperties["connection_name"]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}
