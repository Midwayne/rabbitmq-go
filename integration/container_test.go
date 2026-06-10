package integration

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
)

const (
	// brokerEnvVar, when set, points the suite at an existing broker and skips
	// starting a container. Useful for fast local iteration or a CI service.
	brokerEnvVar = "RABBITMQ_TEST_URL"

	// mgmtEnvVar, when set alongside brokerEnvVar, points connection-recovery
	// tests at the broker's HTTP management API (e.g. http://host:15672). When it
	// is unset and a container is not used, those tests skip.
	mgmtEnvVar = "RABBITMQ_TEST_MGMT_URL"

	// rabbitmqImage is the broker image started by the testcontainers harness. It
	// ships the management plugin so recovery tests can force connection drops
	// through the HTTP API.
	rabbitmqImage = "rabbitmq:4-management-alpine"

	// toxiproxyImage is the Toxiproxy image used to inject network faults
	// (connection cuts, latency) for the fault-injection tests.
	toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"

	// defaultAdminUser / defaultAdminPass are the admin credentials configured on
	// the testcontainers broker and reused for the management API.
	defaultAdminUser = "rabbitmq"
	defaultAdminPass = "rabbitmq"

	// rabbitmqAlias is the network alias the Toxiproxy container uses to reach
	// RabbitMQ over the shared Docker network.
	rabbitmqAlias = "rabbitmq"
	// toxiproxyName is the name of the proxy that fronts RabbitMQ.
	toxiproxyName = "rabbitmq"
	// toxiproxyListenPort is the in-container port the proxy listens on (the
	// module assigns the first proxy to firstProxiedPort = 8666).
	toxiproxyListenPort = 8666
)

// testBrokerURL is the AMQP URL shared by every test in the package. It is
// populated by TestMain and read via brokerURL.
var testBrokerURL string

// testManagementURL is the base HTTP management URL (e.g. http://host:15672),
// or empty when the management API is unavailable. Recovery tests read it via
// managementClient.
var testManagementURL string

// testAdminUser / testAdminPass are the credentials used for the management API.
var (
	testAdminUser = defaultAdminUser
	testAdminPass = defaultAdminPass
)

// testProxiedBrokerURL is the AMQP URL routed through Toxiproxy, and
// testToxiproxyURI is its control-API endpoint. Both are empty when Toxiproxy is
// unavailable, in which case the fault-injection tests skip.
var (
	testProxiedBrokerURL string
	testToxiproxyURI     string
)

// TestMain boots a single RabbitMQ broker for the whole package (tests use
// unique exchange/queue names, so one broker is enough) and tears it down
// afterwards. If RABBITMQ_TEST_URL is set, that broker is used instead. If no
// broker can be started, the tests skip locally but fail in CI.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	if rawURL := os.Getenv(brokerEnvVar); rawURL != "" {
		testBrokerURL = rawURL
		testManagementURL = os.Getenv(mgmtEnvVar)
		if user, pass, ok := credentialsFromURL(rawURL); ok {
			testAdminUser, testAdminPass = user, pass
		}
		// Toxiproxy is only wired up for the testcontainers broker; against an
		// external broker the fault-injection tests skip.
		return m.Run()
	}

	ctx := context.Background()

	// A user-defined network lets the Toxiproxy container reach RabbitMQ by alias.
	// If it cannot be created, RabbitMQ still starts (host-exposed) and only the
	// fault-injection tests skip.
	nw, nwErr := network.New(ctx)
	if nwErr == nil {
		defer func() { _ = nw.Remove(context.Background()) }()
	} else {
		fmt.Fprintf(
			os.Stderr,
			"integration: no Docker network; toxiproxy tests will skip: %v\n",
			nwErr,
		)
	}

	rmqOpts := []testcontainers.ContainerCustomizer{
		tcrabbitmq.WithAdminUsername(defaultAdminUser),
		tcrabbitmq.WithAdminPassword(defaultAdminPass),
	}
	if nwErr == nil {
		rmqOpts = append(rmqOpts, network.WithNetwork([]string{rabbitmqAlias}, nw))
	}

	container, err := tcrabbitmq.Run(ctx, rabbitmqImage, rmqOpts...)
	if err != nil {
		if isCI() {
			fmt.Fprintf(os.Stderr, "integration: could not start RabbitMQ container: %v\n", err)
			return 1
		}
		// Most commonly Docker is not available. Skip rather than fail so the
		// module's `go test ./...` is still green in such environments.
		fmt.Fprintf(
			os.Stderr,
			"integration: skipping; could not start RabbitMQ container: %v\n",
			err,
		)
		return m.Run()
	}
	defer func() { _ = testcontainers.TerminateContainer(container) }()

	amqpURL, err := container.AmqpURL(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: could not resolve AMQP URL: %v\n", err)
		if isCI() {
			return 1
		}
		return m.Run()
	}
	testBrokerURL = amqpURL

	if httpURL, err := container.HttpURL(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "integration: could not resolve management URL: %v\n", err)
	} else {
		testManagementURL = httpURL
	}

	if nwErr == nil {
		if cleanup := startToxiproxy(ctx, nw); cleanup != nil {
			defer cleanup()
		}
	}

	return m.Run()
}

// startToxiproxy starts a Toxiproxy container on the shared network fronting
// RabbitMQ and populates testProxiedBrokerURL/testToxiproxyURI. It is
// best-effort: on any failure the fault-injection tests skip. The returned
// function terminates the container (nil when none was started).
func startToxiproxy(ctx context.Context, nw *testcontainers.DockerNetwork) func() {
	toxi, err := tctoxiproxy.Run(
		ctx,
		toxiproxyImage,
		tctoxiproxy.WithProxy(toxiproxyName, fmt.Sprintf("%s:5672", rabbitmqAlias)),
		network.WithNetwork([]string{"toxiproxy"}, nw),
	)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"integration: toxiproxy unavailable; fault-injection tests will skip: %v\n",
			err,
		)
		if toxi != nil {
			return func() { _ = testcontainers.TerminateContainer(toxi) }
		}
		return nil
	}

	uri, err := toxi.URI(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: toxiproxy URI unavailable: %v\n", err)
		return func() { _ = testcontainers.TerminateContainer(toxi) }
	}
	host, port, err := toxi.ProxiedEndpoint(toxiproxyListenPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: toxiproxy proxied endpoint unavailable: %v\n", err)
		return func() { _ = testcontainers.TerminateContainer(toxi) }
	}

	testToxiproxyURI = uri
	testProxiedBrokerURL = fmt.Sprintf(
		"amqp://%s:%s@%s:%s/",
		defaultAdminUser,
		defaultAdminPass,
		host,
		port,
	)
	return func() { _ = testcontainers.TerminateContainer(toxi) }
}

// credentialsFromURL extracts the userinfo from an AMQP URL so the management
// API can authenticate against an externally provided broker.
func credentialsFromURL(rawURL string) (user, pass string, ok bool) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.User == nil {
		return "", "", false
	}
	user = parsed.User.Username()
	pass, _ = parsed.User.Password()
	if user == "" {
		return "", "", false
	}
	return user, pass, true
}

func isCI() bool {
	return os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != ""
}
