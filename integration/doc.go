// Package integration holds end-to-end tests that run the rabbitmq library
// against a real RabbitMQ broker.
//
// These tests live in their own Go module so the heavyweight OpenTelemetry SDK
// they use for trace assertions never enters the published library's dependency
// graph. They are skipped automatically unless RABBITMQ_TEST_URL points at a
// reachable broker.
//
// Run them with:
//
//	docker run -d --rm -p 5672:5672 --name rmq rabbitmq:3-management
//	cd integration
//	RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ go test ./...
package integration
