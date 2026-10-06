# Orbit Events RabbitMQ

`orbit-events-rabbitmq` is an optional RabbitMQ transport for Orbit Core's typed `EventTransport`
contract. It is installed independently; Core and its in-process event bus do not acquire an AMQP
client dependency.

```bash
python -m pip install orbit-core orbit-events orbit-events-rabbitmq
```

## Configure and publish

Provide an AMQP/AMQPS URL through protected environment configuration or a secret manager. The URL
is represented by Pydantic's redacted `SecretStr`. Use TLS, a least-privilege RabbitMQ account, and
restricted exchange/queue permissions in deployed environments.

```python
from orbit import Application, ApplicationConfig
from orbit_events_rabbitmq import RabbitMQConfig, RabbitMQEventTransport, RabbitMQTransportPlugin

application = Application(ApplicationConfig(name="orders"))
transport = RabbitMQEventTransport(
    RabbitMQConfig(url="amqps://orbit-user:secret@broker.example/vhost")
)
application.plugins.register(RabbitMQTransportPlugin(transport))
await application.startup()

await transport.publish(order_created_event)
```

The plugin registers the adapter under `RABBITMQ_EVENT_TRANSPORT_KEY` and closes its robust
connection during Core shutdown. Concurrent shutdown callers share one cleanup, and cancellation is
propagated only after owned channels and the connection have been closed. Exchange and queue
declarations are durable by default. Publishing
uses persistent JSON event messages, mandatory routing, and aio-pika publisher confirms.

## Delivery behavior and limits

Events route to a durable topic exchange using the Core event name as routing key. Each subscription
declares one durable queue named `<queue_prefix>.<event_name>`, so subscriber replicas compete within
that queue. Duplicate handlers for one event name on the same transport instance are rejected.
Consumers use manual acknowledgements and ack only after handler success. A handler is retried a
bounded number of times with a bounded delay; after exhaustion, the message is rejected without
requeue. Configure a dead-letter exchange if failed messages must be retained; without one, RabbitMQ
discards rejected messages. Inspect `failed_subscriptions` and operational logs for failures.

Publisher confirms and manual acknowledgements do not create exactly-once processing: publish or
handler outcomes can be ambiguous during connection loss, so handlers should be idempotent. The
package does not create dead-letter queues, outboxes, schemas, exchanges beyond its configured topic
exchange, or broker infrastructure. Broker permissions, backups, TLS, monitoring, retention, and
deployment health remain operator responsibilities. Tests use fake clients; they do not claim a live
RabbitMQ broker was tested.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development and status

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

This package is pre-alpha, its API is not stable, and it supports Python 3.11 through 3.14.

The transport follows aio-pika's documented [publisher confirms](https://docs.aio-pika.com/rabbitmq-tutorial/7-publisher-confirms.html)
and [manual acknowledgement](https://docs.aio-pika.com/rabbitmq-tutorial/2-work-queues.html) behavior.

Licensed under Apache-2.0.

