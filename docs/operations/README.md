# Orbit Events RabbitMQ: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-events-rabbitmq` is an optional RabbitMQ transport for Orbit Core's typed `EventTransport`
contract. It is installed independently; Core and its in-process event bus do not acquire an AMQP
client dependency.

```bash
python -m pip install orbit-core orbit-events-rabbitmq
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

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
