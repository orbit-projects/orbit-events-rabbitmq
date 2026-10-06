# Orbit Events RabbitMQ: architecture and boundaries

## Responsibility

`orbit-events-rabbitmq` is an optional RabbitMQ transport for Orbit Core's typed `EventTransport`
contract. It is installed independently; Core and its in-process event bus do not acquire an AMQP
client dependency.

```bash
python -m pip install orbit-core orbit-events-rabbitmq
```

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `aio-pika>=9,<10`
- `orbit-core>=0.1.0a1,<0.2`
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `.tox/wheel-install/orbit_events_rabbitmq/__init__.py`
- `.tox/wheel-install/orbit_events_rabbitmq/config.py`
- `.tox/wheel-install/orbit_events_rabbitmq/plugin.py`
- `.tox/wheel-install/orbit_events_rabbitmq/transport.py`
- `src/orbit_events_rabbitmq/__init__.py`
- `src/orbit_events_rabbitmq/config.py`
- `src/orbit_events_rabbitmq/plugin.py`
- `src/orbit_events_rabbitmq/transport.py`

## Public contract and scope

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

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
