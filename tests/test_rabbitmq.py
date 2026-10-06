# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""RabbitMQ transport tests exercise delivery semantics with fake aio-pika clients."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import pytest
from orbit import Application, ApplicationConfig
from orbit.events import Event
from pydantic import SecretStr, ValidationError

from orbit_events_rabbitmq import (
    RABBITMQ_EVENT_TRANSPORT_KEY,
    RabbitMQConfig,
    RabbitMQEventTransport,
    RabbitMQTransportError,
    RabbitMQTransportPlugin,
)


def make_config(**updates: Any) -> RabbitMQConfig:
    """Build the shared fake-broker config with optional field overrides."""
    values: dict[str, Any] = {"url": SecretStr("amqps://user:password@localhost/vhost")}
    values.update(updates)
    return RabbitMQConfig(**values)


class FakeMessage:
    """A test delivery or published message with tracked ack/reject actions."""

    def __init__(self, body: bytes, **options: object) -> None:
        """Capture body/options while emulating aio-pika's message operations."""
        self.body = body
        self.options = options
        self.acked = False
        self.rejected: bool | None = None

    async def ack(self) -> object:
        """Record explicit successful processing acknowledgement."""
        self.acked = True
        return object()

    async def reject(self, *, requeue: bool = False) -> object:
        """Record failure disposition without silently accepting the message."""
        self.rejected = requeue
        return object()


class FakeExchange:
    """Capture publisher-confirm calls and routing keys."""

    def __init__(self) -> None:
        """Start with no published messages."""
        self.published: list[tuple[object, str, bool]] = []

    async def publish(self, message: object, routing_key: str, *, mandatory: bool) -> object:
        """Capture the message and mandatory-routing policy."""
        self.published.append((message, routing_key, mandatory))
        return object()


class FakeQueue:
    """Retain queue declaration and callback details for manual-ack tests."""

    def __init__(self, name: str) -> None:
        """Capture the broker queue name and initialize callback tracking."""
        self.name = name
        self.callback: Callable[[FakeMessage], Awaitable[None]] | None = None
        self.no_ack: bool | None = None
        self.routing_key: str | None = None
        self.consumer_tag = "consumer-tag"
        self.cancelled = False

    async def bind(self, exchange: object, *, routing_key: str) -> object:
        """Record the event routing key bound to this queue."""
        del exchange
        self.routing_key = routing_key
        return object()

    async def consume(
        self, callback: Callable[[FakeMessage], Awaitable[None]], *, no_ack: bool
    ) -> str:
        """Capture the asynchronous consumer callback and acknowledgement setting."""
        self.callback = callback
        self.no_ack = no_ack
        return self.consumer_tag

    async def cancel(self, consumer_tag: str) -> object:
        """Record queue consumer cancellation."""
        assert consumer_tag == self.consumer_tag
        self.cancelled = True
        return object()


class FakeChannel:
    """Record channel settings, durable declarations, and cleanup."""

    def __init__(self, exchange: FakeExchange, queue: FakeQueue) -> None:
        """Bind shared fake topology and initialize lifecycle counters."""
        self.exchange = exchange
        self.queue = queue
        self.publisher_confirms = False
        self.on_return_raises = False
        self.exchange_options: dict[str, object] = {}
        self.queue_options: dict[str, object] = {}
        self.prefetch: int | None = None
        self.closed = False

    async def declare_exchange(self, name: str, type: object, *, durable: bool) -> FakeExchange:
        """Capture durable topic-exchange declaration details."""
        self.exchange_options = {"name": name, "type": type, "durable": durable}
        return self.exchange

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool,
        arguments: Mapping[str, object] | None,
    ) -> FakeQueue:
        """Capture queue name, durability, and optional dead-letter arguments."""
        self.queue_options = {"name": name, "durable": durable, "arguments": arguments}
        return self.queue

    async def set_qos(self, *, prefetch_count: int) -> object:
        """Capture the upper bound on in-flight unacknowledged messages."""
        self.prefetch = prefetch_count
        return object()

    async def close(self) -> None:
        """Record channel shutdown."""
        self.closed = True


class FakeConnection:
    """Return fake channels and expose connection closure."""

    def __init__(self) -> None:
        """Initialize deterministic exchange, queue, and channel fixtures."""
        self.exchange = FakeExchange()
        self.queue = FakeQueue("orbit.orders.created")
        self.channels: list[FakeChannel] = []
        self.channel_options: list[dict[str, object]] = []
        self.closed = False

    async def channel(
        self, *, publisher_confirms: bool = True, on_return_raises: bool = False
    ) -> FakeChannel:
        """Create a new fake channel with the requested confirm configuration."""
        self.channel_options.append(
            {"publisher_confirms": publisher_confirms, "on_return_raises": on_return_raises}
        )
        channel = FakeChannel(self.exchange, self.queue)
        channel.publisher_confirms = publisher_confirms
        channel.on_return_raises = on_return_raises
        self.channels.append(channel)
        return channel

    async def close(self) -> None:
        """Record robust connection closure."""
        self.closed = True


def factories() -> tuple[FakeConnection, Callable[..., Any], Callable[..., FakeMessage]]:
    """Return a connection plus injected async connection and message factories."""
    connection = FakeConnection()

    async def connect(url: str) -> FakeConnection:
        assert url == "amqps://user:password@localhost/vhost"
        return connection

    def message(body: bytes, **options: object) -> FakeMessage:
        return FakeMessage(body, **options)

    return connection, connect, message


def test_config_validates_url_and_topology_and_redacts_secret() -> None:
    """Credentials are masked and invalid AMQP URLs/names fail during validation."""
    config = make_config(dead_letter_exchange="orbit.events.dlx")
    assert "password" not in repr(config)
    assert config.dead_letter_exchange == "orbit.events.dlx"
    with pytest.raises(ValidationError):
        make_config(url=SecretStr("https://localhost"))
    with pytest.raises(ValidationError):
        make_config(exchange="bad/name")


@pytest.mark.asyncio
async def test_publish_uses_durable_topic_exchange_and_confirmed_persistent_messages() -> None:
    """Publishing creates resources lazily and awaits confirmed mandatory routing."""
    connection, connect, make_message = factories()
    transport = RabbitMQEventTransport(
        make_config(), connection_factory=connect, message_factory=make_message
    )
    event = Event(name="orders.created", payload={"order_id": "o-1"})
    await transport.publish(event)
    publisher = connection.channels[0]
    assert connection.channel_options == [{"publisher_confirms": True, "on_return_raises": True}]
    assert publisher.exchange_options["name"] == "orbit.events"
    assert publisher.exchange_options["durable"] is True
    published, routing_key, mandatory = connection.exchange.published[0]
    assert routing_key == "orders.created"
    assert mandatory is True
    assert isinstance(published, FakeMessage)
    assert published.options["content_type"] == "application/json"
    await transport.close()
    assert publisher.closed and connection.closed
    with pytest.raises(RabbitMQTransportError):
        await transport.publish(event)


@pytest.mark.asyncio
async def test_subscription_acks_only_after_handler_and_unsubscribe_closes_channel() -> None:
    """Queue is durable, prefetch is bounded, and callback acknowledges successful work."""
    connection, connect, make_message = factories()
    transport = RabbitMQEventTransport(
        make_config(dead_letter_exchange="orbit.events.dlx"),
        connection_factory=connect,
        message_factory=make_message,
    )
    handled: list[Event[Any]] = []

    async def handler(event: Event[Any]) -> None:
        handled.append(event)

    token = await transport.subscribe("orders.created", handler)
    consumer = connection.channels[0]
    assert consumer.prefetch == 16
    assert consumer.queue_options == {
        "name": "orbit.orders.created",
        "durable": True,
        "arguments": {"x-dead-letter-exchange": "orbit.events.dlx"},
    }
    assert connection.queue.no_ack is False
    assert connection.queue.routing_key == "orders.created"
    assert connection.queue.callback is not None
    incoming = FakeMessage(
        Event(name="orders.created", payload={"order_id": "o-1"}).model_dump_json().encode("utf-8")
    )
    await connection.queue.callback(incoming)
    assert incoming.acked
    assert incoming.rejected is None
    assert len(handled) == 1
    await transport.unsubscribe(token)
    assert connection.queue.cancelled and consumer.closed
    await transport.close()


@pytest.mark.asyncio
async def test_handler_failure_retries_then_rejects_for_dead_letter_routing() -> None:
    """Exhausted handler retries reject without requeue and expose only safe failure metadata."""
    connection, connect, make_message = factories()
    transport = RabbitMQEventTransport(
        make_config(max_handler_retries=2, retry_backoff_seconds=0.001),
        connection_factory=connect,
        message_factory=make_message,
    )
    attempts = 0

    async def handler(event: Event[Any]) -> None:
        nonlocal attempts
        del event
        attempts += 1
        raise RuntimeError("private payload and credentials")

    token = await transport.subscribe("orders.failed", handler)
    assert connection.queue.callback is not None
    message = FakeMessage(Event(name="orders.failed", payload={}).model_dump_json().encode())
    await connection.queue.callback(message)
    assert attempts == 3
    assert message.acked is False
    assert message.rejected is False
    assert transport.failed_subscriptions == (token,)
    await transport.close()


@pytest.mark.asyncio
async def test_core_plugin_registers_and_closes_transport() -> None:
    """Core resolves the adapter under its key and owns connection cleanup on shutdown."""
    connection, connect, make_message = factories()
    transport = RabbitMQEventTransport(
        make_config(), connection_factory=connect, message_factory=make_message
    )
    app = Application(ApplicationConfig(name="rabbitmq-plugin-test"))
    app.plugins.register(RabbitMQTransportPlugin(transport))
    await app.startup()
    assert await app.container.aresolve(RABBITMQ_EVENT_TRANSPORT_KEY) is transport
    await transport.publish(Event(name="orders.created", payload={"order_id": "o-2"}))
    await app.stop()
    assert transport.closed
    assert connection.closed


@pytest.mark.asyncio
async def test_cancelled_close_waits_for_shared_cleanup_and_can_be_retried() -> None:
    """Cancellation cannot strand AMQP resources or duplicate concurrent shutdown."""
    connection, connect, make_message = factories()
    transport = RabbitMQEventTransport(
        make_config(), connection_factory=connect, message_factory=make_message
    )
    await transport.publish(Event(name="orders.created", payload={"order_id": "o-3"}))
    entered = asyncio.Event()
    release = asyncio.Event()
    close_calls = 0

    async def delayed_close() -> None:
        nonlocal close_calls
        close_calls += 1
        entered.set()
        await release.wait()
        connection.closed = True

    connection.close = delayed_close  # type: ignore[method-assign]
    cancelled = asyncio.create_task(transport.close())
    await entered.wait()
    waiter = asyncio.create_task(transport.close())
    cancelled.cancel()
    await asyncio.sleep(0)
    assert not cancelled.done()
    assert not waiter.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await waiter
    await transport.close()
    assert close_calls == 1
    assert connection.closed
