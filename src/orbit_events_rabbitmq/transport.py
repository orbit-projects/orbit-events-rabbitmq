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
"""RabbitMQ transport implementation for Orbit Core events."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

import aio_pika
from orbit.events import Event, EventHandler, EventTransport

from orbit_events_rabbitmq.config import RabbitMQConfig

_LOG = logging.getLogger(__name__)
_EVENT_NAME = re.compile(r"[a-z][a-z0-9.-]{0,126}\Z")


class RabbitMQTransportError(Exception):
    """Sanitized AMQP failure without broker URLs or provider exception details."""

    def __init__(self, operation: str) -> None:
        """Identify the failed operation without copying connection or message contents."""
        self.operation = operation
        super().__init__(f"RabbitMQ {operation} operation failed.")


class _IncomingMessage(Protocol):
    """Minimal incoming-message surface used for acknowledged event delivery."""

    body: bytes

    async def ack(self) -> object:
        """Acknowledge completed event-handler processing."""
        ...

    async def reject(self, *, requeue: bool = False) -> object:
        """Reject a malformed or repeatedly failing message."""
        ...


class _Exchange(Protocol):
    """Minimal exchange surface required for confirmed event publication."""

    async def publish(self, message: object, routing_key: str, *, mandatory: bool = True) -> object:
        """Publish a message and await the configured publisher confirmation."""
        ...


class _Queue(Protocol):
    """Minimal queue surface for binding and acknowledged callback consumption."""

    async def bind(self, exchange: object, *, routing_key: str) -> object:
        """Bind the queue to one event routing key."""
        ...

    async def consume(
        self, callback: Callable[[_IncomingMessage], Awaitable[None]], *, no_ack: bool = False
    ) -> str:
        """Register a manually acknowledged callback and return its consumer tag."""
        ...

    async def cancel(self, consumer_tag: str) -> object:
        """Stop delivery for one consumer tag."""
        ...


class _Channel(Protocol):
    """Minimal AMQP channel surface shared by publisher and consumer ownership."""

    async def declare_exchange(
        self, name: str, type: object, *, durable: bool = False
    ) -> _Exchange:
        """Declare or retrieve a named exchange."""
        ...

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool = False,
        arguments: Mapping[str, object] | None = None,
    ) -> _Queue:
        """Declare or retrieve a named queue."""
        ...

    async def set_qos(self, *, prefetch_count: int = 0) -> object:
        """Bound unacknowledged deliveries for this channel."""
        ...

    async def close(self) -> None:
        """Release this channel and its consumer registrations."""
        ...


class _Connection(Protocol):
    """Minimal robust connection surface used by the transport."""

    async def channel(
        self, *, publisher_confirms: bool = True, on_return_raises: bool = False
    ) -> _Channel:
        """Open a channel with optional publisher-confirm and unroutable handling."""
        ...

    async def close(self) -> None:
        """Close the robust AMQP connection and its channels."""
        ...


ConnectionFactory = Callable[..., Awaitable[_Connection]]
MessageFactory = Callable[..., object]


@dataclass
class _Subscription:
    """Owned consumer channel, queue, and tag for one event handler."""

    event_name: str
    channel: _Channel
    queue: _Queue
    consumer_tag: str


class RabbitMQEventTransport(EventTransport):
    """Publish and consume Core events through a durable RabbitMQ topic exchange.

    The exchange and durable queues are declared by this adapter. Consumers acknowledge only after
    successful handler completion; retries are local to one delivery, and exhausted failures are
    rejected so RabbitMQ can route them to a configured dead-letter exchange.
    """

    def __init__(
        self,
        config: RabbitMQConfig,
        *,
        connection_factory: ConnectionFactory | None = None,
        message_factory: MessageFactory | None = None,
    ) -> None:
        """Bind validated settings and optional factories for deterministic tests."""
        if not isinstance(config, RabbitMQConfig):
            raise TypeError("RabbitMQEventTransport requires a RabbitMQConfig instance.")
        self.config = config
        self._connection_factory: ConnectionFactory = connection_factory or cast(
            ConnectionFactory, aio_pika.connect_robust
        )
        self._message_factory = message_factory or aio_pika.Message
        self._connection: _Connection | None = None
        self._publisher_channel: _Channel | None = None
        self._exchange: _Exchange | None = None
        self._connection_lock = asyncio.Lock()
        self._publisher_lock = asyncio.Lock()
        self._publish_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._subscriptions: dict[UUID, _Subscription] = {}
        self._event_tokens: dict[str, UUID] = {}
        self._failed: set[UUID] = set()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def failed_subscriptions(self) -> tuple[UUID, ...]:
        """Return IDs whose consumer encountered invalid data or handler/AMQP failure."""
        return tuple(sorted(self._failed, key=str))

    @property
    def closed(self) -> bool:
        """Whether shutdown has begun and new transport operations are rejected."""
        return self._closed

    def routing_key_for(self, event_name: str) -> str:
        """Return a validated event-name routing key."""
        if not isinstance(event_name, str) or _EVENT_NAME.fullmatch(event_name) is None:
            raise ValueError(
                "RabbitMQ event names must match Orbit's lowercase event-name contract."
            )
        return event_name

    async def _ensure_connection(self) -> _Connection:
        """Establish one robust connection and share it among publisher and consumer channels."""
        async with self._connection_lock:
            if self._closed:
                raise RabbitMQTransportError("connect")
            if self._connection is None:
                try:
                    self._connection = await self._connection_factory(self.config._connection_url())
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise RabbitMQTransportError("connect") from None
            return self._connection

    async def _ensure_publisher(self) -> tuple[_Channel, _Exchange]:
        """Open one confirmed publisher channel and declare its durable topic exchange."""
        async with self._publisher_lock:
            if self._closed:
                raise RabbitMQTransportError("publish")
            if self._publisher_channel is None or self._exchange is None:
                connection = await self._ensure_connection()
                channel: _Channel | None = None
                try:
                    channel = await connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                    exchange = await channel.declare_exchange(
                        self.config.exchange, aio_pika.ExchangeType.TOPIC, durable=True
                    )
                except asyncio.CancelledError:
                    if channel is not None:
                        with suppress(Exception):
                            await channel.close()
                    raise
                except Exception:
                    if channel is not None:
                        with suppress(Exception):
                            await channel.close()
                    raise RabbitMQTransportError("publish setup") from None
                self._publisher_channel = channel
                self._exchange = exchange
            return self._publisher_channel, self._exchange

    async def publish(self, event: Event[Any]) -> None:
        """Publish one persistent JSON event and wait for broker confirmation."""
        if not isinstance(event, Event):
            raise TypeError("RabbitMQEventTransport.publish requires an Orbit Event.")
        if self._closed:
            raise RabbitMQTransportError("publish")
        routing_key = self.routing_key_for(event.name)
        body = event.model_dump_json().encode("utf-8")
        if len(body) > self.config.max_message_bytes:
            raise ValueError("Encoded event exceeds RabbitMQConfig.max_message_bytes.")
        _channel, exchange = await self._ensure_publisher()
        message = self._message_factory(
            body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        async with self._publish_lock:
            if self._closed:
                raise RabbitMQTransportError("publish")
            try:
                await exchange.publish(message, routing_key, mandatory=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise RabbitMQTransportError("publish") from None

    async def subscribe(self, event_name: str, handler: EventHandler) -> UUID:
        """Declare a durable queue and consume with explicit acknowledgements."""
        routing_key = self.routing_key_for(event_name)
        if not callable(handler):
            raise TypeError("RabbitMQ event handler must be callable.")
        async with self._state_lock:
            if self._closed:
                raise RabbitMQTransportError("subscribe")
            if event_name in self._event_tokens:
                raise ValueError(
                    "Only one handler per event name is allowed per transport instance."
                )
            connection = await self._ensure_connection()
            channel: _Channel | None = None
            try:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=self.config.prefetch_count)
                exchange = await channel.declare_exchange(
                    self.config.exchange, aio_pika.ExchangeType.TOPIC, durable=True
                )
                queue_arguments: dict[str, object] | None = None
                if self.config.dead_letter_exchange is not None:
                    queue_arguments = {"x-dead-letter-exchange": self.config.dead_letter_exchange}
                queue = await channel.declare_queue(
                    f"{self.config.queue_prefix}.{event_name}",
                    durable=True,
                    arguments=queue_arguments,
                )
                await queue.bind(exchange, routing_key=routing_key)
                token = uuid4()

                async def on_message(message: _IncomingMessage) -> None:
                    await self._handle_message(token, event_name, handler, message)

                consumer_tag = await queue.consume(on_message, no_ack=False)
            except asyncio.CancelledError:
                if channel is not None:
                    await channel.close()
                raise
            except Exception:
                if channel is not None:
                    with suppress(Exception):
                        await channel.close()
                raise RabbitMQTransportError("subscribe") from None
            subscription = _Subscription(event_name, channel, queue, consumer_tag)
            self._subscriptions[token] = subscription
            self._event_tokens[event_name] = token
            return token

    async def _handle_message(
        self, token: UUID, event_name: str, handler: EventHandler, message: _IncomingMessage
    ) -> None:
        """Validate, retry, then ack or reject one delivery without exposing payload contents."""
        try:
            event = Event[Any].model_validate_json(message.body)
            if event.name != event_name:
                raise ValueError("Event envelope name does not match its RabbitMQ routing key.")
        except Exception as exc:
            self._failed.add(token)
            _LOG.error("RabbitMQ consumer rejected invalid event (%s)", type(exc).__name__)
            with suppress(Exception):
                await message.reject(requeue=False)
            return

        for attempt in range(self.config.max_handler_retries + 1):
            try:
                await handler(event)
                await message.ack()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt < self.config.max_handler_retries:
                    await asyncio.sleep(self.config.retry_backoff_seconds)
                    continue
                self._failed.add(token)
                _LOG.error("RabbitMQ handler rejected after retries (%s)", type(exc).__name__)
                with suppress(Exception):
                    await message.reject(requeue=False)

    async def unsubscribe(self, token: object) -> None:
        """Cancel a queue consumer and close its channel, rejecting unknown tokens."""
        if not isinstance(token, UUID):
            raise TypeError("RabbitMQ subscription token must be a UUID.")
        async with self._state_lock:
            subscription = self._subscriptions.pop(token, None)
            if subscription is None:
                raise ValueError("Unknown RabbitMQ subscription token.")
            self._event_tokens.pop(subscription.event_name, None)
        try:
            await subscription.queue.cancel(subscription.consumer_tag)
            await subscription.channel.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise RabbitMQTransportError("unsubscribe") from None

    async def close(self) -> None:
        """Cancel consumers and drain one shared cleanup before propagating cancellation."""
        task = self._close_task
        if task is None:
            self._closed = True
            task = asyncio.create_task(self._finish_close())
            self._close_task = task

        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True

        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _finish_close(self) -> None:
        """Stop consumers and close all owned AMQP resources, attempting every cleanup step."""
        async with self._state_lock:
            subscriptions = tuple(self._subscriptions.values())
            self._subscriptions.clear()
            self._event_tokens.clear()
        failures = 0
        for subscription in subscriptions:
            try:
                await subscription.queue.cancel(subscription.consumer_tag)
            except Exception:
                failures += 1
            try:
                await subscription.channel.close()
            except Exception:
                failures += 1
        async with self._publish_lock, self._publisher_lock:
            if self._publisher_channel is not None:
                try:
                    await self._publisher_channel.close()
                except Exception:
                    failures += 1
            if self._connection is not None:
                try:
                    await self._connection.close()
                except Exception:
                    failures += 1
            self._publisher_channel = None
            self._exchange = None
            self._connection = None
        if failures:
            raise RabbitMQTransportError("close") from None


__all__ = ["RabbitMQEventTransport", "RabbitMQTransportError"]
