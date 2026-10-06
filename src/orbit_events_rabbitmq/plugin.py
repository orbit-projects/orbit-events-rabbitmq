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
"""Optional Core plugin that registers and owns the RabbitMQ event transport."""

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata

from orbit_events_rabbitmq.transport import RabbitMQEventTransport

RABBITMQ_EVENT_TRANSPORT_KEY = "orbit.events.transport.rabbitmq"


class RabbitMQTransportPlugin(Plugin):
    """Register the configured adapter and close it during Core application shutdown."""

    metadata = PluginMetadata(
        name="orbit-events-rabbitmq",
        version="0.1.0a1",
        capabilities=frozenset({"events.rabbitmq"}),
    )

    def __init__(self, transport: RabbitMQEventTransport) -> None:
        """Take lifecycle ownership of a RabbitMQ transport configured by the application."""
        if not isinstance(transport, RabbitMQEventTransport):
            raise TypeError("RabbitMQTransportPlugin requires a RabbitMQEventTransport instance.")
        self.transport = transport

    def setup(self, application: Application) -> None:
        """Expose the transport under its stable optional-package container key."""
        if not isinstance(application, Application):
            raise TypeError("RabbitMQTransportPlugin requires an Orbit Application.")
        application.container.register_instance(RABBITMQ_EVENT_TRANSPORT_KEY, self.transport)

    async def deactivate(self) -> None:
        """Close the AMQP connection on normal shutdown or partial-startup rollback."""
        await self.transport.close()


__all__ = ["RABBITMQ_EVENT_TRANSPORT_KEY", "RabbitMQTransportPlugin"]
