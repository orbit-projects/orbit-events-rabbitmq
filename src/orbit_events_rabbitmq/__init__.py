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
"""Optional RabbitMQ event transport for Orbit Core."""

from orbit_events_rabbitmq.config import RabbitMQConfig
from orbit_events_rabbitmq.plugin import RABBITMQ_EVENT_TRANSPORT_KEY, RabbitMQTransportPlugin
from orbit_events_rabbitmq.transport import RabbitMQEventTransport, RabbitMQTransportError

__all__ = [
    "RABBITMQ_EVENT_TRANSPORT_KEY",
    "RabbitMQConfig",
    "RabbitMQEventTransport",
    "RabbitMQTransportError",
    "RabbitMQTransportPlugin",
]
