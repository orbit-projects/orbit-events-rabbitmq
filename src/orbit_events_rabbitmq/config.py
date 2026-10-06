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
"""Validated, redacted settings for the optional RabbitMQ transport."""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}\Z")


class RabbitMQConfig(BaseModel):
    """Connection, topology, and bounded delivery settings for the RabbitMQ adapter."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    url: SecretStr
    exchange: str = "orbit.events"
    queue_prefix: str = "orbit"
    dead_letter_exchange: str | None = None
    prefetch_count: int = Field(default=16, ge=1, le=10_000)
    max_message_bytes: int = Field(default=1_048_576, ge=1, le=10_485_760)
    max_handler_retries: int = Field(default=3, ge=0, le=100)
    retry_backoff_seconds: float = Field(default=0.25, gt=0, le=60)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: SecretStr) -> SecretStr:
        """Accept only bounded, printable AMQP connection URLs."""
        url = value.get_secret_value()
        if (
            not 1 <= len(url) <= 4_096
            or not url.startswith(("amqp://", "amqps://"))
            or any(not character.isprintable() or character.isspace() for character in url)
        ):
            raise ValueError("RabbitMQ URL must be a bounded amqp:// or amqps:// URL.")
        return value

    @field_validator("exchange", "queue_prefix")
    @classmethod
    def validate_topology_names(cls, value: str) -> str:
        """Constrain configured exchange and queue prefixes to portable AMQP names."""
        if _NAME.fullmatch(value) is None:
            raise ValueError(
                "RabbitMQ topology names must use portable ASCII identifier characters."
            )
        return value

    @field_validator("dead_letter_exchange")
    @classmethod
    def validate_dead_letter_exchange(cls, value: str | None) -> str | None:
        """Validate an optional dead-letter exchange before using it in queue arguments."""
        if value is not None and _NAME.fullmatch(value) is None:
            raise ValueError("Dead-letter exchange must use portable ASCII identifier characters.")
        return value

    def _connection_url(self) -> str:
        """Reveal the secret only at the aio-pika connection boundary."""
        return self.url.get_secret_value()


__all__ = ["RabbitMQConfig"]
