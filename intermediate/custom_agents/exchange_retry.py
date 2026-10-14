"""Reconnect the Academy cloud exchange listener when its stream is cut.

HttpExchangeTransport.listen() keeps a streaming GET open to the exchange. It
reconnects when the server ends the stream, but a connection that is cut in the
middle of a response (the ALCF proxy on Aurora compute nodes does this) raises
aiohttp.ClientPayloadError, and the agent shuts down. This wrapper opens the
stream again instead. A message that was on the stream when it was cut is not
sent again, so callers must not wait forever for a reply.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any

import aiohttp
from academy.exchange.cloud.client import HttpExchangeTransport
from academy.message import Message

logger = logging.getLogger(__name__)

_original_listen = HttpExchangeTransport.listen


async def _listen_with_reconnect(
    self: HttpExchangeTransport,
    timeout: float | None = None,
) -> AsyncGenerator[Message[Any]]:
    while True:
        try:
            async for message in _original_listen(self, timeout):
                yield message
            return
        except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
            logger.warning("Exchange stream for %s was cut (%s); reconnecting", self.mailbox_id, e)
            await asyncio.sleep(1)


def install() -> None:
    """Patch HttpExchangeTransport.listen once per process."""
    if HttpExchangeTransport.listen is not _listen_with_reconnect:
        HttpExchangeTransport.listen = _listen_with_reconnect
