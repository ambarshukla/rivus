"""The collector loop: connect, subscribe, forward every frame, reconnect on trouble."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from . import metrics
from .config import KINDS, VenueConfig
from .sink import Sink

log = logging.getLogger(__name__)

# Sent to the venue as the User-Agent. Deliberately generic: no personal details.
USER_AGENT = "market-data-collector/0.1"

BACKOFF_FIRST_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
# A session that lasted this long counts as healthy and resets the backoff.
HEALTHY_SESSION_SECONDS = 60.0


def next_backoff(current: float) -> float:
    return min(current * 2, BACKOFF_MAX_SECONDS)


class Collector:
    def __init__(
        self,
        config: VenueConfig,
        sink: Sink,
        connect: Callable[..., Any] = ws_connect,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.config = config
        self.sink = sink
        self._connect = connect
        self._sleep = sleep
        self.sessions = 0
        for kind in KINDS:  # make every series visible from the start
            metrics.MESSAGES.labels(config.venue, kind)

    async def run(self) -> None:
        """Run sessions forever. Only cancellation stops it."""
        backoff = BACKOFF_FIRST_SECONDS
        venue = self.config.venue
        while True:
            started = time.monotonic()
            reason = await self._guarded_session()
            metrics.CONNECTED.labels(venue).set(0)
            metrics.DISCONNECTS.labels(venue, reason).inc()
            if time.monotonic() - started >= HEALTHY_SESSION_SECONDS:
                backoff = BACKOFF_FIRST_SECONDS
            # Jitter, so that several collectors do not reconnect in step.
            delay = backoff * random.uniform(0.5, 1.0)
            log.warning("%s: session ended (%s); reconnecting in %.1fs", venue, reason, delay)
            await self._sleep(delay)
            backoff = next_backoff(backoff)

    async def _guarded_session(self) -> str:
        try:
            return await self.session()
        except ConnectionClosed as exc:
            log.info("%s: connection closed: %s", self.config.venue, exc)
            return "closed"
        except TimeoutError:
            return "connect_timeout"
        except OSError as exc:
            log.info("%s: network error: %s", self.config.venue, exc)
            return "network"
        except Exception:  # noqa: BLE001 - a collector must never die; log and reconnect
            log.exception("%s: unexpected error", self.config.venue)
            return "error"

    async def session(self) -> str:
        """One WebSocket connection, from connect to the reason it ended."""
        cfg = self.config
        async with self._connect(
            cfg.url,
            user_agent_header=USER_AGENT,
            open_timeout=15,
            close_timeout=2,  # do not wait long for a goodbye from a dead connection
            ping_interval=cfg.ws_ping_seconds,
            ping_timeout=cfg.ws_ping_seconds,
            max_size=16 * 1024 * 1024,
        ) as ws:
            self.sessions += 1
            # conn + seq identify every message: seq restarts at 0 on each connection.
            conn = str(time.time_ns() // 1_000_000)
            metrics.CONNECTS.labels(cfg.venue).inc()
            metrics.CONNECTED.labels(cfg.venue).set(1)
            log.info("%s: connected to %s (conn %s)", cfg.venue, cfg.url, conn)
            for frame in cfg.subscribe:
                await ws.send(frame)

            seq = 0
            while True:
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=cfg.stale_after_seconds)
                except TimeoutError:
                    log.warning("%s: no message for %ss", cfg.venue, cfg.stale_after_seconds)
                    return "stale"
                recv_ns = time.time_ns()
                if isinstance(frame, str):
                    raw, text = frame.encode("utf-8"), frame
                else:
                    raw, text = frame, frame.decode("utf-8", errors="replace")
                kind, reconnect = cfg.classify(text)
                self.sink.send(
                    cfg.topic(kind),
                    raw,
                    {"venue": cfg.venue, "conn": conn, "seq": str(seq), "recv_ns": str(recv_ns)},
                    recv_ns // 1_000_000,
                )
                seq += 1
                metrics.MESSAGES.labels(cfg.venue, kind).inc()
                metrics.BYTES.labels(cfg.venue, kind).inc(len(raw))
                metrics.LAST_MESSAGE.labels(cfg.venue, kind).set(recv_ns / 1e9)
                if reconnect:
                    return "requested"
