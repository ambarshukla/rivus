"""Collector behaviour against a local WebSocket server (no venue, no Kafka)."""

import asyncio
import contextlib
import json

import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from rivus_collect.collector import BACKOFF_MAX_SECONDS, USER_AGENT, Collector, next_backoff
from rivus_collect.config import VenueConfig


class ListSink:
    def __init__(self):
        self.sent = []

    def send(self, topic, raw, headers, timestamp_ms):
        self.sent.append((topic, raw, headers, timestamp_ms))

    def close(self):
        pass


def make_config(port, **overrides):
    values = {
        "venue": "testvenue",
        "url": f"ws://127.0.0.1:{port}",
        "subscribe": ('{"op":"sub","ch":"trades"}',),
        "classify_field": "type",
        "kind_by_value": {"trade": "trades", "quote": "book"},
        "reconnect_on": frozenset({"please_reconnect"}),
        "stale_after_seconds": 0.3,
    }
    values.update(overrides)
    return VenueConfig(**values)


@contextlib.asynccontextmanager
async def venue_server(handler):
    async with serve(handler, "127.0.0.1", 0) as server:
        yield server.sockets[0].getsockname()[1]


async def test_frames_are_forwarded_unchanged_with_metadata():
    # Odd spacing and key order on purpose: the bytes must survive untouched.
    frames = [
        '{"type":"trade",  "price":"1.50","id":7}',
        '{"type":"quote","bid":"1.49"}',
        '{"type":"heartbeat"}',
        "not json at all",
    ]
    received = {}

    async def handler(ws):
        received["subscribe"] = await ws.recv()
        received["user_agent"] = ws.request.headers["User-Agent"]
        for frame in frames:
            await ws.send(frame)
        await ws.close()

    async with venue_server(handler) as port:
        sink = ListSink()
        collector = Collector(make_config(port), sink)
        with pytest.raises(ConnectionClosed):  # the server closing ends the session
            await collector.session()

    assert json.loads(received["subscribe"]) == {"op": "sub", "ch": "trades"}
    assert received["user_agent"] == USER_AGENT
    assert [raw.decode() for _, raw, _, _ in sink.sent] == frames
    assert [topic for topic, *_ in sink.sent] == [
        "raw.testvenue.trades",
        "raw.testvenue.book",
        "raw.testvenue.control",
        "raw.testvenue.control",
    ]
    headers = [h for _, _, h, _ in sink.sent]
    assert [h["seq"] for h in headers] == ["0", "1", "2", "3"]
    assert len({h["conn"] for h in headers}) == 1
    assert all(h["venue"] == "testvenue" for h in headers)
    for _, _, h, timestamp_ms in sink.sent:
        assert int(h["recv_ns"]) // 1_000_000 == timestamp_ms


async def test_silence_ends_the_session_as_stale():
    async def handler(ws):
        await ws.recv()
        await ws.send('{"type":"trade"}')
        await asyncio.sleep(1)  # then say nothing

    async with venue_server(handler) as port:
        sink = ListSink()
        reason = await Collector(make_config(port), sink).session()

    assert reason == "stale"
    assert len(sink.sent) == 1


async def test_venue_can_ask_for_a_reconnect():
    async def handler(ws):
        await ws.recv()
        await ws.send('{"type":"please_reconnect"}')
        await asyncio.sleep(1)

    async with venue_server(handler) as port:
        sink = ListSink()
        reason = await Collector(make_config(port), sink).session()

    assert reason == "requested"
    assert sink.sent[0][0] == "raw.testvenue.control"  # the request itself is kept


async def test_run_reconnects_after_a_drop_and_restarts_seq():
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        await ws.recv()
        await ws.send(json.dumps({"type": "trade", "n": connections}))
        await ws.close()

    delays = []

    async def fake_sleep(seconds):
        delays.append(seconds)
        if len(delays) == 3:
            raise asyncio.CancelledError

    async with venue_server(handler) as port:
        sink = ListSink()
        collector = Collector(make_config(port), sink, sleep=fake_sleep)
        with pytest.raises(asyncio.CancelledError):
            await collector.run()

    assert collector.sessions == 3
    assert [json.loads(raw)["n"] for _, raw, _, _ in sink.sent] == [1, 2, 3]
    assert [h["seq"] for _, _, h, _ in sink.sent] == ["0", "0", "0"]
    # Backoff doubles (1, 2, 4 seconds) with jitter between half and all of it.
    assert 0.5 <= delays[0] <= 1 and 1 <= delays[1] <= 2 and 2 <= delays[2] <= 4


async def test_run_survives_a_venue_that_is_down():
    delays = []

    async def fake_sleep(seconds):
        delays.append(seconds)
        if len(delays) == 2:
            raise asyncio.CancelledError

    # Nothing listens on this port.
    async with venue_server(lambda ws: ws.close()) as port:
        pass
    collector = Collector(make_config(port), ListSink(), sleep=fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    assert len(delays) == 2 and collector.sessions == 0


def test_backoff_is_capped():
    value = 1.0
    for _ in range(20):
        value = next_backoff(value)
    assert value == BACKOFF_MAX_SECONDS
