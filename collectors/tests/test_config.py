"""The shipped venue configs, checked against real captured messages.

tests/fixtures/<venue>/<kind>.jsonl holds messages captured from the live feeds
(see docs/PROVENANCE.md), one per line, exactly as received.
"""

from pathlib import Path

import pytest

from rivus_collect.config import KINDS, load_config, parse_config

ROOT = Path(__file__).resolve().parent.parent
VENUES = ["coinbase", "kraken", "bitstamp"]


@pytest.mark.parametrize("venue", VENUES)
def test_shipped_config_loads(venue):
    config = load_config(ROOT / "venues" / f"{venue}.yaml")
    assert config.venue == venue
    assert config.topic("trades") == f"raw.{venue}.trades"
    assert config.subscribe
    assert set(config.kind_by_value.values()) == {"trades", "book"}


@pytest.mark.parametrize("venue", VENUES)
@pytest.mark.parametrize("kind", KINDS)
def test_captured_messages_are_classified(venue, kind):
    config = load_config(ROOT / "venues" / f"{venue}.yaml")
    lines = (ROOT / "tests" / "fixtures" / venue / f"{kind}.jsonl").read_text("utf-8").splitlines()
    assert lines, "fixture file is empty"
    for line in lines:
        assert config.classify(line) == (kind, False), line[:120]


def test_bitstamp_reconnect_request_is_recognised():
    config = load_config(ROOT / "venues" / "bitstamp.yaml")
    raw = '{"event":"bts:request_reconnect","channel":"","data":""}'
    assert config.classify(raw) == ("control", True)


@pytest.mark.parametrize("raw", ["", "[1,2]", "42", '{"type":5}', '{"other":"x"}', "{broken"])
def test_unrecognised_messages_are_control(raw):
    config = load_config(ROOT / "venues" / "coinbase.yaml")
    assert config.classify(raw) == ("control", False)


BASE = {
    "venue": "v",
    "url": "wss://example.invalid",
    "subscribe": [{"a": 1}],
    "classify": {"field": "type", "trades": ["t"], "book": ["b"]},
}


@pytest.mark.parametrize(
    "change, message",
    [
        ({"url": "ws://example.invalid"}, "wss://"),
        ({"classify": {"trades": ["t"]}}, "classify.field"),
        ({"classify": {"field": "type", "orders": ["t"]}}, "unknown kind"),
        ({"classify": {"field": "type", "trades": ["t"], "book": ["t"]}}, "listed twice"),
    ],
)
def test_bad_configs_are_rejected(change, message):
    with pytest.raises(ValueError, match=message):
        parse_config({**BASE, **change})


def test_missing_key_is_rejected():
    with pytest.raises(ValueError, match="missing 'url'"):
        parse_config({k: v for k, v in BASE.items() if k != "url"})


@pytest.mark.parametrize("venue", VENUES)
@pytest.mark.parametrize("kind", ["trades", "book"])
def test_captured_messages_are_keyed_by_pair(venue, kind):
    config = load_config(ROOT / "venues" / f"{venue}.yaml")
    lines = (ROOT / "tests" / "fixtures" / venue / f"{kind}.jsonl").read_text("utf-8").splitlines()
    keys = {config.key(line) for line in lines}
    assert keys <= {"BTC-USD", "ETH-USD"}, keys


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('{"data":[{"symbol":"BTC/USD"}]}', "BTC-USD"),
        ('{"data":[{"symbol":"SOL/USD"}]}', None),  # not in the list: no key
        ('{"data":[]}', None),
        ('{"data":"x"}', None),
        ("{broken", None),
    ],
)
def test_key_follows_the_path(raw, expected):
    key = {"path": "data.0.symbol", "pairs": {"BTC/USD": "BTC-USD"}}
    config = parse_config({**BASE, "key": key})
    assert config.key(raw) == expected


def test_key_without_pairs_uses_the_value():
    config = parse_config({**BASE, "key": {"path": "product_id"}})
    assert config.key('{"product_id":"BTC-USD"}') == "BTC-USD"
    assert parse_config(BASE).key('{"product_id":"BTC-USD"}') is None


def test_pairs_without_path_are_rejected():
    with pytest.raises(ValueError, match="key.pairs needs key.path"):
        parse_config({**BASE, "key": {"pairs": {"a": "b"}}})

