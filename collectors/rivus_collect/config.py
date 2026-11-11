"""Venue configuration: one YAML file describes everything venue-specific."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

KINDS = ("trades", "book", "control")


@dataclass(frozen=True)
class VenueConfig:
    venue: str
    url: str
    subscribe: tuple[str, ...]  # JSON text of each frame to send after connecting
    classify_field: str
    kind_by_value: dict[str, str]  # value of classify_field -> "trades" | "book"
    reconnect_on: frozenset[str] = frozenset()
    stale_after_seconds: float = 15.0
    ws_ping_seconds: float | None = 20.0
    extra: dict[str, Any] = field(default_factory=dict)

    def topic(self, kind: str) -> str:
        return f"raw.{self.venue}.{kind}"

    def classify(self, raw: str) -> tuple[str, bool]:
        """Return (kind, reconnect_requested) for one raw message.

        The message is only inspected, never changed. Anything that cannot be
        parsed or is not listed in the config is "control".
        """
        try:
            message = json.loads(raw)
        except ValueError:
            return "control", False
        if not isinstance(message, dict):
            return "control", False
        value = message.get(self.classify_field)
        if not isinstance(value, str):
            return "control", False
        return self.kind_by_value.get(value, "control"), value in self.reconnect_on


def parse_config(data: dict[str, Any]) -> VenueConfig:
    for key in ("venue", "url", "subscribe", "classify"):
        if key not in data:
            raise ValueError(f"venue config is missing '{key}'")
    classify = dict(data["classify"])
    classify_field = classify.pop("field", None)
    if not classify_field:
        raise ValueError("classify.field is required")
    kind_by_value: dict[str, str] = {}
    for kind, values in classify.items():
        if kind not in ("trades", "book"):
            raise ValueError(f"classify: unknown kind '{kind}' (use trades or book)")
        for value in values:
            if value in kind_by_value:
                raise ValueError(f"classify: '{value}' is listed twice")
            kind_by_value[value] = kind
    if not str(data["url"]).startswith("wss://"):
        raise ValueError("url must start with wss://")
    return VenueConfig(
        venue=str(data["venue"]),
        url=str(data["url"]),
        subscribe=tuple(json.dumps(frame, separators=(",", ":")) for frame in data["subscribe"]),
        classify_field=str(classify_field),
        kind_by_value=kind_by_value,
        reconnect_on=frozenset(data.get("reconnect_on", [])),
        stale_after_seconds=float(data.get("stale_after_seconds", 15)),
        ws_ping_seconds=data.get("ws_ping_seconds", 20.0),
    )


def load_config(path: str | Path) -> VenueConfig:
    with open(path, encoding="utf-8") as handle:
        return parse_config(yaml.safe_load(handle))
