"""Folding a batch of trades and quotes into the latest state per venue and pair."""

from __future__ import annotations

TRADE_FIELDS = {"price": "last_price", "qty": "last_qty", "side": "last_side",
                "event_time": "last_trade_time"}
QUOTE_FIELDS = {"bid": "bid", "bid_qty": "bid_qty", "ask": "ask", "ask_qty": "ask_qty",
                "event_time": "quote_time"}


def redis_key(venue: str, pair: str) -> str:
    return f"latest:{venue}:{pair}"


class LatestState:
    """Remembers the newest trade and quote time per key so that an older
    message arriving afterwards cannot overwrite a newer price."""

    def __init__(self) -> None:
        self._newest: dict[tuple[str, str, str], str] = {}

    def fold(self, messages: list[tuple[str, dict]]) -> dict[tuple[str, str], dict[str, str]]:
        """messages: (kind, row) with kind 'trade' or 'quote'.

        Returns the fields to write per (venue, pair). Many messages for the
        same key collapse into one update.
        """
        updates: dict[tuple[str, str], dict[str, str]] = {}
        for kind, row in messages:
            fields = TRADE_FIELDS if kind == "trade" else QUOTE_FIELDS
            try:
                venue, pair, event_time = row["venue"], row["pair"], row["event_time"]
                values = {target: str(row[source]) for source, target in fields.items()}
            except (KeyError, TypeError):
                continue  # not a row this writer understands
            marker = (venue, pair, kind)
            # Times are 'YYYY-MM-DD HH:MM:SS.mmm' strings, which sort correctly as text.
            if event_time < self._newest.get(marker, ""):
                continue
            self._newest[marker] = event_time
            updates.setdefault((venue, pair), {"venue": venue, "pair": pair}).update(values)
        return updates
