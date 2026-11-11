"""Shared pieces of the hourly batch: where files go, and talking to ClickHouse."""

from __future__ import annotations

import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

VENUES = os.environ.get("VENUES", "coinbase kraken bitstamp").split()
DATA_DIR = Path(os.environ.get("BATCH_DATA_DIR", "/data/batch"))
DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/rivus/dbt"))


@dataclass(frozen=True)
class Hour:
    """One completed hour, the unit of reconciliation. Times are UTC."""

    start: datetime

    @classmethod
    def parse(cls, text: str) -> Hour:
        start = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        start = start.astimezone(UTC)
        if (start.minute, start.second, start.microsecond) != (0, 0, 0):
            raise ValueError(f"not the start of an hour: {text}")
        return cls(start)

    @property
    def end(self) -> datetime:
        return self.start + timedelta(hours=1)

    @property
    def sql(self) -> str:
        """The start as ClickHouse and DuckDB both read it: 2026-11-11 13:00:00"""
        return self.start.strftime("%Y-%m-%d %H:%M:%S")

    @property
    def dir(self) -> Path:
        return DATA_DIR / self.start.strftime("%Y-%m-%dT%H")

    @property
    def raw_dir(self) -> Path:
        return self.dir / "raw"

    @property
    def stream_dir(self) -> Path:
        return self.dir / "stream"

    @property
    def duckdb(self) -> Path:
        return self.dir / "recon.duckdb"


def clickhouse(sql: str, data: bytes | None = None, **params: str) -> bytes:
    """Run one statement over ClickHouse's HTTP interface.

    Values are passed as named parameters ({name:Type} in the SQL), never
    pasted into the text. `data` is the body of an INSERT.
    """
    query = {"database": "rivus", "query": sql}
    query.update({f"param_{name}": value for name, value in params.items()})
    url = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123") + "/?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(url, data=data if data is not None else b"", method="POST")
    request.add_header("X-ClickHouse-User", os.environ.get("CLICKHOUSE_USER", "rivus"))
    request.add_header("X-ClickHouse-Key", os.environ.get("CLICKHOUSE_PASSWORD", "rivus"))
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"ClickHouse: {error.read().decode(errors='replace')[:500]}") from error
