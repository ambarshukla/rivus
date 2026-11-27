"""Batches raw messages per partition into Parquet files, and says what may be committed.

A file is named after the partition and the offset of its first message:

    <root>/<topic>/date=2026-11-27/hour=12/p0-000000029487902.parquet

The consumer commits a partition's position only after the file holding everything
before it has been written (to a temporary name, then renamed, so a reader never sees
half a file). After a crash the consumer starts again from the last commit, which is
the first offset of the next file: it rebuilds that file under the same name and
replaces it. So each message is in the archive once, however often the archiver
restarts. Resetting the group's offsets by hand to a time is different: the first
offset is then wherever the reset lands, so some messages end up in two files. Every
row carries its partition and offset; those two identify a message.

This module knows nothing about Kafka clients: the main loop hands it plain values,
which keeps it testable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("recv_time", pa.timestamp("ms", tz="UTC")),  # the Kafka record timestamp
        ("key", pa.string()),
        ("collector", pa.string()),  # which collector sent it: a, b
        ("value", pa.string()),  # the message exactly as received
    ]
)


@dataclass
class Message:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: str | None
    collector: str | None
    value: str


@dataclass
class Batch:
    rows: list[Message] = field(default_factory=list)
    opened_at: float = 0.0  # wall clock, when the first row arrived

    @property
    def hour(self) -> datetime:
        return hour_of(self.rows[0].timestamp_ms)


def hour_of(timestamp_ms: int) -> datetime:
    moment = datetime.fromtimestamp(timestamp_ms / 1000, UTC)
    return moment.replace(minute=0, second=0, microsecond=0)


class Archiver:
    """Holds one open batch per partition and writes it when it is full, old, or the hour turns."""

    def __init__(self, root: Path, max_rows: int = 20_000, max_age_seconds: float = 60.0) -> None:
        self.root = root
        self.max_rows = max_rows
        self.max_age_seconds = max_age_seconds
        self.batches: dict[tuple[str, int], Batch] = {}
        # Per partition, the offset to commit: everything before it is in a file.
        self.committable: dict[tuple[str, int], int] = {}
        self.files_written = 0
        self.rows_written = 0

    def add(self, message: Message, now: float) -> None:
        tp = (message.topic, message.partition)
        batch = self.batches.get(tp)
        # A file holds one hour: a message from a later hour closes the batch first.
        if batch and batch.rows and hour_of(message.timestamp_ms) != batch.hour:
            self._write(tp)
            batch = None
        if batch is None:
            batch = self.batches[tp] = Batch(opened_at=now)
        batch.rows.append(message)
        if len(batch.rows) >= self.max_rows:
            self._write(tp)

    def flush_old(self, now: float) -> None:
        for tp, batch in list(self.batches.items()):
            if batch.rows and now - batch.opened_at >= self.max_age_seconds:
                self._write(tp)

    def flush_all(self) -> None:
        for tp in list(self.batches):
            if self.batches[tp].rows:
                self._write(tp)

    def take_committable(self) -> dict[tuple[str, int], int]:
        """Offsets that are safe to commit now; each is handed out once."""
        ready, self.committable = self.committable, {}
        return ready

    def path_for(self, batch: Batch) -> Path:
        first = batch.rows[0]
        hour = batch.hour
        return (
            self.root
            / first.topic
            / f"date={hour:%Y-%m-%d}"
            / f"hour={hour:%H}"
            / f"p{first.partition}-{first.offset:015d}.parquet"
        )

    def _write(self, tp: tuple[str, int]) -> None:
        batch = self.batches.pop(tp)
        path = self.path_for(batch)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(
            [
                {
                    "topic": m.topic,
                    "partition": m.partition,
                    "offset": m.offset,
                    "recv_time": m.timestamp_ms,
                    "key": m.key,
                    "collector": m.collector,
                    "value": m.value,
                }
                for m in batch.rows
            ],
            schema=SCHEMA,
        )
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, path)  # atomic: the file appears whole or not at all
        self.committable[tp] = batch.rows[-1].offset + 1
        self.files_written += 1
        self.rows_written += len(batch.rows)


def prune(root: Path, keep_days: int, today: datetime) -> int:
    """Delete date folders older than keep_days. Returns how many were deleted."""
    removed = 0
    for date_dir in root.glob("*/date=*"):
        try:
            day = datetime.strptime(date_dir.name[5:], "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            continue
        if (today - day).days > keep_days:
            for file in sorted(date_dir.rglob("*"), reverse=True):
                file.unlink() if file.is_file() else file.rmdir()
            date_dir.rmdir()
            removed += 1
    return removed
