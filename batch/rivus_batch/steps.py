"""The steps of the hourly batch, one function each. Airflow runs them in order."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

from .common import DBT_DIR, VENUES, Hour, clickhouse

# Raw messages are picked by the time they were received. Take a margin either
# side of the hour so that a trade received a little before or after the hour
# it belongs to is still found; the models then filter on the trade's own time.
MARGIN_BEFORE = timedelta(minutes=2)
MARGIN_AFTER = timedelta(minutes=5)


def export_raw(hour: Hour, bootstrap: str | None = None) -> dict[str, int]:
    """Copy the hour's raw trade messages from Kafka to one file per venue.

    Each line is {"recv_ms": <when received>, "msg": <the message as received>}.
    """
    from confluent_kafka import Consumer, TopicPartition

    start_ms = int((hour.start - MARGIN_BEFORE).timestamp() * 1000)
    end_ms = int((hour.end + MARGIN_AFTER).timestamp() * 1000)
    consumer = Consumer({
        "bootstrap.servers": bootstrap or os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        "group.id": "rivus-batch-export",  # never commits: every run reads by time
        "enable.auto.commit": False,
    })
    hour.raw_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    try:
        for venue in VENUES:
            partition = TopicPartition(f"raw.{venue}.trades", 0, start_ms)
            # "Which offset holds the first message at or after this time?"
            first = consumer.offsets_for_times([partition], timeout=30)[0]
            _, high = consumer.get_watermark_offsets(TopicPartition(partition.topic, 0), timeout=30)
            written = 0
            with open(hour.raw_dir / f"{venue}.jsonl", "w", encoding="utf-8", newline="\n") as out:
                if first.offset >= 0:
                    consumer.assign([first])
                    position = first.offset
                    while position < high:
                        message = consumer.poll(10)
                        if message is None:
                            raise RuntimeError(f"timed out reading {partition.topic} at {position}")
                        if message.error():
                            raise RuntimeError(str(message.error()))
                        position = message.offset() + 1
                        recv_ms = message.timestamp()[1]
                        if recv_ms > end_ms:
                            break
                        try:
                            parsed = json.loads(message.value())
                        except ValueError:
                            continue  # the stream ignores unparseable messages too
                        out.write(json.dumps({"recv_ms": recv_ms, "msg": parsed}, separators=(",", ":")))
                        out.write("\n")
                        written += 1
            counts[venue] = written
    finally:
        consumer.close()
    print(f"raw messages exported for {hour.sql}: {counts}")
    return counts


STREAM_EXPORTS = {
    "bars.csv": """
        SELECT venue, pair, window_start, open, high, low, close, volume, vwap, trades
        FROM rivus.bars_1m FINAL
        WHERE window_start >= {start:DateTime} AND window_start < {end:DateTime}
        ORDER BY venue, pair, window_start""",
    "late.csv": """
        SELECT venue, pair, trade_id, event_time
        FROM rivus.late_trades FINAL
        WHERE missed_1m AND event_time >= {start:DateTime} AND event_time < {end:DateTime}
        ORDER BY venue, pair, trade_id""",
    # A gap is stamped with the time of the first trade after it.
    "gaps.csv": """
        SELECT venue, pair, prev_trade_id, trade_id, missing, event_time
        FROM rivus.gaps FINAL
        WHERE event_time >= {start:DateTime} AND event_time < {end:DateTime}
        ORDER BY venue, pair, trade_id""",
}


def export_stream(hour: Hour) -> None:
    """Copy what the stream produced for the hour from ClickHouse to CSV files."""
    hour.stream_dir.mkdir(parents=True, exist_ok=True)
    end = hour.end.strftime("%Y-%m-%d %H:%M:%S")
    for name, sql in STREAM_EXPORTS.items():
        body = clickhouse(sql + "\nFORMAT CSVWithNames", start=hour.sql, end=end)
        (hour.stream_dir / name).write_bytes(body)
        print(f"{name}: {max(body.count(b'\n') - 1, 0)} rows")


def run_dbt(hour: Hour, command: str = "build") -> None:
    """Rebuild the bars from the raw files and compare, with dbt on DuckDB."""
    env = dict(os.environ)
    env.update({
        "RIVUS_DUCKDB": str(hour.duckdb),
        "DBT_TARGET_PATH": str(hour.dir / "dbt-target"),
        "DBT_LOG_PATH": str(hour.dir / "dbt-logs"),
    })
    variables = {"hour_start": hour.sql, "raw_dir": str(hour.raw_dir), "stream_dir": str(hour.stream_dir)}
    dbt = os.path.join(os.path.dirname(sys.executable), "dbt")
    subprocess.run(
        [dbt, command, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
         "--vars", json.dumps(variables), "--no-use-colors"],
        env=env, check=True,
    )


def load_results(hour: Hour) -> dict[str, int]:
    """Put the comparison into ClickHouse, replacing any earlier run of the hour."""
    import duckdb

    checked_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    loaded = {}
    with duckdb.connect(str(hour.duckdb), read_only=True) as db:
        for table in ("recon_runs", "recon_diffs"):
            cursor = db.execute(f"SELECT * FROM {table}")  # noqa: S608 - fixed names
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, (str(value) for value in row), strict=True)) | {"checked_at": checked_at}
                    for row in cursor.fetchall()]
            clickhouse(f"DELETE FROM rivus.{table} WHERE hour_start = {{start:DateTime}}", start=hour.sql)
            if rows:
                body = "\n".join(json.dumps(row) for row in rows).encode()
                clickhouse(f"INSERT INTO rivus.{table} FORMAT JSONEachRow", data=body)
            loaded[table] = len(rows)
    print(f"loaded for {hour.sql}: {loaded}")
    return loaded


def rollup(day: str) -> None:
    """Summarise one day's hourly results (day as YYYY-MM-DD)."""
    clickhouse("DELETE FROM rivus.recon_daily WHERE day = {day:Date}", day=day)
    clickhouse("""
        INSERT INTO rivus.recon_daily
        SELECT toDate(hour_start) AS day, venue, pair, count() AS hours,
               sum(stream_bars), sum(batch_bars), sum(matched), sum(differences), now64(3)
        FROM rivus.recon_runs FINAL
        WHERE toDate(hour_start) = {day:Date}
        GROUP BY day, venue, pair""", day=day)
    print(clickhouse("SELECT * FROM rivus.recon_daily FINAL WHERE day = {day:Date} FORMAT TSVWithNames",
                     day=day).decode())
