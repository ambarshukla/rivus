"""The archive's batching, file naming and commit rules, without Kafka."""

from datetime import UTC, datetime

import pyarrow.parquet as pq

from rivus_archive.writer import Archiver, Message, prune

T0 = int(datetime(2026, 11, 27, 12, 59, 0, tzinfo=UTC).timestamp() * 1000)


def msg(offset, ts=T0, partition=0, topic="raw.coinbase.trades", collector="a"):
    return Message(topic, partition, offset, ts, "BTC-USD", collector, f'{{"n":{offset}}}')


def test_a_full_batch_is_written_and_its_end_becomes_committable(tmp_path):
    a = Archiver(tmp_path, max_rows=3)
    for offset in (100, 101, 102):
        a.add(msg(offset), now=0)
    files = list(tmp_path.rglob("*.parquet"))
    assert [f.relative_to(tmp_path).as_posix() for f in files] == [
        "raw.coinbase.trades/date=2026-11-27/hour=12/p0-000000000000100.parquet"
    ]
    assert a.take_committable() == {("raw.coinbase.trades", 0): 103}
    assert a.take_committable() == {}  # handed out once
    table = pq.read_table(files[0])
    assert table.column("offset").to_pylist() == [100, 101, 102]
    assert table.column("value").to_pylist()[0] == '{"n":100}'
    assert table.column("collector").to_pylist() == ["a", "a", "a"]


def test_nothing_is_committable_before_a_file_exists(tmp_path):
    a = Archiver(tmp_path, max_rows=10, max_age_seconds=60)
    a.add(msg(1), now=0)
    a.flush_old(now=30)
    assert a.take_committable() == {}
    assert not list(tmp_path.rglob("*.parquet"))
    a.flush_old(now=61)
    assert a.take_committable() == {("raw.coinbase.trades", 0): 2}


def test_a_new_hour_closes_the_batch_so_a_file_never_spans_two_hours(tmp_path):
    a = Archiver(tmp_path, max_rows=100)
    a.add(msg(1, ts=T0), now=0)
    a.add(msg(2, ts=T0 + 30_000), now=0)
    a.add(msg(3, ts=T0 + 61_000), now=0)  # 13:00:01
    a.flush_all()
    names = sorted(f.relative_to(tmp_path).as_posix() for f in tmp_path.rglob("*.parquet"))
    assert names == [
        "raw.coinbase.trades/date=2026-11-27/hour=12/p0-000000000000001.parquet",
        "raw.coinbase.trades/date=2026-11-27/hour=13/p0-000000000000003.parquet",
    ]


def test_partitions_are_batched_and_committed_separately(tmp_path):
    a = Archiver(tmp_path, max_rows=2)
    a.add(msg(10, partition=0), now=0)
    a.add(msg(50, partition=2), now=0)
    a.add(msg(11, partition=0), now=0)  # partition 0 full
    assert a.take_committable() == {("raw.coinbase.trades", 0): 12}
    a.flush_all()
    assert a.take_committable() == {("raw.coinbase.trades", 2): 51}


def test_a_restart_rebuilds_the_same_file_under_the_same_name(tmp_path):
    # First run: writes 100-101, commits 102, then dies holding 102-103 unwritten.
    first = Archiver(tmp_path, max_rows=2)
    for offset in (100, 101, 102, 103):
        first.add(msg(offset), now=0)
    committed = first.take_committable()[("raw.coinbase.trades", 0)]
    assert committed == 104  # 102-103 filled the second batch too
    # Simulate a crash between writing the second file and committing it.
    committed = 102
    second = Archiver(tmp_path, max_rows=2)
    for offset in range(committed, 104):
        second.add(msg(offset), now=0)
    files = sorted(f.name for f in tmp_path.rglob("*.parquet"))
    assert files == ["p0-000000000000100.parquet", "p0-000000000000102.parquet"]
    rows = sum(pq.read_table(f).num_rows for f in tmp_path.rglob("*.parquet"))
    assert rows == 4  # replaced, not doubled
    assert not list(tmp_path.rglob("*.tmp"))


def test_prune_deletes_only_old_days(tmp_path):
    for day in ("2026-10-01", "2026-11-26"):
        d = tmp_path / "raw.kraken.trades" / f"date={day}" / "hour=01"
        d.mkdir(parents=True)
        (d / "p0-000000000000001.parquet").write_bytes(b"x")
    removed = prune(tmp_path, keep_days=30, today=datetime(2026, 11, 27, tzinfo=UTC))
    assert removed == 1
    assert [p.name for p in (tmp_path / "raw.kraken.trades").iterdir()] == ["date=2026-11-26"]
