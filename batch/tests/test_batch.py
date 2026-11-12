"""The dbt project run for real against captured venue messages.

Each test builds a small "hour" on disk (raw files, plus what the stream is
supposed to have produced), runs `dbt build`, and reads the result from DuckDB.
"""

import csv
import json
from pathlib import Path

import duckdb
import pytest

from rivus_batch import common, steps
from rivus_batch.common import Hour

FIXTURES = Path(__file__).parent / "fixtures"
HOUR = "2026-11-11T12:00:00"  # the captured messages are from 12:43 to 12:46
BAR_COLUMNS = ["venue", "pair", "window_start", "open", "high", "low", "close", "volume", "vwap", "trades"]


@pytest.fixture
def hour(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "DATA_DIR", tmp_path)
    monkeypatch.setattr(steps, "DBT_DIR", Path(__file__).parent.parent / "dbt")
    hour = Hour.parse(HOUR)
    hour.raw_dir.mkdir(parents=True)
    hour.stream_dir.mkdir(parents=True)
    for venue in common.VENUES:
        lines = (FIXTURES / f"{venue}_trades.jsonl").read_text("utf-8").splitlines()
        with open(hour.raw_dir / f"{venue}.jsonl", "w", encoding="utf-8") as out:
            for number, line in enumerate(lines):
                out.write(json.dumps({"recv_ms": 1_794_401_100_000 + number, "msg": json.loads(line)}) + "\n")
    write_stream(hour, bars=[])
    return hour


def write_stream(hour, bars, late=(), gaps=(), restarts=()):
    def write(name, header, rows):
        with open(hour.stream_dir / name, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(header)
            writer.writerows(rows)

    write("bars.csv", BAR_COLUMNS, bars)
    write("late.csv", ["venue", "pair", "trade_id", "event_time"], late)
    write("gaps.csv", ["venue", "pair", "prev_trade_id", "trade_id", "missing", "event_time"], gaps)
    write("restarts.csv", ["job", "started_at", "down_since", "resumed"], restarts)


def query(hour, sql):
    with duckdb.connect(str(hour.duckdb), read_only=True) as db:
        return db.execute(sql).fetchall()


def batch_bars(hour):
    """The batch's bars as rows for bars.csv: a stream that agrees completely."""
    return [[str(value) for value in row]
            for row in query(hour, f"select {', '.join(BAR_COLUMNS)} from batch_bars_1m order by all")]


def diffs(hour):
    return query(hour, "select venue, pair, field, classification from recon_diffs order by all")


def test_hour_must_start_on_the_hour():
    assert Hour.parse("2026-11-11T13:00:00Z").sql == "2026-11-11 13:00:00"
    assert Hour.parse("2026-11-11T14:00:00+01:00").sql == "2026-11-11 13:00:00"
    with pytest.raises(ValueError):
        Hour.parse("2026-11-11T13:10:00")


def test_raw_messages_become_trades_once_each(hour):
    steps.run_dbt(hour)  # also runs the dbt tests: uniqueness, positive prices, bar shape

    counts = dict(query(hour, "select venue, count(*) from trades group by venue"))
    # 5 Coinbase messages, 5 Bitstamp messages; Kraken: a 50-trade replay plus 3 messages.
    assert counts["coinbase"] == 5 and counts["bitstamp"] == 5 and counts["kraken"] > 50
    assert query(hour, "select count(*) from (select 1 from trades group by venue, pair, trade_id having count(*) > 1)") == [(0,)]
    # Coinbase's maker side is flipped to the taker's; the capture has a maker "sell".
    first_raw = json.loads((FIXTURES / "coinbase_trades.jsonl").read_text("utf-8").splitlines()[0])
    side = query(hour, f"select side from trades where venue = 'coinbase' and trade_id = {first_raw['trade_id']}")
    assert side == [("buy" if first_raw["side"] == "sell" else "sell",)]


def test_duplicate_messages_do_not_double_count(hour):
    raw = hour.raw_dir / "coinbase.jsonl"
    raw.write_text(raw.read_text("utf-8") * 2, "utf-8")

    steps.run_dbt(hour)

    assert query(hour, "select count(*) from trades where venue = 'coinbase'") == [(5,)]


def test_trades_outside_the_hour_are_left_out(hour):
    # Same files, but reconciling the following hour: nothing belongs to it.
    later = Hour.parse("2026-11-11T13:00:00")
    later.dir.mkdir(parents=True)
    (later.dir / "raw").symlink_to(hour.raw_dir)
    (later.dir / "stream").symlink_to(hour.stream_dir)

    steps.run_dbt(later)

    assert query(later, "select count(*) from trades") == [(0,)]
    assert query(later, "select count(*) from recon_runs") == [(0,)]


def test_a_stream_that_agrees_has_no_differences(hour):
    steps.run_dbt(hour)
    bars = batch_bars(hour)
    assert bars
    write_stream(hour, bars)

    steps.run_dbt(hour)

    assert diffs(hour) == []
    for _, _, stream_bars, batch_count, matched, differences in query(
            hour, "select venue, pair, stream_bars, batch_bars, matched, differences from recon_runs"):
        assert stream_bars == batch_count == matched and differences == 0


def test_differences_are_found_and_classified(hour):
    steps.run_dbt(hour)
    bars = batch_bars(hour)
    by_venue = {venue: next(bar for bar in bars if bar[0] == venue) for venue in common.VENUES}
    volume, vwap, close = BAR_COLUMNS.index("volume"), BAR_COLUMNS.index("vwap"), BAR_COLUMNS.index("close")

    # Coinbase: the stream missed some volume, and flagged a late trade in that minute.
    coinbase = by_venue["coinbase"]
    coinbase[volume] = str(float(coinbase[volume]) / 2)
    late = [[coinbase[0], coinbase[1], 1, coinbase[2]]]
    # Kraken: a vwap that differs in the last decimal place only.
    kraken = by_venue["kraken"]
    kraken[vwap] = f"{float(kraken[vwap]) + 0.00000001:.8f}"
    # Bitstamp: a different close with the same trades and volume, and no excuse.
    bitstamp = by_venue["bitstamp"]
    bitstamp[close] = str(float(bitstamp[close]) + 5)
    write_stream(hour, bars, late=late)

    steps.run_dbt(hour)

    found = {(venue, field): reason for venue, _, field, reason in diffs(hour)}
    assert found == {
        ("coinbase", "volume"): "late event",
        ("kraken", "vwap"): "rounding",
        ("bitstamp", "close"): "ordering",
    }
    verdict = {venue: (matched, differences) for venue, matched, differences in query(
        hour, "select venue, sum(matched), sum(differences) from recon_runs group by venue")}
    total = {venue: sum(1 for bar in bars if bar[0] == venue) for venue in common.VENUES}
    assert verdict["coinbase"] == (total["coinbase"] - 1, 1)
    assert verdict["kraken"] == (total["kraken"], 0)  # rounding alone is not a difference
    assert verdict["bitstamp"] == (total["bitstamp"] - 1, 1)


def test_gaps_and_missing_bars(hour):
    steps.run_dbt(hour)
    bars = batch_bars(hour)
    dropped = next(bar for bar in bars if bar[0] == "kraken")
    wrong = next(bar for bar in bars if bar[0] == "coinbase")
    wrong[BAR_COLUMNS.index("trades")] = "999"
    stream = [bar for bar in bars if bar is not dropped]
    stream.append(["bitstamp", "XRP-USD", "2026-11-11 12:59:00", "1", "1", "1", "1", "1", "1", "1"])
    write_stream(hour, stream, gaps=[[wrong[0], wrong[1], 1, 5, 3, wrong[2]]])

    steps.run_dbt(hour)

    rows = query(hour, "select venue, pair, field, stream_value, batch_value, classification from recon_diffs order by all")
    assert ("kraken", dropped[1], "bar", "missing", "present", "unexplained") in rows
    assert ("bitstamp", "XRP-USD", "bar", "present", "missing", "unexplained") in rows
    assert [row[5] for row in rows if row[0] == "coinbase" and row[2] == "trades"] == ["gap"]


def test_differences_while_a_job_was_restarting_are_labelled_restart(hour):
    steps.run_dbt(hour)
    bars = batch_bars(hour)
    minutes = sorted({bar[2] for bar in bars})
    assert len(minutes) >= 3
    trades = BAR_COLUMNS.index("trades")
    # The stream lost trades in the first minute and has no bar at all for the second.
    stream = [bar for bar in bars if bar[2] != minutes[1]]
    for bar in stream:
        if bar[2] in (minutes[0], minutes[-1]):
            bar[trades] = "999"
    # A job's last checkpoint was 20 seconds into the second minute; it was started again
    # 50 seconds into the same minute. The first minute counts too: its bar was not out yet.
    restart = [["process", f"{minutes[1][:17]}50.000", f"{minutes[1][:17]}20.000", "false"]]
    # A gap in the first minute as well: the restart is the better explanation.
    first = next(bar for bar in stream if bar[2] == minutes[0])
    write_stream(hour, stream, gaps=[[first[0], first[1], 1, 5, 3, first[2]]], restarts=restart)

    steps.run_dbt(hour)

    found = query(hour, "select strftime(window_start, '%Y-%m-%d %H:%M:%S'), classification from recon_diffs")
    reasons = {minute: {reason for at, reason in found if at == minute} - {"rounding"} for minute in minutes}
    assert reasons[minutes[0]] == {"restart"}
    assert reasons[minutes[1]] == {"restart"}
    assert reasons[minutes[-1]] == {"unexplained"}  # after the job was back: no excuse


def test_export_waits_for_the_stream_to_pass_the_hour(monkeypatch):
    answers = iter([b"0\n", b"0\n", b"1\n"])
    asked = []
    monkeypatch.setattr(steps, "clickhouse", lambda sql, **params: asked.append(params) or next(answers))
    monkeypatch.setattr(steps.time, "sleep", lambda seconds: None)

    assert steps.wait_for_stream(Hour.parse(HOUR), timeout=60, poll=1) is True
    assert asked == [{"end": "2026-11-11 13:00:00"}] * 3


def test_export_gives_up_waiting_for_a_stream_that_is_not_coming(monkeypatch, capsys):
    monkeypatch.setattr(steps, "clickhouse", lambda sql, **params: b"0\n")

    assert steps.wait_for_stream(Hour.parse(HOUR), timeout=0, poll=0) is False
    assert "comparing anyway" in capsys.readouterr().out
