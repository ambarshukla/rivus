from rivus_latest.state import LatestState, redis_key

TRADE = {"venue": "kraken", "pair": "BTC-USD", "trade_id": 1, "price": 100.5, "qty": 0.25,
         "side": "buy", "event_time": "2026-11-11 12:00:01.000", "recv_time": "x"}
QUOTE = {"venue": "kraken", "pair": "BTC-USD", "bid": 100.4, "bid_qty": 1, "ask": 100.6,
         "ask_qty": 2, "event_time": "2026-11-11 12:00:01.500", "recv_time": "x"}


def test_key_format():
    assert redis_key("kraken", "BTC-USD") == "latest:kraken:BTC-USD"


def test_trade_and_quote_merge_into_one_update():
    updates = LatestState().fold([("trade", TRADE), ("quote", QUOTE)])
    assert updates == {
        ("kraken", "BTC-USD"): {
            "venue": "kraken", "pair": "BTC-USD",
            "last_price": "100.5", "last_qty": "0.25", "last_side": "buy",
            "last_trade_time": "2026-11-11 12:00:01.000",
            "bid": "100.4", "bid_qty": "1", "ask": "100.6", "ask_qty": "2",
            "quote_time": "2026-11-11 12:00:01.500",
        }
    }


def test_last_message_in_a_batch_wins():
    newer = {**TRADE, "price": 101, "event_time": "2026-11-11 12:00:02.000"}
    updates = LatestState().fold([("trade", TRADE), ("trade", newer)])
    assert updates[("kraken", "BTC-USD")]["last_price"] == "101"


def test_older_message_does_not_overwrite_newer_across_batches():
    state = LatestState()
    newer = {**TRADE, "price": 101, "event_time": "2026-11-11 12:00:02.000"}
    state.fold([("trade", newer)])
    assert state.fold([("trade", TRADE)]) == {}
    # A late trade must not block a quote: they are tracked separately.
    assert ("kraken", "BTC-USD") in state.fold([("quote", QUOTE)])


def test_venues_and_pairs_are_kept_apart():
    other = {**TRADE, "venue": "coinbase"}
    eth = {**TRADE, "pair": "ETH-USD"}
    assert set(LatestState().fold([("trade", TRADE), ("trade", other), ("trade", eth)])) == {
        ("kraken", "BTC-USD"), ("coinbase", "BTC-USD"), ("kraken", "ETH-USD")}


def test_rows_with_missing_fields_are_skipped():
    assert LatestState().fold([("trade", {"venue": "kraken"}), ("quote", None)]) == {}
