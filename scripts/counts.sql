-- Row counts and time range per table and venue. Run with:
--   docker compose exec -T clickhouse clickhouse-client --user rivus --password rivus < scripts/counts.sql
SELECT 'trades' AS t, venue, count() AS n, min(event_time) AS lo, max(event_time) AS hi FROM rivus.trades FINAL GROUP BY venue
UNION ALL SELECT 'quotes', venue, count(), min(event_time), max(event_time) FROM rivus.quotes GROUP BY venue
UNION ALL SELECT 'bars_1s', venue, count(), min(window_start), max(window_start) FROM rivus.bars_1s FINAL GROUP BY venue
UNION ALL SELECT 'bars_1m', venue, count(), min(window_start), max(window_start) FROM rivus.bars_1m FINAL GROUP BY venue
UNION ALL SELECT 'late_trades', venue, count(), min(event_time), max(event_time) FROM rivus.late_trades FINAL GROUP BY venue
UNION ALL SELECT 'gaps', venue, count(), min(event_time), max(event_time) FROM rivus.gaps FINAL GROUP BY venue
UNION ALL SELECT 'venue_checks', pair, count(), min(window_start), max(window_start) FROM rivus.venue_checks FINAL GROUP BY pair
ORDER BY 1, 2
FORMAT PrettyCompactMonoBlock;
