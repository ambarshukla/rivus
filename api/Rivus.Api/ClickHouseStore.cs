using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Rivus.Api;

public interface IHistoryStore
{
    Task<IReadOnlyList<Bar>> GetBarsAsync(BarQuery query, CancellationToken ct);
    Task<IReadOnlyList<Gap>> GetGapsAsync(int hours, CancellationToken ct);
    Task<IReadOnlyList<LateSummary>> GetLateSummaryAsync(int hours, CancellationToken ct);
    Task<IReadOnlyList<VenueCheck>> GetVenueChecksAsync(string pair, int minutes, CancellationToken ct);
    Task<IReadOnlyList<Stat>> GetStatsAsync(CancellationToken ct);
    Task<IReadOnlyList<ReconRun>> GetReconRunsAsync(int hours, CancellationToken ct);
    Task<IReadOnlyList<ReconDiff>> GetReconDiffsAsync(int hours, CancellationToken ct);
}

/// <summary>
/// Reads history from ClickHouse over its HTTP interface: the SQL is the body
/// of a POST and each result row comes back as one line of JSON.
///
/// Values from the caller never go into the SQL text. They are sent separately
/// as "param_name=value" and referred to in the SQL as {name:Type}; ClickHouse
/// puts them in safely.
/// </summary>
public sealed class ClickHouseStore(HttpClient http, RivusSettings settings) : IHistoryStore
{
    private static readonly JsonSerializerOptions RowJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    };

    // Dates as ISO text with a Z, and numbers as numbers rather than quoted strings.
    private const string OutputSettings =
        "date_time_output_format=iso&output_format_json_quote_64bit_integers=0" +
        "&output_format_json_quote_decimals=0";

    public Task<IReadOnlyList<Bar>> GetBarsAsync(BarQuery query, CancellationToken ct) =>
        QueryAsync<Bar>("""
            SELECT venue, pair, window_start, open, high, low, close, volume, vwap, trades
            FROM BARS_TABLE FINAL
            WHERE pair = {pair:String}
              AND ({venue:String} = '' OR venue = {venue:String})
              AND window_start >= now() - toIntervalMinute({minutes:UInt32})
            ORDER BY window_start, venue
            LIMIT 20000
            """.Replace("BARS_TABLE", query.Table),
            new() { ["pair"] = query.Pair, ["venue"] = query.Venue, ["minutes"] = query.Minutes.ToString() },
            ct);

    public Task<IReadOnlyList<Gap>> GetGapsAsync(int hours, CancellationToken ct) =>
        QueryAsync<Gap>("""
            SELECT venue, pair, prev_trade_id, trade_id, missing, event_time, detected_at
            FROM rivus.gaps FINAL
            WHERE event_time >= now() - toIntervalHour({hours:UInt32})
            ORDER BY event_time DESC
            LIMIT 200
            """, new() { ["hours"] = hours.ToString() }, ct);

    public Task<IReadOnlyList<LateSummary>> GetLateSummaryAsync(int hours, CancellationToken ct) =>
        QueryAsync<LateSummary>("""
            SELECT venue, pair, count() AS late,
                   countIf(missed_1s) AS missed_second_bar,
                   countIf(missed_1m) AS missed_minute_bar,
                   max(dateDiff('millisecond', event_time, watermark_time)) / 1000 AS max_seconds_late
            FROM rivus.late_trades FINAL
            WHERE event_time >= now() - toIntervalHour({hours:UInt32})
            GROUP BY venue, pair
            ORDER BY venue, pair
            """, new() { ["hours"] = hours.ToString() }, ct);

    public Task<IReadOnlyList<VenueCheck>> GetVenueChecksAsync(string pair, int minutes, CancellationToken ct) =>
        QueryAsync<VenueCheck>("""
            SELECT pair, window_start, venues, min_vwap, max_vwap, spread_bps, detail
            FROM rivus.venue_checks FINAL
            WHERE pair = {pair:String}
              AND window_start >= now() - toIntervalMinute({minutes:UInt32})
            ORDER BY window_start DESC
            LIMIT 1500
            """, new() { ["pair"] = pair, ["minutes"] = minutes.ToString() }, ct);

    /// <summary>
    /// One query that measures the health of the stream. It feeds both the
    /// Prometheus gauges and the status strip on the web page.
    /// </summary>
    public Task<IReadOnlyList<Stat>> GetStatsAsync(CancellationToken ct) =>
        QueryAsync<Stat>("""
            SELECT 'trades_last_minute' AS metric, venue AS label, toFloat64(count()) AS value
              FROM rivus.trades WHERE event_time > now() - INTERVAL 1 MINUTE GROUP BY venue
            UNION ALL
            SELECT 'last_trade_age_seconds', venue, dateDiff('millisecond', max(event_time), now64(3)) / 1000
              FROM rivus.trades WHERE event_time > now() - INTERVAL 1 DAY GROUP BY venue
            UNION ALL
            SELECT 'last_bar_age_seconds', venue, dateDiff('millisecond', max(window_end), now64(3)) / 1000
              FROM rivus.bars_1s WHERE window_start > now() - INTERVAL 1 DAY GROUP BY venue
            UNION ALL
            SELECT 'bar_delay_seconds', venue, avg(dateDiff('millisecond', window_end, stored_at)) / 1000
              FROM rivus.bars_1s WHERE window_start > now() - INTERVAL 5 MINUTE GROUP BY venue
            UNION ALL
            SELECT 'gaps_last_hour', venue, toFloat64(count())
              FROM rivus.gaps FINAL WHERE event_time > now() - INTERVAL 1 HOUR GROUP BY venue
            UNION ALL
            SELECT 'missing_trades_last_hour', venue, toFloat64(sum(missing))
              FROM rivus.gaps FINAL WHERE event_time > now() - INTERVAL 1 HOUR GROUP BY venue
            UNION ALL
            SELECT 'late_trades_last_hour', venue, toFloat64(count())
              FROM rivus.late_trades FINAL WHERE event_time > now() - INTERVAL 1 HOUR GROUP BY venue
            UNION ALL
            SELECT 'venue_spread_bps', pair, toFloat64(argMax(spread_bps, window_start))
              FROM rivus.venue_checks FINAL WHERE window_start > now() - INTERVAL 10 MINUTE GROUP BY pair
            UNION ALL
            SELECT 'recon_differences_last_day', venue, toFloat64(sum(differences))
              FROM rivus.recon_runs FINAL WHERE hour_start > now() - INTERVAL 1 DAY GROUP BY venue
            UNION ALL
            SELECT 'recon_last_hour_age_seconds', venue, toFloat64(dateDiff('second', max(hour_start), now()))
              FROM rivus.recon_runs FINAL GROUP BY venue
            """, [], ct);

    public Task<IReadOnlyList<ReconRun>> GetReconRunsAsync(int hours, CancellationToken ct) =>
        QueryAsync<ReconRun>("""
            SELECT hour_start, venue, pair, stream_bars, batch_bars, matched, differences, checked_at
            FROM rivus.recon_runs FINAL
            WHERE hour_start >= now() - toIntervalHour({hours:UInt32})
            ORDER BY hour_start DESC, venue, pair
            LIMIT 1000
            """, new() { ["hours"] = hours.ToString() }, ct);

    public Task<IReadOnlyList<ReconDiff>> GetReconDiffsAsync(int hours, CancellationToken ct) =>
        QueryAsync<ReconDiff>("""
            SELECT hour_start, venue, pair, window_start, field, stream_value, batch_value, classification
            FROM rivus.recon_diffs FINAL
            WHERE hour_start >= now() - toIntervalHour({hours:UInt32})
            ORDER BY window_start DESC, venue, pair, field
            LIMIT 500
            """, new() { ["hours"] = hours.ToString() }, ct);

    private async Task<IReadOnlyList<T>> QueryAsync<T>(
        string sql, Dictionary<string, string> parameters, CancellationToken ct)
    {
        var url = new StringBuilder($"{settings.ClickHouseUrl.TrimEnd('/')}/?{OutputSettings}");
        foreach (var (name, value) in parameters)
        {
            url.Append($"&param_{name}={Uri.EscapeDataString(value)}");
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, url.ToString());
        request.Headers.Add("X-ClickHouse-User", settings.ClickHouseUser);
        request.Headers.Add("X-ClickHouse-Key", settings.ClickHousePassword);
        request.Content = new StringContent(sql + "\nFORMAT JSONEachRow", Encoding.UTF8);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");

        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(ct);
            throw new HistoryStoreException(
                $"ClickHouse answered {(int)response.StatusCode}: {body[..Math.Min(body.Length, 300)]}");
        }

        var rows = new List<T>();
        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        using var reader = new StreamReader(stream);
        while (await reader.ReadLineAsync(ct) is { } line)
        {
            if (line.Length == 0) continue;
            rows.Add(JsonSerializer.Deserialize<T>(line, RowJson)
                     ?? throw new HistoryStoreException("ClickHouse returned an empty row"));
        }
        return rows;
    }
}

public sealed class HistoryStoreException(string message) : Exception(message);
