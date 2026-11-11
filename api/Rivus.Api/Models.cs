namespace Rivus.Api;

// Settings, read from the "Rivus" section of configuration. In Docker they come
// from environment variables such as Rivus__ClickHouseUrl.
public sealed class RivusSettings
{
    public string ClickHouseUrl { get; set; } = "http://clickhouse:8123";
    public string ClickHouseUser { get; set; } = "rivus";
    public string ClickHousePassword { get; set; } = "rivus";
    public string Redis { get; set; } = "redis:6379";
    /// <summary>False in tests: no Redis subscription, no periodic statistics.</summary>
    public bool BackgroundServices { get; set; } = true;
}

// One record per kind of row the API returns. Property names map to ClickHouse
// column names by turning PascalCase into snake_case (WindowStart = window_start).

public sealed record Bar(
    string Venue, string Pair, DateTimeOffset WindowStart,
    decimal Open, decimal High, decimal Low, decimal Close,
    decimal Volume, decimal Vwap, long Trades);

public sealed record Gap(
    string Venue, string Pair, long PrevTradeId, long TradeId, long Missing,
    DateTimeOffset EventTime, DateTimeOffset DetectedAt);

public sealed record LateSummary(
    string Venue, string Pair, long Late, long MissedSecondBar, long MissedMinuteBar,
    double MaxSecondsLate);

public sealed record VenueCheck(
    string Pair, DateTimeOffset WindowStart, long Venues,
    decimal MinVwap, decimal MaxVwap, decimal SpreadBps, string Detail);

/// <summary>One measurement: a name, what it is about (a venue or a pair), a number.</summary>
public sealed record Stat(string Metric, string Label, double Value);

public sealed record ReconRun(
    DateTimeOffset HourStart, string Venue, string Pair, long StreamBars, long BatchBars,
    long Matched, long Differences, DateTimeOffset CheckedAt);

public sealed record ReconDiff(
    DateTimeOffset HourStart, string Venue, string Pair, DateTimeOffset WindowStart,
    string Field, string StreamValue, string BatchValue, string Classification);
