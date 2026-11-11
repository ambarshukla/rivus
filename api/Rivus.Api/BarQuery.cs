using System.Text.RegularExpressions;

namespace Rivus.Api;

/// <summary>
/// A checked request for bars. Everything that reaches the database has passed
/// through here, so no raw text from the URL is ever put into SQL.
/// </summary>
public sealed partial record BarQuery(string Pair, string Venue, string Interval, int Minutes)
{
    public const int MaxMinutesForSeconds = 30;
    public const int MaxMinutesForMinutes = 24 * 60;

    /// <summary>The table for this interval. A fixed choice of two, never user text.</summary>
    public string Table => Interval == "1s" ? "rivus.bars_1s" : "rivus.bars_1m";

    [GeneratedRegex("^[A-Z0-9]{2,10}-[A-Z0-9]{2,10}$")]
    private static partial Regex PairPattern();

    [GeneratedRegex("^[a-z0-9]{1,20}$")]
    private static partial Regex VenuePattern();

    public static bool IsPair(string? value) => value is not null && PairPattern().IsMatch(value);

    public static bool IsVenue(string? value) => value is not null && VenuePattern().IsMatch(value);

    public static bool TryCreate(
        string? pair, string? venue, string? interval, int? minutes,
        out BarQuery query, out string error)
    {
        query = new BarQuery("", "", "1m", 0);
        interval ??= "1m";
        if (!IsPair(pair))
        {
            error = "pair is required and looks like BTC-USD";
            return false;
        }
        if (!string.IsNullOrEmpty(venue) && !IsVenue(venue))
        {
            error = "venue is lower-case letters and digits, such as kraken";
            return false;
        }
        if (interval is not ("1s" or "1m"))
        {
            error = "interval is 1s or 1m";
            return false;
        }
        var limit = interval == "1s" ? MaxMinutesForSeconds : MaxMinutesForMinutes;
        var span = minutes ?? (interval == "1s" ? 5 : 60);
        if (span < 1 || span > limit)
        {
            error = $"minutes is between 1 and {limit} for {interval} bars";
            return false;
        }
        query = new BarQuery(pair!, venue ?? "", interval, span);
        error = "";
        return true;
    }
}
