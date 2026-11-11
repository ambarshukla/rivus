using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;
using Rivus.Api;
using Xunit;

namespace Rivus.Api.Tests;

public sealed class BarQueryTests
{
    [Fact]
    public void Defaults_are_one_minute_bars_for_an_hour_across_all_venues()
    {
        Assert.True(BarQuery.TryCreate("ETH-USD", null, null, null, out var query, out _));
        Assert.Equal(new BarQuery("ETH-USD", "", "1m", 60), query);
        Assert.Equal("rivus.bars_1m", query.Table);
    }

    [Fact]
    public void One_second_bars_use_their_own_table_and_a_shorter_default()
    {
        Assert.True(BarQuery.TryCreate("ETH-USD", "kraken", "1s", null, out var query, out _));
        Assert.Equal("rivus.bars_1s", query.Table);
        Assert.Equal(5, query.Minutes);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("BTCUSD")]
    [InlineData("BTC-USD ")]
    [InlineData("BTC-USD' OR 1=1")]
    public void Malformed_pairs_are_refused(string? pair)
    {
        Assert.False(BarQuery.TryCreate(pair, null, null, null, out _, out var error));
        Assert.Contains("pair", error);
    }

    [Theory]
    [InlineData("1m", 0)]
    [InlineData("1m", 1441)]
    [InlineData("1s", 31)]
    public void Spans_outside_the_limits_are_refused(string interval, int minutes)
    {
        Assert.False(BarQuery.TryCreate("BTC-USD", null, interval, minutes, out _, out var error));
        Assert.Contains("minutes", error);
    }
}

public sealed class ClickHouseStoreTests
{
    /// <summary>Stands in for ClickHouse: records the request and replies with fixed text.</summary>
    private sealed class StubHandler(HttpStatusCode status, string body) : HttpMessageHandler
    {
        public HttpRequestMessage? Request { get; private set; }
        public string Sql { get; private set; } = "";

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Request = request;
            Sql = await request.Content!.ReadAsStringAsync(ct);
            return new HttpResponseMessage(status) { Content = new StringContent(body, Encoding.UTF8) };
        }
    }

    private static ClickHouseStore Store(StubHandler handler) =>
        new(new HttpClient(handler), new RivusSettings { ClickHouseUrl = "http://ch:8123" });

    [Fact]
    public async Task Rows_are_read_from_json_lines_with_exact_decimals()
    {
        // The shape ClickHouse returns with the output settings the store asks for.
        var handler = new StubHandler(HttpStatusCode.OK, """
            {"venue":"kraken","pair":"BTC-USD","window_start":"2026-11-11T13:08:00.000Z","open":73778.4,"high":73791.02930800,"low":73700,"close":73750.1,"volume":12.34567891,"vwap":73766.39764800,"trades":18446744073}

            {"venue":"coinbase","pair":"BTC-USD","window_start":"2026-11-11T13:09:00.000Z","open":1,"high":2,"low":0.5,"close":1.5,"volume":3,"vwap":1.25,"trades":2}
            """);

        Assert.True(BarQuery.TryCreate("BTC-USD", "kraken", "1m", 15, out var query, out _));
        var bars = await Store(handler).GetBarsAsync(query, CancellationToken.None);

        Assert.Equal(2, bars.Count);
        Assert.Equal(73791.02930800m, bars[0].High);
        Assert.Equal(12.34567891m, bars[0].Volume);
        Assert.Equal(18446744073L, bars[0].Trades);
        Assert.Equal(new DateTimeOffset(2026, 11, 11, 13, 8, 0, TimeSpan.Zero), bars[0].WindowStart);
    }

    [Fact]
    public async Task Values_travel_as_parameters_never_inside_the_sql()
    {
        var handler = new StubHandler(HttpStatusCode.OK, "");

        Assert.True(BarQuery.TryCreate("BTC-USD", "kraken", "1s", 7, out var query, out _));
        await Store(handler).GetBarsAsync(query, CancellationToken.None);

        var url = handler.Request!.RequestUri!.ToString();
        Assert.Contains("param_pair=BTC-USD", url);
        Assert.Contains("param_venue=kraken", url);
        Assert.Contains("param_minutes=7", url);
        Assert.Contains("FROM rivus.bars_1s FINAL", handler.Sql);
        Assert.Contains("{pair:String}", handler.Sql);
        Assert.DoesNotContain("BTC-USD", handler.Sql);
        Assert.DoesNotContain("kraken", handler.Sql);
        Assert.EndsWith("FORMAT JSONEachRow", handler.Sql);
        Assert.Equal("rivus", handler.Request.Headers.GetValues("X-ClickHouse-User").Single());
    }

    [Fact]
    public async Task A_database_error_becomes_a_store_exception()
    {
        var handler = new StubHandler(HttpStatusCode.InternalServerError, "Code: 60. DB::Exception: Unknown table");

        var error = await Assert.ThrowsAsync<HistoryStoreException>(
            () => Store(handler).GetGapsAsync(24, CancellationToken.None));
        Assert.Contains("Unknown table", error.Message);
    }
}

public sealed class StatsCollectorTests
{
    private static async Task<string> Scrape()
    {
        using var stream = new MemoryStream();
        await Metrics.DefaultRegistry.CollectAndExportAsTextAsync(stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    [Fact]
    public async Task Counts_fall_back_to_zero_when_their_row_disappears_but_ages_do_not()
    {
        var collector = new StatsCollector(null!, NullLogger<StatsCollector>.Instance);

        collector.Publish([
            new Stat("gaps_last_hour", "testvenue", 3),
            new Stat("last_trade_age_seconds", "testvenue", 1.5),
            new Stat("venue_spread_bps", "TST-USD", 2.25),
        ]);
        var first = await Scrape();
        Assert.Contains("rivus_gaps_last_hour{venue=\"testvenue\"} 3", first);
        Assert.Contains("rivus_venue_spread_bps{pair=\"TST-USD\"} 2.25", first);

        collector.Publish([]);
        var second = await Scrape();
        Assert.Contains("rivus_gaps_last_hour{venue=\"testvenue\"} 0", second);
        Assert.Contains("rivus_last_trade_age_seconds{venue=\"testvenue\"} 1.5", second);
    }
}
