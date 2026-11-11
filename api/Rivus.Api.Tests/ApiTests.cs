using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Rivus.Api;
using Xunit;

namespace Rivus.Api.Tests;

/// <summary>Starts the real application in memory with fake stores behind it.</summary>
public sealed class ApiFactory : WebApplicationFactory<Program>
{
    public FakeHistory History { get; } = new();
    public FakeLatest Latest { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("Rivus:BackgroundServices", "false");
        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IHistoryStore>();
            services.RemoveAll<ILatestStore>();
            services.AddSingleton<IHistoryStore>(History);
            services.AddSingleton<ILatestStore>(Latest);
        });
    }
}

public sealed class FakeHistory : IHistoryStore
{
    public BarQuery? LastBarQuery { get; private set; }
    public int LastHours { get; private set; }
    public bool Fail { get; set; }

    private static readonly DateTimeOffset T = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private void Check()
    {
        if (Fail) throw new HistoryStoreException("down");
    }

    public Task<IReadOnlyList<Bar>> GetBarsAsync(BarQuery query, CancellationToken ct)
    {
        Check();
        LastBarQuery = query;
        return Task.FromResult<IReadOnlyList<Bar>>(
            [new Bar("kraken", query.Pair, T, 100m, 102m, 99m, 101m, 8m, 100.125m, 4)]);
    }

    public Task<IReadOnlyList<Gap>> GetGapsAsync(int hours, CancellationToken ct)
    {
        Check();
        LastHours = hours;
        return Task.FromResult<IReadOnlyList<Gap>>([new Gap("coinbase", "BTC-USD", 102, 105, 2, T, T)]);
    }

    public Task<IReadOnlyList<LateSummary>> GetLateSummaryAsync(int hours, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<LateSummary>>([]);

    public Task<IReadOnlyList<VenueCheck>> GetVenueChecksAsync(string pair, int minutes, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<VenueCheck>>([new VenueCheck(pair, T, 3, 100m, 101m, 87.3908m, "x")]);

    public Task<IReadOnlyList<Stat>> GetStatsAsync(CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<Stat>>([new Stat("trades_last_minute", "kraken", 42)]);

    public Task<IReadOnlyList<ReconRun>> GetReconRunsAsync(int hours, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<ReconRun>>([]);

    public Task<IReadOnlyList<ReconDiff>> GetReconDiffsAsync(int hours, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<ReconDiff>>([]);
}

public sealed class FakeLatest : ILatestStore
{
    public Task<IReadOnlyList<IReadOnlyDictionary<string, string>>> GetAllAsync(CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<IReadOnlyDictionary<string, string>>>(
            [new Dictionary<string, string> { ["venue"] = "kraken", ["pair"] = "BTC-USD", ["last_price"] = "100.5" }]);
}

public sealed class ApiTests(ApiFactory factory) : IClassFixture<ApiFactory>
{
    private readonly HttpClient _client = factory.CreateClient();

    [Fact]
    public async Task Bars_returns_rows_and_passes_the_checked_query_on()
    {
        var bars = await _client.GetFromJsonAsync<JsonElement>("/api/bars?pair=BTC-USD&venue=kraken&interval=1s&minutes=10");

        Assert.Equal(1, bars.GetArrayLength());
        Assert.Equal("kraken", bars[0].GetProperty("venue").GetString());
        Assert.Equal(100.125m, bars[0].GetProperty("vwap").GetDecimal());
        Assert.Equal(new BarQuery("BTC-USD", "kraken", "1s", 10), factory.History.LastBarQuery);
    }

    [Theory]
    [InlineData("/api/bars")]
    [InlineData("/api/bars?pair=btc-usd")]
    [InlineData("/api/bars?pair=BTC-USD&interval=5m")]
    [InlineData("/api/bars?pair=BTC-USD&interval=1s&minutes=31")]
    [InlineData("/api/bars?pair=BTC-USD&venue=kraken';DROP")]
    [InlineData("/api/venue-checks")]
    public async Task Bad_requests_are_rejected_with_a_reason(string url)
    {
        var response = await _client.GetAsync(url);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(string.IsNullOrEmpty(body.GetProperty("error").GetString()));
    }

    [Fact]
    public async Task Hours_are_clamped_to_a_sane_range()
    {
        await _client.GetAsync("/api/gaps?hours=999999");
        Assert.Equal(24 * 30, factory.History.LastHours);

        await _client.GetAsync("/api/gaps?hours=-5");
        Assert.Equal(1, factory.History.LastHours);
    }

    [Fact]
    public async Task Latest_returns_the_redis_rows_unchanged()
    {
        var rows = await _client.GetFromJsonAsync<JsonElement>("/api/latest");

        Assert.Equal("100.5", rows[0].GetProperty("last_price").GetString());
    }

    [Theory]
    [InlineData("/api/gaps")]
    [InlineData("/api/late-trades")]
    [InlineData("/api/venue-checks?pair=ETH-USD")]
    [InlineData("/api/stats")]
    [InlineData("/api/reconciliation/runs")]
    [InlineData("/api/reconciliation/differences")]
    [InlineData("/healthz")]
    [InlineData("/metrics")]
    public async Task Endpoints_answer(string url)
    {
        var response = await _client.GetAsync(url);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task The_page_and_its_scripts_are_served()
    {
        var page = await _client.GetStringAsync("/");
        Assert.Contains("<title>rivus</title>", page);

        foreach (var file in new[] { "/app.js", "/style.css", "/lib/signalr.min.js" })
        {
            Assert.Equal(HttpStatusCode.OK, (await _client.GetAsync(file)).StatusCode);
        }
    }

    [Fact]
    public async Task The_signalr_hub_accepts_connections()
    {
        var response = await _client.PostAsync("/hubs/prices/negotiate?negotiateVersion=1", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("connectionId", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task A_store_failure_is_a_503_not_a_stack_trace()
    {
        factory.History.Fail = true;
        try
        {
            var response = await _client.GetAsync("/api/gaps");

            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            Assert.DoesNotContain("Exception", await response.Content.ReadAsStringAsync());
        }
        finally
        {
            factory.History.Fail = false;
        }
    }
}
