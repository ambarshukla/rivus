using Prometheus;
using Rivus.Api;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var settings = builder.Configuration.GetSection("Rivus").Get<RivusSettings>() ?? new RivusSettings();
builder.Services.AddSingleton(settings);

// History comes from ClickHouse, the present from Redis. Both sit behind an
// interface so the tests can swap in fakes.
builder.Services.AddHttpClient<IHistoryStore, ClickHouseStore>(http => http.Timeout = TimeSpan.FromSeconds(20));
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var options = ConfigurationOptions.Parse(settings.Redis);
    options.AbortOnConnectFail = false; // start even if Redis is not up yet; it reconnects
    return ConnectionMultiplexer.Connect(options);
});
builder.Services.AddSingleton<ILatestStore, RedisLatestStore>();

builder.Services.AddSignalR();
if (settings.BackgroundServices)
{
    builder.Services.AddHostedService<TickRelay>();
    builder.Services.AddHostedService<StatsCollector>();
}

var app = builder.Build();

// A database problem becomes a 503 with a short message, not a stack trace.
app.Use(async (context, next) =>
{
    try
    {
        await next(context);
    }
    catch (Exception ex) when (ex is HistoryStoreException or HttpRequestException or RedisException)
    {
        app.Logger.LogWarning("store unavailable: {Message}", ex.Message);
        if (!context.Response.HasStarted)
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsJsonAsync(new { error = "a data store is unavailable" });
        }
    }
});

app.UseDefaultFiles(); // "/" serves wwwroot/index.html
app.UseStaticFiles();
app.UseHttpMetrics();  // request counts and timings for Prometheus

var api = app.MapGroup("/api");

api.MapGet("/latest", (ILatestStore latest, CancellationToken ct) => latest.GetAllAsync(ct));

api.MapGet("/bars", async (
    string? pair, string? venue, string? interval, int? minutes,
    IHistoryStore history, CancellationToken ct) =>
    BarQuery.TryCreate(pair, venue, interval, minutes, out var query, out var error)
        ? Results.Ok(await history.GetBarsAsync(query, ct))
        : Results.BadRequest(new { error }));

api.MapGet("/gaps", async (int? hours, IHistoryStore history, CancellationToken ct) =>
    Results.Ok(await history.GetGapsAsync(Hours(hours), ct)));

api.MapGet("/late-trades", async (int? hours, IHistoryStore history, CancellationToken ct) =>
    Results.Ok(await history.GetLateSummaryAsync(Hours(hours), ct)));

api.MapGet("/venue-checks", async (string? pair, int? minutes, IHistoryStore history, CancellationToken ct) =>
    BarQuery.IsPair(pair)
        ? Results.Ok(await history.GetVenueChecksAsync(pair!, Math.Clamp(minutes ?? 60, 1, 1440), ct))
        : Results.BadRequest(new { error = "pair is required and looks like BTC-USD" }));

api.MapGet("/stats", (IHistoryStore history, CancellationToken ct) => history.GetStatsAsync(ct));

api.MapGet("/reconciliation/runs", async (int? hours, IHistoryStore history, CancellationToken ct) =>
    Results.Ok(await history.GetReconRunsAsync(Hours(hours), ct)));

api.MapGet("/reconciliation/differences", async (int? hours, IHistoryStore history, CancellationToken ct) =>
    Results.Ok(await history.GetReconDiffsAsync(Hours(hours), ct)));

app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));
app.MapHub<PricesHub>("/hubs/prices");
app.MapMetrics(); // GET /metrics

app.Run();

static int Hours(int? hours) => Math.Clamp(hours ?? 24, 1, 24 * 30);

// Lets the test project start the application in memory.
public partial class Program;
