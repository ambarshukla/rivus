using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using Prometheus;
using StackExchange.Redis;

namespace Rivus.Api;

// ------------------------------------------------------------- latest prices

public interface ILatestStore
{
    /// <summary>The current state of every venue and pair, as written by the latest-price writer.</summary>
    Task<IReadOnlyList<IReadOnlyDictionary<string, string>>> GetAllAsync(CancellationToken ct);
}

public sealed class RedisLatestStore(IConnectionMultiplexer redis) : ILatestStore
{
    public const string IndexKey = "latest:keys";
    public const string TickChannel = "rivus:ticks";

    public async Task<IReadOnlyList<IReadOnlyDictionary<string, string>>> GetAllAsync(CancellationToken ct)
    {
        var db = redis.GetDatabase();
        var keys = await db.SetMembersAsync(IndexKey);
        // Ask for all hashes at once; the replies come back together.
        var reads = keys.Select(key => db.HashGetAllAsync(key.ToString())).ToArray();
        var hashes = await Task.WhenAll(reads);
        return hashes
            .Where(hash => hash.Length > 0)
            .Select(hash => (IReadOnlyDictionary<string, string>)hash.ToDictionary(
                entry => entry.Name.ToString(), entry => entry.Value.ToString()))
            .OrderBy(row => row.GetValueOrDefault("pair"))
            .ThenBy(row => row.GetValueOrDefault("venue"))
            .ToList();
    }
}

// ------------------------------------------------------------------ SignalR

/// <summary>
/// The hub browsers connect to. It has no methods of its own: clients only
/// listen, and <see cref="TickRelay"/> sends them "tick" messages.
/// </summary>
public sealed class PricesHub : Hub;

/// <summary>
/// Listens to the Redis channel the latest-price writer publishes on and
/// passes every message to all connected browsers.
/// </summary>
public sealed class TickRelay(
    IConnectionMultiplexer redis, IHubContext<PricesHub> hub, ILogger<TickRelay> log)
    : BackgroundService
{
    private static readonly Counter Relayed =
        Metrics.CreateCounter("rivus_api_ticks_relayed_total", "Ticks pushed to browsers");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = RedisChannel.Literal(RedisLatestStore.TickChannel);
        var queue = await redis.GetSubscriber().SubscribeAsync(channel);
        log.LogInformation("relaying {Channel} to SignalR clients", RedisLatestStore.TickChannel);
        try
        {
            await foreach (var message in queue.WithCancellation(stoppingToken))
            {
                try
                {
                    // Parse so that browsers receive an object, not a string of JSON.
                    using var tick = JsonDocument.Parse(message.Message.ToString());
                    await hub.Clients.All.SendAsync("tick", tick.RootElement.Clone(), stoppingToken);
                    Relayed.Inc();
                }
                catch (JsonException)
                {
                    // Not ours; ignore.
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }
}

// --------------------------------------------------------------- statistics

/// <summary>
/// Every 15 seconds, asks ClickHouse for the health figures and publishes them
/// as Prometheus gauges named rivus_&lt;metric&gt;.
/// </summary>
public sealed class StatsCollector(IServiceScopeFactory scopes, ILogger<StatsCollector> log)
    : BackgroundService
{
    private static readonly TimeSpan Every = TimeSpan.FromSeconds(15);
    private readonly Dictionary<string, Gauge> _gauges = [];
    private readonly Dictionary<string, HashSet<string>> _seen = [];

    private static readonly Gauge Up = Metrics.CreateGauge(
        "rivus_stats_up", "1 if the last statistics query succeeded");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Every);
        do
        {
            try
            {
                using var scope = scopes.CreateScope();
                var store = scope.ServiceProvider.GetRequiredService<IHistoryStore>();
                Publish(await store.GetStatsAsync(stoppingToken));
                Up.Set(1);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Up.Set(0);
                log.LogWarning("statistics query failed: {Message}", ex.Message);
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    public void Publish(IReadOnlyList<Stat> stats)
    {
        // A count that drops to nothing produces no row at all, so first set
        // every count seen before back to zero. Ages are left alone: an age
        // with no row means "unknown", not "zero seconds old".
        foreach (var (metric, labels) in _seen)
        {
            if (metric.Contains("age") || metric.Contains("delay") || metric.Contains("spread")) continue;
            foreach (var label in labels) _gauges[metric].WithLabels(label).Set(0);
        }
        foreach (var stat in stats)
        {
            if (!_gauges.TryGetValue(stat.Metric, out var gauge))
            {
                var labelName = stat.Metric.StartsWith("venue_spread") ? "pair" : "venue";
                gauge = Metrics.CreateGauge($"rivus_{stat.Metric}", $"Stream health: {stat.Metric}", labelName);
                _gauges[stat.Metric] = gauge;
                _seen[stat.Metric] = [];
            }
            _seen[stat.Metric].Add(stat.Label);
            gauge.WithLabels(stat.Label).Set(stat.Value);
        }
    }
}
