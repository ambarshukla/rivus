"""Writes dashboards/rivus.json. The dashboard is described here as a short list
of panels; edit this file and run it again rather than editing the JSON.

    docker run --rm -v "$PWD/monitoring/grafana:/g" -w /g python:3.13-slim python make_dashboard.py
"""

import json

DS = {"type": "prometheus", "uid": "prometheus"}

# (title, unit, [(query, legend)], description)
ROWS = [
    ("Collect", [
        ("Messages received per second", "ops",
         [("sum by (venue, kind) (rate(rivus_collector_messages_total[1m]))", "{{venue}} {{kind}}")],
         "Raw messages arriving from each venue, by kind."),
        ("Seconds since the last trade message", "s",
         [('time() - rivus_collector_last_message_timestamp_seconds{kind="trades"}', "{{venue}}")],
         "A rising line means a venue has gone quiet or a collector is stuck."),
        ("Collector sessions ended, per hour", "short",
         [("sum by (venue, reason) (increase(rivus_collector_disconnects_total[1h]))", "{{venue}} {{reason}}")],
         "Each ended session is a reconnect, and usually a gap."),
    ]),
    ("Process", [
        ("Consumer lag (messages behind)", "short",
         [("sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}")],
         "How far each reader is behind the end of its topics. Flink's groups only "
         "report at each checkpoint, so they move in 30-second steps."),
        ("Delay from window end to stored bar", "s",
         [("rivus_bar_delay_seconds", "{{venue}}")],
         "Average over the last 5 minutes for 1-second bars."),
        ("Flink job restarts and checkpoint duration", "short",
         [("flink_jobmanager_job_numRestarts", "restarts {{job_name}}"),
          ("flink_jobmanager_job_lastCheckpointDuration / 1000", "checkpoint seconds {{job_name}}")],
         "Restarts should stay flat."),
    ]),
    ("Checks", [
        ("Sequence gaps in the last hour", "short",
         [("rivus_gaps_last_hour", "gaps {{venue}}"),
          ("rivus_missing_trades_last_hour", "trades missed {{venue}}")],
         "Jumps in trade ids on Coinbase and Kraken."),
        ("Late trades in the last hour", "short",
         [("rivus_late_trades_last_hour", "{{venue}}")],
         "Trades that arrived behind the watermark."),
        ("Venues apart (basis points)", "short",
         [("rivus_venue_spread_bps", "{{pair}}")],
         "Highest minus lowest 1-minute average price across venues."),
    ]),
    ("Reconcile", [
        ("Bars that differ from the batch rebuild, last 24 hours", "short",
         [("rivus_recon_differences_last_day", "{{venue}}")],
         "Differences found by the hourly reconciliation."),
        ("Age of the newest reconciled hour", "s",
         [("rivus_recon_last_hour_age_seconds", "{{venue}}")],
         "Should stay under two hours."),
        ("Trades stored per minute", "short",
         [("rivus_trades_last_minute", "{{venue}}")],
         "Normalised trades reaching ClickHouse."),
    ]),
]


def build():
    panels, panel_id, y = [], 1, 0
    for row_title, row_panels in ROWS:
        panels.append({"id": panel_id, "type": "row", "title": row_title, "collapsed": False,
                       "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}})
        panel_id += 1
        y += 1
        for i, (title, unit, queries, description) in enumerate(row_panels):
            panels.append({
                "id": panel_id, "type": "timeseries", "title": title, "description": description,
                "datasource": DS, "gridPos": {"h": 8, "w": 8, "x": 8 * i, "y": y},
                "fieldConfig": {"defaults": {"unit": unit, "min": 0,
                                             "custom": {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never"}},
                                "overrides": []},
                "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                            "tooltip": {"mode": "multi", "sort": "desc"}},
                "targets": [{"datasource": DS, "expr": expr, "legendFormat": legend, "refId": chr(65 + n)}
                            for n, (expr, legend) in enumerate(queries)],
            })
            panel_id += 1
        y += 8
    return {"uid": "rivus", "title": "rivus", "tags": ["rivus"], "timezone": "utc",
            "schemaVersion": 39, "version": 1, "refresh": "15s", "editable": False,
            "time": {"from": "now-1h", "to": "now"}, "panels": panels}


if __name__ == "__main__":
    with open("dashboards/rivus.json", "w", encoding="utf-8", newline="\n") as handle:
        json.dump(build(), handle, indent=2)
        handle.write("\n")
    print("wrote dashboards/rivus.json")
