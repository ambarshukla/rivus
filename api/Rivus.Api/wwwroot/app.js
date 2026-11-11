// The page. Latest prices arrive over SignalR as they change; everything else
// is fetched from the REST API every few seconds.
"use strict";

const VENUES = ["coinbase", "kraken", "bitstamp"]; // fixed order = fixed colours
const $ = (selector) => document.querySelector(selector);
const pairSelect = $("#pair");
const latest = new Map(); // "venue|pair" -> row from Redis

const price = (v) => v == null || v === "" ? "–"
  : Number(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const whole = (v) => v == null ? "–" : Number(v).toLocaleString("en-US", { maximumFractionDigits: 0 });
const hhmm = (iso) => new Date(iso).toISOString().slice(11, 16);
const hhmmss = (iso) => new Date(iso).toISOString().slice(11, 19);
const colour = (venue) => `var(--${venue})`;
const swatch = (venue) => `<span class="swatch" style="background:${colour(venue)}"></span>${venue}`;

async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  return response.json();
}

function fillTable(selector, rows, toCells, emptyText) {
  const body = $(`${selector} tbody`);
  const columns = $(`${selector} thead tr`).children.length;
  body.innerHTML = rows.length
    ? rows.map((row) => `<tr>${toCells(row).join("")}</tr>`).join("")
    : `<tr><td class="empty" colspan="${columns}">${emptyText}</td></tr>`;
}
const td = (text, cls = "") => `<td class="${cls}">${text}</td>`;

// ------------------------------------------------------------ latest prices

function renderLatest() {
  const pair = pairSelect.value;
  const rows = VENUES.map((venue) => latest.get(`${venue}|${pair}`)).filter(Boolean);
  fillTable("#latest", rows, (r) => {
    // Times from the stream are UTC without a zone marker: "2026-11-11 13:04:39.804".
    const traded = r.last_trade_time ? r.last_trade_time.slice(11, 19) : "–";
    const spread = r.bid && r.ask ? price(Number(r.ask) - Number(r.bid)) : "–";
    return [td(swatch(r.venue)), td(price(r.last_price), "num"), td(price(r.bid), "num"),
      td(price(r.ask), "num"), td(spread, "num"), td(traded, "num")];
  }, "waiting for prices");
}

function onTick(row) {
  latest.set(`${row.venue}|${row.pair}`, row);
  if (row.pair === pairSelect.value) scheduleLatest();
}

// Many ticks a second: redraw the table at most once per animation frame.
let latestQueued = false;
function scheduleLatest() {
  if (latestQueued) return;
  latestQueued = true;
  requestAnimationFrame(() => { latestQueued = false; renderLatest(); });
}

function connect() {
  const pill = $("#connection");
  const show = (text, cls) => { pill.textContent = text; pill.className = `pill ${cls}`; };
  const hub = new signalR.HubConnectionBuilder().withUrl("/hubs/prices").withAutomaticReconnect().build();
  hub.on("tick", onTick);
  hub.onreconnecting(() => show("reconnecting", "down"));
  hub.onreconnected(() => show("live", "live"));
  hub.onclose(() => { show("disconnected", "down"); setTimeout(start, 5000); });
  const start = () => hub.start().then(() => show("live", "live"))
    .catch(() => { show("disconnected", "down"); setTimeout(start, 5000); });
  start();
}

// -------------------------------------------------------------------- chart

function renderChart(bars) {
  const box = $("#chart");
  const W = Math.max(box.clientWidth, 320), H = 280;
  const m = { top: 12, right: 78, bottom: 24, left: 62 };
  const series = VENUES.map((venue) => ({
    venue, points: bars.filter((b) => b.venue === venue).map((b) => ({ t: Date.parse(b.windowStart), v: b.close })),
  })).filter((s) => s.points.length);
  $("#legend").innerHTML = series.map((s) => `<span>${swatch(s.venue)}</span>`).join("");
  if (!series.length) { box.innerHTML = `<p class="muted">no bars yet for this pair</p>`; return; }

  const all = series.flatMap((s) => s.points);
  const t0 = Math.min(...all.map((p) => p.t)), t1 = Math.max(...all.map((p) => p.t));
  let lo = Math.min(...all.map((p) => p.v)), hi = Math.max(...all.map((p) => p.v));
  const pad = (hi - lo || hi * 0.001) * 0.08; lo -= pad; hi += pad;
  const x = (t) => m.left + (t1 === t0 ? 0.5 : (t - t0) / (t1 - t0)) * (W - m.left - m.right);
  const y = (v) => m.top + (1 - (v - lo) / (hi - lo)) * (H - m.top - m.bottom);

  let svg = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Closing price per venue over the last hour">`;
  for (let i = 0; i <= 4; i++) { // horizontal grid and price labels
    const v = lo + (hi - lo) * i / 4, yy = y(v);
    svg += `<line x1="${m.left}" x2="${W - m.right}" y1="${yy}" y2="${yy}" stroke="var(--grid)"/>`
      + `<text x="${m.left - 8}" y="${yy + 4}" text-anchor="end">${price(v)}</text>`;
  }
  for (let i = 0; i <= 4; i++) { // time labels
    const t = t0 + (t1 - t0) * i / 4;
    svg += `<text x="${x(t)}" y="${H - 6}" text-anchor="middle">${hhmm(t)}</text>`;
  }
  // Labels at the right-hand end of each line, nudged apart so they do not overlap.
  const ends = series.map((s) => ({ venue: s.venue, y: y(s.points.at(-1).v) })).sort((a, b) => a.y - b.y);
  for (let i = 1; i < ends.length; i++) ends[i].y = Math.max(ends[i].y, ends[i - 1].y + 13);
  for (const s of series) {
    const path = s.points.map((p, i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join("");
    svg += `<path d="${path}" fill="none" stroke="${colour(s.venue)}" stroke-width="2" stroke-linejoin="round"/>`;
    const end = ends.find((e) => e.venue === s.venue);
    svg += `<text class="label" x="${W - m.right + 8}" y="${end.y + 4}">${s.venue}</text>`;
  }
  svg += `<line id="cross" y1="${m.top}" y2="${H - m.bottom}" stroke="var(--axis)" visibility="hidden"/>`
    + `<rect id="hit" x="${m.left}" y="${m.top}" width="${W - m.left - m.right}" height="${H - m.top - m.bottom}" fill="transparent"/></svg>`;
  box.innerHTML = svg;

  // Hover: a vertical line at the nearest minute and each venue's value there.
  const times = [...new Set(all.map((p) => p.t))].sort((a, b) => a - b);
  const tip = $("#tip"), cross = box.querySelector("#cross"), hit = box.querySelector("#hit");
  hit.addEventListener("mousemove", (event) => {
    const scale = W / box.querySelector("svg").getBoundingClientRect().width;
    const px = (event.clientX - box.getBoundingClientRect().left) * scale;
    const t = times.reduce((best, c) => Math.abs(x(c) - px) < Math.abs(x(best) - px) ? c : best);
    cross.setAttribute("x1", x(t)); cross.setAttribute("x2", x(t)); cross.setAttribute("visibility", "visible");
    const lines = series.map((s) => {
      const p = s.points.find((q) => q.t === t);
      return `<div>${swatch(s.venue)} <b>${p ? price(p.v) : "no trades"}</b></div>`;
    });
    tip.innerHTML = `<div class="muted">${hhmm(t)} UTC</div>${lines.join("")}`;
    tip.hidden = false;
    tip.style.left = `${Math.min(event.clientX + 14, window.innerWidth - 190)}px`;
    tip.style.top = `${event.clientY + 14}px`;
  });
  hit.addEventListener("mouseleave", () => { tip.hidden = true; cross.setAttribute("visibility", "hidden"); });
}

// ----------------------------------------------------------- polled tables

function renderHealth(stats) {
  const get = (metric, venue) => stats.find((s) => s.metric === metric && s.label === venue)?.value;
  const seconds = (v) => v == null ? "–" : `${v.toFixed(1)} s`;
  const count = (v, bad) => td(whole(v ?? 0), `num${bad && v > 0 ? " warn" : ""}`);
  fillTable("#health", VENUES, (venue) => [
    td(swatch(venue)),
    td(whole(get("trades_last_minute", venue) ?? 0), "num"),
    td(seconds(get("last_trade_age_seconds", venue)) + " ago", "num"),
    td(seconds(get("bar_delay_seconds", venue)), "num"),
    count(get("gaps_last_hour", venue), true),
    count(get("missing_trades_last_hour", venue), true),
    count(get("late_trades_last_hour", venue), true),
  ], "");
}

async function refresh() {
  const pair = pairSelect.value;
  const jobs = [
    getJson(`/api/bars?pair=${pair}&interval=1m&minutes=60`).then(renderChart),
    getJson("/api/stats").then(renderHealth),
    getJson(`/api/venue-checks?pair=${pair}&minutes=10`).then((rows) =>
      fillTable("#checks", rows, (r) => [td(hhmm(r.windowStart)), td(r.venues, "num"), td(price(r.minVwap), "num"),
        td(price(r.maxVwap), "num"), td(r.spreadBps.toFixed(2), `num${r.spreadBps > 10 ? " warn" : ""}`)],
      "no completed minutes yet")),
    getJson("/api/gaps?hours=24").then((rows) =>
      fillTable("#gaps", rows.slice(0, 10), (r) => [td(hhmmss(r.eventTime)), td(swatch(r.venue)), td(r.pair),
        td(whole(r.missing), "num warn")], "none")),
    getJson("/api/reconciliation/runs?hours=24").then((rows) =>
      fillTable("#recon", rows.slice(0, 12), (r) => [td(hhmm(r.hourStart)), td(swatch(r.venue)), td(r.pair),
        td(r.streamBars, "num"), td(r.batchBars, "num"), td(r.matched, "num"),
        td(r.differences, `num${r.differences > 0 ? " warn" : ""}`)], "no hour reconciled yet")),
    getJson("/api/reconciliation/differences?hours=24").then((rows) =>
      fillTable("#diffs", rows.slice(0, 20), (r) => [td(hhmm(r.windowStart)), td(swatch(r.venue)), td(r.pair),
        td(r.field), td(r.streamValue, "num"), td(r.batchValue, "num"), td(r.classification)], "none")),
  ];
  await Promise.allSettled(jobs); // one failing section must not blank the others
}

// --------------------------------------------------------------------- start

pairSelect.addEventListener("change", () => { renderLatest(); refresh(); });
window.addEventListener("resize", () => refresh());
getJson("/api/latest").then((rows) => { rows.forEach((r) => latest.set(`${r.venue}|${r.pair}`, r)); renderLatest(); })
  .catch(() => renderLatest());
connect();
refresh();
setInterval(refresh, 5000);
