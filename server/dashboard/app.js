"use strict";

const $ = (selector) => document.querySelector(selector);
const state = {
  catalog: [], overview: null, snapshot: null,
  experimentId: null, runId: null,
  live: true, playing: false, playheadUs: 0, lastFrame: 0,
  speed: 1, refreshTimer: null, catalogTimer: null, loading: false,
};

const palette = ["#64c7ff", "#b69cff", "#c7f45b", "#ffb15f", "#f472b6", "#81e6d9"];

document.addEventListener("DOMContentLoaded", init);

async function init() {
  bindControls();
  updateClock();
  setInterval(updateClock, 1000);
  await refreshCatalog(true);
  state.catalogTimer = setInterval(() => refreshCatalog(false), 3000);
  requestAnimationFrame(tickReplay);
}

function bindControls() {
  $("#experiment-select").addEventListener("change", async (event) => {
    state.experimentId = event.target.value;
    state.runId = null;
    await loadExperiment();
  });
  $("#run-select").addEventListener("change", async (event) => {
    state.runId = event.target.value;
    state.live = false;
    state.playing = false;
    await loadSnapshot(true);
  });
  $("#refresh-button").addEventListener("click", () => refreshAll(true));
  $("#play-button").addEventListener("click", toggleReplay);
  $("#timeline").addEventListener("input", (event) => {
    if (!state.snapshot) return;
    state.live = false;
    state.playing = false;
    state.playheadUs = state.snapshot.timeline_duration_us * Number(event.target.value) / 1000;
    render();
  });
  $("#speed-select").addEventListener("change", (event) => state.speed = Number(event.target.value));
  $("#live-button").addEventListener("click", jumpToLive);
}

async function api(path) {
  const response = await fetch(path, { cache: "no-store" });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error?.message || `Request failed (${response.status})`);
  return body;
}

async function refreshCatalog(initial) {
  try {
    const data = await api("/api/experiments");
    state.catalog = data.experiments || [];
    renderExperimentSelect();
    if (!state.experimentId && state.catalog.length) {
      const active = state.catalog.find((item) => item.status === "running");
      state.experimentId = (active || state.catalog[0]).id;
      $("#experiment-select").value = state.experimentId;
      await loadExperiment();
    } else if (!initial && state.experimentId) {
      const current = state.catalog.find((item) => item.id === state.experimentId);
      if (current && state.overview) {
        state.overview.experiment = current;
        renderHeader();
      }
    } else if (initial && !state.catalog.length) {
      showEmpty("No experiments found in the configured results directory.");
    }
  } catch (error) {
    toast(error.message);
  }
}

async function loadExperiment() {
  if (!state.experimentId) return;
  setLoading(true);
  try {
    state.overview = await api(`/api/experiments/${encodeURIComponent(state.experimentId)}`);
    renderRunSelect();
    const runs = state.overview.runs || [];
    if (!state.runId || !runs.some((run) => run.run_id === state.runId)) {
      const active = runs.find((run) => run.status === "running");
      state.runId = (active || runs[runs.length - 1])?.run_id || null;
      state.live = Boolean(active);
    }
    $("#run-select").value = state.runId || "";
    renderHeader();
    if (state.runId) await loadSnapshot(true);
    scheduleSnapshotRefresh();
  } catch (error) {
    toast(error.message);
  } finally {
    setLoading(false);
  }
}

async function loadSnapshot(resetPosition = false) {
  if (!state.experimentId || !state.runId) return;
  setLoading(true);
  try {
    const data = await api(`/api/experiments/${encodeURIComponent(state.experimentId)}/runs/${encodeURIComponent(state.runId)}`);
    state.snapshot = data;
    if (resetPosition || state.live) state.playheadUs = data.timeline_duration_us || 0;
    if (resetPosition && !isSnapshotRunning(data)) state.live = false;
    render();
    scheduleSnapshotRefresh();
  } catch (error) {
    toast(error.message);
  } finally {
    setLoading(false);
  }
}

async function refreshAll(manual) {
  if (manual) setLoading(true);
  try {
    if (state.experimentId) {
      state.overview = await api(`/api/experiments/${encodeURIComponent(state.experimentId)}`);
      renderRunSelect();
    }
  } catch (error) {
    toast(error.message);
  } finally {
    if (manual) setLoading(false);
  }
  await loadSnapshot(false);
}

function scheduleSnapshotRefresh() {
  clearInterval(state.refreshTimer);
  const running = isSnapshotRunning(state.snapshot);
  if (running) state.refreshTimer = setInterval(() => loadSnapshot(false), 750);
}

function renderExperimentSelect() {
  const select = $("#experiment-select");
  const selection = state.experimentId;
  select.replaceChildren(...state.catalog.map((experiment) => {
    const option = document.createElement("option");
    option.value = experiment.id;
    option.textContent = `${experiment.status === "running" ? "● " : ""}${experiment.name} · ${formatDate(experiment.started_unix_ms)}`;
    return option;
  }));
  if (selection) select.value = selection;
}

function renderRunSelect() {
  const select = $("#run-select");
  const selection = state.runId;
  const runs = state.overview?.runs || [];
  select.replaceChildren(...runs.map((run) => {
    const option = document.createElement("option");
    option.value = run.run_id;
    option.textContent = `${statusGlyph(run.status)} ${run.algorithm || "unknown"} · ${run.runtime || "unknown"} · R${run.repetition || "?"}`;
    return option;
  }));
  if (selection) select.value = selection;
}

function render() {
  if (!state.snapshot) return;
  renderHeader();
  const duration = Math.max(1, state.snapshot.timeline_duration_us || 0);
  const cutoff = Math.min(state.playheadUs, duration);
  const requests = (state.snapshot.requests || []).filter((item) => requestCompletedUs(item) <= cutoff);
  const resources = (state.snapshot.resource_samples || []).filter((item) => resourceTimeUs(item) <= cutoff);
  const events = (state.snapshot.events || []).filter((item) => eventTimeUs(item) <= cutoff);
  const metrics = deriveMetrics(requests);
  const atLatest = cutoff >= duration - 1;
  if (atLatest && state.snapshot.analysis) applyScientificMetrics(metrics, state.snapshot.analysis);
  renderMetrics(metrics, requests);
  renderLatencyChart(requests, cutoff);
  renderTrafficChart(requests, cutoff);
  renderResources(resources, cutoff);
  renderBackends(requests);
  renderEvents(events);
  renderReplay(cutoff, duration);
}

function renderHeader() {
  const experiment = state.overview?.experiment;
  if (!experiment) return;
  $("#experiment-title").textContent = experiment.name;
  const total = experiment.planned_runs || 0;
  const done = (experiment.completed_runs || 0) + (experiment.failed_runs || 0);
  const run = state.snapshot?.metadata;
  $("#experiment-subtitle").textContent = run
    ? `${run.scenario?.description || run.scenario?.id || "Scenario"} · seed ${state.snapshot.run_seed || run.run_seed}`
    : `${experiment.discovered_runs} runs discovered`;
  $("#matrix-progress-label").textContent = `${done} / ${total || "—"}`;
  $("#matrix-progress-bar").style.width = `${total ? Math.min(100, done / total * 100) : 0}%`;
  const running = isSnapshotRunning(state.snapshot);
  const modeLabel = state.live && running ? "LIVE RUN" : "SAVED REPLAY";
  $("#mode-label").textContent = modeLabel;
  $(".eyebrow").classList.toggle("replay", !(state.live && running));
  $("#mode-dot").classList.toggle("live", state.live && running);
  $("#run-facts").innerHTML = `
    <span><small>Algorithm</small><b>${escapeHtml(run?.algorithm || "—")}</b></span>
    <span><small>Runtime</small><b>${escapeHtml(run?.runtime || "—")}</b></span>
    <span><small>Repetition</small><b>${escapeHtml(run?.repetition ? String(run.repetition).padStart(2, "0") : "—")}</b></span>`;
}

function deriveMetrics(requests) {
  if (!requests.length) return { count: 0, successes: 0, errors: 0, throughput: null, latency: [], p95: null, fairness: null, backendCounts: new Map() };
  const starts = requests.map((r) => r.started_offset_us);
  const ends = requests.map(requestCompletedUs);
  const durationS = (Math.max(...ends) - Math.min(...starts)) / 1e6;
  const latency = requests.map((r) => r.latency_us).sort((a, b) => a - b);
  const successes = requests.filter((r) => r.http_success).length;
  const backendCounts = new Map();
  requests.forEach((r) => { if (r.backend_id) backendCounts.set(r.backend_id, (backendCounts.get(r.backend_id) || 0) + 1); });
  const counts = [...backendCounts.values()];
  const sum = counts.reduce((a, b) => a + b, 0);
  const squares = counts.reduce((a, b) => a + b * b, 0);
  return {
    count: requests.length,
    successes,
    errors: requests.length - successes,
    throughput: durationS > 0 ? requests.length / durationS : null,
    latency,
    p95: percentile(latency, .95),
    fairness: squares > 0 ? sum * sum / (counts.length * squares) : null,
    backendCounts,
  };
}

function applyScientificMetrics(metrics, analysis) {
  metrics.throughput = analysis.throughput_requests_per_s ?? metrics.throughput;
  metrics.p95 = analysis.latency?.p95_us ?? metrics.p95;
  if (analysis.error_rate != null && metrics.count) {
    metrics.errors = Math.round(analysis.error_rate * metrics.count);
    metrics.errorRate = analysis.error_rate;
  }
  metrics.fairness = analysis.fairness?.weighted_jain_index
    ?? analysis.fairness?.jain_index
    ?? metrics.fairness;
}

function renderMetrics(metrics, requests) {
  $("#metric-throughput").textContent = formatNumber(metrics.throughput, 1);
  $("#metric-throughput-delta").textContent = `${metrics.count.toLocaleString()} measured requests`;
  $("#metric-latency").textContent = metrics.p95 == null ? "—" : formatNumber(metrics.p95 / 1000, 2);
  $("#metric-latency-range").textContent = metrics.latency.length
    ? `min ${formatDurationUs(metrics.latency[0])} · max ${formatDurationUs(metrics.latency.at(-1))}` : "min — · max —";
  const errorRate = metrics.errorRate ?? (metrics.count ? metrics.errors / metrics.count : null);
  $("#metric-errors").textContent = errorRate == null ? "—" : `${formatNumber(errorRate * 100, 2)}%`;
  $("#metric-errors-count").textContent = `${metrics.errors.toLocaleString()} errors`;
  $("#metric-fairness").textContent = formatNumber(metrics.fairness, 3);
  $("#metric-backends-count").textContent = `${metrics.backendCounts.size} observed backend${metrics.backendCounts.size === 1 ? "" : "s"}`;

  const buckets = requestBuckets(requests, state.playheadUs, 16);
  renderSpark("#spark-throughput", buckets.map((b) => b.total), "#c7f45b");
  renderSpark("#spark-latency", buckets.map((b) => b.p95 || 0), "#b69cff");
  renderSpark("#spark-errors", buckets.map((b) => b.errors), "#ff745f");
  renderSpark("#spark-fairness", [...metrics.backendCounts.values()], "#64c7ff");
}

function renderLatencyChart(requests, cutoff) {
  const host = $("#latency-chart");
  if (!requests.length) return setChartEmpty(host, "Latency points appear as requests complete");
  const width = 900, height = 238, pad = { l: 48, r: 14, t: 12, b: 25 };
  const maxLatency = Math.max(...requests.map((r) => r.latency_us), 1);
  const x = (value) => pad.l + value / Math.max(cutoff, 1) * (width - pad.l - pad.r);
  const y = (value) => height - pad.b - value / maxLatency * (height - pad.t - pad.b);
  const ticks = [0, .25, .5, .75, 1];
  const grid = ticks.map((t) => `<line class="grid-line" x1="${pad.l}" y1="${y(maxLatency*t)}" x2="${width-pad.r}" y2="${y(maxLatency*t)}"/><text class="axis-label" x="4" y="${y(maxLatency*t)+3}">${escapeHtml(formatDurationUs(maxLatency*t))}</text>`).join("");
  const timeTicks = ticks.map((t) => `<text class="axis-label" text-anchor="middle" x="${x(cutoff*t)}" y="${height-4}">${escapeHtml(formatTimeline(cutoff*t, true))}</text>`).join("");
  const ordered = [...requests].sort((a,b) => requestCompletedUs(a)-requestCompletedUs(b));
  const path = ordered.map((r, i) => `${i ? "L" : "M"}${x(requestCompletedUs(r)).toFixed(1)},${y(r.latency_us).toFixed(1)}`).join(" ");
  const points = samplePoints(ordered, 260).map((r) => `<circle class="point" cx="${x(requestCompletedUs(r)).toFixed(1)}" cy="${y(r.latency_us).toFixed(1)}" r="2.6" fill="${r.http_success ? "#c7f45b" : "#ff745f"}"/>`).join("");
  host.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">${grid}${timeTicks}<path class="line" d="${path}" stroke="#64c7ff" opacity=".28"/>${points}</svg>`;
}

function renderTrafficChart(requests, cutoff) {
  const host = $("#traffic-chart");
  const buckets = requestBuckets(requests, cutoff, 22);
  if (!requests.length) {
    $("#success-rate").textContent = "— success";
    return setChartEmpty(host, "Completion windows appear here");
  }
  const width = 600, height = 205, pad = { l: 7, r: 7, t: 8, b: 23 };
  const max = Math.max(...buckets.map((b) => b.total), 1);
  const slot = (width - pad.l - pad.r) / buckets.length;
  const bars = buckets.map((bucket, index) => {
    const totalH = bucket.total / max * (height - pad.t - pad.b);
    const errorH = bucket.errors / max * (height - pad.t - pad.b);
    const x = pad.l + index * slot + 2;
    return `<rect x="${x}" y="${height-pad.b-totalH}" width="${Math.max(2,slot-5)}" height="${totalH}" rx="1.5" fill="#64c7ff" opacity=".7"/><rect x="${x}" y="${height-pad.b-errorH}" width="${Math.max(2,slot-5)}" height="${errorH}" rx="1.5" fill="#ff745f"/>`;
  }).join("");
  const seconds = cutoff / 1e6;
  const labels = `<text class="axis-label" x="${pad.l}" y="${height-4}">0s</text><text class="axis-label" text-anchor="end" x="${width-pad.r}" y="${height-4}">${formatNumber(seconds,1)}s</text>`;
  host.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true"><line class="grid-line" x1="${pad.l}" y1="${height-pad.b}" x2="${width-pad.r}" y2="${height-pad.b}"/>${bars}${labels}</svg>`;
  const successful = requests.filter((r) => r.http_success).length;
  $("#success-rate").textContent = `${formatNumber(successful / requests.length * 100, 1)}% success`;
}

function renderResources(samples, cutoff) {
  const valid = samples.filter((s) => s.cpu_time_us != null || s.resident_memory_bytes != null);
  const cpu = cpuPercent(valid);
  const memory = valid.map((s) => s.resident_memory_bytes).filter((v) => v != null);
  $("#resource-cpu").textContent = cpu == null ? "—" : `${formatNumber(cpu, 1)}%`;
  $("#resource-memory").textContent = memory.length ? formatBytes(Math.max(...memory)) : "—";
  $("#resource-samples").textContent = valid.length.toLocaleString();
  const host = $("#resource-chart");
  if (valid.length < 2) return setChartEmpty(host, "Collecting process samples");
  const width = 500, height = 150, pad = { l: 2, r: 2, t: 5, b: 4 };
  const maxMemory = Math.max(...memory, 1);
  const x = (v) => pad.l + v / Math.max(cutoff,1) * (width-pad.l-pad.r);
  const y = (v) => height-pad.b-v/maxMemory*(height-pad.t-pad.b);
  const points = valid.filter((s) => s.resident_memory_bytes != null).map((s) => [x(resourceTimeUs(s)), y(s.resident_memory_bytes)]);
  const path = points.map((point,i) => `${i?"L":"M"}${point[0].toFixed(1)},${point[1].toFixed(1)}`).join(" ");
  const area = `${path} L${points.at(-1)[0]},${height-pad.b} L${points[0][0]},${height-pad.b} Z`;
  host.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true"><path class="area" d="${area}" fill="#b69cff"/><path class="line" d="${path}" stroke="#b69cff"/></svg>`;
}

function renderBackends(requests) {
  const counts = new Map();
  requests.forEach((r) => { if (r.backend_id) counts.set(r.backend_id, (counts.get(r.backend_id)||0)+1); });
  const host = $("#backend-bars");
  if (!counts.size) {
    host.className = "backend-bars empty-state";
    host.textContent = "Awaiting backend observations";
    return;
  }
  host.className = "backend-bars";
  const total = [...counts.values()].reduce((a,b)=>a+b,0);
  host.innerHTML = [...counts.entries()].sort((a,b)=>b[1]-a[1]).map(([backend,count], index) => `
    <div class="backend-row">
      <div class="backend-copy"><span>${escapeHtml(backend)}</span><small>${count.toLocaleString()} · ${formatNumber(count/total*100,1)}%</small></div>
      <div class="backend-track"><i style="width:${count/total*100}%;--bar:${palette[index%palette.length]}"></i></div>
    </div>`).join("");
}

function renderEvents(events) {
  const host = $("#event-list");
  $("#event-count").textContent = `${events.length} event${events.length === 1 ? "" : "s"}`;
  if (!events.length) {
    host.innerHTML = `<li class="empty-state">Awaiting scenario events</li>`;
    return;
  }
  host.innerHTML = [...events].reverse().slice(0, 80).map((event) => {
    const color = event.success === false ? "#ff745f" : event.event.includes("failure") ? "#ffb15f" : event.event.includes("workload") ? "#c7f45b" : "#64c7ff";
    return `<li class="event-item" style="--event-color:${color}">
      <time class="event-time">${escapeHtml(formatTimeline(eventTimeUs(event), true))}</time>
      <span class="event-rail"><i></i></span>
      <span class="event-body"><b>${escapeHtml(humanize(event.event))}</b><small>${escapeHtml(eventSummary(event))}</small></span>
    </li>`;
  }).join("");
}

function renderReplay(cutoff, duration) {
  $("#timeline").value = Math.round(cutoff / duration * 1000);
  $("#replay-time").textContent = `${formatTimeline(cutoff)} / ${formatTimeline(duration)}`;
  $("#play-button").classList.toggle("playing", state.playing);
  $("#play-button span").textContent = state.playing ? "Ⅱ" : "▶";
  const atLive = state.live && isSnapshotRunning(state.snapshot);
  $("#live-button").classList.toggle("active", atLive);
  $("#replay-caption").textContent = atLive ? "Live position" : "Replay position";
  $("#replay-mode-icon").textContent = atLive ? "●" : "↺";
}

function toggleReplay() {
  if (!state.snapshot) return;
  const duration = state.snapshot.timeline_duration_us || 0;
  state.live = false;
  if (state.playheadUs >= duration) state.playheadUs = 0;
  state.playing = !state.playing;
  state.lastFrame = performance.now();
  render();
}

function jumpToLive() {
  if (!state.snapshot) return;
  state.playing = false;
  state.playheadUs = state.snapshot.timeline_duration_us || 0;
  state.live = isSnapshotRunning(state.snapshot);
  render();
  if (state.live) loadSnapshot(false);
}

function tickReplay(now) {
  if (state.playing && state.snapshot) {
    const elapsed = Math.max(0, now - state.lastFrame) * 1000 * state.speed;
    const duration = state.snapshot.timeline_duration_us || 0;
    state.playheadUs = Math.min(duration, state.playheadUs + elapsed);
    if (state.playheadUs >= duration) state.playing = false;
    render();
  }
  state.lastFrame = now;
  requestAnimationFrame(tickReplay);
}

function requestBuckets(requests, cutoff, count) {
  const buckets = Array.from({ length: count }, () => ({ total: 0, errors: 0, latencies: [], p95: 0 }));
  requests.forEach((request) => {
    const index = Math.min(count - 1, Math.floor(requestCompletedUs(request) / Math.max(cutoff, 1) * count));
    buckets[index].total++;
    if (!request.http_success) buckets[index].errors++;
    buckets[index].latencies.push(request.latency_us);
  });
  buckets.forEach((bucket) => bucket.p95 = percentile(bucket.latencies.sort((a,b)=>a-b), .95));
  return buckets;
}

function percentile(sorted, probability) {
  if (!sorted.length) return null;
  const position = probability * (sorted.length - 1);
  const lower = Math.floor(position), upper = Math.ceil(position), fraction = position - lower;
  return sorted[lower] + (sorted[upper] - sorted[lower]) * fraction;
}

function cpuPercent(samples) {
  const values = samples.filter((s) => s.cpu_time_us != null);
  if (values.length < 2) return null;
  const first = values[0], last = values.at(-1);
  const elapsed = last.elapsed_us - first.elapsed_us;
  return elapsed > 0 ? (last.cpu_time_us - first.cpu_time_us) / elapsed * 100 : null;
}

function renderSpark(selector, values, color) {
  const host = $(selector);
  if (!values.length) { host.innerHTML = ""; return; }
  const max = Math.max(...values, 1), width = 48, height = 15;
  const points = values.map((value,index) => `${values.length === 1 ? width : index/(values.length-1)*width},${height-value/max*height}`).join(" ");
  host.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"><polyline points="${points}" fill="none" stroke="${color}" stroke-width="1.4" vector-effect="non-scaling-stroke"/></svg>`;
}

function setChartEmpty(host, message) {
  host.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
}

function samplePoints(items, limit) {
  if (items.length <= limit) return items;
  const step = items.length / limit;
  return Array.from({length: limit}, (_, index) => items[Math.floor(index * step)]);
}

function eventSummary(event) {
  const details = event.details || {};
  if (details.error) return `${event.label} · ${details.error}`;
  if (details.command) return `${event.label} · ${details.command}`;
  if (details.measurements != null) return `${event.label} · ${details.measurements} measurements`;
  if (details.duration_ms != null) return `${event.label} · ${details.duration_ms} ms`;
  if (details.algorithm || details.runtime) return [details.algorithm, details.runtime].filter(Boolean).join(" · ");
  return event.label || "event";
}

function requestCompletedUs(request) { return (state.snapshot?.workload_offset_us || 0) + (request.started_offset_us || 0) + (request.latency_us || 0); }
function resourceTimeUs(sample) { return (state.snapshot?.workload_offset_us || 0) + (sample.elapsed_us || 0); }
function eventTimeUs(event) { return event.timeline_us ?? 0; }
function isSnapshotRunning(snapshot) { return snapshot?.effective_status === "running"; }
function statusGlyph(status) { return status === "running" ? "●" : status === "completed" ? "✓" : status === "failed" ? "×" : "○"; }
function humanize(value) { return String(value || "event").replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase()); }
function formatNumber(value, digits) { return value == null || !Number.isFinite(value) ? "—" : value.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits }); }
function formatDurationUs(value) { return value >= 1000 ? `${formatNumber(value/1000, value >= 100000 ? 0 : 1)} ms` : `${Math.round(value)} µs`; }
function formatBytes(value) { const units=["B","KiB","MiB","GiB"]; let index=0; while(value>=1024&&index<units.length-1){value/=1024;index++;} return `${formatNumber(value, index?1:0)} ${units[index]}`; }
function formatTimeline(us, compact=false) { const ms=Math.max(0,us||0)/1000; const minutes=Math.floor(ms/60000); const seconds=Math.floor(ms%60000/1000); const millis=Math.floor(ms%1000); return compact ? `${minutes}:${String(seconds).padStart(2,"0")}` : `${String(minutes).padStart(2,"0")}:${String(seconds).padStart(2,"0")}.${String(millis).padStart(3,"0")}`; }
function formatDate(timestamp) { return timestamp ? new Date(timestamp).toLocaleString(undefined,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}) : "unknown time"; }
function escapeHtml(value) { return String(value ?? "").replace(/[&<>"']/g, (character) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"})[character]); }

function updateClock() { $("#clock").textContent = new Date().toLocaleTimeString([], { hour12: false }); }
function setLoading(value) { state.loading = value; $("#refresh-button").classList.toggle("loading", value); }
function toast(message) { const host=$("#toast"); host.textContent=message; host.classList.add("visible"); clearTimeout(host.timer); host.timer=setTimeout(()=>host.classList.remove("visible"),4000); }
function showEmpty(message) { $("#experiment-title").textContent="No experiment data"; $("#experiment-subtitle").textContent=message; }
