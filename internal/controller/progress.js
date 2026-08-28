// Diagnostics are display-only. No maintenance or migration actions live here.
const pgProgress = (() => {
  const maxAge = 15000;
  let latest = null, status = null, pending = false, samples = [];

  function lsnBytes(value) {
    if (!/^[0-9a-f]{1,8}\/[0-9a-f]{1,8}$/i.test(String(value || ''))) return null;
    const [high, low] = value.split('/');
    return (BigInt('0x' + high) << 32n) + BigInt('0x' + low);
  }

  function walRates(points) {
    if (points.length < 2) return null;
    const first = points[0], last = points[points.length - 1];
    const seconds = (last.at - first.at) / 1000;
    if (seconds < 15) return null;
    const source = Number(last.source - first.source) / seconds;
    const capture = Number(last.capture - first.capture) / seconds;
    const replay = Number(last.replay - first.replay) / seconds;
    const drain = Number((last.replay - first.replay) - (last.source - first.source)) / seconds;
    return {seconds, source, capture, replay, drain};
  }

  function addSample(points, data) {
    const w = data.wal, at = Date.parse(data.sampled_at);
    const source = lsnBytes(w.source_lsn), capture = lsnBytes(w.staged_lsn), replay = lsnBytes(w.applied_lsn);
    if (w.error || !Number.isFinite(at) || w.total_bytes == null || [source, capture, replay].some(x => x === null) || source < capture || capture < replay) return [];
    const last = points[points.length - 1];
    if (last && (data.revision !== last.revision || at < last.at || source < last.source || capture < last.capture || replay < last.replay)) points = [];
    else if (last && at === last.at) return points;
    points.push({at, source, capture, replay, revision: data.revision});
    while (points.length > 2 && points[1].at <= at - 300000) points.shift();
    return points;
  }

  const byID = id => document.getElementById(id);
  const text = (id, value) => { byID(id).textContent = value; };
  const bytes = value => value == null ? 'unavailable' : fmtBytes(Number(value));
  // Server age plus a local monotonic clock avoids comparing clocks on two hosts.
  const fresh = (data, now = performance.now()) => data && Number.isFinite(data.sample_age_ms) && data.sample_age_ms >= 0 && now >= data.receivedAt && data.sample_age_ms + now - data.receivedAt < maxAge;

  function summary() {
    const w = latest?.wal, usable = fresh(latest) && !w.error && w.total_bytes != null;
    const running = ['running', 'stopping'].includes(status?.operations?.migration?.state);
    const replaying = running && ['catchup', 'follow', 'drained'].includes(status?.snapshot?.phase);
    text('lag', usable ? bytes(w.total_bytes) : '—');
    text('lagTrend', '—'); text('lagTrendLabel', 'end-to-end WAL trend unavailable');
    text('replayIO', '—'); text('replayIOLabel', 'WAL apply / actual source generation');
    if (!usable) return;
    const rate = replaying ? walRates(samples) : null;
    if (!rate) {
      text('lagTrendLabel', replaying ? 'collecting 15s of end-to-end samples' : 'replay is not running');
      return;
    }
    text('replayIO', `${fmtBytes(rate.replay)}/s`);
    text('replayIOLabel', `WAL apply · source ${fmtBytes(rate.source)}/s · ${Math.round(rate.seconds)}s avg`);
    text('lagTrend', `${rate.drain < 0 ? '+' : rate.drain > 0 ? '−' : ''}${fmtBytes(Math.abs(rate.drain))}/s`);
    const eta = rate.drain > 0 && BigInt(w.total_bytes) > 0n ? ` · ETA ${fmtDuration(Number(w.total_bytes) / rate.drain * 1e9)}` : '';
    text('lagTrendLabel', BigInt(w.total_bytes) === 0n ? 'no WAL gap at sampled checkpoints' : rate.drain > 0 ? `end-to-end drain${eta}` : rate.drain < 0 ? 'end-to-end backlog growing · no convergent ETA' : 'end-to-end backlog unchanged · no convergent ETA');
  }

  function renderMaintenance(id, view) {
    const root = byID(id);
    root.replaceChildren();
    if (view.error) { root.textContent = view.error; return; }
    if (view.restricted) {
      const warning = document.createElement('p');
      warning.textContent = 'Some sessions are hidden: pg_read_all_stats is required for full visibility.';
      root.append(warning);
    }
    if (!view.jobs?.length) {
      const empty = document.createElement('p');
      empty.className = 'muted'; empty.textContent = 'No visible active VACUUM or index builds at this sample.';
      root.append(empty); return;
    }
    for (const job of view.jobs) {
      const card = document.createElement('div'); card.className = 'maintenance-job';
      const name = document.createElement('strong');
      name.textContent = `${job.command} · ${job.table || 'relation unavailable'}${job.index ? ` · ${job.index}` : ''}`;
      const phase = document.createElement('p');
      phase.textContent = `${job.phase} · PID ${job.pid} · ${Math.max(0, Math.floor(job.elapsed_seconds))}s${job.wait_event ? ` · ${job.wait_event}` : ''}${job.locker_pid ? ` · waiting for PID ${job.locker_pid}` : ''}`;
      card.append(name, phase);
      if (Number.isFinite(job.percent)) {
        const progress = document.createElement('progress');
        progress.max = 100; progress.value = job.percent;
        progress.setAttribute('aria-label', `${job.table} ${job.phase} phase progress`);
        const label = document.createElement('p');
        label.textContent = `${job.percent.toFixed(1)}% of this phase · ${job.done} / ${job.total} ${job.unit}`;
        card.append(progress, label);
      } else {
        const unknown = document.createElement('p'); unknown.className = 'muted';
        unknown.textContent = 'PostgreSQL does not report a measurable total for this phase.';
        card.append(unknown);
      }
      if (job.index_cycles) {
        const cycles = document.createElement('p'); cycles.textContent = `${job.index_cycles} index vacuum cycles completed`; card.append(cycles);
      }
      root.append(card);
    }
    if (view.truncated) {
      const note = document.createElement('p'); note.textContent = 'Results limited to the first 100 sessions.'; root.append(note);
    }
  }

  function render(data) {
    const w = data.wal;
    text('diagnosticsTime', `Sampled ${new Date(data.sampled_at).toISOString()} · refresh every 5s`);
    for (const [id, value] of [['sourceLSN', w.source_lsn], ['capturedLSN', w.staged_lsn], ['appliedLSN', w.applied_lsn]]) text(id, value || 'unavailable');
    for (const [id, value] of [['uncapturedGap', w.uncaptured_bytes], ['replayGap', w.replay_bytes], ['totalGap', w.total_bytes]]) {
      text(id, bytes(value)); byID(id).title = value == null ? 'unavailable' : `${value} bytes`;
    }
    text('walMessage', w.error || (w.total_bytes == null ? 'Waiting for source WAL and initialized durable checkpoints.' : 'Source sampled after the recorded checkpoints; gaps are conservative WAL-byte distances, not rows, queue-file sizes, or data validation. Zero is not cutover authorization.'));
    text('walTimes', `Checkpoints sampled: ${w.checkpoint_sampled_at || 'unavailable'} · source sampled: ${w.source_sampled_at || 'unavailable'} · last durable apply: ${w.apply_updated_at || 'unavailable'}`);
    const rate = walRates(samples);
    text('walRates', rate ? `Source generation ${fmtBytes(rate.source)}/s · capture ${fmtBytes(rate.capture)}/s · replay ${fmtBytes(rate.replay)}/s · ${Math.round(rate.seconds)}s average` : 'Rates need at least 15s of valid, fresh samples.');
    renderMaintenance('sourceMaintenance', data.source);
    renderMaintenance('targetMaintenance', data.target);
    summary();
  }

  function unavailable(message) {
    latest = null; samples = [];
    text('diagnosticsTime', message);
    for (const id of ['sourceLSN', 'capturedLSN', 'appliedLSN', 'uncapturedGap', 'replayGap', 'totalGap']) { text(id, 'unavailable'); byID(id).removeAttribute('title'); }
    text('walMessage', 'Live diagnostics unavailable; durable migration status is independent.');
    text('walTimes', ''); text('walRates', '');
    text('sourceMaintenance', 'Live progress unavailable.'); text('targetMaintenance', 'Live progress unavailable.');
    summary();
  }

  async function refresh() {
    if (pending || !configurationLoaded || configurationToken !== token.value) return;
    pending = true;
    const revision = configurationRevision, auth = token.value;
    try {
      const response = await fetch('/api/diagnostics', {headers: {'X-PGMigrate-Token': auth}, signal: AbortSignal.timeout(4500)});
      if (!response.ok) throw new Error('Diagnostics unavailable; authentication or connection failed.');
      const data = await response.json();
      data.receivedAt = performance.now();
      if (auth !== token.value || revision !== configurationRevision) return;
      if (data.revision !== revision) throw new Error('Configuration changed; waiting for matching diagnostics.');
      if (!fresh(data)) throw new Error('Diagnostics sample is stale; waiting for a fresh sample.');
      samples = addSample(samples, data); latest = data; render(data);
    } catch (error) {
      if (auth === token.value && revision === configurationRevision) unavailable(error.message);
    } finally { pending = false; }
  }

  function updateStatus(data) {
    status = data;
    if (latest && (!fresh(latest) || latest.revision !== configurationRevision)) unavailable('Diagnostics sample expired or configuration changed.');
    summary();
  }

  function start() {
    refresh();
    setInterval(refresh, 5000);
    setInterval(() => { if (latest && !fresh(latest)) unavailable('Diagnostics sample expired.'); }, 1000);
  }

  return {lsnBytes, walRates, addSample, fresh, updateStatus, unavailable, start};
})();
