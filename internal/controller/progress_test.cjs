const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const context = vm.createContext({});
vm.runInContext(fs.readFileSync(path.join(__dirname, 'progress.js'), 'utf8'), context);
const progress = vm.runInContext('pgProgress', context);
const sample = (at, source, capture, replay, revision = 'one') => ({
  sampled_at: new Date(at).toISOString(), revision,
  wal: {source_lsn: source, staged_lsn: capture, applied_lsn: replay, total_bytes: '16'},
});

test('sample expiry uses server age and local elapsed time, not clock synchronization', () => {
  const data = {sampled_at: '1999-01-01T00:00:00Z', sample_age_ms: 3000, receivedAt: 100};
  assert.equal(progress.fresh(data, 100), true);
  assert.equal(progress.fresh(data, 12099), true);
  assert.equal(progress.fresh(data, 12100), false);
  assert.equal(progress.fresh({...data, sample_age_ms: 16000}, 100), false);
  assert.equal(progress.fresh({...data, sample_age_ms: -1}, 100), false);
  assert.equal(progress.fresh({...data, sample_age_ms: undefined}, 100), false);
});

test('LSNs retain every bit, including above the JS integer limit', () => {
  assert.equal(progress.lsnBytes('FFFFFFFF/FFFFFFFF'), 18446744073709551615n);
  assert.equal(progress.lsnBytes('200000/1') - progress.lsnBytes('200000/0'), 1n);
  assert.equal(progress.lsnBytes('2/0') - progress.lsnBytes('1/FFFFFFFF'), 1n);
  for (const value of ['', '1/', '/1', '1/2junk', '1/+2', '100000000/0', '0/100000000', '1/2/3', 'NaN/1']) assert.equal(progress.lsnBytes(value), null, value);
});

test('real source generation, capture and replay are separate rates', () => {
  let points = progress.addSample([], sample(0, '2/100', '2/80', '2/40'));
  points = progress.addSample(points, sample(20000, '2/200', '2/C0', '2/80'));
  const rates = progress.walRates(points);
  assert.equal(rates.source, 256 / 20);
  assert.equal(rates.capture, 64 / 20);
  assert.equal(rates.replay, 64 / 20);
  assert.equal(rates.drain, -192 / 20);
});

test('unchanged positions produce zero throughput, not a frozen old rate', () => {
  let points = progress.addSample([], sample(0, '200000/3', '200000/2', '200000/1'));
  points = progress.addSample(points, sample(20000, '200000/4', '200000/3', '200000/2'));
  assert.equal(progress.walRates(points).replay, 1 / 20);
  points = progress.addSample(points, sample(330000, '200000/4', '200000/3', '200000/2'));
  assert.equal(progress.walRates(points).replay, 0);
  assert.equal(progress.walRates(points).source, 0);
});

test('duplicate cached samples, reset positions and configuration changes', () => {
  let points = progress.addSample([], sample(1000, '2/30', '2/20', '2/10'));
  assert.equal(progress.addSample(points, sample(1000, '2/30', '2/20', '2/10')).length, 1);
  points = progress.addSample(points, sample(5000, '2/30', '2/20', '2/10'));
  assert.equal(progress.walRates(points), null);
  assert.equal(progress.addSample(points, sample(6000, '2/30', '2/20', '2/10', 'two')).length, 1);
  assert.equal(progress.addSample(points, sample(6000, '1/30', '1/20', '1/10')).length, 1);
  assert.equal(progress.addSample(points, sample(0, '2/30', '2/20', '2/10')).length, 1);
});

test('unknown, failed and reversed comparisons never become a zero backlog', () => {
  const missing = sample(0, '2/30', '2/20', '2/10');
  missing.wal.total_bytes = null;
  assert.equal(progress.addSample([], missing).length, 0);
  const failed = sample(0, '2/30', '2/20', '2/10');
  failed.wal.error = 'source identity mismatch';
  assert.equal(progress.addSample([], failed).length, 0);
  assert.equal(progress.addSample([], sample(0, '2/10', '2/20', '2/10')).length, 0);
});

test('the shipped replay sampler uses exact LSN deltas and samples idle time', () => {
  const html = fs.readFileSync(path.join(__dirname, 'ui.html'), 'utf8');
  const functions = html.split('\n').filter(line => line.startsWith('function lsnBytes(') || line.startsWith('function sampleReplay(')).join('\n');
  vm.runInContext('let replaySamples=[];\n' + functions, context);
  const replay = vm.runInContext('sampleReplay', context);
  const state = {transactions: 0, rows: 0, applied_lsn: '200000/0', staged_lsn: '200000/1', lag_bytes: 1};
  assert.equal(replay(state, true, 1000), null);
  const next = {...state, applied_lsn: '200000/1', staged_lsn: '200000/2'};
  assert.equal(replay(next, true, 2000).appliedBytes, 1);
  assert.equal(replay(next, true, 303000).appliedBytes, 0);
});
