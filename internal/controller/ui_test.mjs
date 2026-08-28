import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const html = readFileSync(new URL('./ui.html', import.meta.url), 'utf8');
const render = html.match(/^function renderVerification\(.*$/m)[0];

function element() {
  return {
    children: [], style: {}, textContent: '',
    append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; },
    setAttribute() {},
  };
}

function resultCell(row) {
  const root = element();
  const context = vm.createContext({
    document: { createElement: element }, el: () => root,
    fmtCount: value => String(value ?? 0), fmtDuration: () => '—',
  });
  vm.runInContext(render, context);
  context.renderVerification([{ table: 'public.items', coverage: 1, ...row }]);
  return root.children[0].children[1].children[0].children[6];
}

test('controller script parses', () => {
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  assert.doesNotThrow(() => new vm.Script(script));
});

for (const stage of ['pending cdc recheck', 'rechecking cdc']) {
  test(`${stage} remains pending`, () => {
    const result = resultCell({ stage, complete: false, converged: false });
    assert.equal(result.textContent, 'pending CDC recheck');
    assert.equal(result.style.color, 'var(--amber)');
  });
  test(`${stage} does not hide confirmed heap divergence`, () => {
    const result = resultCell({ stage, complete: false, unresolved_rows: 2 });
    assert.equal(result.textContent, '2 divergent · pending CDC recheck');
    assert.equal(result.style.color, 'var(--red)');
  });
}

test('completed convergence replaces pending status', () => {
  const result = resultCell({ stage: 'done', complete: true, converged: true });
  assert.equal(result.textContent, 'converged');
  assert.equal(result.style.color, 'var(--green)');
});

test('stalled mismatches are divergent', () => {
  const result = resultCell({ stage: 'done', complete: true, converged: false, unresolved_rows: 1 });
  assert.equal(result.textContent, '1 divergent');
  assert.equal(result.style.color, 'var(--red)');
});

test('exhausted deferred-check budget is incomplete', () => {
  const result = resultCell({ stage: 'done', complete: false, converged: false });
  assert.equal(result.textContent, 'incomplete');
  assert.equal(result.style.color, 'var(--amber)');
});
