import assert from 'node:assert/strict';

import { listTasks } from './api.js';
import { createPollingLoop, runIndependentRefreshes } from './polling.js';

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

let concurrent = 0;
let maxConcurrent = 0;

const loop = createPollingLoop({
  intervalMs: 10,
  run: async () => {
    concurrent += 1;
    maxConcurrent = Math.max(maxConcurrent, concurrent);
    await delay(40);
    concurrent -= 1;
  },
});

loop.start();
await delay(100);
loop.stop();
await delay(50);

assert.equal(maxConcurrent, 1);

let resolveBody: ((value: { items: [] }) => void) | undefined;
let resolveJsonStarted: (() => void) | undefined;
let requestSignal: AbortSignal | null | undefined;
const body = new Promise<{ items: [] }>((resolve) => {
  resolveBody = resolve;
});
const jsonStarted = new Promise<void>((resolve) => {
  resolveJsonStarted = resolve;
});
const originalFetch = globalThis.fetch;
globalThis.fetch = async (_input, init) => {
  requestSignal = init?.signal;
  return {
    ok: true,
    status: 200,
    json() {
      resolveJsonStarted?.();
      return body;
    },
  } as Response;
};

try {
  const caller = new AbortController();
  const result = listTasks(caller.signal);
  await jsonStarted;
  caller.abort();
  resolveBody?.({ items: [] });
  await result;
  assert.equal(requestSignal?.aborted, true);
} finally {
  globalThis.fetch = originalFetch;
}

const refreshed: string[] = [];
await runIndependentRefreshes(
  async () => { refreshed.push('tasks'); },
  async () => { throw new Error('comparison endpoint unavailable'); },
);
assert.deepEqual(refreshed, ['tasks']);
