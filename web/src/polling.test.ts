import assert from 'node:assert/strict';

import { createPollingLoop } from './polling.js';

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
