export interface PollingOptions {
  intervalMs: number;
  run: (signal: AbortSignal) => Promise<void>;
}

export interface PollingLoop {
  start(): void;
  stop(): void;
}

export async function runIndependentRefreshes(
  refreshTasks: () => Promise<void>,
  refreshComparisons: () => Promise<void>,
): Promise<void> {
  await Promise.allSettled([refreshTasks(), refreshComparisons()]);
}

export function createPollingLoop({ intervalMs, run }: PollingOptions): PollingLoop {
  let stopped = true;
  let running = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let controller: AbortController | undefined;

  const tick = async (): Promise<void> => {
    running = true;
    controller = new AbortController();
    try {
      await run(controller.signal);
    } catch {
      // The next tick is still scheduled after failed polling requests.
    } finally {
      controller = undefined;
      running = false;
      if (!stopped) timer = setTimeout(() => void tick(), intervalMs);
    }
  };

  return {
    start() {
      if (!stopped) return;
      stopped = false;
      if (!running) void tick();
    },
    stop() {
      stopped = true;
      if (timer !== undefined) clearTimeout(timer);
      controller?.abort();
    },
  };
}
