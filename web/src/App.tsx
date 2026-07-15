import { FormEvent, useCallback, useEffect, useState } from 'react';
import {
  BucketStat,
  cancelTask,
  createTask,
  deleteTask,
  getOverview,
  listBuckets,
  listPages,
  listTasks,
  PageResult,
  SpaceSummary,
  startTask,
  Task,
} from './api';

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let result = value / 1024;
  let unit = units[0];
  for (let index = 1; result >= 1024 && index < units.length; index += 1) {
    result /= 1024;
    unit = units[index];
  }
  return `${result.toFixed(1)} ${unit}`;
}

export default function App() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [selectedTask, setSelectedTask] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setTasks(await listTasks());
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Unable to load tasks');
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 2000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setMessage('Importing source file…');
    const form = new FormData(event.currentTarget);
    try {
      await createTask({
        name: String(form.get('name') ?? ''),
        inputPath: String(form.get('inputPath') ?? ''),
        inputType: String(form.get('inputType') ?? 'snapshot') as 'snapshot' | 'raw-db',
        etcdVersion: String(form.get('etcdVersion') ?? ''),
      });
      event.currentTarget.reset();
      setMessage('Task created');
      await refresh();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Unable to create task');
    } finally {
      setBusy(false);
    }
  }

  async function action(operation: () => Promise<void>, success: string) {
    setBusy(true);
    try {
      await operation();
      setMessage(success);
      await refresh();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Task operation failed');
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="shell">
      <header>
        <p className="eyebrow">Offline forensics · v{__APP_VERSION__}</p>
        <h1>ETCD DBSize Analyzer</h1>
        <p>Import an immutable local snapshot or raw backend copy and track its analysis.</p>
      </header>

      <section className="panel" aria-labelledby="new-task-heading">
        <h2 id="new-task-heading">New analysis task</h2>
        <form onSubmit={submit} className="task-form">
          <label>Task name<input name="name" required /></label>
          <label>Local input path<input name="inputPath" required placeholder="/data/snapshot.db" /></label>
          <label>Input type<select name="inputType"><option value="snapshot">Snapshot</option><option value="raw-db">Raw DB</option></select></label>
          <label>etcd version<input name="etcdVersion" placeholder="3.4.13" /></label>
          <button type="submit" disabled={busy}>Create task</button>
        </form>
      </section>

      <p className="status" role="status" aria-live="polite">{message}</p>

      <section className="panel" aria-labelledby="tasks-heading">
        <div className="section-title"><h2 id="tasks-heading">Tasks</h2><button type="button" onClick={() => void refresh()}>Refresh</button></div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Name</th><th>Type</th><th>Size</th><th>Status</th><th>Progress</th><th>Created</th><th>Actions</th></tr></thead>
            <tbody>
              {tasks.map((task) => (
                <tr key={task.taskId}>
                  <td><strong>{task.name}</strong><small>{task.sha256.slice(0, 12)}</small></td>
                  <td>{task.inputType}</td><td>{formatBytes(task.inputSize)}</td><td><span className={`badge ${task.status}`}>{task.status}</span></td>
                  <td><progress max="1" value={task.progress}>{Math.round(task.progress * 100)}%</progress></td>
                  <td>{new Date(task.createdAt).toLocaleString()}</td>
                  <td className="actions">
                    {task.status === 'completed' && <button onClick={() => setSelectedTask(task.taskId)}>Inspect</button>}
                    {task.status === 'pending' && <button disabled={busy} onClick={() => void action(() => startTask(task.taskId), 'Task started')}>Start</button>}
                    {task.status === 'running' && <button disabled={busy} onClick={() => void action(() => cancelTask(task.taskId), 'Cancellation requested')}>Cancel</button>}
                    {task.status !== 'running' && <button className="danger" disabled={busy} onClick={() => void action(() => deleteTask(task.taskId), 'Task deleted')}>Delete</button>}
                  </td>
                </tr>
              ))}
              {tasks.length === 0 && <tr><td colSpan={7} className="empty">No tasks yet.</td></tr>}
            </tbody>
          </table>
        </div>
      </section>
      {selectedTask && <PhysicalAnalysis taskId={selectedTask} onClose={() => setSelectedTask(null)} />}
    </main>
  );
}

function PhysicalAnalysis({ taskId, onClose }: { taskId: string; onClose: () => void }) {
  const [summary, setSummary] = useState<SpaceSummary | null>(null);
  const [buckets, setBuckets] = useState<BucketStat[]>([]);
  const [pages, setPages] = useState<PageResult | null>(null);
  const [page, setPage] = useState(1);
  const [pageType, setPageType] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    Promise.all([getOverview(taskId), listBuckets(taskId), listPages(taskId, page, pageType)])
      .then(([nextSummary, nextBuckets, nextPages]) => {
        if (!active) return;
        setSummary(nextSummary); setBuckets(nextBuckets); setPages(nextPages); setError('');
      })
      .catch((reason: unknown) => {
        if (active) setError(reason instanceof Error ? reason.message : 'Unable to load physical analysis');
      });
    return () => { active = false; };
  }, [taskId, page, pageType]);

  const freePercent = Math.min(100, Math.max(0, (summary?.fragmentationRatio ?? 0) * 100));
  return (
    <section className="panel analysis" aria-labelledby="analysis-heading">
      <div className="section-title"><h2 id="analysis-heading">Physical bbolt analysis</h2><button onClick={onClose}>Close</button></div>
      {error && <p role="alert">{error}</p>}
      {summary && <>
        <div className="metrics">
          <Metric label="Physical file" value={formatBytes(summary.physicalFileSize)} />
          <Metric label="In use" value={formatBytes(summary.inUsePageBytes)} />
          <Metric label="Free" value={formatBytes(summary.freePageBytes)} />
          <Metric label="Fragmentation" value={`${freePercent.toFixed(1)}%`} />
          <Metric label="Pages" value={String(summary.pageCount)} />
        </div>
        <div className="composition" aria-label={`${freePercent.toFixed(1)}% free space`}>
          <span className="in-use" style={{ width: `${100 - freePercent}%` }} /><span className="free" style={{ width: `${freePercent}%` }} />
        </div>
      </>}

      <h3>Largest buckets</h3>
      <div className="table-wrap"><table><thead><tr><th>Bucket</th><th>Keys</th><th>Allocated</th><th>Used</th><th>Overflow</th></tr></thead>
        <tbody>{buckets.map((bucket) => <tr key={bucket.bucketPath}><td>{bucket.bucketPath}</td><td>{bucket.keyCount}</td><td>{formatBytes(bucket.totalBytes)}</td><td>{formatBytes(bucket.usedBytes)}</td><td>{formatBytes(bucket.overflowBytes)}</td></tr>)}</tbody>
      </table></div>

      <div className="section-title"><h3>Pages</h3><label>Type<select value={pageType} onChange={(event) => { setPageType(event.target.value); setPage(1); }}><option value="">All</option><option value="meta">Meta</option><option value="branch">Branch</option><option value="leaf">Leaf</option><option value="freelist">Freelist</option><option value="free">Free</option></select></label></div>
      <div className="table-wrap"><table><thead><tr><th>ID</th><th>Type</th><th>Overflow</th><th>Bytes</th><th>Utilization</th></tr></thead>
        <tbody>{pages?.items.map((item) => <tr key={item.pageId}><td>{item.pageId}</td><td>{item.pageType}</td><td>{item.overflow}</td><td>{formatBytes(item.totalBytes)}</td><td>{Math.round(item.utilization * 100)}%</td></tr>)}</tbody>
      </table></div>
      <div className="pager"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>Previous</button><span>Page {page} · {pages?.total ?? 0} records</span><button disabled={!pages || page * pages.pageSize >= pages.total} onClick={() => setPage((value) => value + 1)}>Next</button></div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="metric"><span>{label}</span><strong>{value}</strong></div>;
}
