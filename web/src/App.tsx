import { FormEvent, useCallback, useEffect, useState } from 'react';
import { cancelTask, createTask, deleteTask, listTasks, startTask, Task } from './api';

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
    </main>
  );
}
