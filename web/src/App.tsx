import { FormEvent, useCallback, useEffect, useState } from 'react';
import {
  BucketStat,
  cancelComparison,
  Comparison,
  createComparison,
  cancelTask,
  createTask,
  deleteTask,
  deleteComparison,
  DiffKeyResult,
  DiffNamespace,
  DiffPrefix,
  DiffResource,
  DiffSummary,
  getComparison,
  getDiffOverview,
  getKubernetesObject,
  getKubernetesSummary,
  getMVCCSummary,
  getOverview,
  KeyFilters,
  KeyRecord,
  KeyResult,
  listBuckets,
  listComparisons,
  listDiffKeys,
  listDiffNamespaces,
  listDiffPrefixes,
  listDiffResources,
  listKeyRevisions,
  listKeys,
  listNamespaces,
  listObjectRevisions,
  listObjects,
  listPages,
  listPrefixes,
  listResources,
  listTasks,
  NamespaceStat,
  MVCCSummary,
  ObjectFilters,
  ObjectResult,
  ObjectRevisionResult,
  PageResult,
  PrefixStat,
  RevisionRecord,
  ResourceStat,
  SpaceSummary,
  startTask,
  Task,
  KubernetesObject,
  KubernetesSummary,
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

function formatSignedBytes(value: number): string {
  if (value === 0) return '0 B';
  return `${value > 0 ? '+' : '−'}${formatBytes(Math.abs(value))}`;
}

function formatSigned(value: number): string {
  if (value === 0) return '0';
  return `${value > 0 ? '+' : '−'}${Math.abs(value)}`;
}

export default function App() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [comparisons, setComparisons] = useState<Comparison[]>([]);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [selectedTask, setSelectedTask] = useState<string | null>(null);
  const [baselineTask, setBaselineTask] = useState<string | null>(null);
  const [selectedDiff, setSelectedDiff] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [nextTasks, nextComparisons] = await Promise.all([listTasks(), listComparisons()]);
      setTasks(nextTasks);
      setComparisons(nextComparisons);
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

  async function compare(target: Task) {
    if (!baselineTask) return;
    setBusy(true);
    try {
      const baseline = tasks.find((item) => item.taskId === baselineTask);
      const created = await createComparison({
        name: `${baseline?.name ?? baselineTask} → ${target.name}`,
        baselineTaskId: baselineTask,
        targetTaskId: target.taskId,
      });
      setSelectedDiff(created.diffId);
      setMessage('Comparison started');
      await refresh();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Unable to create comparison');
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="shell">
      <header>
        <p className="eyebrow">Offline forensics · v{__APP_VERSION__}</p>
        <h1>etcd Space Inspector</h1>
        <p>Import an immutable local snapshot or raw backend copy and track its analysis.</p>
      </header>

      <section className="panel" aria-labelledby="new-task-heading">
        <h2 id="new-task-heading">New analysis task</h2>
        <form onSubmit={submit} className="task-form">
          <label>Task name<input name="name" required /></label>
          <label>Local input path<input name="inputPath" required placeholder={'C:\\data\\snapshot.db or /data/snapshot.db'} /></label>
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
                    {task.status === 'completed' && baselineTask !== task.taskId && <button disabled={busy} onClick={() => baselineTask ? void compare(task) : setBaselineTask(task.taskId)}>{baselineTask ? 'Compare' : 'Set baseline'}</button>}
                    {task.status === 'completed' && baselineTask === task.taskId && <button type="button" onClick={() => setBaselineTask(null)}>Baseline ✓</button>}
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
      <section className="panel comparison-list" aria-labelledby="comparisons-heading">
        <h2 id="comparisons-heading">Snapshot comparisons</h2>
        <div className="table-wrap"><table><thead><tr><th>Name</th><th>Status</th><th>Progress</th><th>Created</th><th></th></tr></thead><tbody>
          {comparisons.map((item) => <tr key={item.diffId}><td>{item.name}</td><td><span className={`badge ${item.status}`}>{item.status}</span></td><td><progress max="1" value={item.progress} /></td><td>{new Date(item.createdAt).toLocaleString()}</td><td className="actions">{item.status === 'completed' && <button onClick={() => setSelectedDiff(item.diffId)}>Open</button>}{item.status === 'running' && <button disabled={busy} onClick={() => void action(() => cancelComparison(item.diffId), 'Comparison cancellation requested')}>Cancel</button>}{item.status !== 'running' && <button className="danger" disabled={busy} onClick={() => void action(() => deleteComparison(item.diffId), 'Comparison deleted')}>Delete</button>}</td></tr>)}
          {comparisons.length === 0 && <tr><td colSpan={5} className="empty">No comparisons yet.</td></tr>}
        </tbody></table></div>
      </section>
      {selectedTask && <PhysicalAnalysis taskId={selectedTask} onClose={() => setSelectedTask(null)} />}
      {selectedDiff && <DiffAnalysis diffId={selectedDiff} onClose={() => setSelectedDiff(null)} />}
    </main>
  );
}

function DiffAnalysis({ diffId, onClose }: { diffId: string; onClose: () => void }) {
  const [comparison, setComparison] = useState<Comparison | null>(null);
  const [summary, setSummary] = useState<DiffSummary | null>(null);
  const [growth, setGrowth] = useState<DiffKeyResult | null>(null);
  const [shrink, setShrink] = useState<DiffKeyResult | null>(null);
  const [prefixes, setPrefixes] = useState<DiffPrefix[]>([]);
  const [resources, setResources] = useState<DiffResource[]>([]);
  const [namespaces, setNamespaces] = useState<DiffNamespace[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    let timer: number | undefined;
    async function load() {
      try {
        const nextComparison = await getComparison(diffId);
        if (!active) return;
        setComparison(nextComparison);
        if (nextComparison.status === 'completed') {
          const [nextSummary, nextGrowth, nextShrink, nextPrefixes, nextResources, nextNamespaces] = await Promise.all([
            getDiffOverview(diffId), listDiffKeys(diffId, 'desc'), listDiffKeys(diffId, 'asc'),
            listDiffPrefixes(diffId), listDiffResources(diffId), listDiffNamespaces(diffId),
          ]);
          if (!active) return;
          setSummary(nextSummary); setGrowth(nextGrowth); setShrink(nextShrink);
          setPrefixes(nextPrefixes); setResources(nextResources); setNamespaces(nextNamespaces); setError('');
        } else if (nextComparison.status === 'pending' || nextComparison.status === 'running') {
          timer = window.setTimeout(() => void load(), 1000);
        }
      } catch (reason) {
        if (active) setError(reason instanceof Error ? reason.message : 'Unable to load comparison');
      }
    }
    void load();
    return () => {
      active = false;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [diffId]);

  return <section className="panel analysis comparison" aria-labelledby="comparison-heading">
    <div className="section-title"><h2 id="comparison-heading">Snapshot comparison</h2><button onClick={onClose}>Close</button></div>
    {error && <p role="alert">{error}</p>}
    {comparison && <p><strong>{comparison.name}</strong> · <span className={`badge ${comparison.status}`}>{comparison.status}</span> · {comparison.baselineTaskId} → {comparison.targetTaskId}</p>}
    {comparison && comparison.status !== 'completed' && comparison.status !== 'failed' && comparison.status !== 'cancelled' && <p>Calculating… {Math.round(comparison.progress * 100)}%</p>}
    {comparison?.errorMessage && <p className="notice">{comparison.errorMessage}</p>}
    {summary && <>
      <div className="metrics">
        <Metric label="DB file" value={formatSignedBytes(summary.physicalFileSizeDelta)} />
        <Metric label="Current data" value={formatSignedBytes(summary.currentStoredBytesDelta)} />
        <Metric label="Historical revisions" value={formatSignedBytes(summary.historicalBytesDelta)} />
        <Metric label="Tombstones" value={formatSignedBytes(summary.tombstoneBytesDelta)} />
        <Metric label="Free pages" value={formatSignedBytes(summary.freePageBytesDelta)} />
      </div>
      {!summary.physicalAvailable && <p className="notice">Physical comparison unavailable: {summary.physicalUnavailableReason}</p>}
      {!summary.mvccAvailable && <p className="notice">MVCC comparison unavailable: {summary.mvccUnavailableReason}. Physical results remain valid.</p>}
      {!summary.kubernetesAvailable && <p className="notice">Kubernetes comparison unavailable: {summary.kubernetesUnavailableReason}</p>}
      {summary.mvccAvailable && <div className="metrics">
        <Metric label="Current keys" value={formatSigned(summary.currentKeyCountDelta)} />
        <Metric label="Revision records" value={formatSigned(summary.revisionCountDelta)} />
        <Metric label="Historical versions" value={formatSigned(summary.historicalVersionsDelta)} />
        <Metric label="Tombstones" value={formatSigned(summary.tombstoneCountDelta)} />
        <Metric label="Revision rate" value={summary.revisionRateAvailable ? `${summary.averageRevisionsPerSecond?.toFixed(2)} /s` : 'Unavailable'} />
      </div>}

      {summary.mvccAvailable && <>
        <div className="ranking-grid">
          <DiffKeyTable title="Top growth keys" result={growth} />
          <DiffKeyTable title="Top shrinking keys" result={shrink} />
        </div>
        <h3>Prefix growth</h3>
        <div className="table-wrap"><table><thead><tr><th>Prefix</th><th>Current</th><th>History</th><th>Tombstone</th><th>Total</th></tr></thead><tbody>
          {prefixes.map((item) => <tr key={item.prefix}><td><code>{item.prefix}</code></td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td><td>{formatSignedBytes(item.tombstoneBytesDelta)}</td><td>{formatSignedBytes(item.totalBytesDelta)}</td></tr>)}
        </tbody></table></div>
      </>}

      {summary.kubernetesAvailable && <div className="ranking-grid">
        <div><h3>Resource growth</h3><div className="table-wrap"><table><thead><tr><th>Resource</th><th>Objects</th><th>Current</th><th>History</th></tr></thead><tbody>
          {resources.map((item) => <tr key={`${item.apiGroup}/${item.resource}`}><td>{item.apiGroup || 'core'}/{item.resource}</td><td>{formatSigned(item.currentObjectsDelta)}</td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td></tr>)}
        </tbody></table></div></div>
        <div><h3>Namespace growth</h3><div className="table-wrap"><table><thead><tr><th>Namespace</th><th>Objects</th><th>Current</th><th>History</th></tr></thead><tbody>
          {namespaces.map((item) => <tr key={item.namespace || 'cluster-scoped'}><td>{item.namespace || '(cluster-scoped)'}</td><td>{formatSigned(item.currentObjectsDelta)}</td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td></tr>)}
        </tbody></table></div></div>
      </div>}
    </>}
  </section>;
}

function DiffKeyTable({ title, result }: { title: string; result: DiffKeyResult | null }) {
  return <div><h3>{title}</h3><div className="table-wrap"><table><thead><tr><th>Key</th><th>Change</th><th>Current</th><th>History</th><th>Total</th></tr></thead><tbody>
    {result?.items.map((item) => <tr key={item.keyHash}><td><code>{item.key}</code></td><td>{item.changeType}</td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td><td>{formatSignedBytes(item.totalBytesDelta)}</td></tr>)}
  </tbody></table></div></div>;
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
      <SemanticAnalysis taskId={taskId} />
    </section>
  );
}

const initialKeyFilters: KeyFilters = { prefix: '', minSize: '', minRevisions: '', tombstone: false, sort: 'historical_bytes' };

function SemanticAnalysis({ taskId }: { taskId: string }) {
  const [summary, setSummary] = useState<MVCCSummary | null>(null);
  const [prefixes, setPrefixes] = useState<PrefixStat[]>([]);
  const [keys, setKeys] = useState<KeyResult | null>(null);
  const [filters, setFilters] = useState<KeyFilters>(initialKeyFilters);
  const [page, setPage] = useState(1);
  const [selectedKey, setSelectedKey] = useState<KeyRecord | null>(null);
  const [revisions, setRevisions] = useState<RevisionRecord[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    Promise.all([getMVCCSummary(taskId), listPrefixes(taskId), listKeys(taskId, page, filters)])
      .then(([nextSummary, nextPrefixes, nextKeys]) => {
        if (!active) return;
        setSummary(nextSummary); setPrefixes(nextPrefixes); setKeys(nextKeys); setError('');
      })
      .catch((reason: unknown) => {
        if (active) setError(reason instanceof Error ? reason.message : 'Unable to load MVCC analysis');
      });
    return () => { active = false; };
  }, [taskId, page, filters]);

  async function inspect(key: KeyRecord) {
    setSelectedKey(key);
    try {
      setRevisions(await listKeyRevisions(taskId, key.id));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Unable to load revisions');
    }
  }

  if (!summary && !error) return <p>Loading MVCC analysis…</p>;
  return <div className="semantic">
    <h3>MVCC history</h3>
    {error && <p role="alert">{error}</p>}
    {summary && !summary.semanticAvailable && <p className="notice">Semantic decoding was skipped because the source was not confirmed as etcd 3.4.x. Physical results remain valid.</p>}
    {summary?.semanticAvailable && <>
      <div className="metrics">
        <Metric label="Current keys" value={String(summary.currentKeyCount)} />
        <Metric label="Current stored" value={formatBytes(summary.currentStoredBytes)} />
        <Metric label="Historical versions" value={String(summary.historicalVersions)} />
        <Metric label="Historical bytes" value={formatBytes(summary.historicalBytes)} />
        <Metric label="Tombstones" value={String(summary.tombstoneCount)} />
      </div>

      <h3>Top prefixes</h3>
      <div className="table-wrap"><table><thead><tr><th>Prefix</th><th>Current keys</th><th>History</th><th>Tombstones</th></tr></thead>
        <tbody>{prefixes.map((prefix) => <tr key={prefix.prefix}><td><code>{prefix.prefix}</code></td><td>{prefix.currentKeyCount}</td><td>{formatBytes(prefix.historicalBytes)}</td><td>{prefix.tombstoneCount}</td></tr>)}</tbody>
      </table></div>

      <div className="section-title"><h3>Keys</h3><button type="button" onClick={() => { setFilters(initialKeyFilters); setPage(1); }}>Reset filters</button></div>
      <div className="filters">
        <label>Prefix<input value={filters.prefix} onChange={(event) => { setFilters({ ...filters, prefix: event.target.value }); setPage(1); }} placeholder="/registry" /></label>
        <label>Minimum bytes<input type="number" min="0" value={filters.minSize} onChange={(event) => { setFilters({ ...filters, minSize: event.target.value }); setPage(1); }} /></label>
        <label>Minimum revisions<input type="number" min="0" value={filters.minRevisions} onChange={(event) => { setFilters({ ...filters, minRevisions: event.target.value }); setPage(1); }} /></label>
        <label>Sort<select value={filters.sort} onChange={(event) => { setFilters({ ...filters, sort: event.target.value as KeyFilters['sort'] }); setPage(1); }}><option value="historical_bytes">Historical bytes</option><option value="current_bytes">Current bytes</option><option value="revision_count">Revisions</option><option value="tombstone_count">Tombstones</option><option value="key">Key</option></select></label>
        <label className="check"><input type="checkbox" checked={filters.tombstone} onChange={(event) => { setFilters({ ...filters, tombstone: event.target.checked }); setPage(1); }} />Has tombstones</label>
      </div>
      <div className="table-wrap"><table><thead><tr><th>Key</th><th>Present</th><th>Current</th><th>History</th><th>Revisions</th><th>Tombstones</th><th></th></tr></thead>
        <tbody>{keys?.items.map((key) => <tr key={key.id}><td><code>{key.keyText}</code></td><td>{key.present ? 'yes' : 'no'}</td><td>{formatBytes(key.currentStoredBytes)}</td><td>{formatBytes(key.historicalBytes)}</td><td>{key.revisionCount}</td><td>{key.tombstoneCount}</td><td><button type="button" onClick={() => void inspect(key)}>Revisions</button></td></tr>)}</tbody>
      </table></div>
      <div className="pager"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>Previous</button><span>Page {page} · {keys?.total ?? 0} keys</span><button disabled={!keys || page * keys.pageSize >= keys.total} onClick={() => setPage((value) => value + 1)}>Next</button></div>

      {selectedKey && <div className="drawer" role="region" aria-label="Key revision details">
        <div className="section-title"><h3><code>{selectedKey.keyText}</code></h3><button type="button" onClick={() => { setSelectedKey(null); setRevisions([]); }}>Close</button></div>
        <p className="hash">Key hash: {selectedKey.keyHash}</p>
        <div className="table-wrap"><table><thead><tr><th>Main</th><th>Sub</th><th>Version</th><th>Stored bytes</th><th>Tombstone</th><th>Value hash</th></tr></thead>
          <tbody>{revisions.map((revision) => <tr key={`${revision.mainRevision}-${revision.subRevision}`}><td>{revision.mainRevision}</td><td>{revision.subRevision}</td><td>{revision.version}</td><td>{formatBytes(revision.storedBytes)}</td><td>{revision.tombstone ? 'yes' : 'no'}</td><td><code>{revision.valueHash.slice(0, 16)}</code></td></tr>)}</tbody>
        </table></div>
      </div>}
    </>}
    <KubernetesAnalysis taskId={taskId} />
  </div>;
}

const initialObjectFilters: ObjectFilters = {
  group: '', resource: '', namespace: '', minSize: '', minRevisions: '',
  decodeStatus: '', field: '', sort: 'historical_bytes',
};

function KubernetesAnalysis({ taskId }: { taskId: string }) {
  const [summary, setSummary] = useState<KubernetesSummary | null>(null);
  const [resources, setResources] = useState<ResourceStat[]>([]);
  const [namespaces, setNamespaces] = useState<NamespaceStat[]>([]);
  const [objects, setObjects] = useState<ObjectResult | null>(null);
  const [filters, setFilters] = useState<ObjectFilters>(initialObjectFilters);
  const [page, setPage] = useState(1);
  const [selectedObject, setSelectedObject] = useState<KubernetesObject | null>(null);
  const [revisions, setRevisions] = useState<ObjectRevisionResult | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    Promise.all([
      getKubernetesSummary(taskId), listResources(taskId), listNamespaces(taskId),
      listObjects(taskId, page, filters),
    ]).then(([nextSummary, nextResources, nextNamespaces, nextObjects]) => {
      if (!active) return;
      setSummary(nextSummary); setResources(nextResources); setNamespaces(nextNamespaces);
      setObjects(nextObjects); setError('');
    }).catch((reason: unknown) => {
      if (active) setError(reason instanceof Error ? reason.message : 'Unable to load Kubernetes analysis');
    });
    return () => { active = false; };
  }, [taskId, page, filters]);

  async function inspect(item: KubernetesObject) {
    try {
      const [nextObject, nextRevisions] = await Promise.all([
        getKubernetesObject(taskId, item.id), listObjectRevisions(taskId, item.id),
      ]);
      setSelectedObject(nextObject); setRevisions(nextRevisions); setError('');
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Unable to load object revisions');
    }
  }

  return <div className="kubernetes">
    <h3>Kubernetes storage</h3>
    {error && <p role="alert">{error}</p>}
    {!summary && !error && <p>Loading Kubernetes analysis…</p>}
    {summary && !summary.semanticAvailable && <p className="notice">Kubernetes semantics were skipped because MVCC decoding was unavailable.</p>}
    {summary?.semanticAvailable && <>
      <div className="metrics">
        <Metric label="Current objects" value={String(summary.currentObjects)} />
        <Metric label="Current bytes" value={formatBytes(summary.currentBytes)} />
        <Metric label="Historical bytes" value={formatBytes(summary.historicalBytes)} />
        <Metric label="JSON revisions" value={String(summary.decodedJson)} />
        <Metric label="Protobuf revisions" value={String(summary.decodedProtobuf)} />
        <Metric label="Encrypted" value={String(summary.encrypted)} />
      </div>

      <div className="ranking-grid">
        <div><h4>Top resources</h4><div className="table-wrap"><table><thead><tr><th>Group/resource</th><th>Objects</th><th>Current</th><th>History</th></tr></thead>
          <tbody>{resources.map((item) => <tr key={`${item.apiGroup}/${item.resource}`}><td>{item.apiGroup || 'core'}/{item.resource}</td><td>{item.currentObjects}</td><td>{formatBytes(item.currentBytes)}</td><td>{formatBytes(item.historicalBytes)}</td></tr>)}</tbody>
        </table></div></div>
        <div><h4>Top namespaces</h4><div className="table-wrap"><table><thead><tr><th>Namespace</th><th>Objects</th><th>Current</th><th>History</th></tr></thead>
          <tbody>{namespaces.map((item) => <tr key={item.namespace || 'cluster-scoped'}><td>{item.namespace || '(cluster-scoped)'}</td><td>{item.currentObjects}</td><td>{formatBytes(item.currentBytes)}</td><td>{formatBytes(item.historicalBytes)}</td></tr>)}</tbody>
        </table></div></div>
      </div>

      <div className="section-title"><h4>Objects</h4><button type="button" onClick={() => { setFilters(initialObjectFilters); setPage(1); }}>Reset filters</button></div>
      <div className="filters object-filters">
        <label>API group<input value={filters.group} onChange={(event) => { setFilters({ ...filters, group: event.target.value }); setPage(1); }} placeholder="apps" /></label>
        <label>Resource<input value={filters.resource} onChange={(event) => { setFilters({ ...filters, resource: event.target.value }); setPage(1); }} placeholder="deployments" /></label>
        <label>Namespace<input value={filters.namespace} onChange={(event) => { setFilters({ ...filters, namespace: event.target.value }); setPage(1); }} /></label>
        <label>Minimum bytes<input type="number" min="0" value={filters.minSize} onChange={(event) => { setFilters({ ...filters, minSize: event.target.value }); setPage(1); }} /></label>
        <label>Minimum revisions<input type="number" min="0" value={filters.minRevisions} onChange={(event) => { setFilters({ ...filters, minRevisions: event.target.value }); setPage(1); }} /></label>
        <label>Decode status<select value={filters.decodeStatus} onChange={(event) => { setFilters({ ...filters, decodeStatus: event.target.value }); setPage(1); }}><option value="">All</option><option value="decoded_json">JSON</option><option value="decoded_protobuf">Protobuf</option><option value="encrypted">Encrypted</option><option value="protobuf_unsupported">Unsupported Protobuf</option><option value="decode_failed">Decode failed</option><option value="format_unknown">Unknown format</option><option value="path_unknown">Unknown path</option></select></label>
        <label>Field<select value={filters.field} onChange={(event) => { setFilters({ ...filters, field: event.target.value }); setPage(1); }}><option value="">All</option><option value="spec">spec</option><option value="status">status</option><option value="managedFields">managedFields</option><option value="annotations">annotations</option><option value="labels">labels</option><option value="data">data</option><option value="binaryData">binaryData</option></select></label>
        <label>Sort<select value={filters.sort} onChange={(event) => { setFilters({ ...filters, sort: event.target.value as ObjectFilters['sort'] }); setPage(1); }}><option value="historical_bytes">Historical bytes</option><option value="current_bytes">Current bytes</option><option value="revision_count">Revisions</option><option value="largest_field">Largest field</option><option value="name">Name</option></select></label>
      </div>
      <div className="table-wrap"><table><thead><tr><th>Group/resource</th><th>Namespace</th><th>Name</th><th>Status</th><th>Current</th><th>History</th><th>Revisions</th><th>Largest field</th><th></th></tr></thead>
        <tbody>{objects?.items.map((item) => <tr key={item.id}><td>{item.identity.apiGroup || 'core'}/{item.identity.resource}</td><td>{item.identity.namespace || '(cluster-scoped)'}</td><td>{item.identity.displayName}</td><td>{item.decodeStatus}</td><td>{formatBytes(item.currentBytes)}</td><td>{formatBytes(item.historicalBytes)}</td><td>{item.revisionCount}</td><td>{item.largestFieldPath || '—'} {item.largestFieldBytes ? `(${formatBytes(item.largestFieldBytes)})` : ''}</td><td><button type="button" onClick={() => void inspect(item)}>Inspect</button></td></tr>)}</tbody>
      </table></div>
      <div className="pager"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>Previous</button><span>Page {page} · {objects?.total ?? 0} objects</span><button disabled={!objects || page * objects.pageSize >= objects.total} onClick={() => setPage((value) => value + 1)}>Next</button></div>

      {selectedObject && <div className="drawer" role="region" aria-label="Kubernetes object details">
        <div className="section-title"><h4>{selectedObject.identity.displayName}</h4><button type="button" onClick={() => { setSelectedObject(null); setRevisions(null); }}>Close</button></div>
        <p>{selectedObject.identity.apiGroup || 'core'}/{selectedObject.identity.resource} · {selectedObject.identity.namespace || '(cluster-scoped)'} · {selectedObject.decodeStatus}</p>
        <p className="hash">Key hash: {selectedObject.keyHash}</p>
        <h4>Selected field sizes</h4>
        <div className="table-wrap"><table><thead><tr><th>Revision</th><th>Path</th><th>Bytes</th><th>Type</th><th>Hash</th></tr></thead><tbody>
          {revisions?.items.flatMap((revision) => revision.fields.map((field) => <tr key={`${revision.mainRevision}-${revision.subRevision}-${field.path}`}><td>{revision.mainRevision}.{revision.subRevision}</td><td>{field.path}</td><td>{formatBytes(field.byteSize)}</td><td>{field.typeClass}</td><td><code>{field.hash.slice(0, 16)}</code></td></tr>))}
        </tbody></table></div>
        <h4>Adjacent revision changes</h4>
        <div className="table-wrap"><table><thead><tr><th>Revisions</th><th>Changed paths</th><th>Byte delta</th><th>Classification</th></tr></thead><tbody>
          {revisions?.diffs.map((diff) => <tr key={`${diff.previousMainRevision}-${diff.currentMainRevision}`}><td>{diff.previousMainRevision} → {diff.currentMainRevision}</td><td>{[...diff.addedPaths, ...diff.removedPaths, ...diff.modifiedPaths].join(', ') || 'none'}</td><td>{diff.byteDelta}</td><td>{diff.timestampOnly ? 'timestamps only' : diff.statusOnly ? 'status only' : diff.managedFieldsOnly ? 'managed fields only' : 'structural'}</td></tr>)}
        </tbody></table></div>
      </div>}
    </>}
  </div>;
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="metric"><span>{label}</span><strong>{value}</strong></div>;
}
