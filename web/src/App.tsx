import { createContext, FormEvent, useCallback, useContext, useEffect, useState } from 'react';
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
  getTimeline,
  getKubernetesObject,
  getKubernetesSummary,
  getMVCCSummary,
  getOverview,
  KeyFilters,
  KeyRecord,
  KeyResult,
  LogEvent,
  LogTimeline,
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
import {
  languagePreferenceKey,
  Locale,
  metric,
  MetricKey,
  resolveLocale,
  text,
  TextKey,
} from './locales';

type Translate = (key: TextKey, values?: Record<string, string | number>) => string;

const TranslationContext = createContext<{ locale: Locale; t: Translate } | null>(null);

function useTranslation(): { locale: Locale; t: Translate } {
  const translation = useContext(TranslationContext);
  if (!translation) throw new Error('translation context is missing');
  return translation;
}

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

function formatDate(value: string, locale: Locale): string {
  return new Date(value).toLocaleString(locale === 'zh' ? 'zh-CN' : 'en-US');
}

function statusLabel(status: string, t: Translate): string {
  const keys: Record<string, TextKey> = {
    pending: 'status.pending', running: 'status.running', completed: 'status.completed',
    failed: 'status.failed', cancelled: 'status.cancelled',
  };
  return t(keys[status] ?? 'value.unavailable');
}

function inputTypeLabel(inputType: string, t: Translate): string {
  const keys: Record<string, TextKey> = { snapshot: 'type.snapshot', 'raw-db': 'type.raw-db', log: 'type.log' };
  return t(keys[inputType] ?? 'value.unavailable');
}

function decodeStatusLabel(status: string, t: Translate): string {
  const keys: Record<string, TextKey> = {
    decoded_json: 'decode.decoded_json', decoded_protobuf: 'decode.decoded_protobuf',
    encrypted: 'decode.encrypted', protobuf_unsupported: 'decode.protobuf_unsupported',
    decode_failed: 'decode.decode_failed', format_unknown: 'decode.format_unknown',
    path_unknown: 'decode.path_unknown',
  };
  return t(keys[status] ?? 'value.unavailable');
}

function pageTypeLabel(pageType: string, t: Translate): string {
  const keys: Record<string, TextKey> = {
    meta: 'physical.meta', branch: 'physical.branch', leaf: 'physical.leaf',
    freelist: 'physical.freelist', free: 'physical.free',
  };
  return t(keys[pageType] ?? 'value.unavailable');
}

function changeTypeLabel(changeType: string, t: Translate): string {
  const keys: Record<string, TextKey> = {
    added: 'change.added', deleted: 'change.deleted', modified: 'change.modified',
  };
  return t(keys[changeType] ?? 'value.unavailable');
}

export default function App() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [comparisons, setComparisons] = useState<Comparison[]>([]);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [baselineTask, setBaselineTask] = useState<string | null>(null);
	const [comparisonTarget, setComparisonTarget] = useState<Task | null>(null);
  const [selectedDiff, setSelectedDiff] = useState<string | null>(null);
  const [locale, setLocale] = useState<Locale>(() => resolveLocale(
    window.localStorage.getItem(languagePreferenceKey), window.navigator.language,
  ));
  const t = useCallback<Translate>((key, values) => text(locale, key, values), [locale]);

  function selectLocale(nextLocale: Locale) {
    setLocale(nextLocale);
    window.localStorage.setItem(languagePreferenceKey, nextLocale);
  }

  const refresh = useCallback(async () => {
    try {
      const [nextTasks, nextComparisons] = await Promise.all([listTasks(), listComparisons()]);
      setTasks(nextTasks);
      setComparisons(nextComparisons);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : t('tasks.loadFailed'));
    }
  }, [t]);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 2000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setMessage(t('tasks.importing'));
    const form = new FormData(event.currentTarget);
    try {
      await createTask({
        name: String(form.get('name') ?? ''),
        inputPath: String(form.get('inputPath') ?? ''),
        inputType: String(form.get('inputType') ?? 'snapshot') as 'snapshot' | 'raw-db' | 'log',
        etcdVersion: String(form.get('etcdVersion') ?? ''),
      });
      event.currentTarget.reset();
      setMessage(t('tasks.createdMessage'));
      await refresh();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : t('tasks.operationFailed'));
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
      setMessage(error instanceof Error ? error.message : t('tasks.operationFailed'));
    } finally {
      setBusy(false);
    }
  }

  function configureComparison(target: Task) {
    if (baselineTask) setComparisonTarget(target);
  }

  async function compare(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
	if (!baselineTask || !comparisonTarget) return;
    setBusy(true);
	const form = new FormData(event.currentTarget);
    try {
      const baseline = tasks.find((item) => item.taskId === baselineTask);
      const created = await createComparison({
		name: `${baseline?.name ?? baselineTask} → ${comparisonTarget.name}`,
        baselineTaskId: baselineTask,
		targetTaskId: comparisonTarget.taskId,
		...(String(form.get('baselineObservedAt') ?? '') ? { baselineObservedAt: new Date(String(form.get('baselineObservedAt'))).toISOString() } : {}),
		...(String(form.get('targetObservedAt') ?? '') ? { targetObservedAt: new Date(String(form.get('targetObservedAt'))).toISOString() } : {}),
      });
      setSelectedDiff(created.diffId);
		setBaselineTask(null);
		setComparisonTarget(null);
      setMessage(t('comparisons.started'));
      await refresh();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : t('tasks.operationFailed'));
    } finally {
      setBusy(false);
    }
  }

  return (
    <TranslationContext.Provider value={{ locale, t }}>
    <main className="shell" lang={locale === 'zh' ? 'zh-CN' : 'en'}>
      <header className="header">
        <div>
          <p className="eyebrow">{t('app.eyebrow', { version: __APP_VERSION__ })}</p>
          <h1>etcd Space Inspector</h1>
          <p>{t('app.description')}</p>
        </div>
        <div className="language-switch" aria-label={t('language.switch')}>
          <button type="button" className={locale === 'zh' ? 'active' : ''} aria-pressed={locale === 'zh'} onClick={() => selectLocale('zh')}>{t('language.zh')}</button>
          <button type="button" className={locale === 'en' ? 'active' : ''} aria-pressed={locale === 'en'} onClick={() => selectLocale('en')}>{t('language.en')}</button>
        </div>
      </header>

      <section className="panel" aria-labelledby="new-task-heading">
        <h2 id="new-task-heading">{t('form.newTask')}</h2>
        <form onSubmit={submit} className="task-form">
          <label>{t('form.name')}<input name="name" required /></label>
          <label>{t('form.inputPath')}<input name="inputPath" required placeholder={'C:\\data\\snapshot.db or /data/snapshot.db'} /></label>
          <label>{t('form.inputType')}<select name="inputType"><option value="snapshot">{t('form.snapshot')}</option><option value="raw-db">{t('form.rawDb')}</option><option value="log">{t('form.log')}</option></select><small>{t('form.logHint')}</small></label>
          <label>{t('form.versionOverride')}<input name="etcdVersion" placeholder="3.4.13" /></label>
          <button type="submit" disabled={busy}>{t('form.createTask')}</button>
        </form>
      </section>

      <p className="status" role="status" aria-live="polite">{message}</p>

      <section className="panel" aria-labelledby="tasks-heading">
        <div className="section-title"><h2 id="tasks-heading">{t('tasks.title')}</h2><button type="button" onClick={() => void refresh()}>{t('tasks.refresh')}</button></div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>{t('tasks.name')}</th><th>{t('tasks.type')}</th><th>{t('tasks.size')}</th><th>{t('tasks.status')}</th><th>{t('tasks.progress')}</th><th>{t('tasks.created')}</th><th>{t('tasks.actions')}</th></tr></thead>
            <tbody>
              {tasks.map((task) => (
                <tr key={task.taskId}>
                  <td><strong>{task.name}</strong><small>{task.sha256.slice(0, 12)}</small><small>{versionEvidence(task, t)}</small></td>
                  <td>{inputTypeLabel(task.inputType, t)}</td><td>{formatBytes(task.inputSize)}</td><td><span className={`badge ${task.status}`}>{statusLabel(task.status, t)}</span></td>
                  <td><progress max="1" value={task.progress}>{Math.round(task.progress * 100)}%</progress></td>
                  <td>{formatDate(task.createdAt, locale)}</td>
                  <td className="actions">
                    {task.status === 'completed' && <button onClick={() => setSelectedTask(task)}>{t('tasks.inspect')}</button>}
                    {task.status === 'completed' && task.inputType !== 'log' && baselineTask !== task.taskId && <button disabled={busy} onClick={() => baselineTask ? configureComparison(task) : setBaselineTask(task.taskId)}>{baselineTask ? t('tasks.compare') : t('tasks.setBaseline')}</button>}
                    {task.status === 'completed' && task.inputType !== 'log' && baselineTask === task.taskId && <button type="button" onClick={() => setBaselineTask(null)}>{t('tasks.baseline')}</button>}
                    {task.status === 'pending' && <button disabled={busy} onClick={() => void action(() => startTask(task.taskId), t('tasks.started'))}>{t('tasks.start')}</button>}
                    {task.status === 'running' && <button disabled={busy} onClick={() => void action(() => cancelTask(task.taskId), t('tasks.cancelled'))}>{t('tasks.cancel')}</button>}
                    {task.status !== 'running' && <button className="danger" disabled={busy} onClick={() => void action(() => deleteTask(task.taskId), t('tasks.deleted'))}>{t('tasks.delete')}</button>}
                  </td>
                </tr>
              ))}
              {tasks.length === 0 && <tr><td colSpan={7} className="empty">{t('tasks.empty')}</td></tr>}
            </tbody>
          </table>
        </div>
      </section>
      {comparisonTarget && baselineTask && <section className="panel" aria-labelledby="comparison-config-heading">
        <h2 id="comparison-config-heading">{t('comparison.configure')}</h2>
        <form onSubmit={compare} className="task-form">
          <label>{t('comparison.baselineObservedAt')}<input name="baselineObservedAt" type="datetime-local" step="1" /></label>
          <label>{t('comparison.targetObservedAt')}<input name="targetObservedAt" type="datetime-local" step="1" /></label>
          <button type="submit" disabled={busy}>{t('tasks.compare')}</button>
          <button type="button" disabled={busy} onClick={() => setComparisonTarget(null)}>{t('actions.close')}</button>
        </form>
        <p>{t('comparison.collectionTimePair')}</p>
      </section>}
      <section className="panel comparison-list" aria-labelledby="comparisons-heading">
        <h2 id="comparisons-heading">{t('comparisons.title')}</h2>
        <div className="table-wrap"><table><thead><tr><th>{t('comparisons.name')}</th><th>{t('comparisons.status')}</th><th>{t('comparisons.progress')}</th><th>{t('comparisons.created')}</th><th></th></tr></thead><tbody>
          {comparisons.map((item) => <tr key={item.diffId}><td>{item.name}</td><td><span className={`badge ${item.status}`}>{statusLabel(item.status, t)}</span></td><td><progress max="1" value={item.progress} /></td><td>{formatDate(item.createdAt, locale)}</td><td className="actions">{item.status === 'completed' && <button onClick={() => setSelectedDiff(item.diffId)}>{t('comparisons.open')}</button>}{item.status === 'running' && <button disabled={busy} onClick={() => void action(() => cancelComparison(item.diffId), t('comparisons.cancelled'))}>{t('tasks.cancel')}</button>}{item.status !== 'running' && <button className="danger" disabled={busy} onClick={() => void action(() => deleteComparison(item.diffId), t('comparisons.deleted'))}>{t('tasks.delete')}</button>}</td></tr>)}
          {comparisons.length === 0 && <tr><td colSpan={5} className="empty">{t('comparisons.empty')}</td></tr>}
        </tbody></table></div>
      </section>
      {selectedTask && (selectedTask.inputType === 'log'
        ? <LogTimelineAnalysis task={selectedTask} onClose={() => setSelectedTask(null)} />
        : <PhysicalAnalysis taskId={selectedTask.taskId} onClose={() => setSelectedTask(null)} />)}
      {selectedDiff && <DiffAnalysis diffId={selectedDiff} onClose={() => setSelectedDiff(null)} />}
    </main>
    </TranslationContext.Provider>
  );
}

function DiffAnalysis({ diffId, onClose }: { diffId: string; onClose: () => void }) {
  const { t } = useTranslation();
  const [comparison, setComparison] = useState<Comparison | null>(null);
  const [summary, setSummary] = useState<DiffSummary | null>(null);
  const [growth, setGrowth] = useState<DiffKeyResult | null>(null);
  const [shrink, setShrink] = useState<DiffKeyResult | null>(null);
	const [churn, setChurn] = useState<DiffKeyResult | null>(null);
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
		  const [nextSummary, nextGrowth, nextShrink, nextChurn, nextPrefixes, nextResources, nextNamespaces] = await Promise.all([
			getDiffOverview(diffId), listDiffKeys(diffId, 'desc'), listDiffKeys(diffId, 'asc'), listDiffKeys(diffId, 'desc', 'revision_count'),
            listDiffPrefixes(diffId), listDiffResources(diffId), listDiffNamespaces(diffId),
          ]);
          if (!active) return;
		  setSummary(nextSummary); setGrowth(nextGrowth); setShrink(nextShrink); setChurn(nextChurn);
          setPrefixes(nextPrefixes); setResources(nextResources); setNamespaces(nextNamespaces); setError('');
        } else if (nextComparison.status === 'pending' || nextComparison.status === 'running') {
          timer = window.setTimeout(() => void load(), 1000);
        }
      } catch (reason) {
        if (active) setError(reason instanceof Error ? reason.message : t('comparisons.loadFailed'));
      }
    }
    void load();
    return () => {
      active = false;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [diffId, t]);

  return <section className="panel analysis comparison" aria-labelledby="comparison-heading">
    <div className="section-title"><h2 id="comparison-heading">{t('comparison.title')}</h2><button onClick={onClose}>{t('actions.close')}</button></div>
    {error && <p role="alert">{error}</p>}
    {comparison && <p><strong>{comparison.name}</strong> · <span className={`badge ${comparison.status}`}>{statusLabel(comparison.status, t)}</span> · {comparison.baselineTaskId} → {comparison.targetTaskId}</p>}
    {comparison && comparison.status !== 'completed' && comparison.status !== 'failed' && comparison.status !== 'cancelled' && <p>{t('comparisons.calculating', { progress: Math.round(comparison.progress * 100) })}</p>}
    {comparison?.errorMessage && <p className="notice">{comparison.errorMessage}</p>}
    {summary && <>
      <div className="metrics">
        <Metric metricKey="comparison.physicalFile" value={formatSignedBytes(summary.physicalFileSizeDelta)} />
        <Metric metricKey="comparison.currentData" value={formatSignedBytes(summary.currentStoredBytesDelta)} />
        <Metric metricKey="comparison.historicalBytes" value={formatSignedBytes(summary.historicalBytesDelta)} />
        <Metric metricKey="comparison.tombstoneBytes" value={formatSignedBytes(summary.tombstoneBytesDelta)} />
        <Metric metricKey="comparison.freePageBytes" value={formatSignedBytes(summary.freePageBytesDelta)} />
      </div>
      {!summary.physicalAvailable && <p className="notice">{t('comparisons.physicalUnavailable', { reason: summary.physicalUnavailableReason ?? t('value.unavailable') })}</p>}
      {!summary.mvccAvailable && <p className="notice">{t('comparisons.mvccUnavailable', { reason: summary.mvccUnavailableReason ?? t('value.unavailable') })}</p>}
      {!summary.kubernetesAvailable && <p className="notice">{t('comparisons.kubernetesUnavailable', { reason: summary.kubernetesUnavailableReason ?? t('value.unavailable') })}</p>}
      {summary.mvccAvailable && <div className="metrics">
        <Metric metricKey="comparison.currentKeys" value={formatSigned(summary.currentKeyCountDelta)} />
        <Metric metricKey="comparison.revisionRecords" value={formatSigned(summary.revisionCountDelta)} />
        <Metric metricKey="comparison.historicalVersions" value={formatSigned(summary.historicalVersionsDelta)} />
        <Metric metricKey="comparison.tombstoneCount" value={formatSigned(summary.tombstoneCountDelta)} />
        <Metric metricKey="comparison.revisionRate" value={summary.revisionRateAvailable ? `${(summary.averageRevisionsPerSecond ?? 0) * 3600 >= 0 ? '+' : ''}${((summary.averageRevisionsPerSecond ?? 0) * 3600).toFixed(2)} /h` : t('value.unavailable')} />
      </div>}

      {summary.mvccAvailable && <>
        <div className="ranking-grid">
          <DiffKeyTable title={t('comparison.topGrowth')} result={growth} />
          <DiffKeyTable title={t('comparison.topShrinking')} result={shrink} />
        </div>
		<h3>{t('comparison.highChurnKeys')}</h3>
		{summary.observationWindowSeconds > 0
		  ? <p>{t('comparison.observationWindow', { hours: (summary.observationWindowSeconds / 3600).toFixed(2) })}</p>
		  : <p className="notice">{t('comparison.rateUnavailable')}</p>}
		<div className="table-wrap"><table><thead><tr><th>{t('comparison.key')}</th><th>{t('comparison.revisionDelta')}</th><th>{t('comparison.revisionsPerHour')}</th></tr></thead><tbody>
		  {churn?.items.map((item) => {
			const rate = summary.observationWindowSeconds > 0 ? item.revisionCountDelta / summary.observationWindowSeconds * 3600 : null;
			return <tr key={item.keyHash}><td><code>{item.key}</code></td><td>{formatSigned(item.revisionCountDelta)}</td><td>{rate === null ? '—' : `${rate >= 0 ? '+' : ''}${rate.toFixed(2)}`}</td></tr>;
		  })}
		</tbody></table></div>
        <h3>{t('comparison.prefixGrowth')}</h3>
        <div className="table-wrap"><table><thead><tr><th>{t('comparison.prefix')}</th><th>{t('comparison.current')}</th><th>{t('comparison.history')}</th><th>{t('comparison.tombstone')}</th><th>{t('comparison.total')}</th></tr></thead><tbody>
          {prefixes.map((item) => <tr key={item.prefix}><td><code>{item.prefix}</code></td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td><td>{formatSignedBytes(item.tombstoneBytesDelta)}</td><td>{formatSignedBytes(item.totalBytesDelta)}</td></tr>)}
        </tbody></table></div>
      </>}

      {summary.kubernetesAvailable && <div className="ranking-grid">
        <div><h3>{t('comparison.resourceGrowth')}</h3><div className="table-wrap"><table><thead><tr><th>{t('comparison.resource')}</th><th>{t('comparison.objects')}</th><th>{t('comparison.current')}</th><th>{t('comparison.history')}</th></tr></thead><tbody>
          {resources.map((item) => <tr key={`${item.apiGroup}/${item.resource}`}><td>{item.apiGroup || t('value.core')}/{item.resource}</td><td>{formatSigned(item.currentObjectsDelta)}</td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td></tr>)}
        </tbody></table></div></div>
        <div><h3>{t('comparison.namespaceGrowth')}</h3><div className="table-wrap"><table><thead><tr><th>{t('comparison.namespace')}</th><th>{t('comparison.objects')}</th><th>{t('comparison.current')}</th><th>{t('comparison.history')}</th></tr></thead><tbody>
          {namespaces.map((item) => <tr key={item.namespace || 'cluster-scoped'}><td>{item.namespace || t('value.clusterScoped')}</td><td>{formatSigned(item.currentObjectsDelta)}</td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td></tr>)}
        </tbody></table></div></div>
      </div>}
    </>}
  </section>;
}

function DiffKeyTable({ title, result }: { title: string; result: DiffKeyResult | null }) {
  const { t } = useTranslation();
  return <div><h3>{title}</h3><div className="table-wrap"><table><thead><tr><th>{t('comparison.key')}</th><th>{t('comparison.change')}</th><th>{t('comparison.current')}</th><th>{t('comparison.history')}</th><th>{t('comparison.total')}</th></tr></thead><tbody>
    {result?.items.map((item) => <tr key={item.keyHash}><td><code>{item.key}</code></td><td>{changeTypeLabel(item.changeType, t)}</td><td>{formatSignedBytes(item.currentBytesDelta)}</td><td>{formatSignedBytes(item.historicalBytesDelta)}</td><td>{formatSignedBytes(item.totalBytesDelta)}</td></tr>)}
  </tbody></table></div></div>;
}

const logEventTypes = [
  'nospace', 'quota_exceeded', 'compaction', 'defrag', 'slow_apply',
  'slow_backend_commit', 'slow_fdatasync', 'wal_fsync', 'leader_change',
  'request_timeout', 'snapshot_save', 'snapshot_restore', 'lease_revoke',
  'corruption_check', 'large_request', 'backend_commit', 'unknown',
];
const logSources = ['unknown', 'etcdserver', 'mvcc', 'backend', 'wal', 'raft', 'lease'];

type LogFilters = { from: string; to: string; eventType: string; severity: string; source: string };
const initialLogFilters: LogFilters = { from: '', to: '', eventType: '', severity: '', source: '' };

function logTime(value: string | undefined, locale: Locale, t: Translate): string {
  return value ? formatDate(value, locale) : t('log.unknownTime');
}

function logValue(value: number | undefined): string {
  return value === undefined ? '—' : String(value);
}

function LogTimelineAnalysis({ task, onClose }: { task: Task; onClose: () => void }) {
  const { locale, t } = useTranslation();
  const [timeline, setTimeline] = useState<LogTimeline | null>(null);
  const [filters, setFilters] = useState<LogFilters>(initialLogFilters);
  const [page, setPage] = useState(1);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    const query = {
      ...(filters.from ? { from: new Date(filters.from).toISOString() } : {}),
      ...(filters.to ? { to: new Date(filters.to).toISOString() } : {}),
      ...(filters.eventType ? { eventType: filters.eventType } : {}),
      ...(filters.severity ? { severity: filters.severity } : {}),
      ...(filters.source ? { source: filters.source } : {}),
      page,
      pageSize: 20,
    };
    getTimeline(task.taskId, query)
      .then((nextTimeline) => {
        if (!active) return;
        setTimeline(nextTimeline);
        setError('');
      })
      .catch((reason: unknown) => {
        if (active) setError(reason instanceof Error ? reason.message : t('log.loadFailed'));
      });
    return () => { active = false; };
  }, [task.taskId, filters, page, t]);

  function updateFilter(name: keyof LogFilters, value: string) {
    setFilters((current) => ({ ...current, [name]: value }));
    setPage(1);
  }

  return <section className="panel analysis log-timeline" aria-labelledby="log-heading">
    <div className="section-title"><h2 id="log-heading">{t('log.title')}</h2><button onClick={onClose}>{t('actions.close')}</button></div>
    <p><strong>{task.name}</strong> · {inputTypeLabel(task.inputType, t)} · {formatBytes(task.inputSize)}</p>
    {error && <p role="alert">{error}</p>}
    {timeline && <>
      <div className="metrics">
        <Metric metricKey="log.totalLines" value={String(timeline.summary.totalLines)} />
        <Metric metricKey="log.recognizedEvents" value={String(timeline.summary.recognizedEvents)} />
        <Metric metricKey="log.unknownLines" value={String(timeline.summary.unknownLines)} />
        <Metric metricKey="log.parseErrors" value={String(timeline.summary.parseErrors)} />
      </div>
      <p className="log-range">{t('log.inputSummary')}: {t('log.firstObservedAt')} {logTime(timeline.summary.firstObservedAt, locale, t)} · {t('log.lastObservedAt')} {logTime(timeline.summary.lastObservedAt, locale, t)}</p>
      <p className="notice">{t('log.safetyBoundary')} {t('log.noAttribution')}</p>
      <div className="filters log-filters">
        <label>{t('log.from')}<input type="datetime-local" value={filters.from} onChange={(event) => updateFilter('from', event.target.value)} /></label>
        <label>{t('log.to')}<input type="datetime-local" value={filters.to} onChange={(event) => updateFilter('to', event.target.value)} /></label>
        <label>{t('log.eventType')}<select value={filters.eventType} onChange={(event) => updateFilter('eventType', event.target.value)}><option value="">{t('log.allEvents')}</option>{logEventTypes.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <label>{t('log.severity')}<select value={filters.severity} onChange={(event) => updateFilter('severity', event.target.value)}><option value="">{t('log.allSeverities')}</option><option value="ERROR">ERROR</option><option value="WARN">WARN</option><option value="INFO">INFO</option><option value="UNKNOWN">UNKNOWN</option></select></label>
        <label>{t('log.source')}<select value={filters.source} onChange={(event) => updateFilter('source', event.target.value)}><option value="">{t('log.allSources')}</option>{logSources.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
      </div>
      <div className="table-wrap"><table><thead><tr><th>{t('log.time')}</th><th>{t('log.event')}</th><th>{t('log.severity')}</th><th>{t('log.source')}</th><th>{t('log.line')}</th><th>{t('log.duration')}</th><th>{t('log.revision')}</th><th>{t('log.dbSize')}</th><th>{t('log.parseStatus')}</th><th>{t('log.fingerprint')}</th></tr></thead><tbody>
        {timeline.items.map((event: LogEvent) => <tr key={event.eventId}><td>{logTime(event.observedAt, locale, t)}</td><td><code>{event.eventType}</code></td><td><span className={`badge ${event.severity.toLowerCase()}`}>{event.severity}</span></td><td>{event.source}</td><td>{event.lineNumber}</td><td>{logValue(event.durationMs)}</td><td>{logValue(event.revision)}</td><td>{event.dbSizeBytes === undefined ? '—' : formatBytes(event.dbSizeBytes)}</td><td>{event.parseStatus}</td><td><code>{event.messageFingerprint.slice(0, 12)}</code></td></tr>)}
        {timeline.items.length === 0 && <tr><td colSpan={10} className="empty">{t('log.empty')}</td></tr>}
      </tbody></table></div>
      <div className="pager"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>{t('log.previous')}</button><span>{t('pagination.records', { page, count: timeline.total })}</span><button disabled={page * timeline.pageSize >= timeline.total} onClick={() => setPage((value) => value + 1)}>{t('log.next')}</button></div>
    </>}
  </section>;
}

function PhysicalAnalysis({ taskId, onClose }: { taskId: string; onClose: () => void }) {
  const { t } = useTranslation();
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
        if (active) setError(reason instanceof Error ? reason.message : t('physical.loadFailed'));
      });
    return () => { active = false; };
  }, [taskId, page, pageType, t]);

  const freePercent = Math.min(100, Math.max(0, (summary?.fragmentationRatio ?? 0) * 100));
  return (
    <section className="panel analysis" aria-labelledby="analysis-heading">
      <div className="section-title"><h2 id="analysis-heading">{t('physical.title')}</h2><button onClick={onClose}>{t('actions.close')}</button></div>
      {error && <p role="alert">{error}</p>}
      {summary && <>
        <div className="metrics">
          <Metric metricKey="physical.file" value={formatBytes(summary.physicalFileSize)} />
          <Metric metricKey="physical.inUse" value={formatBytes(summary.inUsePageBytes)} />
          <Metric metricKey="physical.free" value={formatBytes(summary.freePageBytes)} />
          <Metric metricKey="physical.fragmentation" value={`${freePercent.toFixed(1)}%`} />
          <Metric metricKey="physical.pages" value={String(summary.pageCount)} />
        </div>
        <div className="composition" aria-label={t('physical.freeSpace', { percent: freePercent.toFixed(1) })}>
          <span className="in-use" style={{ width: `${100 - freePercent}%` }} /><span className="free" style={{ width: `${freePercent}%` }} />
        </div>
      </>}

      <h3>{t('physical.largestBuckets')}</h3>
      <div className="table-wrap"><table><thead><tr><th>{t('physical.bucket')}</th><th>{t('physical.keys')}</th><th>{t('physical.allocated')}</th><th>{t('physical.used')}</th><th>{t('physical.overflow')}</th></tr></thead>
        <tbody>{buckets.map((bucket) => <tr key={bucket.bucketPath}><td>{bucket.bucketPath}</td><td>{bucket.keyCount}</td><td>{formatBytes(bucket.totalBytes)}</td><td>{formatBytes(bucket.usedBytes)}</td><td>{formatBytes(bucket.overflowBytes)}</td></tr>)}</tbody>
      </table></div>

      <div className="section-title"><h3>{t('physical.pages')}</h3><label>{t('physical.pageType')}<select value={pageType} onChange={(event) => { setPageType(event.target.value); setPage(1); }}><option value="">{t('physical.all')}</option><option value="meta">{t('physical.meta')}</option><option value="branch">{t('physical.branch')}</option><option value="leaf">{t('physical.leaf')}</option><option value="freelist">{t('physical.freelist')}</option><option value="free">{t('physical.free')}</option></select></label></div>
      <div className="table-wrap"><table><thead><tr><th>{t('physical.id')}</th><th>{t('physical.pageType')}</th><th>{t('physical.overflow')}</th><th>{t('physical.bytes')}</th><th>{t('physical.utilization')}</th></tr></thead>
        <tbody>{pages?.items.map((item) => <tr key={item.pageId}><td>{item.pageId}</td><td>{pageTypeLabel(item.pageType, t)}</td><td>{item.overflow}</td><td>{formatBytes(item.totalBytes)}</td><td>{Math.round(item.utilization * 100)}%</td></tr>)}</tbody>
      </table></div>
      <div className="pager"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>{t('pagination.previous')}</button><span>{t('pagination.records', { page, count: pages?.total ?? 0 })}</span><button disabled={!pages || page * pages.pageSize >= pages.total} onClick={() => setPage((value) => value + 1)}>{t('pagination.next')}</button></div>
      <SemanticAnalysis taskId={taskId} />
    </section>
  );
}

const initialKeyFilters: KeyFilters = { prefix: '', minSize: '', minRevisions: '', tombstone: false, sort: 'historical_bytes' };

function SemanticAnalysis({ taskId }: { taskId: string }) {
  const { t } = useTranslation();
  const [summary, setSummary] = useState<MVCCSummary | null>(null);
  const [prefixes, setPrefixes] = useState<PrefixStat[]>([]);
  const [keys, setKeys] = useState<KeyResult | null>(null);
	const [churnKeys, setChurnKeys] = useState<KeyResult | null>(null);
  const [filters, setFilters] = useState<KeyFilters>(initialKeyFilters);
  const [page, setPage] = useState(1);
  const [selectedKey, setSelectedKey] = useState<KeyRecord | null>(null);
  const [revisions, setRevisions] = useState<RevisionRecord[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
	Promise.all([getMVCCSummary(taskId), listPrefixes(taskId), listKeys(taskId, page, filters), listKeys(taskId, 1, { ...initialKeyFilters, sort: 'revision_count' })])
	  .then(([nextSummary, nextPrefixes, nextKeys, nextChurnKeys]) => {
        if (!active) return;
		setSummary(nextSummary); setPrefixes(nextPrefixes); setKeys(nextKeys); setChurnKeys(nextChurnKeys); setError('');
      })
      .catch((reason: unknown) => {
        if (active) setError(reason instanceof Error ? reason.message : t('semantic.loadFailed'));
      });
    return () => { active = false; };
  }, [taskId, page, filters, t]);

  async function inspect(key: KeyRecord) {
    setSelectedKey(key);
    try {
      setRevisions(await listKeyRevisions(taskId, key.id));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t('semantic.revisionLoadFailed'));
    }
  }

  if (!summary && !error) return <p>{t('semantic.loading')}</p>;
  return <div className="semantic">
    <h3>{t('semantic.title')}</h3>
    {error && <p role="alert">{error}</p>}
    {summary && !summary.semanticAvailable && <p className="notice">{t('semantic.unavailable')}</p>}
    {summary?.semanticAvailable && <>
      <div className="metrics">
        <Metric metricKey="mvcc.currentKeys" value={String(summary.currentKeyCount)} />
        <Metric metricKey="mvcc.currentStored" value={formatBytes(summary.currentStoredBytes)} />
        <Metric metricKey="mvcc.historicalVersions" value={String(summary.historicalVersions)} />
        <Metric metricKey="mvcc.historicalBytes" value={formatBytes(summary.historicalBytes)} />
        <Metric metricKey="mvcc.tombstones" value={String(summary.tombstoneCount)} />
      </div>
	  <h3>{t('semantic.highChurnKeys')}</h3>
	  <p>{t('semantic.retainedCaveat')}</p>
	  <div className="table-wrap"><table><thead><tr><th>{t('comparison.key')}</th><th>{t('semantic.retainedRevisions')}</th><th>{t('semantic.history')}</th><th>{t('semantic.tombstones')}</th></tr></thead>
		<tbody>{churnKeys?.items.map((key) => <tr key={key.id}><td><code>{key.keyText}</code></td><td>{key.revisionCount}</td><td>{formatBytes(key.historicalBytes)}</td><td>{key.tombstoneCount}</td></tr>)}</tbody>
	  </table></div>

      <h3>{t('semantic.topPrefixes')}</h3>
      <div className="table-wrap"><table><thead><tr><th>{t('semantic.prefix')}</th><th>{t('semantic.currentKeys')}</th><th>{t('semantic.history')}</th><th>{t('semantic.tombstones')}</th></tr></thead>
        <tbody>{prefixes.map((prefix) => <tr key={prefix.prefix}><td><code>{prefix.prefix}</code></td><td>{prefix.currentKeyCount}</td><td>{formatBytes(prefix.historicalBytes)}</td><td>{prefix.tombstoneCount}</td></tr>)}</tbody>
      </table></div>

      <div className="section-title"><h3>{t('semantic.keys')}</h3><button type="button" onClick={() => { setFilters(initialKeyFilters); setPage(1); }}>{t('semantic.resetFilters')}</button></div>
      <div className="filters">
        <label>{t('semantic.prefix')}<input value={filters.prefix} onChange={(event) => { setFilters({ ...filters, prefix: event.target.value }); setPage(1); }} placeholder="/registry" /></label>
        <label>{t('semantic.minimumBytes')}<input type="number" min="0" value={filters.minSize} onChange={(event) => { setFilters({ ...filters, minSize: event.target.value }); setPage(1); }} /></label>
        <label>{t('semantic.minimumRevisions')}<input type="number" min="0" value={filters.minRevisions} onChange={(event) => { setFilters({ ...filters, minRevisions: event.target.value }); setPage(1); }} /></label>
        <label>{t('semantic.sort')}<select value={filters.sort} onChange={(event) => { setFilters({ ...filters, sort: event.target.value as KeyFilters['sort'] }); setPage(1); }}><option value="historical_bytes">{t('semantic.sortHistory')}</option><option value="current_bytes">{t('semantic.sortCurrent')}</option><option value="revision_count">{t('semantic.sortRevisions')}</option><option value="tombstone_count">{t('semantic.sortTombstones')}</option><option value="key">{t('semantic.sortKey')}</option></select></label>
        <label className="check"><input type="checkbox" checked={filters.tombstone} onChange={(event) => { setFilters({ ...filters, tombstone: event.target.checked }); setPage(1); }} />{t('semantic.hasTombstones')}</label>
      </div>
      <div className="table-wrap"><table><thead><tr><th>{t('comparison.key')}</th><th>{t('semantic.present')}</th><th>{t('semantic.current')}</th><th>{t('semantic.history')}</th><th>{t('semantic.revisions')}</th><th>{t('semantic.tombstones')}</th><th></th></tr></thead>
        <tbody>{keys?.items.map((key) => <tr key={key.id}><td><code>{key.keyText}</code></td><td>{key.present ? t('boolean.yes') : t('boolean.no')}</td><td>{formatBytes(key.currentStoredBytes)}</td><td>{formatBytes(key.historicalBytes)}</td><td>{key.revisionCount}</td><td>{key.tombstoneCount}</td><td><button type="button" onClick={() => void inspect(key)}>{t('semantic.revisions')}</button></td></tr>)}</tbody>
      </table></div>
      <div className="pager"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>{t('pagination.previous')}</button><span>{t('pagination.keys', { page, count: keys?.total ?? 0 })}</span><button disabled={!keys || page * keys.pageSize >= keys.total} onClick={() => setPage((value) => value + 1)}>{t('pagination.next')}</button></div>

      {selectedKey && <div className="drawer" role="region" aria-label={t('semantic.keyRevisionDetails')}>
        <div className="section-title"><h3><code>{selectedKey.keyText}</code></h3><button type="button" onClick={() => { setSelectedKey(null); setRevisions([]); }}>{t('actions.close')}</button></div>
        <p className="hash">{t('semantic.keyHash', { hash: selectedKey.keyHash })}</p>
        <div className="table-wrap"><table><thead><tr><th>{t('semantic.main')}</th><th>{t('semantic.sub')}</th><th>{t('semantic.version')}</th><th>{t('semantic.storedBytes')}</th><th>{t('semantic.tombstone')}</th><th>{t('semantic.valueHash')}</th></tr></thead>
          <tbody>{revisions.map((revision) => <tr key={`${revision.mainRevision}-${revision.subRevision}`}><td>{revision.mainRevision}</td><td>{revision.subRevision}</td><td>{revision.version}</td><td>{formatBytes(revision.storedBytes)}</td><td>{revision.tombstone ? t('boolean.yes') : t('boolean.no')}</td><td><code>{revision.valueHash.slice(0, 16)}</code></td></tr>)}</tbody>
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
  const { t } = useTranslation();
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
      if (active) setError(reason instanceof Error ? reason.message : t('kubernetes.loadFailed'));
    });
    return () => { active = false; };
  }, [taskId, page, filters, t]);

  async function inspect(item: KubernetesObject) {
    try {
      const [nextObject, nextRevisions] = await Promise.all([
        getKubernetesObject(taskId, item.id), listObjectRevisions(taskId, item.id),
      ]);
      setSelectedObject(nextObject); setRevisions(nextRevisions); setError('');
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t('kubernetes.objectRevisionLoadFailed'));
    }
  }

  return <div className="kubernetes">
    <h3>{t('kubernetes.title')}</h3>
    {error && <p role="alert">{error}</p>}
    {!summary && !error && <p>{t('kubernetes.loading')}</p>}
    {summary && !summary.semanticAvailable && <p className="notice">{t('kubernetes.unavailable')}</p>}
    {summary?.semanticAvailable && <>
      <div className="metrics">
        <Metric metricKey="kubernetes.currentObjects" value={String(summary.currentObjects)} />
        <Metric metricKey="kubernetes.currentBytes" value={formatBytes(summary.currentBytes)} />
        <Metric metricKey="kubernetes.historicalBytes" value={formatBytes(summary.historicalBytes)} />
        <Metric metricKey="kubernetes.jsonRevisions" value={String(summary.decodedJson)} />
        <Metric metricKey="kubernetes.protobufRevisions" value={String(summary.decodedProtobuf)} />
        <Metric metricKey="kubernetes.encrypted" value={String(summary.encrypted)} />
      </div>

      <div className="ranking-grid">
        <div><h4>{t('kubernetes.topResources')}</h4><div className="table-wrap"><table><thead><tr><th>{t('kubernetes.groupResource')}</th><th>{t('kubernetes.objects')}</th><th>{t('kubernetes.current')}</th><th>{t('kubernetes.history')}</th></tr></thead>
          <tbody>{resources.map((item) => <tr key={`${item.apiGroup}/${item.resource}`}><td>{item.apiGroup || t('value.core')}/{item.resource}</td><td>{item.currentObjects}</td><td>{formatBytes(item.currentBytes)}</td><td>{formatBytes(item.historicalBytes)}</td></tr>)}</tbody>
        </table></div></div>
        <div><h4>{t('kubernetes.topNamespaces')}</h4><div className="table-wrap"><table><thead><tr><th>{t('kubernetes.namespace')}</th><th>{t('kubernetes.objects')}</th><th>{t('kubernetes.current')}</th><th>{t('kubernetes.history')}</th></tr></thead>
          <tbody>{namespaces.map((item) => <tr key={item.namespace || 'cluster-scoped'}><td>{item.namespace || t('value.clusterScoped')}</td><td>{item.currentObjects}</td><td>{formatBytes(item.currentBytes)}</td><td>{formatBytes(item.historicalBytes)}</td></tr>)}</tbody>
        </table></div></div>
      </div>

      <div className="section-title"><h4>{t('kubernetes.objectList')}</h4><button type="button" onClick={() => { setFilters(initialObjectFilters); setPage(1); }}>{t('semantic.resetFilters')}</button></div>
      <div className="filters object-filters">
        <label>{t('kubernetes.apiGroup')}<input value={filters.group} onChange={(event) => { setFilters({ ...filters, group: event.target.value }); setPage(1); }} placeholder="apps" /></label>
        <label>{t('kubernetes.resource')}<input value={filters.resource} onChange={(event) => { setFilters({ ...filters, resource: event.target.value }); setPage(1); }} placeholder="deployments" /></label>
        <label>{t('kubernetes.namespace')}<input value={filters.namespace} onChange={(event) => { setFilters({ ...filters, namespace: event.target.value }); setPage(1); }} /></label>
        <label>{t('kubernetes.minimumBytes')}<input type="number" min="0" value={filters.minSize} onChange={(event) => { setFilters({ ...filters, minSize: event.target.value }); setPage(1); }} /></label>
        <label>{t('kubernetes.minimumRevisions')}<input type="number" min="0" value={filters.minRevisions} onChange={(event) => { setFilters({ ...filters, minRevisions: event.target.value }); setPage(1); }} /></label>
        <label>{t('kubernetes.decodeStatus')}<select value={filters.decodeStatus} onChange={(event) => { setFilters({ ...filters, decodeStatus: event.target.value }); setPage(1); }}><option value="">{t('physical.all')}</option><option value="decoded_json">{t('decode.decoded_json')}</option><option value="decoded_protobuf">{t('decode.decoded_protobuf')}</option><option value="encrypted">{t('decode.encrypted')}</option><option value="protobuf_unsupported">{t('decode.protobuf_unsupported')}</option><option value="decode_failed">{t('decode.decode_failed')}</option><option value="format_unknown">{t('decode.format_unknown')}</option><option value="path_unknown">{t('decode.path_unknown')}</option></select></label>
        <label>{t('kubernetes.field')}<select value={filters.field} onChange={(event) => { setFilters({ ...filters, field: event.target.value }); setPage(1); }}><option value="">{t('physical.all')}</option><option value="spec">spec</option><option value="status">status</option><option value="managedFields">managedFields</option><option value="annotations">annotations</option><option value="labels">labels</option><option value="data">data</option><option value="binaryData">binaryData</option></select></label>
        <label>{t('kubernetes.sort')}<select value={filters.sort} onChange={(event) => { setFilters({ ...filters, sort: event.target.value as ObjectFilters['sort'] }); setPage(1); }}><option value="historical_bytes">{t('kubernetes.sortHistory')}</option><option value="current_bytes">{t('kubernetes.sortCurrent')}</option><option value="revision_count">{t('kubernetes.sortRevisions')}</option><option value="largest_field">{t('kubernetes.sortLargestField')}</option><option value="name">{t('kubernetes.sortName')}</option></select></label>
      </div>
      <div className="table-wrap"><table><thead><tr><th>{t('kubernetes.groupResource')}</th><th>{t('kubernetes.namespace')}</th><th>{t('kubernetes.name')}</th><th>{t('kubernetes.status')}</th><th>{t('kubernetes.current')}</th><th>{t('kubernetes.history')}</th><th>{t('kubernetes.revisions')}</th><th>{t('kubernetes.largestField')}</th><th></th></tr></thead>
        <tbody>{objects?.items.map((item) => <tr key={item.id}><td>{item.identity.apiGroup || t('value.core')}/{item.identity.resource}</td><td>{item.identity.namespace || t('value.clusterScoped')}</td><td>{item.identity.displayName}</td><td>{decodeStatusLabel(item.decodeStatus, t)}</td><td>{formatBytes(item.currentBytes)}</td><td>{formatBytes(item.historicalBytes)}</td><td>{item.revisionCount}</td><td>{item.largestFieldPath || '—'} {item.largestFieldBytes ? `(${formatBytes(item.largestFieldBytes)})` : ''}</td><td><button type="button" onClick={() => void inspect(item)}>{t('kubernetes.inspect')}</button></td></tr>)}</tbody>
      </table></div>
      <div className="pager"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>{t('pagination.previous')}</button><span>{t('pagination.objects', { page, count: objects?.total ?? 0 })}</span><button disabled={!objects || page * objects.pageSize >= objects.total} onClick={() => setPage((value) => value + 1)}>{t('pagination.next')}</button></div>

      {selectedObject && <div className="drawer" role="region" aria-label={t('kubernetes.objectDetails')}>
        <div className="section-title"><h4>{selectedObject.identity.displayName}</h4><button type="button" onClick={() => { setSelectedObject(null); setRevisions(null); }}>{t('actions.close')}</button></div>
        <p>{selectedObject.identity.apiGroup || t('value.core')}/{selectedObject.identity.resource} · {selectedObject.identity.namespace || t('value.clusterScoped')} · {decodeStatusLabel(selectedObject.decodeStatus, t)}</p>
        <p className="hash">{t('semantic.keyHash', { hash: selectedObject.keyHash })}</p>
        <h4>{t('kubernetes.selectedFields')}</h4>
        <div className="table-wrap"><table><thead><tr><th>{t('kubernetes.revision')}</th><th>{t('kubernetes.path')}</th><th>{t('kubernetes.bytes')}</th><th>{t('kubernetes.type')}</th><th>{t('kubernetes.hash')}</th></tr></thead><tbody>
          {revisions?.items.flatMap((revision) => revision.fields.map((field) => <tr key={`${revision.mainRevision}-${revision.subRevision}-${field.path}`}><td>{revision.mainRevision}.{revision.subRevision}</td><td>{field.path}</td><td>{formatBytes(field.byteSize)}</td><td>{field.typeClass}</td><td><code>{field.hash.slice(0, 16)}</code></td></tr>))}
        </tbody></table></div>
        <h4>{t('kubernetes.adjacentChanges')}</h4>
        <div className="table-wrap"><table><thead><tr><th>{t('kubernetes.revisions')}</th><th>{t('kubernetes.changedPaths')}</th><th>{t('kubernetes.byteDelta')}</th><th>{t('kubernetes.classification')}</th></tr></thead><tbody>
          {revisions?.diffs.map((diff) => <tr key={`${diff.previousMainRevision}-${diff.currentMainRevision}`}><td>{diff.previousMainRevision} → {diff.currentMainRevision}</td><td>{[...diff.addedPaths, ...diff.removedPaths, ...diff.modifiedPaths].join(', ') || t('value.none')}</td><td>{diff.byteDelta}</td><td>{diff.timestampOnly ? t('kubernetes.timestampsOnly') : diff.statusOnly ? t('kubernetes.statusOnly') : diff.managedFieldsOnly ? t('kubernetes.managedFieldsOnly') : t('kubernetes.structural')}</td></tr>)}
        </tbody></table></div>
      </div>}
    </>}
  </div>;
}

function Metric({ metricKey, value }: { metricKey: MetricKey; value: string }) {
  const { locale } = useTranslation();
  const copy = metric(locale, metricKey);
  return <div className="metric"><span className="metric-label">{copy.label}<span className="metric-help" role="img" aria-label={copy.help} title={copy.help} tabIndex={0}>?</span></span><strong>{value}</strong></div>;
}

function versionEvidence(task: Task, t: Translate): string {
  if (task.etcdVersionSource === 'database_metadata') {
    return t('version.database', { version: task.etcdVersion ?? t('value.unavailable') });
  }
  if (task.etcdVersionSource === 'manual') {
    const version = task.etcdVersion ?? t('value.unavailable');
    const detected = task.detectedEtcdVersion;
    if (detected && !version.replace(/^v/, '').startsWith(`${detected}.`)) {
      return t('version.manualDetected', { manual: version, detected });
    }
    return task.etcdVersionExact ? t('version.manual', { version }) : t('version.manualUnconfirmed', { version });
  }
  return task.etcdVersion ? t('version.unknownSource', { version: task.etcdVersion }) : t('version.unknown');
}
