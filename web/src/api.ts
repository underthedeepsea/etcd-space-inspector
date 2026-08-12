export type TaskStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface Task {
  taskId: string;
  name: string;
  inputType: string;
  etcdVersion?: string;
  etcdVersionSource?: 'manual' | 'database_metadata' | 'unknown';
  etcdVersionExact?: boolean;
  detectedEtcdVersion?: string;
  inputFile: string;
  inputSize: number;
  sha256: string;
  status: TaskStatus;
  progress: number;
  currentStage?: string;
  errorCode?: string;
  errorMessage?: string;
  createdAt: string;
}

export interface CreateTask {
  name: string;
  inputPath: string;
  inputType: 'snapshot' | 'raw-db' | 'log' | 'audit' | 'metrics';
  etcdVersion: string;
}

export interface AuditEvent {
  eventId: number; lineNumber: number; auditIdHash: string; observedAt?: string;
  stage: string; stageRank: number; verb: string; username: string; usernameHash: string;
  userAgent: string; userAgentHash: string; sourceNetwork: string; sourceIpHash: string;
  apiGroup: string; resource: string; subresource: string; namespace: string;
  objectName: string; displayName: string; objectKeyHash: string; responseCode: number;
  requestObjectBytes: number; responseObjectBytes: number; parseStatus: string;
}

export interface AuditTimeline {
  summary: { totalLines: number; validEvents: number; writeEvents: number; unknownLines: number; parseErrors: number; deduplicatedEvents: number; firstObservedAt?: string; lastObservedAt?: string };
  items: AuditEvent[]; total: number; page: number; pageSize: number;
  byUsername: EvidenceCount[]; byUserAgent: EvidenceCount[]; bySourceNetwork: EvidenceCount[];
  byVerb: EvidenceCount[]; byResource: EvidenceCount[]; byNamespace: EvidenceCount[];
}

export type AuditMatchLevel = 'high' | 'medium' | 'low' | 'unverified';
export interface AuditCandidate {
  username: string; usernameHash: string; userAgent: string; userAgentHash: string;
  sourceNetwork: string; sourceIpHash: string; highestMatchLevel: AuditMatchLevel;
  exactObjectMatches: number; resourceMatches: number; namespaceMatches: number; writes: number;
  requestObjectBytes: number; responseObjectBytes: number;
}
export interface AuditEvidence {
  diffId: string; auditTaskId: string; auditTaskName: string; auditTaskSha256: string;
  from: string; to: string; windowSeconds: number; coverage: EvidenceCoverage;
  sourceCompatibility: 'unverified'; objectsAvailable: boolean; candidates: AuditCandidate[];
  items: AuditEvent[]; total: number; page: number; pageSize: number;
}

export interface LogEvent {
  eventId: number;
  lineNumber: number;
  observedAt?: string;
  eventType: string;
  severity: 'INFO' | 'WARN' | 'ERROR' | 'UNKNOWN';
  source: string;
  durationMs?: number;
  revision?: number;
  dbSizeBytes?: number;
  parseStatus: string;
  messageFingerprint: string;
}

export interface LogTimeline {
  summary: {
    totalLines: number;
    recognizedEvents: number;
    unknownLines: number;
    parseErrors: number;
    firstObservedAt?: string;
    lastObservedAt?: string;
  };
  items: LogEvent[];
  total: number;
  page: number;
  pageSize: number;
}

export type EvidenceCoverage = 'full' | 'partial' | 'none' | 'unknown';

export type MetricType = 'db_total_bytes' | 'db_in_use_bytes' | 'quota_bytes' | 'put_total' | 'delete_total' | 'backend_commit_seconds' | 'wal_fsync_seconds';
export interface MetricPoint { observedAt: string; value: number }
export interface MetricCurve { metricType: MetricType; points: MetricPoint[] }
export interface MetricSeries { series: { metricType: MetricType; sourceMetricName: string; instance: string; job: string; memberId: string; histogramLe?: number; seriesHash: string }; samples: MetricPoint[] }
export interface MetricsSummary { totalSeries: number; supportedSeries: number; unsupportedSeries: number; totalSamples: number; validSamples: number; discardedSamples: number; firstObservedAt?: string; lastObservedAt?: string; instanceCount: number; metricTypes: MetricType[] }
export interface MetricsTimeline { summary: MetricsSummary; series: MetricSeries[]; total: number; curves: MetricCurve[]; page: number; pageSize: number }
export interface MetricPeak { observedAt: string; value: number }
export interface MetricsEvidence {
  diffId: string; metricsTaskId: string; metricsTaskName: string; metricsTaskSha256: string;
  from: string; to: string; windowSeconds: number; coverage: EvidenceCoverage;
  sourceCompatibility: 'unverified'; evidenceOnly: true; causalityEstablished: false;
  growthMetric: MetricType; growthBaselineBytes: number; growthThresholdBytes: number; growthStartedAt?: string;
  dbTotalDeltaBytes: number; dbInUseDeltaBytes: number; maxDefragReclaimableBytes: number; quotaPeakRatio: number;
  largestGrowthInterval?: { from: string; to: string; deltaBytes: number };
  peakPutRate: MetricPeak; peakDeleteRate: MetricPeak; putTemporallyAligned: boolean; deleteTemporallyAligned: boolean;
  backendCommitP99: MetricPeak; walFsyncP99: MetricPeak; curves: MetricCurve[];
}

export interface EvidenceCount {
  name: string;
  count: number;
}

export interface DiffLogEvidence {
  diffId: string;
  logTaskId: string;
  logTaskName: string;
  logTaskSha256: string;
  logFirstObservedAt?: string;
  logLastObservedAt?: string;
  coverage: EvidenceCoverage;
  sourceCompatibility: 'unverified';
  from: string;
  to: string;
  windowSeconds: number;
  total: number;
  byEventType: EvidenceCount[];
  bySeverity: EvidenceCount[];
  bySource: EvidenceCount[];
  items: LogEvent[];
  page: number;
  pageSize: number;
  evidenceOnly: true;
  attributionAvailable: false;
}

export interface SpaceSummary {
  physicalFileSize: number;
  pageSize: number;
  pageCount: number;
  inUsePageBytes: number;
  freePageBytes: number;
  fragmentationRatio: number;
  metaPages: number;
  branchPages: number;
  leafPages: number;
  freelistPages: number;
  overflowPages: number;
  freePages: number;
  unknownPages: number;
}

export interface PageStat {
  pageId: number;
  pageType: string;
  overflow: number;
  totalBytes: number;
  usedBytes: number;
  freeBytes: number;
  utilization: number;
}

export interface BucketStat {
  bucketPath: string;
  depth: number;
  keyCount: number;
  branchBytes: number;
  leafBytes: number;
  overflowBytes: number;
  totalBytes: number;
  usedBytes: number;
}

export interface PageResult {
  items: PageStat[];
  total: number;
  page: number;
  pageSize: number;
}

export interface MVCCSummary {
  semanticAvailable: boolean;
  revisionCount: number;
  decodeErrors: number;
  currentKeyCount: number;
  currentStoredBytes: number;
  historicalVersions: number;
  historicalBytes: number;
  tombstoneCount: number;
  tombstoneBytes: number;
}

export interface KeyRecord {
  id: number;
  keyHash: string;
  keyText: string;
  prefix: string;
  present: boolean;
  createRevision: number;
  modRevision: number;
  version: number;
  leaseId: number;
  currentKeyBytes: number;
  currentValueBytes: number;
  currentStoredBytes: number;
  historicalVersions: number;
  historicalBytes: number;
  tombstoneCount: number;
  tombstoneBytes: number;
  revisionCount: number;
  historicalAmplification: number;
}

export interface PrefixStat {
  prefix: string;
  depth: number;
  currentKeyCount: number;
  currentValueBytes: number;
  historicalVersions: number;
  historicalBytes: number;
  tombstoneCount: number;
  tombstoneBytes: number;
  maxValueBytes: number;
}

export interface RevisionRecord {
  keyHash: string;
  keyText: string;
  mainRevision: number;
  subRevision: number;
  createRevision: number;
  modRevision: number;
  version: number;
  leaseId: number;
  valueBytes: number;
  storedBytes: number;
  tombstone: boolean;
  valueHash: string;
}

export interface KeyResult {
  items: KeyRecord[];
  total: number;
  page: number;
  pageSize: number;
}

export interface KeyFilters {
  prefix: string;
  minSize: string;
  minRevisions: string;
  tombstone: boolean;
  sort: 'key' | 'current_bytes' | 'historical_bytes' | 'revision_count' | 'tombstone_count';
}

export interface KubernetesIdentity {
  storagePrefix: string;
  apiGroup: string;
  resource: string;
  namespace: string;
  name: string;
  displayName: string;
  crd: boolean;
  clusterScoped: boolean;
  sensitive: boolean;
}

export interface KubernetesSummary {
  semanticAvailable: boolean;
  currentObjects: number;
  currentBytes: number;
  historicalBytes: number;
  decodedJson: number;
  decodedProtobuf: number;
  encrypted: number;
  decodeFailures: number;
}

export interface ResourceStat {
  apiGroup: string;
  resource: string;
  currentObjects: number;
  currentBytes: number;
  historicalBytes: number;
}

export interface NamespaceStat {
  namespace: string;
  currentObjects: number;
  currentBytes: number;
  historicalBytes: number;
}

export interface KubernetesObject {
  id: number;
  keyHash: string;
  identity: KubernetesIdentity;
  decodeStatus: string;
  present: boolean;
  currentBytes: number;
  historicalBytes: number;
  revisionCount: number;
  largestFieldPath: string;
  largestFieldBytes: number;
}

export interface KubernetesField {
  path: string;
  byteSize: number;
  typeClass: string;
  hash: string;
}

export interface KubernetesRevision {
  keyHash: string;
  mainRevision: number;
  subRevision: number;
  identity: KubernetesIdentity;
  contentType: string;
  decodeStatus: string;
  valueBytes: number;
  fields: KubernetesField[];
}

export interface KubernetesDiff {
  previousMainRevision: number;
  currentMainRevision: number;
  addedPaths: string[];
  removedPaths: string[];
  modifiedPaths: string[];
  byteDelta: number;
  timestampOnly: boolean;
  statusOnly: boolean;
  managedFieldsOnly: boolean;
}

export interface ObjectResult {
  items: KubernetesObject[];
  total: number;
  page: number;
  pageSize: number;
}

export interface ObjectRevisionResult {
  items: KubernetesRevision[];
  diffs: KubernetesDiff[];
  total: number;
  page: number;
  pageSize: number;
}

export interface ObjectFilters {
  group: string;
  resource: string;
  namespace: string;
  minSize: string;
  minRevisions: string;
  decodeStatus: string;
  field: string;
  sort: 'name' | 'current_bytes' | 'historical_bytes' | 'revision_count' | 'largest_field';
}

export interface Comparison {
  diffId: string;
  name: string;
  baselineTaskId: string;
  targetTaskId: string;
	baselineObservedAt?: string;
	targetObservedAt?: string;
  status: TaskStatus;
  progress: number;
  currentStage?: string;
  errorCode?: string;
  errorMessage?: string;
  createdAt: string;
}

export interface CreateComparison {
  name: string;
  baselineTaskId: string;
  targetTaskId: string;
	baselineObservedAt?: string;
	targetObservedAt?: string;
}

export interface DiffSummary {
  baselineTaskId: string;
  targetTaskId: string;
  physicalAvailable: boolean;
  physicalUnavailableReason?: string;
  mvccAvailable: boolean;
  mvccUnavailableReason?: string;
  kubernetesAvailable: boolean;
  kubernetesUnavailableReason?: string;
  physicalFileSizeDelta: number;
  inUsePageBytesDelta: number;
  freePageBytesDelta: number;
  fragmentationRatioDelta: number;
  revisionCountDelta: number;
  currentKeyCountDelta: number;
  currentStoredBytesDelta: number;
  historicalVersionsDelta: number;
  historicalBytesDelta: number;
  tombstoneCountDelta: number;
  tombstoneBytesDelta: number;
  currentObjectsDelta: number;
  kubernetesCurrentBytesDelta: number;
  kubernetesHistoricalBytesDelta: number;
	observationWindowSeconds: number;
  revisionRateAvailable: boolean;
  averageRevisionsPerSecond?: number;
}

export interface DiffKey {
  keyHash: string;
  key: string;
  prefix: string;
  changeType: 'added' | 'deleted' | 'modified';
  currentBytesDelta: number;
  historicalBytesDelta: number;
  tombstoneBytesDelta: number;
  revisionCountDelta: number;
  totalBytesDelta: number;
}

export interface DiffKeyResult {
  items: DiffKey[];
  total: number;
  page: number;
  pageSize: number;
}

export interface DiffPrefix {
  prefix: string;
  currentKeyCountDelta: number;
  currentBytesDelta: number;
  historicalBytesDelta: number;
  tombstoneBytesDelta: number;
  totalBytesDelta: number;
}

export interface DiffResource {
  apiGroup: string;
  resource: string;
  currentObjectsDelta: number;
  currentBytesDelta: number;
  historicalBytesDelta: number;
  totalBytesDelta: number;
}

export interface DiffNamespace {
  namespace: string;
  currentObjectsDelta: number;
  currentBytesDelta: number;
  historicalBytesDelta: number;
  totalBytesDelta: number;
}

export interface DiffObject {
  keyHash: string; apiGroup: string; resource: string; namespace: string; displayName: string;
  changeType: 'added' | 'deleted' | 'modified'; currentBytesDelta: number;
  historicalBytesDelta: number; revisionCountDelta: number; totalBytesDelta: number;
}
export interface DiffObjectResult { items: DiffObject[]; total: number; objectsAvailable: boolean; page: number; pageSize: number }

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: init?.body ? { 'Content-Type': 'application/json', ...init.headers } : init?.headers,
  });
  if (!response.ok) {
    const error = (await response.json().catch(() => null)) as { message?: string } | null;
    throw new Error(error?.message ?? `Request failed (${response.status})`);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export async function listTasks(): Promise<Task[]> {
  const response = await request<{ items: Task[] }>('/api/v1/tasks');
  return response.items;
}

export function createTask(input: CreateTask): Promise<Task> {
  return request('/api/v1/tasks', { method: 'POST', body: JSON.stringify(input) });
}

export function startTask(id: string): Promise<void> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/start`, { method: 'POST' });
}

export function cancelTask(id: string): Promise<void> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
}

export function deleteTask(id: string): Promise<void> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}`, { method: 'DELETE' });
}

export function getOverview(id: string): Promise<SpaceSummary> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/overview`);
}

export function getTimeline(id: string, query: {
  from?: string;
  to?: string;
  eventType?: string;
  severity?: string;
  source?: string;
  page?: number;
  pageSize?: number;
} = {}): Promise<LogTimeline> {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== '') params.set(key, String(value));
  }
  const suffix = params.toString() ? `?${params.toString()}` : '';
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/timeline${suffix}`);
}

export function getAuditTimeline(id: string, query: { from?: string; to?: string; verb?: string; username?: string; resource?: string; namespace?: string; page?: number; pageSize?: number } = {}): Promise<AuditTimeline> {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) if (value !== undefined && value !== '') params.set(key, String(value));
  const suffix = params.toString() ? `?${params}` : '';
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/audit-timeline${suffix}`);
}

export function getMetricsTimeline(id: string, query: { from?: string; to?: string; metricType?: MetricType | ''; instance?: string; page?: number; pageSize?: number } = {}): Promise<MetricsTimeline> {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) if (value !== undefined && value !== '') params.set(key, String(value));
  const suffix = params.toString() ? `?${params}` : '';
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/metrics-timeline${suffix}`);
}

export function listPages(id: string, page: number, pageType: string): Promise<PageResult> {
  const query = new URLSearchParams({ page: String(page), pageSize: '50', sort: 'page_id' });
  if (pageType) query.set('type', pageType);
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/pages?${query}`);
}

export async function listBuckets(id: string): Promise<BucketStat[]> {
  const response = await request<{ items: BucketStat[] }>(`/api/v1/tasks/${encodeURIComponent(id)}/buckets?limit=20`);
  return response.items;
}

export function getMVCCSummary(id: string): Promise<MVCCSummary> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/mvcc-summary`);
}

export async function listPrefixes(id: string): Promise<PrefixStat[]> {
  const response = await request<{ items: PrefixStat[] }>(`/api/v1/tasks/${encodeURIComponent(id)}/prefixes?limit=20`);
  return response.items;
}

export function listKeys(id: string, page: number, filters: KeyFilters): Promise<KeyResult> {
  const query = new URLSearchParams({ page: String(page), pageSize: '50', sort: filters.sort, order: filters.sort === 'key' ? 'asc' : 'desc' });
  if (filters.prefix) query.set('prefix', filters.prefix);
  if (filters.minSize) query.set('minSize', filters.minSize);
  if (filters.minRevisions) query.set('minRevisions', filters.minRevisions);
  if (filters.tombstone) query.set('tombstone', 'true');
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/keys?${query}`);
}

export async function listKeyRevisions(id: string, keyId: number): Promise<RevisionRecord[]> {
  const response = await request<{ items: RevisionRecord[] }>(`/api/v1/tasks/${encodeURIComponent(id)}/keys/${keyId}/revisions?pageSize=100`);
  return response.items;
}

export function getKubernetesSummary(id: string): Promise<KubernetesSummary> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/kubernetes-summary`);
}

export async function listResources(id: string): Promise<ResourceStat[]> {
  const response = await request<{ items: ResourceStat[] }>(`/api/v1/tasks/${encodeURIComponent(id)}/resources?limit=20`);
  return response.items;
}

export async function listNamespaces(id: string): Promise<NamespaceStat[]> {
  const response = await request<{ items: NamespaceStat[] }>(`/api/v1/tasks/${encodeURIComponent(id)}/namespaces?limit=20`);
  return response.items;
}

export function listObjects(id: string, page: number, filters: ObjectFilters): Promise<ObjectResult> {
  const query = new URLSearchParams({
    page: String(page), pageSize: '50', sort: filters.sort,
    order: filters.sort === 'name' ? 'asc' : 'desc',
  });
  if (filters.group) query.set('group', filters.group);
  if (filters.resource) query.set('resource', filters.resource);
  if (filters.namespace) query.set('namespace', filters.namespace);
  if (filters.minSize) query.set('minSize', filters.minSize);
  if (filters.minRevisions) query.set('minRevisions', filters.minRevisions);
  if (filters.decodeStatus) query.set('decodeStatus', filters.decodeStatus);
  if (filters.field) query.set('field', filters.field);
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/objects?${query}`);
}

export function getKubernetesObject(id: string, objectId: number): Promise<KubernetesObject> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/objects/${objectId}`);
}

export function listObjectRevisions(id: string, objectId: number): Promise<ObjectRevisionResult> {
  return request(`/api/v1/tasks/${encodeURIComponent(id)}/objects/${objectId}/revisions?pageSize=100`);
}

export async function listComparisons(): Promise<Comparison[]> {
  const response = await request<{ items: Comparison[] }>('/api/v1/diffs');
  return response.items;
}

export function createComparison(input: CreateComparison): Promise<Comparison> {
  return request('/api/v1/diffs', { method: 'POST', body: JSON.stringify(input) });
}

export function getComparison(id: string): Promise<Comparison> {
	return request(`/api/v1/diffs/${encodeURIComponent(id)}`);
}

export function getDiffLogEvidence(diffId: string, logTaskId: string, page: number): Promise<DiffLogEvidence> {
  const query = new URLSearchParams({ logTaskId, page: String(page), pageSize: '50' });
  return request(`/api/v1/diffs/${encodeURIComponent(diffId)}/log-evidence?${query}`);
}

export function getDiffAuditEvidence(diffId: string, auditTaskId: string, page: number): Promise<AuditEvidence> {
  const query = new URLSearchParams({ auditTaskId, page: String(page), pageSize: '50' });
  return request(`/api/v1/diffs/${encodeURIComponent(diffId)}/audit-evidence?${query}`);
}

export function getMetricsEvidence(diffId: string, metricsTaskId: string): Promise<MetricsEvidence> {
  const query = new URLSearchParams({ metricsTaskId });
  return request(`/api/v1/diffs/${encodeURIComponent(diffId)}/metrics-evidence?${query}`);
}

export function listDiffObjects(id: string): Promise<DiffObjectResult> {
  return request(`/api/v1/diffs/${encodeURIComponent(id)}/objects?sort=total_bytes&order=desc&pageSize=20`);
}

export function cancelComparison(id: string): Promise<void> {
  return request(`/api/v1/diffs/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
}

export function deleteComparison(id: string): Promise<void> {
  return request(`/api/v1/diffs/${encodeURIComponent(id)}`, { method: 'DELETE' });
}

export function getDiffOverview(id: string): Promise<DiffSummary> {
  return request(`/api/v1/diffs/${encodeURIComponent(id)}/overview`);
}

export function listDiffKeys(id: string, order: 'asc' | 'desc', sort: 'total_bytes' | 'revision_count' = 'total_bytes'): Promise<DiffKeyResult> {
  return request(`/api/v1/diffs/${encodeURIComponent(id)}/keys?pageSize=20&sort=${sort}&order=${order}`);
}

export async function listDiffPrefixes(id: string): Promise<DiffPrefix[]> {
  const response = await request<{ items: DiffPrefix[] }>(`/api/v1/diffs/${encodeURIComponent(id)}/prefixes?limit=20&order=desc`);
  return response.items;
}

export async function listDiffResources(id: string): Promise<DiffResource[]> {
  const response = await request<{ items: DiffResource[] }>(`/api/v1/diffs/${encodeURIComponent(id)}/resources?limit=20&order=desc`);
  return response.items;
}

export async function listDiffNamespaces(id: string): Promise<DiffNamespace[]> {
  const response = await request<{ items: DiffNamespace[] }>(`/api/v1/diffs/${encodeURIComponent(id)}/namespaces?limit=20&order=desc`);
  return response.items;
}
