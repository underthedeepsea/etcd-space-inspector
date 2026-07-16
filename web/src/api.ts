export type TaskStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface Task {
  taskId: string;
  name: string;
  inputType: string;
  etcdVersion?: string;
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
  inputType: 'snapshot' | 'raw-db';
  etcdVersion: string;
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
