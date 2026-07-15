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
