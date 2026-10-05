/**
 * AetherDB TypeScript Type Definitions
 * "A persistent distributed state + semantic memory layer for AI agents."
 */

export interface AetherConfig {
  endpoint?: string;
  apiKey?: string;
  timeoutMs?: number;
  timeout?: number;
  headers?: Record<string, string>;
}

export class AetherError extends Error {
  statusCode: number | null;
  responseBody: any;
  constructor(message: string, statusCode?: number | null, responseBody?: any);
}

export interface VectorSearchResult<T = any> {
  id: string;
  score: number;
  metadata: T | null;
}

export interface RememberParams<M = Record<string, any>> {
  id?: string;
  memory_id?: string;
  memoryId?: string;
  text: string;
  embedding: number[];
  metadata?: M;
}

export interface RememberResult {
  status: string;
  agentId: string;
  memoryId: string;
  id: string;
}

export interface RecallParams {
  query?: string;
  embedding: number[];
  topK?: number;
  top_k?: number;
}

export interface RecallResult<M = Record<string, any>> {
  id: string;
  memoryId: string;
  score: number;
  text: string;
  metadata: M | null;
}

export class AetherVectorClient {
  constructor(transport: any);
  upsert(id: string, vector: number[], metadata?: Record<string, any> | string | null): Promise<{ status: string }>;
  search<T = any>(vector: number[], topK?: number): Promise<VectorSearchResult<T>[]>;
}

export class AetherAtomicClient {
  constructor(transport: any);
  incr(key: string, amount?: number): Promise<number>;
}

export class AgentStateClient {
  constructor(transport: any, agentId: string);
  set<T = any>(state: T): Promise<{ status: string; agent_id: string }>;
  set<T = any>(key: string, state: T): Promise<{ status: string; agent_id: string }>;
  get<T = any>(key?: string): Promise<T | null>;
  delete(key?: string): Promise<{ status: string; deleted: boolean; agent_id: string }>;
  del(key?: string): Promise<{ status: string; deleted: boolean; agent_id: string }>;
  incr(key: string, amount?: number): Promise<number>;
}

export class AgentMemoryClient {
  constructor(transport: any, agentId: string);
  remember<M = Record<string, any>>(params: RememberParams<M>): Promise<RememberResult>;
  recall<M = Record<string, any>>(params: RecallParams): Promise<RecallResult<M>[]>;
}

export class AgentClient {
  agentId: string;
  state: AgentStateClient;
  memory: AgentMemoryClient;
  constructor(transport: any, agentId: string);
}

export class AetherDB {
  constructor(config?: string | AetherConfig);
  endpoint: string;
  headers: Record<string, string>;
  vector: AetherVectorClient;
  atomic: AetherAtomicClient;

  agent(agentId: string): AgentClient;
  get<T = any>(key: string): Promise<T | null>;
  set(key: string, value: any): Promise<{ status: string }>;
  del(key: string): Promise<{ status: string }>;
  incr(key: string, amount?: number): Promise<number>;
  health(): Promise<{ status: string; engine: string; version: string }>;
}
