export interface AetherConfig {
  endpoint?: string;
  apiKey?: string;
}

export interface VectorSearchResult<T = any> {
  id: string;
  score: number;
  metadata: T | null;
}

export class AetherVectorClient {
  upsert(id: string, vector: number[], metadata?: Record<string, any> | string | null): Promise<{ status: string }>;
  search<T = any>(vector: number[], topK?: number): Promise<VectorSearchResult<T>[]>;
}

export class AetherAtomicClient {
  incr(key: string, delta?: number): Promise<number>;
}

export class AetherDB {
  constructor(config?: AetherConfig);
  vector: AetherVectorClient;
  atomic: AetherAtomicClient;

  get<T = any>(key: string): Promise<T | null>;
  set(key: string, value: any): Promise<{ status: string }>;
  del(key: string): Promise<{ status: string }>;
  health(): Promise<{ status: string; engine: string; version: string }>;
}
