# Deep Dive: AVX2 SIMD & HNSW Vector Engine

AetherDB integrates high-dimensional vector search directly into the database engine. Instead of offloading embeddings to an external vector database, vector math and graph indexing execute natively in Rust using CPU SIMD intrinsics.

---

## 1. AVX2 & FMA Hardware SIMD Acceleration

Dense vector embeddings (e.g. 1536-dimensional OpenAI embeddings, 768-dimensional BGE embeddings) require intensive floating-point dot product and norm computations:

$$\text{Cosine Similarity}(A, B) = \frac{A \cdot B}{\|A\| \|B\|} = \frac{\sum_{i=1}^D A_i B_i}{\sqrt{\sum_{i=1}^D A_i^2} \sqrt{\sum_{i=1}^D B_i^2}}$$

### Vectorized Execution Pipeline

`aether-simd` utilizes x86-64 **AVX2 (Advanced Vector Extensions 2)** and **FMA (Fused Multiply-Add)** instruction sets:
- Loads **8 single-precision 32-bit floats** per 256-bit vector register (`__m256`).
- Executes 8 multiply-accumulate operations in a single CPU cycle via `_mm256_fmadd_ps`.
- Unrolls loops 4x (processing 32 dimensions per iteration) to maximize CPU pipeline occupancy and instruction-level parallelism.

```text
256-bit AVX2 Register (a):  [ a7 | a6 | a5 | a4 | a3 | a2 | a1 | a0 ]
                             ×    ×    ×    ×    ×    ×    ×    ×
256-bit AVX2 Register (b):  [ b7 | b6 | b5 | b4 | b3 | b2 | b1 | b0 ]
                             +    +    +    +    +    +    +    +
Accumulator Register (acc): [ s7 | s6 | s5 | s4 | s3 | s2 | s1 | s0 ]
```

### Dynamic CPU Dispatch

At runtime, AetherDB probes CPU feature flags using `is_x86_feature_detected!("avx2")`:
- **AVX2 Supported:** Executes vectorized hardware kernels (`~300% to 500% speedup`).
- **Fallback:** Executes scalar Rust fallback kernels with auto-vectorization hints for ARM / older CPUs.

---

## 2. Hierarchical Navigable Small World (HNSW) Indexing

For large vector collections ($>10,000$ vectors), linear brute-force scanning is $O(N \cdot D)$. AetherDB implements an in-memory **HNSW multi-layer graph index** (`aether-vector`):

```text
Layer 2 (Express):    [Node A] ─────────────────────────► [Node X]
                        │                                    │
Layer 1 (Highway):    [Node A] ────────► [Node M] ────────► [Node X]
                        │                   │                │
Layer 0 (Dense Base): [Node A] ─► [Node B] ─► [Node M] ─► [Node Q] ─► [Node X]
```

### Properties & Search Invariants
- **Logarithmic Traversal:** Queries start at the sparse top layer and greedily zoom into nearest neighbors before descending to denser base layers ($O(\log N)$ average query complexity).
- **Partitioned Graphs:** Each agent’s vector memories are namespace-isolated by tenant and agent ID (`t:<tenant_id>:agent:<agent_id>:`), preventing cross-agent memory pollution.
- **LSM Recovery:** When vectors are indexed, metadata and raw embedding floats are persisted into the LSM Write-Ahead Log. On startup, the HNSW graph is reconstructed deterministically from disk.
