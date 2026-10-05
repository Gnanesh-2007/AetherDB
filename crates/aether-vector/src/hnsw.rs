use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};

use aether_core::error::Result;

pub type NodeId = u32;

#[derive(Debug, Clone, PartialEq)]
struct DistNode {
    dist: f32,
    node: NodeId,
}

impl Eq for DistNode {}

impl Ord for DistNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap behavior in BinaryHeap (or natural for max-heap)
        self.dist.partial_cmp(&other.dist).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for DistNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Hierarchical Navigable Small World (HNSW) Approximate Nearest Neighbor Graph Index.
#[derive(Debug, Serialize, Deserialize)]
pub struct HnswIndex {
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub ml: f64,
    pub max_layer: usize,
    pub entry_point: Option<NodeId>,
    pub vectors: Vec<Vec<f32>>,
    pub id_to_key: Vec<String>,
    pub key_to_id: HashMap<String, NodeId>,
    pub metadata: Vec<Option<String>>,
    /// layers[layer][node_id] = Vec<neighbor_id>
    pub layers: Vec<Vec<Vec<NodeId>>>,
}

impl HnswIndex {
    pub fn new(m: usize, ef_construction: usize, ef_search: usize) -> Self {
        let m = m.max(4);
        let m0 = 2 * m;
        let ml = 1.0 / (m as f64).ln();

        Self {
            m,
            m0,
            ef_construction,
            ef_search,
            ml,
            max_layer: 0,
            entry_point: None,
            vectors: Vec::new(),
            id_to_key: Vec::new(),
            key_to_id: HashMap::new(),
            metadata: Vec::new(),
            layers: Vec::new(),
        }
    }

    /// Calculate distance between query and stored node vector (Cosine Distance: 1.0 - CosineSimilarity)
    #[inline]
    fn dist(&self, query: &[f32], node: NodeId) -> f32 {
        let target = &self.vectors[node as usize];
        let sim = aether_simd::cosine_similarity(query, target);
        1.0 - sim
    }

    /// Calculate distance between two stored node vectors
    #[inline]
    fn dist_nodes(&self, node_a: NodeId, node_b: NodeId) -> f32 {
        let va = &self.vectors[node_a as usize];
        let vb = &self.vectors[node_b as usize];
        let sim = aether_simd::cosine_similarity(va, vb);
        1.0 - sim
    }

    /// Random layer generation following exponential decay distribution
    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let r: f64 = rng.gen_range(0.0000001..1.0);
        (-r.ln() * self.ml) as usize
    }

    /// Inserts a vector with ID and optional metadata into the HNSW graph.
    pub fn insert(&mut self, key: &str, vector: Vec<f32>, meta: Option<String>) -> Result<NodeId> {
        if let Some(&existing_id) = self.key_to_id.get(key) {
            // Update vector in-place
            self.vectors[existing_id as usize] = vector;
            self.metadata[existing_id as usize] = meta;
            return Ok(existing_id);
        }

        let new_id = self.vectors.len() as NodeId;
        let level = self.random_level();

        self.vectors.push(vector);
        self.id_to_key.push(key.to_string());
        self.key_to_id.insert(key.to_string(), new_id);
        self.metadata.push(meta);

        // Ensure layers exist up to max(level, max_layer)
        let required_layers = level.max(self.max_layer) + 1;
        while self.layers.len() < required_layers {
            self.layers.push(Vec::new());
        }

        for l in 0..=level {
            while self.layers[l].len() <= new_id as usize {
                self.layers[l].push(Vec::new());
            }
        }

        let curr_entry = match self.entry_point {
            None => {
                self.entry_point = Some(new_id);
                self.max_layer = level;
                return Ok(new_id);
            }
            Some(ep) => ep,
        };

        let mut curr_obj = curr_entry;
        let q = self.vectors[new_id as usize].clone();

        // 1. Traverse top down from max_layer to level + 1 greedily
        if level < self.max_layer {
            for l in ((level + 1)..=self.max_layer).rev() {
                let mut changed = true;
                while changed {
                    changed = false;
                    let curr_dist = self.dist(&q, curr_obj);
                    if let Some(neighbors) = self.layers[l].get(curr_obj as usize) {
                        for &neighbor in neighbors {
                            let d = self.dist(&q, neighbor);
                            if d < curr_dist {
                                curr_obj = neighbor;
                                changed = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        // 2. From min(level, max_layer) down to 0, search layer and connect
        let top_l = level.min(self.max_layer);
        for l in (0..=top_l).rev() {
            let candidates = self.search_layer_internal(&q, curr_obj, self.ef_construction, l);
            let m_max = if l == 0 { self.m0 } else { self.m };
            
            // Select M closest neighbors
            let neighbors = self.select_neighbors(&candidates, m_max);

            for &neighbor in &neighbors {
                self.layers[l][new_id as usize].push(neighbor);
                self.layers[l][neighbor as usize].push(new_id);

                // Prune neighbor's connections if exceeding m_max
                if self.layers[l][neighbor as usize].len() > m_max {
                    self.prune_connections(neighbor, l, m_max);
                }
            }

            if let Some(closest) = candidates.first() {
                curr_obj = closest.node;
            }
        }

        if level > self.max_layer {
            self.max_layer = level;
            self.entry_point = Some(new_id);
        }

        Ok(new_id)
    }

    /// Internal bounded beam search on a specific layer
    fn search_layer_internal(
        &self,
        query: &[f32],
        ep: NodeId,
        ef: usize,
        layer: usize,
    ) -> Vec<DistNode> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new(); // Min-heap (closest on top)
        let mut w = BinaryHeap::new();          // Max-heap (furthest on top) to maintain top-ef

        let ep_dist = self.dist(query, ep);
        visited.insert(ep);

        // For min-heap, we negate dist or use Custom struct
        candidates.push(std::cmp::Reverse(DistNode { dist: ep_dist, node: ep }));
        w.push(DistNode { dist: ep_dist, node: ep });

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            let furthest_w_dist = w.peek().unwrap().dist;
            if c.dist > furthest_w_dist && w.len() >= ef {
                break;
            }

            if let Some(neighbors) = self.layers[layer].get(c.node as usize) {
                for &neighbor in neighbors {
                    if visited.insert(neighbor) {
                        let furthest_w_dist = w.peek().unwrap().dist;
                        let d = self.dist(query, neighbor);

                        if d < furthest_w_dist || w.len() < ef {
                            candidates.push(std::cmp::Reverse(DistNode { dist: d, node: neighbor }));
                            w.push(DistNode { dist: d, node: neighbor });

                            if w.len() > ef {
                                w.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut results: Vec<DistNode> = w.into_vec();
        results.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
        results
    }

    fn select_neighbors(&self, candidates: &[DistNode], m_max: usize) -> Vec<NodeId> {
        candidates.iter().take(m_max).map(|cn| cn.node).collect()
    }

    fn prune_connections(&mut self, node: NodeId, layer: usize, m_max: usize) {
        let neighbors = &self.layers[layer][node as usize];
        let mut dist_nodes: Vec<DistNode> = neighbors
            .iter()
            .map(|&n| DistNode {
                dist: self.dist_nodes(node, n),
                node: n,
            })
            .collect();

        dist_nodes.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
        dist_nodes.truncate(m_max);

        self.layers[layer][node as usize] = dist_nodes.into_iter().map(|dn| dn.node).collect();
    }

    /// Query the graph for Top-K approximate nearest neighbors.
    /// Returns: Vec<(Key, SimilarityScore, Metadata)>
    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<(String, f32, Option<String>)> {
        if self.vectors.is_empty() || self.entry_point.is_none() {
            return Vec::new();
        }

        let mut curr_obj = self.entry_point.unwrap();

        // 1. Greedy routing down to layer 1
        for l in (1..=self.max_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                let curr_dist = self.dist(query, curr_obj);
                if let Some(neighbors) = self.layers[l].get(curr_obj as usize) {
                    for &neighbor in neighbors {
                        let d = self.dist(query, neighbor);
                        if d < curr_dist {
                            curr_obj = neighbor;
                            changed = true;
                            break;
                        }
                    }
                }
            }
        }

        // 2. Beam search on layer 0 with ef_search
        let ef = self.ef_search.max(top_k);
        let candidates = self.search_layer_internal(query, curr_obj, ef, 0);

        candidates
            .into_iter()
            .take(top_k)
            .map(|dn| {
                let id_idx = dn.node as usize;
                let key = self.id_to_key[id_idx].clone();
                let score = 1.0 - dn.dist; // Convert distance back to similarity
                let meta = self.metadata[id_idx].clone();
                (key, score, meta)
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// Thread-safe concurrent HNSW Vector Store
pub struct ConcurrentHnswIndex {
    inner: RwLock<HnswIndex>,
}

impl ConcurrentHnswIndex {
    pub fn new(m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self {
            inner: RwLock::new(HnswIndex::new(m, ef_construction, ef_search)),
        }
    }

    pub fn insert(&self, key: &str, vector: Vec<f32>, meta: Option<String>) -> Result<NodeId> {
        self.inner.write().insert(key, vector, meta)
    }

    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<(String, f32, Option<String>)> {
        self.inner.read().search(query, top_k)
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic_insert_and_search() {
        let mut index = HnswIndex::new(8, 32, 16);

        index.insert("doc_1", vec![1.0, 0.0, 0.0, 0.0], Some("math".into())).unwrap();
        index.insert("doc_2", vec![0.9, 0.1, 0.0, 0.0], Some("physics".into())).unwrap();
        index.insert("doc_3", vec![0.0, 0.0, 1.0, 0.0], Some("art".into())).unwrap();

        let query = vec![0.95, 0.05, 0.0, 0.0];
        let results = index.search(&query, 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "doc_1");
        assert_eq!(results[1].0, "doc_2");
        assert!(results[0].1 > 0.95);
    }
}
