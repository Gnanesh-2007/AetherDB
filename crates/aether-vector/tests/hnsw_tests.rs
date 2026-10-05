use aether_vector::HnswIndex;
use rand::Rng;

fn random_unit_vector(dims: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    let mut vec: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut vec {
            *x /= norm;
        }
    }
    vec
}

#[test]
fn test_hnsw_high_dimensional_recall_accuracy() {
    let dims = 64;
    let n_vectors = 500;
    let mut index = HnswIndex::new(16, 64, 32);

    let mut ground_truth_vectors = Vec::new();

    for i in 0..n_vectors {
        let v = random_unit_vector(dims);
        let id = format!("vec_{}", i);
        index.insert(&id, v.clone(), None).unwrap();
        ground_truth_vectors.push((id, v));
    }

    assert_eq!(index.len(), n_vectors);

    // Query 20 test vectors and measure recall @ 5 against brute-force linear scan
    let mut total_hits = 0;
    let k = 5;
    let test_queries = 20;

    for _ in 0..test_queries {
        let query = random_unit_vector(dims);

        // Ground truth linear scan
        let mut ground_truth: Vec<(String, f32)> = ground_truth_vectors
            .iter()
            .map(|(id, v)| (id.clone(), aether_simd::cosine_similarity(&query, v)))
            .collect();
        ground_truth.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top_gt: Vec<String> = ground_truth.into_iter().take(k).map(|(id, _)| id).collect();

        // HNSW search
        let hnsw_results = index.search(&query, k);
        let top_hnsw: Vec<String> = hnsw_results.into_iter().map(|(id, _, _)| id).collect();

        for id in &top_hnsw {
            if top_gt.contains(id) {
                total_hits += 1;
            }
        }
    }

    let recall = (total_hits as f64) / ((test_queries * k) as f64);
    println!("HNSW Recall @ {}: {:.2}%", k, recall * 100.0);
    // HNSW graph must have >= 85% recall even on modest test parameters
    assert!(recall >= 0.85, "Expected high recall, got: {}", recall);
}
