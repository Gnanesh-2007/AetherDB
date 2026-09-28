/// Hardware-accelerated SIMD vector similarity kernels.

/// Calculates dot product of two f32 vectors with chunked loop unrolling for SIMD vectorization.
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let len = a.len();
    let chunks = len / 8;
    let remainder = len % 8;

    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut sum4 = 0.0f32;
    let mut sum5 = 0.0f32;
    let mut sum6 = 0.0f32;
    let mut sum7 = 0.0f32;

    for i in 0..chunks {
        let idx = i * 8;
        sum0 += a[idx] * b[idx];
        sum1 += a[idx + 1] * b[idx + 1];
        sum2 += a[idx + 2] * b[idx + 2];
        sum3 += a[idx + 3] * b[idx + 3];
        sum4 += a[idx + 4] * b[idx + 4];
        sum5 += a[idx + 5] * b[idx + 5];
        sum6 += a[idx + 6] * b[idx + 6];
        sum7 += a[idx + 7] * b[idx + 7];
    }

    let mut total = sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7;

    let rem_start = chunks * 8;
    for i in 0..remainder {
        total += a[rem_start + i] * b[rem_start + i];
    }

    total
}

/// Calculates squared Euclidean distance (L2 norm squared) between two f32 vectors.
pub fn l2_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let len = a.len();
    let chunks = len / 8;
    let remainder = len % 8;

    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut sum4 = 0.0f32;
    let mut sum5 = 0.0f32;
    let mut sum6 = 0.0f32;
    let mut sum7 = 0.0f32;

    for i in 0..chunks {
        let idx = i * 8;
        let d0 = a[idx] - b[idx];
        let d1 = a[idx + 1] - b[idx + 1];
        let d2 = a[idx + 2] - b[idx + 2];
        let d3 = a[idx + 3] - b[idx + 3];
        let d4 = a[idx + 4] - b[idx + 4];
        let d5 = a[idx + 5] - b[idx + 5];
        let d6 = a[idx + 6] - b[idx + 6];
        let d7 = a[idx + 7] - b[idx + 7];

        sum0 += d0 * d0;
        sum1 += d1 * d1;
        sum2 += d2 * d2;
        sum3 += d3 * d3;
        sum4 += d4 * d4;
        sum5 += d5 * d5;
        sum6 += d6 * d6;
        sum7 += d7 * d7;
    }

    let mut total = sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7;

    let rem_start = chunks * 8;
    for i in 0..remainder {
        let d = a[rem_start + i] - b[rem_start + i];
        total += d * d;
    }

    total
}

/// Calculates Cosine Similarity between two f32 vectors.
/// Range: [-1.0, 1.0] where 1.0 means identical orientation.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let norm_a = dot_product(a, a).sqrt();
    let norm_b = dot_product(b, b).sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot / (norm_a * norm_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        assert_eq!(dot_product(&a, &b), 45.0);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);

        let c = vec![0.0, 1.0, 0.0];
        assert!((cosine_similarity(&a, &c) - 0.0).abs() < 1e-6);
    }
}
