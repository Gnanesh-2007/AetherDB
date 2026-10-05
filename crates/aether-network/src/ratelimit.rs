use aether_core::error::{AetherError, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::Instant;

struct TokenBucket {
    tokens: f64,
    max_tokens: f64,
    refill_rate_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(max_tokens: f64, refill_rate_per_sec: f64) -> Self {
        Self {
            tokens: max_tokens,
            max_tokens,
            refill_rate_per_sec,
            last_refill: Instant::now(),
        }
    }

    fn try_acquire(&mut self, count: f64) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;

        // Refill tokens based on elapsed time
        self.tokens = (self.tokens + elapsed * self.refill_rate_per_sec).min(self.max_tokens);

        if self.tokens >= count {
            self.tokens -= count;
            true
        } else {
            false
        }
    }
}

/// Multi-tenant token bucket rate limiter with backpressure defense.
pub struct RateLimiter {
    buckets: Mutex<HashMap<String, TokenBucket>>,
    default_capacity: f64,
    default_rate: f64,
}

impl RateLimiter {
    pub fn new(default_capacity: f64, default_rate: f64) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            default_capacity,
            default_rate,
        }
    }

    /// Checks if a tenant can execute a request, deducting tokens or returning 429 error.
    pub fn check_limit(&self, tenant_id: &str) -> Result<()> {
        let mut buckets = self.buckets.lock();
        let bucket = buckets
            .entry(tenant_id.to_string())
            .or_insert_with(|| TokenBucket::new(self.default_capacity, self.default_rate));

        if bucket.try_acquire(1.0) {
            Ok(())
        } else {
            Err(AetherError::RateLimitExceeded(format!(
                "Rate limit exceeded for tenant '{}'. Backpressure limit reached.",
                tenant_id
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_burst_and_exhaustion() {
        let limiter = RateLimiter::new(5.0, 1.0);
        for _ in 0..5 {
            assert!(limiter.check_limit("tenant1").is_ok());
        }
        // 6th immediate request should fail
        assert!(limiter.check_limit("tenant1").is_err());
        // Different tenant has independent bucket
        assert!(limiter.check_limit("tenant2").is_ok());
    }
}
