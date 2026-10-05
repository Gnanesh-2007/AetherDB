use aether_core::error::{AetherError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantContext {
    pub tenant_id: String,
    pub is_admin: bool,
}

impl TenantContext {
    pub fn default_tenant() -> Self {
        Self {
            tenant_id: "default".to_string(),
            is_admin: true,
        }
    }

    /// Prefixes key with tenant namespace to guarantee complete multi-tenant physical isolation.
    pub fn partition_key(&self, raw_key: &[u8]) -> Vec<u8> {
        let prefix = format!("t:{}:", self.tenant_id);
        let mut full_key = prefix.into_bytes();
        full_key.extend_from_slice(raw_key);
        full_key
    }

    /// Prefixes vector ID with tenant namespace.
    pub fn partition_vector_id(&self, raw_id: &str) -> String {
        format!("t:{}:{}", self.tenant_id, raw_id)
    }

    /// Strips tenant namespace from returned vector ID for clean client output.
    pub fn unpartition_vector_id<'a>(&self, full_id: &'a str) -> &'a str {
        let prefix = format!("t:{}:", self.tenant_id);
        if full_id.starts_with(&prefix) {
            &full_id[prefix.len()..]
        } else {
            full_id
        }
    }
}

pub struct AuthManager {
    require_auth: bool,
    /// Accepted full API keys (`aether_sk_<tenant>_<secret>`). Only consulted in strict mode.
    api_keys: Vec<String>,
}

/// Extracts the tenant segment from `aether_sk_<tenant>_<secret>`.
fn tenant_from_token(token: &str) -> Option<&str> {
    let rest = token.strip_prefix("aether_sk_")?;
    let (tenant, secret) = rest.split_once('_')?;
    if tenant.is_empty() || secret.is_empty() {
        return None;
    }
    Some(tenant)
}

/// Constant-time byte comparison to avoid timing side channels on key checks.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl AuthManager {
    /// Dev/permissive constructor (no secret verification). Used by local tooling and tests.
    pub fn new(require_auth: bool) -> Self {
        Self {
            require_auth,
            api_keys: Vec::new(),
        }
    }

    /// Strict constructor with an explicit list of accepted API keys.
    pub fn with_keys(require_auth: bool, api_keys: Vec<String>) -> Self {
        Self {
            require_auth,
            api_keys,
        }
    }

    /// Builds from environment:
    /// - `AETHERDB_REQUIRE_AUTH=true|1` enables strict mode.
    /// - `AETHERDB_API_KEYS=key1,key2` lists accepted keys (never logged).
    pub fn from_env() -> Self {
        let require_auth = std::env::var("AETHERDB_REQUIRE_AUTH")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let api_keys: Vec<String> = std::env::var("AETHERDB_API_KEYS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| tenant_from_token(s).is_some())
            .collect();
        if require_auth && api_keys.is_empty() {
            tracing::warn!(
                "AETHERDB_REQUIRE_AUTH is enabled but AETHERDB_API_KEYS has no valid keys; all API requests will be rejected"
            );
        }
        tracing::info!(
            "Auth mode: {} ({} API key(s) configured)",
            if require_auth {
                "strict"
            } else {
                "permissive-dev"
            },
            api_keys.len()
        );
        Self::with_keys(require_auth, api_keys)
    }

    pub fn is_strict(&self) -> bool {
        self.require_auth
    }

    /// Extracts and authenticates tenant context from HTTP Authorization or X-Aether-Tenant headers.
    ///
    /// Strict mode: a configured `Bearer aether_sk_<tenant>_<secret>` key is mandatory. The tenant is
    /// derived from the key; `X-Aether-Tenant`, if sent, must match it and can never grant access alone.
    ///
    /// Dev mode: `X-Aether-Tenant: <tenant_id>` or any well-formed `aether_sk_` token is accepted.
    pub fn authenticate(
        &self,
        auth_header: Option<&str>,
        tenant_header: Option<&str>,
    ) -> Result<TenantContext> {
        let tenant_header = tenant_header.map(str::trim).filter(|t| !t.is_empty());

        if self.require_auth {
            let token = match auth_header.and_then(|h| h.strip_prefix("Bearer ")) {
                Some(t) => t.trim(),
                None => {
                    return Err(AetherError::Unauthorized(
                        "Missing or invalid Authorization header. Expected: Bearer <api_key>"
                            .to_string(),
                    ))
                }
            };
            let known = self
                .api_keys
                .iter()
                .any(|k| ct_eq(k.as_bytes(), token.as_bytes()));
            let tenant = match (known, tenant_from_token(token)) {
                (true, Some(t)) => t,
                _ => return Err(AetherError::Unauthorized("Invalid API key".to_string())),
            };
            if let Some(h) = tenant_header {
                if h != tenant {
                    return Err(AetherError::Unauthorized(
                        "X-Aether-Tenant does not match API key tenant".to_string(),
                    ));
                }
            }
            return Ok(TenantContext {
                tenant_id: tenant.to_string(),
                is_admin: false,
            });
        }

        // ---- Permissive development mode (unchanged semantics) ----
        if let Some(t) = tenant_header {
            return Ok(TenantContext {
                tenant_id: t.to_string(),
                is_admin: true,
            });
        }

        if let Some(header) = auth_header {
            if let Some(token) = header.strip_prefix("Bearer ") {
                if let Some(tenant) = tenant_from_token(token.trim()) {
                    return Ok(TenantContext {
                        tenant_id: tenant.to_string(),
                        is_admin: true,
                    });
                }
                return Err(AetherError::Unauthorized(
                    "Invalid API Key format. Expected format: aether_sk_<tenant>_<token>"
                        .to_string(),
                ));
            }
            return Err(AetherError::Unauthorized(
                "Missing Bearer scheme in Authorization header".to_string(),
            ));
        }

        Ok(TenantContext::default_tenant())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_and_tenant_key_partitioning() {
        let auth = AuthManager::new(false);
        let ctx = auth
            .authenticate(Some("Bearer aether_sk_agent007_secretxyz"), None)
            .unwrap();
        assert_eq!(ctx.tenant_id, "agent007");

        let ctx_header = auth.authenticate(None, Some("tenant-alpha")).unwrap();
        assert_eq!(ctx_header.tenant_id, "tenant-alpha");

        let partitioned = ctx.partition_key(b"user:session");
        assert_eq!(partitioned, b"t:agent007:user:session");

        let vec_id = ctx.partition_vector_id("doc_1");
        assert_eq!(vec_id, "t:agent007:doc_1");
        assert_eq!(ctx.unpartition_vector_id(&vec_id), "doc_1");
    }

    #[test]
    fn test_strict_mode_rejects_bypass_and_forged_keys() {
        let auth = AuthManager::with_keys(true, vec!["aether_sk_tenantA_s3cretA".to_string()]);

        // Tenant header alone must never authenticate.
        assert!(auth.authenticate(None, Some("tenantA")).is_err());
        // Well-formed but unknown key is rejected.
        assert!(auth
            .authenticate(Some("Bearer aether_sk_tenantA_guess"), None)
            .is_err());
        // Malformed / missing scheme rejected.
        assert!(auth
            .authenticate(Some("aether_sk_tenantA_s3cretA"), None)
            .is_err());
        assert!(auth.authenticate(None, None).is_err());
        // Valid key succeeds and derives tenant from the key.
        let ctx = auth
            .authenticate(Some("Bearer aether_sk_tenantA_s3cretA"), None)
            .unwrap();
        assert_eq!(ctx.tenant_id, "tenantA");
        // Matching header ok, mismatched header rejected.
        assert!(auth
            .authenticate(Some("Bearer aether_sk_tenantA_s3cretA"), Some("tenantA"))
            .is_ok());
        assert!(auth
            .authenticate(Some("Bearer aether_sk_tenantA_s3cretA"), Some("tenantB"))
            .is_err());
    }

    #[test]
    fn test_strict_mode_without_keys_fails_closed() {
        let auth = AuthManager::with_keys(true, vec![]);
        assert!(auth
            .authenticate(Some("Bearer aether_sk_any_thing"), None)
            .is_err());
    }
}
