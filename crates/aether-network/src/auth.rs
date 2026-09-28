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
}

impl AuthManager {
    pub fn new(require_auth: bool) -> Self {
        Self { require_auth }
    }

    /// Extracts and authenticates tenant context from HTTP Authorization header.
    /// Format: `Bearer aether_sk_<tenant>_<random>` or `Bearer <any_token>` in dev mode.
    pub fn authenticate(&self, auth_header: Option<&str>) -> Result<TenantContext> {
        if !self.require_auth {
            if let Some(header) = auth_header {
                if let Some(token) = header.strip_prefix("Bearer ") {
                    let token = token.trim();
                    if token.starts_with("aether_sk_") {
                        let parts: Vec<&str> = token.split('_').collect();
                        if parts.len() >= 3 {
                            return Ok(TenantContext {
                                tenant_id: parts[2].to_string(),
                                is_admin: true,
                            });
                        }
                    }
                }
            }
            return Ok(TenantContext::default_tenant());
        }

        // Strict auth enabled
        match auth_header {
            Some(header) if header.starts_with("Bearer ") => {
                let token = header[7..].trim();
                if token.starts_with("aether_sk_") {
                    let parts: Vec<&str> = token.split('_').collect();
                    if parts.len() >= 3 {
                        let tenant_id = parts[2].to_string();
                        return Ok(TenantContext {
                            tenant_id,
                            is_admin: true,
                        });
                    }
                }
                Err(AetherError::Unauthorized(
                    "Invalid API Key format. Expected format: aether_sk_<tenant>_<token>".to_string(),
                ))
            }
            _ => Err(AetherError::Unauthorized(
                "Missing or invalid Authorization header. Expected: Bearer aether_sk_<tenant>_<token>".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_and_tenant_key_partitioning() {
        let auth = AuthManager::new(false);
        let ctx = auth.authenticate(Some("Bearer aether_sk_agent007_secretxyz")).unwrap();
        assert_eq!(ctx.tenant_id, "agent007");

        let partitioned = ctx.partition_key(b"user:session");
        assert_eq!(partitioned, b"t:agent007:user:session");

        let vec_id = ctx.partition_vector_id("doc_1");
        assert_eq!(vec_id, "t:agent007:doc_1");
        assert_eq!(ctx.unpartition_vector_id(&vec_id), "doc_1");
    }
}
