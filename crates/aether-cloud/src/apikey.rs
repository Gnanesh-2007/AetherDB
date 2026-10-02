use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub name: String,
    pub prefix: String,
    pub key_preview: String,
    pub scopes: Vec<String>,
    pub created_at: u64,
    pub revoked_at: Option<u64>,
}

pub struct ApiKeyManager {
    keys: RwLock<HashMap<String, (ApiKey, String)>>, // key_id -> (ApiKey, raw_secret)
}

impl ApiKeyManager {
    pub fn new() -> Self {
        let manager = Self {
            keys: RwLock::new(HashMap::new()),
        };

        // Seed default production API key for immediate quickstart
        manager.create_key(
            "org_default",
            "proj_live_01",
            "Default Agent Production Key",
            vec!["read".to_string(), "write".to_string(), "admin".to_string()],
        );

        manager
    }

    pub fn create_key(
        &self,
        tenant_id: &str,
        project_id: &str,
        name: &str,
        scopes: Vec<String>,
    ) -> (ApiKey, String) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut rng = rand::thread_rng();
        let entropy: [u8; 16] = rng.gen();
        let entropy_hex: String = entropy.iter().map(|b| format!("{:02x}", b)).collect();

        let key_id = format!("key_{}", &entropy_hex[..8]);
        let raw_token = format!("aether_sk_live_{}_{}", tenant_id, entropy_hex);
        let preview = format!("aether_sk_live_{}...{}", tenant_id, &entropy_hex[entropy_hex.len() - 4..]);

        let api_key = ApiKey {
            id: key_id.clone(),
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
            name: name.to_string(),
            prefix: format!("aether_sk_live_{}", tenant_id),
            key_preview: preview,
            scopes,
            created_at: now,
            revoked_at: None,
        };

        self.keys
            .write()
            .insert(key_id, (api_key.clone(), raw_token.clone()));

        (api_key, raw_token)
    }

    pub fn list_keys(&self, project_id: &str) -> Vec<ApiKey> {
        self.keys
            .read()
            .values()
            .filter(|(k, _)| k.project_id == project_id)
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn get_raw_key(&self, key_id: &str) -> Option<String> {
        self.keys.read().get(key_id).map(|(_, raw)| raw.clone())
    }

    pub fn revoke_key(&self, key_id: &str) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut keys = self.keys.write();
        if let Some((k, _)) = keys.get_mut(key_id) {
            k.revoked_at = Some(now);
            true
        } else {
            false
        }
    }

    pub fn verify_token(&self, raw_token: &str) -> Option<ApiKey> {
        self.keys
            .read()
            .values()
            .find(|(k, raw)| raw == raw_token && k.revoked_at.is_none())
            .map(|(k, _)| k.clone())
    }
}
