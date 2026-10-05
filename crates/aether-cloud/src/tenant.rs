use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanTier {
    Free,
    Pro,
    Enterprise,
}

impl PlanTier {
    pub fn max_monthly_ops(&self) -> u64 {
        match self {
            PlanTier::Free => 100_000,
            PlanTier::Pro => 10_000_000,
            PlanTier::Enterprise => 1_000_000_000,
        }
    }

    pub fn max_vectors(&self) -> u64 {
        match self {
            PlanTier::Free => 10_000,
            PlanTier::Pro => 1_000_000,
            PlanTier::Enterprise => 100_000_000,
        }
    }

    pub fn rate_limit_rps(&self) -> f64 {
        match self {
            PlanTier::Free => 50.0,
            PlanTier::Pro => 2_000.0,
            PlanTier::Enterprise => 50_000.0,
        }
    }

    pub fn base_price_usd(&self) -> f64 {
        match self {
            PlanTier::Free => 0.0,
            PlanTier::Pro => 29.0,
            PlanTier::Enterprise => 499.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub region: String,
    pub status: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    pub id: String,
    pub name: String,
    pub email: String,
    pub plan_tier: PlanTier,
    pub created_at: u64,
}

pub struct TenantManager {
    orgs: RwLock<HashMap<String, Organization>>,
    projects: RwLock<HashMap<String, Project>>,
}

impl TenantManager {
    pub fn new() -> Self {
        let mut manager = Self {
            orgs: RwLock::new(HashMap::new()),
            projects: RwLock::new(HashMap::new()),
        };

        // Seed default demo organization and project
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let default_org = Organization {
            id: "org_default".to_string(),
            name: "Default Workspace".to_string(),
            email: "developer@aetherdb.io".to_string(),
            plan_tier: PlanTier::Pro,
            created_at: now,
        };

        let default_proj = Project {
            id: "proj_live_01".to_string(),
            tenant_id: "org_default".to_string(),
            name: "Autonomous Agent Memory Fleet".to_string(),
            region: "us-east-1 (N. Virginia)".to_string(),
            status: "Active".to_string(),
            created_at: now,
        };

        manager
            .orgs
            .get_mut()
            .insert(default_org.id.clone(), default_org);
        manager
            .projects
            .get_mut()
            .insert(default_proj.id.clone(), default_proj);
        manager
    }

    pub fn create_project(&self, tenant_id: &str, name: &str, region: &str) -> Project {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let proj_id = format!("proj_{}", &uuid_v4_simple()[..8]);
        let project = Project {
            id: proj_id.clone(),
            tenant_id: tenant_id.to_string(),
            name: name.to_string(),
            region: region.to_string(),
            status: "Active".to_string(),
            created_at: now,
        };

        self.projects.write().insert(proj_id, project.clone());
        project
    }

    pub fn get_project(&self, project_id: &str) -> Option<Project> {
        self.projects.read().get(project_id).cloned()
    }

    pub fn list_projects(&self, tenant_id: &str) -> Vec<Project> {
        self.projects
            .read()
            .values()
            .filter(|p| p.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    pub fn get_org(&self, tenant_id: &str) -> Option<Organization> {
        self.orgs.read().get(tenant_id).cloned()
    }

    pub fn update_plan(&self, tenant_id: &str, new_tier: PlanTier) -> bool {
        let mut orgs = self.orgs.write();
        if let Some(org) = orgs.get_mut(tenant_id) {
            org.plan_tier = new_tier;
            true
        } else {
            false
        }
    }
}

fn uuid_v4_simple() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 8] = rng.gen();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
