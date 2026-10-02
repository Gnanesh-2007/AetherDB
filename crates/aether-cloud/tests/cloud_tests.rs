use aether_cloud::{ApiKeyManager, BillingCalculator, MeteringEngine, PlanTier, TenantManager};

#[test]
fn test_api_key_lifecycle_and_verification() {
    let manager = ApiKeyManager::new();
    let (key, token) = manager.create_key(
        "org_alpha",
        "proj_01",
        "Agent Fleet Key",
        vec!["read".to_string(), "write".to_string()],
    );

    assert!(token.starts_with("aether_sk_live_org_alpha_"));
    assert_eq!(key.name, "Agent Fleet Key");
    assert!(key.revoked_at.is_none());

    // Verify token resolves to key
    let verified = manager.verify_token(&token);
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().id, key.id);

    // Revoke key
    assert!(manager.revoke_key(&key.id));
    assert!(manager.verify_token(&token).is_none());
}

#[test]
fn test_tenant_and_project_management() {
    let tenants = TenantManager::new();
    let proj = tenants.create_project("org_custom", "Production Vector Shard", "us-east-1");

    assert_eq!(proj.tenant_id, "org_custom");
    assert_eq!(proj.name, "Production Vector Shard");

    let list = tenants.list_projects("org_custom");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, proj.id);
}

#[test]
fn test_metering_and_quota_calculations() {
    let metering = MeteringEngine::new();
    metering.record_operation("proj_test", "GET", 500);
    metering.record_operation("proj_test", "SET", 200);
    metering.record_operation("proj_test", "VECTOR_SEARCH", 100);
    metering.set_storage_bytes("proj_test", 1024 * 1024 * 50); // 50MB

    let usage = metering.get_usage("proj_test");
    assert_eq!(usage.kv_reads, 500);
    assert_eq!(usage.kv_writes, 200);
    assert_eq!(usage.vector_searches, 100);
    assert_eq!(usage.storage_bytes, 52428800);

    let report = metering.generate_report("proj_test", 1000, 1000, 1024 * 1024 * 100);
    assert_eq!(report.ops_percentage, 80.0);
    assert_eq!(report.storage_percentage, 50.0);
}

#[test]
fn test_billing_invoice_pro_rated_estimate() {
    let metering = MeteringEngine::new();
    metering.record_operation("proj_bill", "GET", 100_000);
    metering.record_operation("proj_bill", "VECTOR_SEARCH", 100_000);
    metering.record_operation("proj_bill", "INCR", 100_000);

    let usage = metering.get_usage("proj_bill");
    let invoice_pro = BillingCalculator::calculate_invoice(PlanTier::Pro, &usage);

    assert_eq!(invoice_pro.base_subscription_usd, 29.0);
    // 100K GET ($0.20) + 100K VEC ($0.40) + 100K INCR ($0.15) = $0.75 pay-as-you-go
    // Total = $29.75
    assert_eq!(invoice_pro.estimated_total_usd, 29.75);

    let invoice_free = BillingCalculator::calculate_invoice(PlanTier::Free, &usage);
    assert_eq!(invoice_free.estimated_total_usd, 0.0);
}
