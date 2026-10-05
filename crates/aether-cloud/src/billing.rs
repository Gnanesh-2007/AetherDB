use crate::metering::ProjectUsage;
use crate::tenant::PlanTier;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineItem {
    pub description: String,
    pub quantity: String,
    pub unit_rate: String,
    pub amount_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvoiceEstimate {
    pub plan_tier: PlanTier,
    pub base_subscription_usd: f64,
    pub line_items: Vec<LineItem>,
    pub subtotal_usd: f64,
    pub estimated_total_usd: f64,
    pub currency: String,
    pub billing_period: String,
}

pub struct BillingCalculator;

impl BillingCalculator {
    pub fn calculate_invoice(plan: PlanTier, usage: &ProjectUsage) -> InvoiceEstimate {
        let base_fee = plan.base_price_usd();
        let mut line_items = Vec::new();

        // 1. Subscription Base Fee
        line_items.push(LineItem {
            description: format!("AetherCloud {:?} Plan Base Subscription", plan),
            quantity: "1 Month".to_string(),
            unit_rate: format!("${:.2}/mo", base_fee),
            amount_usd: base_fee,
        });

        // Pay-as-you-go calculation for Pro / Enterprise
        let kv_total = usage.kv_reads + usage.kv_writes;
        let kv_cost = if plan == PlanTier::Free {
            0.0
        } else {
            // $0.20 per 100K ops
            (kv_total as f64 / 100_000.0) * 0.20
        };
        if kv_total > 0 {
            line_items.push(LineItem {
                description: "Transactional KV Operations (Reads + Writes)".to_string(),
                quantity: format!("{} ops", kv_total),
                unit_rate: "$0.20 / 100K ops".to_string(),
                amount_usd: (kv_cost * 100.0).round() / 100.0,
            });
        }

        // Vector Searches ($0.40 per 100K)
        let vec_cost = if plan == PlanTier::Free {
            0.0
        } else {
            (usage.vector_searches as f64 / 100_000.0) * 0.40
        };
        if usage.vector_searches > 0 {
            line_items.push(LineItem {
                description: "SIMD Vector Search Queries".to_string(),
                quantity: format!("{} searches", usage.vector_searches),
                unit_rate: "$0.40 / 100K searches".to_string(),
                amount_usd: (vec_cost * 100.0).round() / 100.0,
            });
        }

        // Token Counter Operations ($0.15 per 100K)
        let token_cost = if plan == PlanTier::Free {
            0.0
        } else {
            (usage.token_operations as f64 / 100_000.0) * 0.15
        };
        if usage.token_operations > 0 {
            line_items.push(LineItem {
                description: "Atomic Agent Rate-Limit Token Ops".to_string(),
                quantity: format!("{} ops", usage.token_operations),
                unit_rate: "$0.15 / 100K ops".to_string(),
                amount_usd: (token_cost * 100.0).round() / 100.0,
            });
        }

        // Storage ($0.10 per GB)
        let storage_gb = (usage.storage_bytes as f64) / (1024.0 * 1024.0 * 1024.0);
        let storage_cost = if plan == PlanTier::Free {
            0.0
        } else {
            storage_gb * 0.10
        };
        if storage_gb > 0.001 {
            line_items.push(LineItem {
                description: "Persistent SSD Storage (LSM + SSTables)".to_string(),
                quantity: format!("{:.2} GB", storage_gb),
                unit_rate: "$0.10 / GB-month".to_string(),
                amount_usd: (storage_cost * 100.0).round() / 100.0,
            });
        }

        let total = base_fee + kv_cost + vec_cost + token_cost + storage_cost;
        let rounded_total = (total * 100.0).round() / 100.0;

        InvoiceEstimate {
            plan_tier: plan,
            base_subscription_usd: base_fee,
            line_items,
            subtotal_usd: rounded_total,
            estimated_total_usd: rounded_total,
            currency: "USD ($)".to_string(),
            billing_period: "Current Monthly Cycle (Real-Time Pro-rated)".to_string(),
        }
    }
}
