# Meta Hub Driver: Surface Manifest & Status

## 1. Overview
The `meta-hub-driver` normalizes Meta (Facebook & Instagram) Marketing and Organic ecosystem data for APIs Hub SaaS. This manifest provides an immediate reference of the driver's current implementation status, entity hierarchy, metric tiers, breakdown passes, and canonical mappings.

---

## 2. Entity Hierarchy & Scopes

```
AdAccount (channeled_accounts)
└── Campaign (channeled_campaigns)
    └── AdSet (channeled_ad_groups)
        └── Ad (channeled_ads) ──[linked to]──> Creative (creatives - Visual QoL preview)
```

* **Metrics Level Support:** `account`, `campaign`, `adset`, `ad`.
* **Creative Scope:** Creatives are normalized for visual preview (thumbnail image, headline title, body text, CTA type, URL tags). Metrics aggregation remains strictly bound to `ChanneledAd` (`creative_id` is null in `MetricConfig` to prevent combinatorial explosion).

---

## 3. Metric Sets & Tiers

The driver supports configurable metric sync depth via `$config['metric_set']` (or `channeled_accounts.settings['analytics']['metric_set']`).

```
MetricSet Tiers
│
├── 🟢 BASIC (Executive & High-Level Health)
│   ├── spend
│   ├── impressions
│   ├── reach
│   ├── clicks
│   ├── ctr
│   ├── cpc
│   ├── frequency
│   ├── results
│   ├── cost_per_result
│   ├── purchase_roas
│   └── actions (Raw array preserved in metadata)
│
├── 🟡 KEY (Default Recommended - Performance Marketers)
│   ├── All metrics from BASIC +
│   ├── cpm
│   ├── result_rate
│   ├── cost_per_objective_result
│   ├── website_purchase_roas
│   ├── mobile_app_purchase_roas
│   ├── objective_results
│   └── ⚡ Normalized Action Metrics:
│       ├── purchases (Extracted from omni_purchase / purchase / offsite pixel)
│       ├── purchase_value (Extracted from action_values:omni_purchase / purchase)
│       ├── cost_per_purchase (Extracted from cost_per_action_type or spend / purchases)
│       ├── leads (Extracted from lead / offsite pixel lead)
│       └── cost_per_lead (Extracted from cost_per_action_type or spend / leads)
│
├── 🔵 FULL (Extended Diagnostics & Video Engagement)
│   ├── All metrics from KEY +
│   ├── unique_clicks, unique_ctr
│   ├── cost_per_unique_click, cost_per_inline_link_click
│   ├── cost_per_inline_post_engagement, cost_per_unique_outbound_click
│   ├── cost_per_estimated_ad_recallers, cost_per_thruplay
│   ├── estimated_ad_recallers, estimated_ad_recall_rate
│   ├── quality_ranking, engagement_rate_ranking, conversion_rate_ranking
│   └── Video Retention Curve:
│       ├── video_play_actions
│       ├── video_avg_time_watched_actions
│       ├── video_30_sec_watched_actions
│       ├── video_thruplay_watched_actions
│       ├── video_p25_watched_actions (Watched 25%)
│       ├── video_p50_watched_actions (Watched 50%)
│       ├── video_p75_watched_actions (Watched 75%)
│       ├── video_p95_watched_actions (Watched 95%)
│       └── video_p100_watched_actions (Watched 100% / Complete)
│
└── 🟣 CUSTOM
    └── Explicit comma-separated fields whitelist passed in config['metrics']
```

---

## 4. Independent Breakdown Passes

To prevent Meta API error `(#100) Cannot combine breakdowns` and protect top-line accuracy from differential privacy / thresholding drops, insights are fetched via **independent breakdown passes**:

| Pass Key | Label | Meta API Breakdowns | Stored Dimensions | Default Status |
| :--- | :--- | :--- | :--- | :--- |
| `unbroken` | **Baseline (Unbroken)** | `null` | `[]` (`dimensions_hash = 'GLOBAL'`) | **Always Active** (Protects source of truth totals) |
| `demographics` | **Demographics** | `['age', 'gender']` | `['age', 'gender']` | Active (Configurable) |
| `placement` | **Placement & Position** | `['publisher_platform', 'platform_position']` | `['publisher_platform', 'platform_position']` | Configurable |
| `geography` | **Geography** | `['country']` | `['country']` | Configurable |

---

## 5. Canonical Metric Dictionary Alignment

| Canonical Concept | Normalized Metrics | Provider Raw Source | Notes |
| :--- | :--- | :--- | :--- |
| `spend` | `spend`, `spend_daily` | `spend` | Direct currency float |
| `clicks` | `clicks`, `clicks_daily` | `clicks` | Total clicks |
| `impressions` | `impressions`, `impressions_daily` | `impressions` | Ad impressions count |
| `reach` | `reach`, `reach_daily` | `reach` | Unique audience reach |
| `frequency` | `frequency`, `frequency_daily` | `frequency` | Impressions / Reach |
| `conversions` | `results`, `results_daily` | `results` | Computed / primary objective result |
| `cost_per_conversion` | `cost_per_result` | `cost_per_result` | Native CPR or calculated spend / results |
| `conversion_rate` | `result_rate` | `result_rate` | (Results / Impressions) * 100 |
| `roas_purchase` | `purchase_roas`, `website_purchase_roas` | `purchase_roas` | Return on Ad Spend ratio |
| `purchases` | `purchases`, `purchases_daily` | `actions:omni_purchase` | Normalized eCommerce / offsite purchases |
| `purchase_value` | `purchase_value`, `purchase_value_daily` | `action_values:omni_purchase` | Total currency revenue generated |
| `cost_per_purchase` | `cost_per_purchase` | `cost_per_action_type:omni_purchase` | Acquisition cost per purchase |
| `leads` | `leads`, `leads_daily` | `actions:lead` | Normalized form / offsite leads count |
| `cost_per_lead` | `cost_per_lead` | `cost_per_action_type:lead` | Acquisition cost per lead |
