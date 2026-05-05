# Meta Hub Driver Memory
## Scope
- Package role: Normalization (Drivers)
- Purpose: This package operates within the Normalization (Drivers) layer of the APIs Hub SaaS hierarchy, providing data normalization for the Meta ecosystem.
- Dependency stance: Consumes `anibalealvarezs/api-client-skeleton`, `anibalealvarezs/api-driver-core`, and `anibalealvarezs/facebook-graph-api`; serves the Orchestrator (apis-hub).
## Local working rules
- Consult `AGENTS.md` first for package-specific instructions.
- Use this `MEMORY.md` for repository-specific decisions, learnings, and follow-up notes.
- Use `D:\laragon\www\_shared\AGENTS.md` and `D:\laragon\www\_shared\MEMORY.md` for cross-repository protocols and workspace-wide learnings.
- Keep secrets, credentials, tokens, and private endpoints out of this file.
## Current notes
- Meta driver should keep Facebook/Instagram normalization separate from UI or persistence logic.
- Use the canonical `instagram_account` account type for Instagram organic filtering and reporting.
- Keep Facebook Organic payloads aligned with the actual stored metric names, including the available `*_daily` content metrics and the IG account-level metric set.
- IG account context should resolve against the IG page identity when syncing mixed FB + IG data, so rows do not attach to the linked Facebook page entity incorrectly.
- Stock-style post aggregates now support `latest_snapshot` and `snapshot_delta`; UI selectors should match the backend aggregate mode and snapshot fallback behavior.
- FB Organic hierarchy should resolve the linked Facebook page from the channel `pages` configuration (`id` FB page platform id ↔ `ig_account` IG platform id) and query the FB subtable via `page_platform_id`, not by reusing the IG `page_id` entity id.
- FB Organic report now relies on backend support for relation-attribute filtering (`isAttribute`) so `page_platform_id` resolves by page platform id value instead of strict FK ID-only semantics.
- Shared Phase 2 pilot now also covers Meta Organic: `FacebookOrganicDriver` implements `AggregationProfileProviderInterface` and exposes aggregation capability profiles consumed by `apis-hub` planner capability checks.
- Read-only canonical metric equivalence pilot now includes Meta Marketing: `FacebookMarketingDriver` implements `CanonicalMetricDictionaryProviderInterface` and publishes canonical -> raw metric mappings (`conversions/results`, `roas_purchase/purchase_roas`, etc.) for aggregation-time SQL resolution without changing synced raw data.
- **Platform Identity**: Drivers now explicitly define their platform entity ID field (e.g., `facebook_page_id`, `instagram_business_account_id`) via `getPlatformEntityIdField()`, allowing the orchestrator to resolve them agnostically.
