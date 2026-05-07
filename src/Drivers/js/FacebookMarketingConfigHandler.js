console.log('Registering Facebook Marketing Config Handler');
window.ConfigHandlers['facebook_marketing'] = {
    getPayload: function() {
        console.log('Executing Facebook Marketing getPayload');
        const payload = {
            enabled: document.getElementById('fb-marketing-enabled')?.checked,
            granular_sync: document.getElementById('fb-marketing-granular-sync')?.checked,
            marketing_history_range: document.getElementById('fb-marketing-history-range')?.value,
            feature_toggles: {
                cron_entities_hour: document.getElementById('fb-marketing-entities-cron-hour')?.value,
                cron_entities_minute: document.getElementById('fb-marketing-entities-cron-minute')?.value,
                cron_recent_hour: document.getElementById('fb-marketing-recent-cron-hour')?.value,
                cron_recent_minute: document.getElementById('fb-marketing-recent-cron-minute')?.value
            },
            entity_filters: {
                CAMPAIGN: document.getElementById('fb-marketing-campaign-filter')?.value || '',
                ADSET: document.getElementById('fb-marketing-adset-filter')?.value || '',
                AD: document.getElementById('fb-marketing-ad-filter')?.value || '',
                CREATIVE: document.getElementById('fb-marketing-creative-filter')?.value || ''
            },
            assets: { ad_accounts: [] }
        };

        const entLevel = document.getElementById('fb-marketing-level')?.value || 'ad_account';
        const metLevel = document.getElementById('fb-marketing-metrics-level')?.value || 'ad_account';

        payload.feature_toggles.campaigns = true;
        payload.feature_toggles.adsets = (entLevel === 'adset' || entLevel === 'ad' || entLevel === 'creative');
        payload.feature_toggles.ads = (entLevel === 'ad' || entLevel === 'creative');
        payload.feature_toggles.creatives = (entLevel === 'creative');

        payload.feature_toggles.ad_account_metrics = (metLevel === 'ad_account');
        payload.feature_toggles.campaign_metrics = (metLevel === 'campaign');
        payload.feature_toggles.adset_metrics = (metLevel === 'adset');
        payload.feature_toggles.ad_metrics = (metLevel === 'ad');
        payload.feature_toggles.creative_metrics = (metLevel === 'creative');

        const stratCustom = document.getElementById('fb-strategy-custom');
        payload.metrics_strategy = stratCustom && stratCustom.checked ? 'custom' : 'default';

        payload.metrics_config = {};
        document.querySelectorAll('.metric-config-card').forEach(card => {
            const nameEl = card.querySelector('.metric-name-label');
            if (!nameEl) return;
            const name = nameEl.textContent.toLowerCase().replace(/ /g, '_');
            const enabled = card.querySelector('.metric-enable').checked;
            const sparkline = card.querySelector('.metric-sparkline').checked;
            const format = card.querySelector('.metric-format').value;
            const precision = parseInt(card.querySelector('.metric-precision').value || 0);
            const rules = [];

            card.querySelectorAll('.rule-item-grid').forEach(ri => {
                const classValue = ri.querySelector('.rule-class').value;
                rules.push({
                    min: parseFloat(ri.querySelector('.rule-min').value || 0),
                    max: parseFloat(ri.querySelector('.rule-max').value || 0),
                    class: 'badge-' + classValue
                });
            });

            payload.metrics_config[name] = {
                enabled,
                sparkline,
                sparkline_direction: card.querySelector('.metric-sparkline-direction').value,
                sparkline_color: card.querySelector('.metric-sparkline-color').value || null,
                format,
                precision,
                conditional: {
                    enabled: rules.length > 0,
                    config: rules
                }
            };
        });

        document.querySelectorAll('.asset-item').forEach(item => {
            const cb = item.querySelector('.fb-marketing-asset-sync');
            if (cb && (cb.checked || item.classList.contains('in-config') || item.classList.contains('lost-access'))) {
                const accId = String(cb.value);
                const original = availableAssetsMaps.ad_accounts[accId] || {};
                const nameEl = item.querySelector('[style*="font-weight:600"]');
                payload.assets.ad_accounts.push({ 
                    id: accId,
                    enabled: cb.checked,
                    name: nameEl ? nameEl.textContent.trim() : null,
                    lost_access: item.classList.contains('lost-access'),
                    created_time: original.created_time || null,
                    data: original.data || []
                });
            }
        });

        return payload;
    }
};
