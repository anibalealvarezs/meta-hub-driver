console.log('Registering Facebook Organic Config Handler');
window.ConfigHandlers['facebook_organic'] = {
    getPayload: function() {
        console.log('Executing Facebook Organic getPayload');
        const payload = {
            enabled: document.getElementById('fb-organic-enabled')?.checked,
            granular_sync: document.getElementById('fb-organic-granular-sync')?.checked,
            organic_history_range: document.getElementById('fb-organic-history-range')?.value,
            feature_toggles: {
                cron_entities_hour: document.getElementById('fb-organic-entities-cron-hour')?.value,
                cron_entities_minute: document.getElementById('fb-organic-entities-cron-minute')?.value,
                cron_recent_hour: document.getElementById('fb-organic-recent-cron-hour')?.value,
                cron_recent_minute: document.getElementById('fb-organic-recent-cron-minute')?.value
            },
            assets: { pages: [] }
        };

        const fbLvl = document.getElementById('fb-organic-level')?.value || 'page';
        const igLvl = document.getElementById('fb-ig-level')?.value || 'accounts';

        payload.feature_toggles.page_metrics = (fbLvl === 'page_metrics' || fbLvl === 'posts' || fbLvl === 'post_metrics');
        payload.feature_toggles.posts = (fbLvl === 'posts' || fbLvl === 'post_metrics');
        payload.feature_toggles.post_metrics = (fbLvl === 'post_metrics');

        payload.feature_toggles.ig_accounts = (igLvl !== 'none');
        payload.feature_toggles.ig_account_metrics = (igLvl === 'metrics' || igLvl === 'media' || igLvl === 'media_metrics');
        payload.feature_toggles.ig_account_media = (igLvl === 'media' || igLvl === 'media_metrics');
        payload.feature_toggles.ig_account_media_metrics = (igLvl === 'media_metrics');

        document.querySelectorAll('.page-config-card').forEach(card => {
            const mainToggle = card.querySelector('.fb-page-main-toggle');
            if (!mainToggle) return;
            
            const pageId = String(mainToggle.dataset.id);
            const igId = card.dataset.ig || null;
            const original = availableAssetsMaps.pages[pageId] || {};
            
            const pageData = {
                id: pageId,
                enabled: !card.classList.contains('lost-access') && mainToggle.checked,
                ig_account: igId,
                lost_access: card.classList.contains('lost-access'),
                hostname: original.hostname || null,
                url: original.url || original.link || null,
                link: original.link || null,
                created_time: original.created_time || null,
                data: original.data || [],
                ig_hostname: original.ig_hostname || null,
                ig_created_time: original.ig_created_time || null,
                ig_data: original.ig_data || []
            };
            
            card.querySelectorAll('.fb-page-opt').forEach(opt => {
                pageData[opt.dataset.opt] = opt.checked;
            });
            
            const titleEl = card.querySelector('[style*="font-weight:700"]');
            if (titleEl) pageData.title = titleEl.textContent.trim();
            
            const igText = card.querySelector('[style*="color:#E1306C"]')?.textContent.trim();
            if (igText) pageData.ig_account_name = igText;

            payload.assets.pages.push(pageData);
        });

        return payload;
    }
};
