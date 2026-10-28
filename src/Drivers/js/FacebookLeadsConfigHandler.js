console.log('Registering Facebook Leads Config Handler');
window.ConfigHandlers['facebook_leads'] = {
    getPayload: function() {
        console.log('Executing Facebook Leads getPayload');
        const payload = {
            enabled: document.getElementById('fb-leads-enabled')?.checked || false,
            assets: { pages: [] }
        };

        document.querySelectorAll('.fb-leads-page-config-card').forEach(card => {
            const mainToggle = card.querySelector('.fb-page-main-toggle');
            if (!mainToggle) return;
            
            const pageId = String(mainToggle.dataset.id);
            const original = availableAssetsMaps.pages[pageId] || {};
            
            const pageData = {
                id: pageId,
                enabled: !card.classList.contains('lost-access') && mainToggle.checked,
                lost_access: card.classList.contains('lost-access'),
                hostname: original.hostname || null,
                url: original.url || original.link || null,
                link: original.link || null,
                created_time: original.created_time || null,
                data: original.data || []
            };
            
            const titleEl = card.querySelector('[style*="font-weight:700"]');
            if (titleEl) pageData.title = titleEl.textContent.trim();

            payload.assets.pages.push(pageData);
        });

        return payload;
    }
};
