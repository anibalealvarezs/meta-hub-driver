/**
 * APIs Hub | Facebook Organic Reports View Logic 🛡️📱
 * Specialized Metrics Analytics Engine for Organic Meta Channels (IG & FB).
 */

// Global state
let currentData = [];
let sortConfig = {key: 'account', dir: 'asc'};
const TREND_DATA_CACHE = {};
const NESTED_DATA_CACHE = {};

const HIERARCHY = {
    instagram: {
        next: 'facebook',
        label: 'Instagram Account',
        icon: 'instagram',
        color: '#E1306C',
        idField: 'channeledAccount_id',
        nameField: 'account',
        filterKey: 'channeledAccount'
    },
    facebook: {
        next: 'content',
        label: 'Facebook Page',
        icon: 'facebook',
        color: '#1877F2',
        idField: 'page_id',
        nameField: 'page_title',
        filterKey: 'page'
    },
    content: {
        next: null,
        label: 'Content Breakdown',
        icon: 'image',
        color: '#8B5CF6',
        idField: 'post_id',
        nameField: 'caption'
    }
};

const INSTAGRAM_ACCOUNT_TYPE = 'instagram_account';
const FACEBOOK_PAGE_ACCOUNT_TYPE = 'facebook_page';
const POSTS_AGGREGATE_MODES = {
    DAILY: 'daily',
    SNAPSHOT: 'snapshot',
    SNAPSHOT_DELTA: 'snapshot_delta'
};
let LINKED_FB_PAGE_PLATFORM_ID_BY_IG_KEY = null;
const SNAPSHOT_FALLBACK_MODES = {
    RESILIENT: 'resilient',
    STRICT: 'strict'
};

// --- Initialization ---
async function initDashboard() {
    lucide.createIcons();

    const headers = getAdminHeaders();
    if (!headers || !headers.Authorization) return;

    let minStoredDate = dayjs().subtract(1, 'year').format('YYYY-MM-DD');
    try {
        const rangeRes = await fetch('/facebook_organic/metric/range', {headers}).then(r => r.json());
        if (rangeRes.status === 'success' && rangeRes.data.minDate) {
            minStoredDate = rangeRes.data.minDate;
        }
    } catch (e) {
        console.error("Range fetch error:", e);
    }

    const flatpickrConfig = {
        dateFormat: "Y-m-d",
        altInput: true,
        altFormat: "F j, Y",
        animate: true,
        disableMobile: "true",
        minDate: minStoredDate
    };

    const yesterday = dayjs().subtract(1, 'day').format('YYYY-MM-DD');
    const lastWeek = dayjs().subtract(30, 'day').format('YYYY-MM-DD');

    flatpickr("#startDate", {...flatpickrConfig, defaultDate: lastWeek > minStoredDate ? lastWeek : minStoredDate});
    flatpickr("#endDate", {...flatpickrConfig, defaultDate: yesterday, maxDate: yesterday});

    bindPostsAggregateControls();

    loadReport();
}

function bindPostsAggregateControls() {
    const modeEl = document.getElementById('postsAggregateMode');
    const fallbackEl = document.getElementById('postsSnapshotFallbackMode');
    if (!modeEl || !fallbackEl) return;

    const syncFallbackEnabledState = () => {
        const mode = modeEl.value;
        const enabled = mode !== POSTS_AGGREGATE_MODES.DAILY;
        fallbackEl.disabled = !enabled;
        fallbackEl.style.opacity = enabled ? '1' : '0.55';
    };

    modeEl.addEventListener('change', () => {
        syncFallbackEnabledState();
        resetExpandedHierarchyRows();
    });
    fallbackEl.addEventListener('change', () => {
        resetExpandedHierarchyRows();
    });
    syncFallbackEnabledState();
}

function resetExpandedHierarchyRows() {
    document.querySelectorAll('tr.hierarchy-row').forEach(row => row.remove());
    document.querySelectorAll('.btn-expand.active').forEach(btn => btn.classList.remove('active'));
}

function getPostsAggregateConfig() {
    const modeEl = document.getElementById('postsAggregateMode');
    const fallbackEl = document.getElementById('postsSnapshotFallbackMode');
    const modeValue = modeEl?.value || POSTS_AGGREGATE_MODES.SNAPSHOT;
    const fallbackValue = fallbackEl?.value || SNAPSHOT_FALLBACK_MODES.RESILIENT;

    const mode = Object.values(POSTS_AGGREGATE_MODES).includes(modeValue)
        ? modeValue
        : POSTS_AGGREGATE_MODES.SNAPSHOT;
    const fallbackMode = Object.values(SNAPSHOT_FALLBACK_MODES).includes(fallbackValue)
        ? fallbackValue
        : SNAPSHOT_FALLBACK_MODES.RESILIENT;

    return {mode, fallbackMode};
}

function toDailyMetricName(metricName) {
    if (!metricName || metricName.endsWith('_daily')) return metricName;
    return `${metricName}_daily`;
}

function pickFirstRowValue(row, keys = []) {
    if (!row || !Array.isArray(keys)) return null;
    for (const key of keys) {
        const value = row[key];
        if (value !== undefined && value !== null && value !== '') return value;
    }
    return null;
}

function normalizeLookupValue(value) {
    return value === undefined || value === null ? '' : String(value).trim();
}

function pickBestPlatformIdCandidate(candidates = []) {
    const normalized = candidates
        .map(normalizeLookupValue)
        .filter(Boolean);
    if (!normalized.length) return '';

    // Prefer the longest candidate to avoid selecting small internal IDs when a real platform ID is present.
    return normalized.sort((a, b) => b.length - a.length)[0];
}

function getOrganicPagesConfig() {
    const pagesConfig = window.FB_METRICS_CONFIG?.pages_config;
    return Array.isArray(pagesConfig) ? pagesConfig : [];
}

function getLinkedFacebookPagePlatformIdMap() {
    if (LINKED_FB_PAGE_PLATFORM_ID_BY_IG_KEY) {
        return LINKED_FB_PAGE_PLATFORM_ID_BY_IG_KEY;
    }

    const relationMap = {};
    getOrganicPagesConfig().forEach(pageConfig => {
        const facebookPagePlatformId = pickBestPlatformIdCandidate([
            pageConfig?.data?.id,
            pageConfig?.platformId,
            pageConfig?.platform_id,
            pageConfig?.facebook_page_id,
            pageConfig?.data?.facebook_page_id,
        ]);
        const instagramPagePlatformId = pickBestPlatformIdCandidate([
            pageConfig?.ig_data?.id,
            pageConfig?.ig_platform_id,
            pageConfig?.igPlatformId,
            pageConfig?.ig_account,
            pageConfig?.data?.instagram_id,
            pageConfig?.instagram_id,
        ]);
        const instagramAccountName = normalizeLookupValue(
            pageConfig?.ig_account_name
            || pageConfig?.ig_data?.username
            || pageConfig?.ig_data?.name
        );

        if (instagramPagePlatformId && facebookPagePlatformId) {
            relationMap[`ig_platform:${instagramPagePlatformId}`] = facebookPagePlatformId;
        }
        if (instagramAccountName && facebookPagePlatformId) {
            relationMap[`ig_name:${instagramAccountName.toLowerCase()}`] = facebookPagePlatformId;
        }
    });

    LINKED_FB_PAGE_PLATFORM_ID_BY_IG_KEY = relationMap;
    return relationMap;
}

function resolveLinkedFacebookPagePlatformId(row) {
    const relationMap = getLinkedFacebookPagePlatformIdMap();
    const instagramPagePlatformId = normalizeLookupValue(
        pickFirstRowValue(row, ['page_platform_id', 'pageplatformid'])
    );
    const instagramAccountName = normalizeLookupValue(
        pickFirstRowValue(row, ['channeledAccount', 'channeledaccount', 'account'])
    ).toLowerCase();

    const fbPlatformId = relationMap[`ig_name:${instagramAccountName}`]
        || relationMap[`ig_platform:${instagramPagePlatformId}`]
        || '';

    // Guardrail: never reuse IG platform id as linked FB id.
    if (fbPlatformId && instagramPagePlatformId && fbPlatformId === instagramPagePlatformId) {
        return '';
    }

    return fbPlatformId;
}

function buildContentMetricsByMode(baseMetrics, mode) {
    if (mode !== POSTS_AGGREGATE_MODES.DAILY) return baseMetrics;

    return baseMetrics.map(metric => ({
        ...metric,
        original: toDailyMetricName(metric.original),
        mode: 'daily'
    }));
}

function getActiveMetrics(level = 'instagram', isFb = false, postsAggregateMode = POSTS_AGGREGATE_MODES.SNAPSHOT) {
    // Config based on level
    if (level === 'instagram') {
        return [
            {key: 'likes', label: 'LIKES', format: 'number', precision: 0, original: 'likes', sparkline: false},
            {key: 'comments', label: 'COMM', format: 'number', precision: 0, original: 'comments', sparkline: false},
            {key: 'reach', label: 'REACH', format: 'number', precision: 0, original: 'reach', sparkline: false},
            {key: 'views', label: 'VIEWS', format: 'number', precision: 0, original: 'views', sparkline: false},
            {
                key: 'profile_views',
                label: 'PRF VIEW',
                format: 'number',
                precision: 0,
                original: 'profile_views',
                sparkline: false
            },
            {
                key: 'website_clicks',
                label: 'WEB CLK',
                format: 'number',
                precision: 0,
                original: 'website_clicks',
                sparkline: false
            },
            {
                key: 'profile_links_taps',
                label: 'LNK TAPS',
                format: 'number',
                precision: 0,
                original: 'profile_links_taps',
                sparkline: false
            },
            {
                key: 'follows_and_unfollows',
                label: 'FOLLOWS',
                format: 'number',
                precision: 0,
                original: 'follows_and_unfollows',
                sparkline: true
            },
            {key: 'saves', label: 'SAVES', format: 'number', precision: 0, original: 'saves', sparkline: false},
            {key: 'shares', label: 'SHARES', format: 'number', precision: 0, original: 'shares', sparkline: false},
            {
                key: 'total_interactions',
                label: 'INTER',
                format: 'number',
                precision: 0,
                original: 'total_interactions',
                sparkline: false
            },
            {key: 'replies', label: 'REPLIES', format: 'number', precision: 0, original: 'replies', sparkline: false},
            {
                key: 'accounts_engaged',
                label: 'ENGAGED',
                format: 'number',
                precision: 0,
                original: 'accounts_engaged',
                sparkline: false
            }
        ];
    }
    if (level === 'facebook') {
        return [
            // FB page-level metrics persisted with post_id = NULL
            {key: 'reach', label: 'REACH', format: 'number', precision: 0, original: 'reach', sparkline: false},
            {key: 'views', label: 'VIEWS', format: 'number', precision: 0, original: 'views', sparkline: false},
            {
                key: 'profile_views',
                label: 'PRF VIEW',
                format: 'number',
                precision: 0,
                original: 'profile_views',
                sparkline: false
            },
            {
                key: 'website_clicks',
                label: 'WEB CLK',
                format: 'number',
                precision: 0,
                original: 'website_clicks',
                sparkline: false
            },
            {
                key: 'profile_links_taps',
                label: 'LNK TAPS',
                format: 'number',
                precision: 0,
                original: 'profile_links_taps',
                sparkline: false
            },
            {
                key: 'follows_and_unfollows',
                label: 'FOLLOWS',
                format: 'number',
                precision: 0,
                original: 'follows_and_unfollows',
                sparkline: true
            },
            {key: 'replies', label: 'REPLIES', format: 'number', precision: 0, original: 'replies', sparkline: false},
            {
                key: 'accounts_engaged',
                label: 'ENGAGED',
                format: 'number',
                precision: 0,
                original: 'accounts_engaged',
                sparkline: false
            },
            {
                key: 'total_interactions',
                label: 'INTER',
                format: 'number',
                precision: 0,
                original: 'total_interactions',
                sparkline: true
            },
            {key: 'likes', label: 'LIKES', format: 'number', precision: 0, original: 'likes', sparkline: false},
            {key: 'comments', label: 'COMM', format: 'number', precision: 0, original: 'comments', sparkline: false},
            {key: 'shares', label: 'SHAR', format: 'number', precision: 0, original: 'shares', sparkline: false},
            {key: 'saves', label: 'SAVES', format: 'number', precision: 0, original: 'saves', sparkline: false}
        ];
    }
    // Level Content
    if (level === 'content') {
        if (isFb) {
            const baseMetrics = [
                // FB post-level report must consume non-daily metrics only.
                {key: 'comments', label: 'COMM', format: 'number', precision: 0, original: 'comments'},
                {key: 'follows', label: 'FOL', format: 'number', precision: 0, original: 'follows'},
                {
                    key: 'ig_reels_avg_watch_time',
                    label: 'REEL AVG WT',
                    format: 'duration_ms',
                    precision: 0,
                    original: 'ig_reels_avg_watch_time'
                },
                {
                    key: 'ig_reels_video_view_total_time',
                    label: 'REEL TOT WT',
                    format: 'duration_ms',
                    precision: 0,
                    original: 'ig_reels_video_view_total_time'
                },
                {key: 'likes', label: 'LIKES', format: 'number', precision: 0, original: 'likes'},
                {key: 'post_clicks', label: 'PST CLK', format: 'number', precision: 0, original: 'post_clicks'},
                {
                    key: 'post_engagements',
                    label: 'PST ENG',
                    format: 'number',
                    precision: 0,
                    original: 'post_engagements'
                },
                {
                    key: 'post_impressions_unique',
                    label: 'PST U IMPR',
                    format: 'number',
                    precision: 0,
                    original: 'post_impressions_unique'
                },
                {
                    key: 'post_media_view',
                    label: 'PST VIEW',
                    format: 'number',
                    precision: 0,
                    original: 'post_media_view'
                },
                {
                    key: 'post_reactions_by_type_total',
                    label: 'PST REACT',
                    format: 'number',
                    precision: 0,
                    original: 'post_reactions_by_type_total'
                },
                {
                    key: 'post_video_avg_time_watched',
                    label: 'VID AVG WT',
                    format: 'duration_ms',
                    precision: 0,
                    original: 'post_video_avg_time_watched'
                },
                {
                    key: 'post_video_views',
                    label: 'VID VIEWS',
                    format: 'number',
                    precision: 0,
                    original: 'post_video_views'
                },
                {
                    key: 'profile_activity',
                    label: 'PRF ACT',
                    format: 'number',
                    precision: 0,
                    original: 'profile_activity'
                },
                {key: 'profile_visits', label: 'PRF VIS', format: 'number', precision: 0, original: 'profile_visits'},
                {key: 'reach', label: 'REACH', format: 'number', precision: 0, original: 'reach'},
                {key: 'reposts', label: 'REPOST', format: 'number', precision: 0, original: 'reposts'},
                {key: 'saved', label: 'SAVED', format: 'number', precision: 0, original: 'saved'},
                {key: 'shares', label: 'SHARES', format: 'number', precision: 0, original: 'shares'},
                {
                    key: 'total_interactions',
                    label: 'INTER',
                    format: 'number',
                    precision: 0,
                    original: 'total_interactions'
                },
                {key: 'views', label: 'VIEWS', format: 'number', precision: 0, original: 'views'}
            ];
            return buildContentMetricsByMode(baseMetrics, postsAggregateMode);
        }
        const baseMetrics = [
            {key: 'comments', label: 'COMM', format: 'number', precision: 0, original: 'comments'},
            {key: 'follows', label: 'FOL', format: 'number', precision: 0, original: 'follows'},
            {
                key: 'ig_reels_avg_watch_time',
                label: 'REEL AVG WT',
                format: 'duration_ms',
                precision: 0,
                original: 'ig_reels_avg_watch_time'
            },
            {
                key: 'ig_reels_video_view_total_time',
                label: 'REEL TOT WT',
                format: 'duration_ms',
                precision: 0,
                original: 'ig_reels_video_view_total_time'
            },
            {key: 'likes', label: 'LIKES', format: 'number', precision: 0, original: 'likes'},
            {key: 'profile_activity', label: 'PRF ACT', format: 'number', precision: 0, original: 'profile_activity'},
            {key: 'profile_visits', label: 'PRF VIS', format: 'number', precision: 0, original: 'profile_visits'},
            {key: 'reach', label: 'REACH', format: 'number', precision: 0, original: 'reach'},
            {key: 'reposts', label: 'REPOST', format: 'number', precision: 0, original: 'reposts'},
            {key: 'saved', label: 'SAVE', format: 'number', precision: 0, original: 'saved'},
            {key: 'shares', label: 'SHAR', format: 'number', precision: 0, original: 'shares'},
            {key: 'total_interactions', label: 'INTER', format: 'number', precision: 0, original: 'total_interactions'},
            {key: 'views', label: 'VIEW', format: 'number', precision: 0, original: 'views'}
        ];
        return buildContentMetricsByMode(baseMetrics, postsAggregateMode);
    }
    return [];
}

async function loadReport() {
    const start = document.getElementById('startDate').value;
    const end = document.getElementById('endDate').value;
    const loader = document.getElementById('loader');
    const emptyMsg = document.getElementById('empty-msg');

    if (loader) loader.style.display = 'flex';
    if (emptyMsg) emptyMsg.style.display = 'none';

    try {
        const headers = getAdminHeaders();
        const metrics = getActiveMetrics('instagram');
        const aggs = {};
        metrics.forEach(m => aggs[m.key] = m.original);

        // 1. Fetch IG Master Accounts
        const payload = {
            aggregations: aggs,
            filters: {account_type: INSTAGRAM_ACCOUNT_TYPE},
            groupBy: ["channeledAccount", "channeled_account_id", "page_platform_id", "linked_fb_page_id"],
            startDate: start, endDate: end
        };
        const resMain = await fetch('/facebook_organic/metric/aggregate', {
            method: 'POST',
            headers,
            body: JSON.stringify(payload)
        }).then(r => r.json());

        console.log("Organic Dashboard - Master Accounts:", resMain.data);

        if (resMain.status === 'success' && resMain.data) {
            currentData = resMain.data;

            // 2. Trend data for Sparklines
            const trendAggs = {};
            metrics.filter(m => m.sparkline).forEach(m => trendAggs[`trend_${m.key}`] = m.original);

            const resTrend = await fetch('/facebook_organic/metric/aggregate', {
                method: 'POST', headers,
                body: JSON.stringify({
                    aggregations: trendAggs,
                    filters: {account_type: INSTAGRAM_ACCOUNT_TYPE},
                    groupBy: ['daily', 'channeledAccount', 'channeled_account_id'],
                    startDate: start, endDate: end
                })
            }).then(r => r.json());

            if (resTrend.status === 'success' && resTrend.data) {
                const trendData = Array.isArray(resTrend.data) ? resTrend.data : (resTrend.data.data || []);
                TREND_DATA_CACHE['instagram'] = {};
                trendData.forEach(d => {
                    const cid = d.channeled_account_id || d.channeledaccount_id || d.channeledAccount_id || d.channeledAccount || d.page_id;
                    if (!cid) return;
                    if (!TREND_DATA_CACHE['instagram'][cid]) TREND_DATA_CACHE['instagram'][cid] = {};
                    metrics.filter(m => m.sparkline).forEach(m => {
                        const valKey = `trend_${m.key}`;
                        const val = d[m.key] || d[valKey] || d[valKey.toLowerCase()] || 0;
                        if (!TREND_DATA_CACHE['instagram'][cid][m.key]) TREND_DATA_CACHE['instagram'][cid][m.key] = [];
                        TREND_DATA_CACHE['instagram'][cid][m.key].push({day: d.daily, val: parseFloat(val || 0)});
                    });
                });
            }
            const recordCountEl = document.getElementById('record-count');
            if (recordCountEl) recordCountEl.textContent = currentData.length + ' ';
            render(start, end);
        } else {
            if (emptyMsg) emptyMsg.style.display = 'block';
        }
    } catch (error) {
        console.error("Organic Load Error:", error);
    } finally {
        if (loader) loader.style.display = 'none';
        lucide.createIcons();
    }
}

function render(start, end) {
    const body = document.getElementById('table-body');
    if (!body) return;
    body.innerHTML = '';
    const metrics = getActiveMetrics('instagram');
    const headRow = document.getElementById('table-head-row');

    headRow.innerHTML = `
        <th class="col-actions">&nbsp;</th>
        <th onclick="sortTable('account')" class="clickable" style="text-align: left;">INSTAGRAM ACCOUNT</th>
        <th style="text-align: left;">LINKED FB</th>
        ${metrics.map(m => `<th style="text-align: right;">${m.label}</th>`).join('')}
    `;

    currentData.forEach((row, idx) => {
        const tr = document.createElement('tr');
        const cid_raw = row.channeledAccount || row.channeledaccount;
        const rowId = `row-ig-${cid_raw}`.replace(/[^a-z0-9\-]/gi, '-');
        tr.id = rowId;
        const linkedFbPagePlatformId = resolveLinkedFacebookPagePlatformId(row);
        const fbDisplay = linkedFbPagePlatformId ? 'Linked' : 'None';
        const accountId = row.channeled_account_id || row.channeled_account_id_id;
        const fbButtonTitle = linkedFbPagePlatformId ? 'View Linked Facebook Page' : 'No linked Facebook Page configured';
        const fbBtnId = `btn-fb-${rowId}`.replace(/[^a-z0-9\-]/gi, '-');
        const igBtnId = `btn-ig-${rowId}`.replace(/[^a-z0-9\-]/gi, '-');

        tr.innerHTML = `
            <td class="col-actions">
                <div style="display: flex; gap: 6px; justify-content: center; align-items: center;">
                    <button id="${fbBtnId}" class="btn-expand next-btn-fb" title="${fbButtonTitle}">
                        <i data-lucide="layers" size="14"></i>
                    </button>
                    <button id="${igBtnId}" class="btn-expand next-btn-ig" title="View Instagram Posts" style="background-color:rgba(139,92,246,0.1); color:#8b5cf6; border-color:rgba(139,92,246,0.3);">
                        <i data-lucide="image" size="14"></i>
                    </button>
                </div>
            </td>
            <td style="text-align: left;"><strong>${cid_raw}</strong></td>
            <td style="text-align: left;"><span class="badge-${linkedFbPagePlatformId ? 'success' : 'dim'}">${fbDisplay}</span></td>
            ${metrics.map(m => {
            const val = row[m.key] || row[String(m.key).toLowerCase()] || 0;
            const cid = row.channeledAccount || row.channeledaccount;
            const sparkId = `spark-ig-${m.key}-${accountId}`.toLowerCase();
            return `<td style="text-align: right;">
                    <div class="metric-flex-end">
                        <span>${formatMetricValue(m, val)}</span>
                        ${m.sparkline ? `<div id="${sparkId}" class="sparkline-inline"></div>` : ''}
                    </div>
                </td>`;
        }).join('')}
        `;
        const fbBtn = tr.querySelector(`#${fbBtnId}`);
        if (fbBtn) {
            fbBtn.addEventListener('click', () => {
                toggleOrganicHierarchy(fbBtn, rowId, 'facebook', String(accountId || ''), String(linkedFbPagePlatformId || ''), 'page_platform_id');
            });
        }
        const igBtn = tr.querySelector(`#${igBtnId}`);
        if (igBtn) {
            igBtn.addEventListener('click', () => {
                toggleOrganicHierarchy(igBtn, rowId, 'content', String(accountId || ''), null);
            });
        }
        body.appendChild(tr);
    });

    for (const row of currentData) {
        const accountId = row.channeled_account_id;
        for (const m of metrics) {
            if (m.sparkline) {
                const sparkId = `spark-ig-${m.key}-${accountId}`.toLowerCase();
                const sparkEl = document.getElementById(sparkId);
                const points = TREND_DATA_CACHE['instagram']?.[accountId]?.[m.key] || [];
                if (sparkEl && points.length > 1) {
                    try {
                        const vals = points.sort((a, b) => a.day.localeCompare(b.day)).map(p => p.val);
                        renderSparkline(sparkEl, vals, m.color || '#6366F1', start, end);
                    } catch (e) {
                        console.error("Sparkline render error:", e);
                    }
                }
            }
        }
    }

    renderSummaryFields();
    lucide.createIcons();
}

async function toggleOrganicHierarchy(btn, rowId, level, parentId, childPlatformId, childFilterKey = 'page') {
    const mainRow = document.getElementById(rowId);
    const nextRow = mainRow?.nextElementSibling;

    if (nextRow?.classList.contains('hierarchy-row') && nextRow.dataset.parentRow === rowId) {
        if (nextRow.dataset.level === level) {
            nextRow.remove();
            btn.classList.remove('active');
            return;
        }
        // If it was another level, remove it first
        nextRow.remove();
        document.querySelectorAll(`#${rowId} .btn-expand`).forEach(b => b.classList.remove('active'));
    }

    btn.classList.add('active');
    const breakdownRow = document.createElement('tr');
    breakdownRow.className = 'hierarchy-row';
    breakdownRow.dataset.parentRow = rowId;
    breakdownRow.dataset.level = level;
    const containerId = `container-${level}-${rowId}`.replace(/[^a-z0-9\-]/gi, '-');
    breakdownRow.innerHTML = `<td colspan="20"><div id="${containerId}" class="nested-container"><div class="spinner"></div> Loading ${level}...</div></td>`;
    mainRow.after(breakdownRow);

    const headers = getAdminHeaders();
    const start = document.getElementById('startDate').value;
    const end = document.getElementById('endDate').value;
    const container = document.getElementById(containerId);

    try {
        if (level === 'facebook') {
            if (!childPlatformId) {
                container.innerHTML = `<div class="empty-state">No linked Facebook Page is configured for this Instagram account.</div>`;
                btn.classList.remove('active');
                return;
            }
            const metrics = getActiveMetrics('facebook');
            const aggs = {};
            metrics.forEach(m => aggs[m.key] = m.original);

            // Search for the specific linked FB Page
            const payload = {
                aggregations: aggs,
                filters: {[childFilterKey]: childPlatformId, account_type: FACEBOOK_PAGE_ACCOUNT_TYPE},
                groupBy: ["page", "page_id", "page_title"],
                startDate: start, endDate: end
            };
            const res = await fetch('/facebook_organic/metric/aggregate', {
                method: 'POST',
                headers,
                body: JSON.stringify(payload)
            }).then(r => r.json());

            if (res.status === 'success' && res.data) {
                renderFacebookSubtable(container, res.data, rowId);
            } else {
                container.innerHTML = `<div class="empty-state">No linked Facebook Page data found for this period.</div>`;
            }
        } else if (level === 'content') {
            // Level content is reached from a FB row (row.page) or IG row (row.channeledAccount)
            // parentId is either a FB Page URL/ID or an IG Account ID
            const isFromFb = rowId.includes('-fb-');
            const postsAggregateConfig = getPostsAggregateConfig();
            const groupBy = ["post", "post_id", "caption", "message", "media_type", "permalink", "permalink_url", "timestamp", "created_time"];
            const filters = isFromFb
                ? {page: parentId, account_type: FACEBOOK_PAGE_ACCOUNT_TYPE, post: 'NOT_NULL'}
                : {
                    channeledAccount: parentId,
                    account_type: INSTAGRAM_ACCOUNT_TYPE,
                    post: 'NOT_NULL'
                };

            const metrics = getActiveMetrics('content', isFromFb, postsAggregateConfig.mode);
            const aggs = {};
            metrics.forEach(m => aggs[m.key] = m.original);

            const aggregateResult = await fetchContentAggregate({
                headers,
                aggregations: aggs,
                filters,
                groupBy,
                startDate: start,
                endDate: end,
                mode: postsAggregateConfig.mode,
                fallbackMode: postsAggregateConfig.fallbackMode,
            });
            const res = {
                status: 'success',
                data: aggregateResult.data,
                meta: aggregateResult.meta || {},
            };

            if (res.status === 'success' && res.data) {
                renderContentSubtable(container, res.data, isFromFb, postsAggregateConfig, res.meta);
            } else {
                container.innerHTML = `<div class="empty-state">No organic content found for this period.</div>`;
            }
        }
    } catch (e) {
        console.error("Hierarchy error:", e);
        container.innerHTML = `<div class="error-state">${e.message}</div>`;
    }
    lucide.createIcons();
}


async function fetchContentAggregate({headers, aggregations, filters, groupBy, startDate, endDate, mode, fallbackMode}) {
    const modeFilters = {
        ...filters,
        snapshot_fallback_mode: fallbackMode,
    };

    if (mode === POSTS_AGGREGATE_MODES.DAILY) {
        modeFilters.period = 'daily';
    } else if (mode === POSTS_AGGREGATE_MODES.SNAPSHOT_DELTA) {
        modeFilters.period = 'lifetime';
        modeFilters.snapshot_delta = true;
    } else {
        modeFilters.period = 'lifetime';
        modeFilters.latest_snapshot = true;
    }

    const payload = {
        aggregations,
        filters: modeFilters,
        groupBy,
        startDate,
        endDate,
    };

    const res = await fetch('/facebook_organic/metric/aggregate', {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
    }).then(r => r.json());

    if (res.status === 'success' && Array.isArray(res.data)) {
        return {data: res.data, meta: res.meta || {}};
    }

    return {data: [], meta: {}};
}

function renderFacebookSubtable(container, data, parentRowId) {
    const metrics = getActiveMetrics('facebook');
    let html = `
        <div class="breakdown-title" style="color:#1877F2;"><i data-lucide="facebook"></i> Linked Facebook Pages Metrics</div>
        <table class="nested-table">
            <thead>
                <tr>
                    <th style="width:50px;"></th>
                    <th style="text-align: left;">PAGE NAME</th>
                    ${metrics.map(m => `<th style="text-align: right;">${m.label}</th>`).join('')}
                </tr>
            </thead>
            <tbody>`;

    data.forEach(row => {
        const platformUrl = row.page;
        const pageNumericId = row.page_id || row.page_id_id;
        const subRowId = `row-fb-${pageNumericId}`.replace(/[^a-z0-9\-]/gi, '-');

        const displayName = row.page_title || row.account || platformUrl;
        let nameHtml = `<strong>${displayName}</strong>`;
        if (platformUrl && platformUrl !== 'N/A' && platformUrl.startsWith('http')) {
            nameHtml = `<a href="${platformUrl}" target="_blank" class="clickable-text"><strong>${displayName}</strong> <i data-lucide="external-link" size="10"></i></a>`;
        }

        html += `
            <tr id="${subRowId}">
                <td class="text-center">
                    <button class="btn-expand" onclick="toggleOrganicHierarchy(this, '${subRowId}', 'content', '${pageNumericId}', null)" title="View Posts">
                        <i data-lucide="image" size="12"></i>
                    </button>
                </td>
                <td style="text-align: left;">${nameHtml}</td>
                ${metrics.map(m => `<td style="text-align: right;">${formatMetricValue(m, row[m.key])}</td>`).join('')}
            </tr>`;
    });
    html += `</tbody></table>`;
    container.innerHTML = html;
}

function renderContentSubtable(container, data, isFb = false, postsAggregateConfig = {mode: POSTS_AGGREGATE_MODES.SNAPSHOT, fallbackMode: SNAPSHOT_FALLBACK_MODES.RESILIENT}, responseMeta = {}) {
    const metrics = getActiveMetrics('content', isFb, postsAggregateConfig.mode);
    const modeLabelMap = {
        [POSTS_AGGREGATE_MODES.DAILY]: 'Daily metrics',
        [POSTS_AGGREGATE_MODES.SNAPSHOT]: 'Latest snapshot',
        [POSTS_AGGREGATE_MODES.SNAPSHOT_DELTA]: 'Snapshot delta'
    };
    const modeLabel = modeLabelMap[postsAggregateConfig.mode] || modeLabelMap[POSTS_AGGREGATE_MODES.SNAPSHOT];
    const fallbackHint = postsAggregateConfig.mode === POSTS_AGGREGATE_MODES.DAILY
        ? ''
        : ` (${(postsAggregateConfig.fallbackMode || SNAPSHOT_FALLBACK_MODES.RESILIENT).toUpperCase()})`;
    const fallbackDate = responseMeta?.fallback_end_date;
    const fallbackMeta = fallbackDate
        ? `<span class="badge-dim" style="margin-left:8px; font-size:.72rem;">fallback_end_date: ${Array.isArray(fallbackDate) ? fallbackDate.join(', ') : fallbackDate}</span>`
        : '';
    let html = `
        <div class="breakdown-title" style="color:#8b5cf6;"><i data-lucide="image"></i> Organic Content Performance <span class="badge-dim" style="margin-left:8px; font-size:.72rem;">${modeLabel}${fallbackHint}</span>${fallbackMeta}</div>
        <table class="nested-table">
            <thead>
                <tr>
                    <th style="text-align: left;">CONTENT / CAPTION</th>
                    <th style="text-align: center;">TYPE</th>
                    <th style="text-align: center;">DATE</th>
                    ${metrics.map(m => `<th style="text-align: right;">${m.label}</th>`).join('')}
                </tr>
            </thead>
            <tbody>`;

    data.forEach(row => {
        let caption = 'No caption';
        if (row.caption && row.caption !== 'N/A') caption = row.caption;
        else if (row.message && row.message !== 'N/A') caption = row.message;

        let linkDetails = '';
        let theLink = null;
        if (row.permalink && row.permalink !== 'N/A') theLink = row.permalink;
        else if (row.permalink_url && row.permalink_url !== 'N/A') theLink = row.permalink_url;

        const shortCaption = caption.length > 80 ? caption.substring(0, 80) + '...' : caption;
        if (theLink) {
            linkDetails = `<a href="${theLink}" target="_blank" class="clickable-text" title="${caption.replace(/"/g, '&quot;')}">${shortCaption} <i data-lucide="external-link" size="10"></i></a>`;
        } else {
            linkDetails = `<div class="clickable-text" title="${caption.replace(/"/g, '&quot;')}">${shortCaption}</div>`;
        }

        let dateVal = 'N/A';
        if (row.timestamp && row.timestamp !== 'N/A') dateVal = row.timestamp;
        else if (row.created_time && row.created_time !== 'N/A') dateVal = row.created_time;

        const formattedDate = dateVal !== 'N/A' ? dayjs(dateVal).format('MMM D, YYYY') : 'Unknown';
        const mediaTypeLabel = (row.media_type && row.media_type !== 'N/A') ? row.media_type : (isFb ? 'POST' : 'IMAGE');

        html += `
            <tr>
                <td style="text-align: left;">${linkDetails}</td>
                <td style="text-align: center;"><span class="badge-dim" style="font-size:0.7em; letter-spacing:0.5px; padding:2px 6px;">${mediaTypeLabel}</span></td>
                <td style="text-align: center;"><span style="color:#94a3b8; font-size:0.85em;">${formattedDate}</span></td>
                ${metrics.map(m => `<td style="text-align: right;">${formatMetricValue(m, row[m.key])}</td>`).join('')}
            </tr>`;
    });
    html += `</tbody></table>`;
    container.innerHTML = html;
}

// --- Utils ---
function getAdminHeaders() {
    if (window.AUTH_BYPASS) return {'Authorization': 'Bearer DEMO_BYPASS', 'Content-Type': 'application/json'};
    let auth = JSON.parse(localStorage.getItem('apis_hub_admin_auth') || '{}');
    return {'Authorization': 'Bearer ' + (auth.token || ''), 'Content-Type': 'application/json'};
}

function formatMetricValue(metric, value) {
    if (metric?.format === 'duration_ms') {
        return formatDurationMs(value);
    }

    return formatNum(value, metric?.precision ?? 0);
}

function formatNum(v, precision = 0) {
    return (parseFloat(v) || 0).toLocaleString('en-US', {
        minimumFractionDigits: precision,
        maximumFractionDigits: precision
    });
}

function formatDurationMs(value) {
    const ms = parseFloat(value) || 0;
    if (ms <= 0) return '0s';

    const totalSeconds = ms / 1000;
    if (totalSeconds < 60) {
        const precision = totalSeconds < 10 && Math.abs(totalSeconds - Math.round(totalSeconds)) > 0.001 ? 1 : 0;
        return `${totalSeconds.toLocaleString('en-US', {
            minimumFractionDigits: precision,
            maximumFractionDigits: precision
        })}s`;
    }

    const totalWholeSeconds = Math.round(totalSeconds);
    const hours = Math.floor(totalWholeSeconds / 3600);
    const minutes = Math.floor((totalWholeSeconds % 3600) / 60);
    const seconds = totalWholeSeconds % 60;

    if (hours > 0) {
        return `${hours}h ${minutes}m ${seconds}s`;
    }

    return `${minutes}m ${seconds}s`;
}

function renderSparkline(container, points, color = '#6366F1', start, end) {
    if (!container || !points || points.length < 2) return;
    const width = 80;
    const height = 24;
    const max = Math.max(...points);
    const min = Math.min(...points);
    const range = (max - min) || 1;
    const svgPoints = points.map((p, i) => `${(i / (points.length - 1)) * width},${height - ((p - min) / range) * (height - 4) - 2}`);
    const strokeColor = points[points.length - 1] >= points[0] ? '#10B981' : '#EF4444';
    container.innerHTML = `<svg width="${width}" height="${height}" style="overflow:visible">
        <polyline fill="none" stroke="${strokeColor}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" points="${svgPoints.join(' ')}" />
    </svg>`;
}

function renderSummaryFields() {
    const sums = currentData.reduce((acc, r) => {
        acc.reach += parseInt(r.reach || 0);
        acc.views += parseInt(r.views || 0);
        acc.inter += parseInt(r.total_interactions || 0);
        acc.follows += parseInt(r.follows_and_unfollows || 0);
        return acc;
    }, {reach: 0, views: 0, inter: 0, follows: 0});

    document.getElementById('total-reach').textContent = formatNum(sums.reach);
    document.getElementById('total-impressions').textContent = formatNum(sums.views);
    document.getElementById('total-interactions').textContent = formatNum(sums.inter);
    document.getElementById('total-followers').textContent = formatNum(sums.follows);
    document.getElementById('total-eng-rate').textContent = ((sums.inter / (sums.reach || 1)) * 100).toFixed(2) + '%';
}

function forceRefresh() {
    const modal = document.getElementById('flush-modal');
    if (modal) modal.classList.add('active');
}

function closeFlushModal() {
    const modal = document.getElementById('flush-modal');
    if (modal) modal.classList.remove('active');
}

async function confirmFlush() {
    closeFlushModal();
    const loader = document.getElementById('loader');
    if (loader) loader.style.display = 'flex';
    try {
        await fetch('/api/config-manager/flush-cache', {
            method: 'POST', headers: getAdminHeaders(), body: JSON.stringify({channel: 'facebook_organic'})
        });
        loadReport();
    } catch (err) {
        console.error(err);
    } finally {
        if (loader) loader.style.display = 'none';
    }
}

window.loadReport = loadReport;
window.forceRefresh = forceRefresh;
window.confirmFlush = confirmFlush;
window.closeFlushModal = closeFlushModal;
window.toggleOrganicHierarchy = toggleOrganicHierarchy;

document.addEventListener('DOMContentLoaded', initDashboard);
