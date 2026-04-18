<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Services;

use Anibalealvarezs\MetaHubDriver\Conversions\FacebookMarketingConvert;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicConvert;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Response;
use Helpers\Helpers;

class FacebookEntitySync
{
    /**
     * @param array $config
     * @param string $entityKey
     * @param string $filterType
     * @return string|null
     */
    public static function getFacebookFilter(array $config, string $entityKey = '', string $filterType = 'cache_include'): ?string
    {
        return $config[$entityKey][$filterType] ?? null;
    }

    private static function matchesFilter(string $value, ?string $include, ?string $exclude): bool
    {
        if ($exclude && preg_match($exclude, $value)) {
            return false;
        }
        if ($include && !preg_match($include, $value)) {
            return false;
        }
        return true;
    }

    /**
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $channeledAccounts
     * @param callable|null $entityProcessor
     * @return Response
     */
    public static function syncCampaigns(
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $channeledAccounts = null,
        ?callable $entityProcessor = null
    ): Response {
        try {
            $authorizedIdsMap = [];
            
            if (empty($channeledAccounts)) {
                $logger?->info("No channeled accounts provided for campaigns sync.");
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccount) {
                $adAccountId = (string)$channeledAccount->getPlatformId();
                $logger?->info("DEBUG: FacebookEntitySync::syncCampaigns - Processing ad account: $adAccountId");

                // Get config for this specific account
                $adAccountCfg = array_values(array_filter($config['ad_accounts'] ?? [], fn ($acc) => (string)$acc['id'] === $adAccountId))[0] ?? [];

                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg['campaigns'])) {
                    $logger?->info("Skipping campaigns sync for ad account: $adAccountId (disabled in config)");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    try {
                        $campaigns = $api->getCampaigns(
                            adAccountId: $adAccountId,
                            limit: $currentLimit,
                            additionalParams: [
                                'filtering' => json_encode([[
                                    'field' => 'effective_status',
                                    'operator' => 'IN',
                                    'value' => ["ACTIVE", "PAUSED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"]
                                ]])
                            ]
                        );
                        if (! empty($campaigns['data'])) {
                            $logger?->info("DEBUG: FacebookEntitySync::syncCampaigns - API found " . count($campaigns['data']) . " campaigns for account: $adAccountId");
                            $includeFilter = self::getFacebookFilter($config, 'CAMPAIGN', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'CAMPAIGN', 'cache_exclude');

                            $filteredCampaigns = [];
                            foreach ($campaigns['data'] as $c) {
                                $cName = $c['name'] ?? '';
                                $cId = (string)$c['id'];
                                if (self::matchesFilter($cName, $includeFilter, $excludeFilter) || self::matchesFilter($cId, $includeFilter, $excludeFilter)) {
                                    $authorizedIdsMap[$adAccountId][] = $cId;
                                    $filteredCampaigns[] = $c;
                                } else {
                                    $logger?->info("Skipping campaign $cId ($cName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredCampaigns)) {
                                $logger?->info("DEBUG: FacebookEntitySync::syncCampaigns - " . count($filteredCampaigns) . " campaigns passed the filters for account: $adAccountId");
                                $converted = FacebookMarketingConvert::campaigns($filteredCampaigns, $channeledAccount->getId());
                                foreach ($converted as $item) {
                                    if ($entityProcessor) {
                                        $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                        ($entityProcessor)($item, 'campaign');
                                    }
                                }
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                            $currentLimit = max(10, (int) floor($currentLimit / 2));
                        }
                        $logger?->error("Error in syncCampaigns loop: " . $e->getMessage());
                        $retryCount++;
                        if ($retryCount >= $maxRetries) break;
                        usleep(200000 * $retryCount);
                    }
                }
            }

            return new Response(json_encode([
                'message' => 'Campaigns synchronized',
                'authorized_ids_map' => $authorizedIdsMap,
            ]), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger?->error("Error in FacebookEntitySync::syncCampaigns: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }


    /**
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $channeledAccounts
     * @param array|null $parentIdsMap
     * @param callable|null $entityProcessor
     * @return Response
     */
    public static function syncAdGroups(
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $channeledAccounts = null,
        ?array $parentIdsMap = null,
        ?callable $entityProcessor = null
    ): Response {
        try {
            $authorizedIdsMap = [];
            
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccount) {
                $adAccountId = (string)$channeledAccount->getPlatformId();
                $logger?->info("DEBUG: FacebookEntitySync::syncAdGroups - Processing ad account: $adAccountId");

                // Get config for this specific account
                $adAccountCfg = array_values(array_filter($config['ad_accounts'] ?? [], fn ($acc) => (string)$acc['id'] === $adAccountId))[0] ?? [];

                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg['adsets'])) {
                    $logger?->info("Skipping adsets sync for ad account: $adAccountId (disabled in config)");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    try {
                        $additionalParams = [];
                        if ($parentIdsMap && isset($parentIdsMap[$adAccountId])) {
                            $additionalParams['filtering'] = json_encode([
                                [
                                    'field' => 'campaign.id',
                                    'operator' => 'IN',
                                    'value' => $parentIdsMap[$adAccountId],
                                ],
                                [
                                    'field' => 'effective_status',
                                    'operator' => 'IN',
                                    'value' => ["ACTIVE", "PAUSED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"]
                                ]
                            ]);
                        } else {
                            $additionalParams['filtering'] = json_encode([[
                                'field' => 'effective_status',
                                'operator' => 'IN',
                                'value' => ["ACTIVE", "PAUSED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"]
                            ]]);
                        }

                        $adsets = $api->getAdsets(
                            adAccountId: $adAccountId, 
                            limit: $currentLimit, 
                            additionalParams: $additionalParams
                        );
                        if (! empty($adsets['data'])) {
                            $includeFilter = self::getFacebookFilter($config, 'ADSET', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'ADSET', 'cache_exclude');

                            $filteredAdsets = [];
                            foreach ($adsets['data'] as $a) {
                                $aName = $a['name'] ?? '';
                                $aId = (string)$a['id'];
                                if (self::matchesFilter($aName, $includeFilter, $excludeFilter) || self::matchesFilter($aId, $includeFilter, $excludeFilter)) {
                                    $authorizedIdsMap[$adAccountId][] = $aId;
                                    $filteredAdsets[] = $a;
                                } else {
                                    $logger?->info("Skipping adset $aId ($aName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredAdsets)) {
                                $converted = FacebookMarketingConvert::adsets($filteredAdsets, $channeledAccount->getId());
                                foreach ($converted as $item) {
                                    if ($entityProcessor) {
                                        $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                        ($entityProcessor)($item, 'ad_group');
                                    }
                                }
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                            $currentLimit = max(10, (int) floor($currentLimit / 2));
                            $logger?->warning("Data limit error for $adAccountId in syncAdGroups (AdSets): Reducing limit to $currentLimit");
                        }
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger?->error("Error syncing adsets for $adAccountId: " . $e->getMessage());
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
            }

            return new Response(json_encode([
                'message' => 'AdGroups synchronized',
                'authorized_ids_map' => $authorizedIdsMap,
            ]), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger?->error("Error in FacebookEntitySync::syncAdGroups: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $channeledAccounts
     * @param array|null $parentIdsMap
     * @param callable|null $entityProcessor
     * @return Response
     */
    public static function syncAds(
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $channeledAccounts = null,
        ?array $parentIdsMap = null,
        ?callable $entityProcessor = null
    ): Response {
        try {
            $authorizedIdsMap = [];
            
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccount) {
                $adAccountId = (string)$channeledAccount->getPlatformId();
                $logger?->info("DEBUG: FacebookEntitySync::syncAds - Processing ad account: $adAccountId");

                // Get config for this specific account
                $adAccountCfg = array_values(array_filter($config['ad_accounts'] ?? [], fn ($acc) => (string)$acc['id'] === $adAccountId))[0] ?? [];

                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg['ads'])) {
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    try {
                        $additionalParams = [];
                        if ($parentIdsMap && isset($parentIdsMap[$adAccountId])) {
                            $additionalParams['filtering'] = json_encode([
                                [
                                    'field' => 'adset.id',
                                    'operator' => 'IN',
                                    'value' => $parentIdsMap[$adAccountId],
                                ],
                                [
                                    'field' => 'effective_status',
                                    'operator' => 'IN',
                                    'value' => ["ACTIVE", "PAUSED", "ARCHIVED", "DELETED", "IN_PROCESS", "WITH_ISSUES"]
                                ]
                            ]);
                        } else {
                            $additionalParams['filtering'] = json_encode([[
                                'field' => 'effective_status',
                                'operator' => 'IN',
                                'value' => ["ACTIVE", "PAUSED", "ARCHIVED", "DELETED", "IN_PROCESS", "WITH_ISSUES"]
                            ]]);
                        }
                        $ads = $api->getAds(
                            adAccountId: $adAccountId, 
                            limit: $currentLimit, 
                            additionalParams: $additionalParams
                        );
                        if (! empty($ads['data'])) {
                            $includeFilter = self::getFacebookFilter($config, 'AD', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'AD', 'cache_exclude');

                            $filteredAds = [];
                            foreach ($ads['data'] as $a) {
                                $aName = $a['name'] ?? '';
                                $aId = (string)$a['id'];
                                if (self::matchesFilter($aName, $includeFilter, $excludeFilter) || self::matchesFilter($aId, $includeFilter, $excludeFilter)) {
                                    $authorizedIdsMap[$adAccountId][] = $aId;
                                    $filteredAds[] = $a;
                                } else {
                                    $logger?->info("Skipping ad $aId ($aName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredAds)) {
                                $converted = FacebookMarketingConvert::ads($filteredAds, $channeledAccount->getId());
                                foreach ($converted as $item) {
                                    if ($entityProcessor) {
                                        $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                        ($entityProcessor)($item, 'ad');
                                    }
                                }
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                            $currentLimit = max(10, (int) floor($currentLimit / 2));
                            $logger?->warning("Data limit error for $adAccountId in syncAds: Reducing limit to $currentLimit");
                        }
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger?->error("Error fetching ads for $adAccountId: " . $e->getMessage());
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
            }

            return new Response(json_encode(['message' => 'Ads synchronized', 'authorized_ids_map' => $authorizedIdsMap]), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger?->error("Error in FacebookEntitySync::syncAds: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $channeledAccounts
     * @param callable|null $entityProcessor
     * @return Response
     */
    public static function syncCreatives(
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $channeledAccounts = null,
        ?callable $entityProcessor = null
    ): Response {
        try {
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccount) {
                $adAccountId = (string)$channeledAccount->getPlatformId();
                $logger?->info("DEBUG: FacebookEntitySync::syncCreatives - Processing ad account: $adAccountId");

                // Get config for this specific account
                $adAccountCfg = array_values(array_filter($config['ad_accounts'] ?? [], fn ($acc) => (string)$acc['id'] === $adAccountId))[0] ?? [];

                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg['creatives'])) {
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    try {
                        $creatives = $api->getCreatives(adAccountId: $adAccountId, limit: $currentLimit);
                        if (! empty($creatives['data'])) {
                            $includeFilter = self::getFacebookFilter($config, 'CREATIVE', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'CREATIVE', 'cache_exclude');

                            $filteredCreatives = [];
                            foreach ($creatives['data'] as $c) {
                                $cName = $c['name'] ?? $c['title'] ?? '';
                                $cId = (string)$c['id'];
                                if (self::matchesFilter($cName, $includeFilter, $excludeFilter) || self::matchesFilter($cId, $includeFilter, $excludeFilter)) {
                                    $filteredCreatives[] = $c;
                                } else {
                                    $logger?->info("Skipping creative $cId ($cName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredCreatives)) {
                                $converted = FacebookMarketingConvert::creatives($filteredCreatives, $channeledAccount->getId());
                                foreach ($converted as $item) {
                                    if ($entityProcessor) {
                                        $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                        ($entityProcessor)($item, 'creative');
                                    }
                                }
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                            $currentLimit = max(10, (int) floor($currentLimit / 2));
                            $logger?->warning("Data limit error for $adAccountId in syncCreatives: Reducing limit to $currentLimit");
                        }
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger?->error("Error fetching creatives for $adAccountId: " . $e->getMessage());
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
            }

            return new Response(json_encode(['message' => 'Creatives synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger?->error("Error in FacebookEntitySync::syncCreatives: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $channeledAccounts
     * @param callable|null $entityProcessor
     * @return Response
     */
    public static function syncPages(
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $channeledAccounts = null,
        ?callable $entityProcessor = null
    ): Response {
        try {
            $pagesFromConfig = $config['pages'] ?? [];

            // Case 1: Organic pages from config (manually configured)
            if (empty($channeledAccounts) && !empty($pagesFromConfig)) {
                foreach ($pagesFromConfig as $pageCfg) {
                    if (empty($pageCfg['enabled'])) continue;
                    $pId = (string)($pageCfg['id'] ?? '');
                    if (!$pId) continue;
                    Helpers::checkJobStatus($jobId);

                    $logger?->info("DEBUG: FacebookEntitySync::syncPages - Syncing Page $pId from config");
                    
                    try {
                        // Prepare data for conversion
                        $pageData = [
                            'id' => $pId,
                            'title' => $pageCfg['title'] ?? $pageCfg['name'] ?? null,
                            'name' => $pageCfg['name'] ?? $pageCfg['title'] ?? null,
                            'url' => $pageCfg['url'] ?? ('https://www.facebook.com/' . $pId),
                            'hostname' => $pageCfg['hostname'] ?? 'facebook.com',
                        ];

                        $converted = FacebookOrganicConvert::pages([$pageData]);
                        foreach ($converted as $item) {
                            if ($entityProcessor) {
                                ($entityProcessor)($item, 'page');
                            }
                        }
                    } catch (\Exception $e) {
                        $logger?->error("Error syncing organic page $pId from config: " . $e->getMessage());
                    }
                }
            }

            // Case 2: Marketing pages discovery via Ad Accounts
            if (!empty($channeledAccounts)) {
                foreach ($channeledAccounts as $channeledAccount) {
                    Helpers::checkJobStatus($jobId);
                    $adAccountId = (string)$channeledAccount->getPlatformId();
                    $logger?->info("DEBUG: FacebookEntitySync::syncPages - Processing discovery for Ad Account $adAccountId");

                    // Check if discovery is enabled for this account in config
                    $adAccountCfg = array_values(array_filter($config['ad_accounts'] ?? [], fn ($acc) => (string)$acc['id'] === $adAccountId))[0] ?? [];
                    if (empty($adAccountCfg['enabled']) || empty($adAccountCfg['pages'])) {
                        continue;
                    }

                    $maxRetries = 3;
                    $retryCount = 0;
                    $fetched = false;

                    while ($retryCount < $maxRetries && ! $fetched) {
                        try {
                            $pages = $api->getPages(userId: $adAccountId);
                            if (! empty($pages['data'])) {
                                $includeFilter = self::getFacebookFilter($config, 'PAGE', 'cache_include');
                                $excludeFilter = self::getFacebookFilter($config, 'PAGE', 'cache_exclude');

                                $filteredPages = [];
                                foreach ($pages['data'] as $p) {
                                    $pName = $p['name'] ?? '';
                                    $pId = (string)$p['id'];
                                    if (self::matchesFilter($pName, $includeFilter, $excludeFilter) || self::matchesFilter($pId, $includeFilter, $excludeFilter)) {
                                        $filteredPages[] = $p;
                                    } else {
                                        $logger?->info("Skipping page $pId ($pName) - filtered out by extraction patterns");
                                    }
                                }

                                if (! empty($filteredPages)) {
                                    $converted = FacebookOrganicConvert::pages($filteredPages, $channeledAccount->getId());
                                    foreach ($converted as $item) {
                                        if ($entityProcessor) {
                                            $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                            ($entityProcessor)($item, 'page');
                                        }
                                    }
                                }
                            }
                            $fetched = true;
                        } catch (\Exception $e) {
                            $retryCount++;
                            if ($retryCount >= $maxRetries) {
                                $logger?->error("Error fetching pages for discovery on $adAccountId: " . $e->getMessage());
                            } else {
                                usleep(200000 * $retryCount);
                            }
                        }
                    }
                }
            }

            return new Response(json_encode(['message' => 'Pages synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger?->error("Error in FacebookEntitySync::syncPages: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $channeledPages
     * @param callable|null $entityProcessor
     * @return Response
     */
    public static function syncPosts(
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $channeledPages = null,
        ?callable $entityProcessor = null,
        int|string|null $channeledAccountId = null,
        int|string|null $accountId = null
    ): Response {
        try {
            if (empty($channeledPages)) {
                return new Response(json_encode(['message' => 'No pages to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledPages as $channeledPage) {
                Helpers::checkJobStatus($jobId);
                $pageId = (string)$channeledPage->getPlatformId();
                $logger?->info(">>> INICIO: Sincronizando posts para FB Page: $pageId (Timeframe: $startDate a $endDate)");
                
                $fetched = false;
                $postLimits = [100, 50, 25, 10];

                foreach ($postLimits as $limit) {
                    if ($fetched) break;
                    $maxRetries = 3;
                    $retryCount = 0;
                    while ($retryCount < $maxRetries && ! $fetched) {
                        try {
                            $api->setPageId($pageId);
                            $api->setSampleBasedToken(\Anibalealvarezs\FacebookGraphApi\Enums\TokenSample::PAGE);
                            $posts = $api->getFacebookPosts(pageId: $pageId, limit: $limit);
                            if (! empty($posts['data'])) {
                                $includeFilter = self::getFacebookFilter($config, 'POST', 'cache_include');
                                $excludeFilter = self::getFacebookFilter($config, 'POST', 'cache_exclude');

                                $filteredPosts = [];
                                foreach ($posts['data'] as $p) {
                                    $pName = $p['message'] ?? $p['story'] ?? '';
                                    $pId = (string)$p['id'];
                                    if (self::matchesFilter($pName, $includeFilter, $excludeFilter) || self::matchesFilter($pId, $includeFilter, $excludeFilter)) {
                                        $filteredPosts[] = $p;
                                    }
                                }

                                if (! empty($filteredPosts)) {
                                    $pageAccountId = $accountId ?: ((method_exists($channeledPage, 'getAccount') && $channeledPage->getAccount()) ? $channeledPage->getAccount()->getId() : null);
                                    $converted = FacebookOrganicConvert::posts(
                                        posts: $filteredPosts, 
                                        pageId: $channeledPage->getId(),
                                        accountId: $pageAccountId,
                                        channeledAccountId: $channeledAccountId
                                    );
                                    $saveCount = 0;
                                    foreach ($converted as $item) {
                                        if ($entityProcessor) {
                                            $item->setContext(array_merge($item->getContext(), ['facebookPage' => $channeledPage]));
                                            ($entityProcessor)($item, 'post');
                                            $saveCount++;
                                        }
                                    }
                                    $logger?->info("<<< EXITO: Se sincronizaron $saveCount posts para FB Page: $pageId");
                                } else {
                                    $logger?->info("--- INFO: No se encontraron posts que coincidan con los filtros para FB Page: $pageId");
                                }
                            }
                            $fetched = true;
                        } catch (\Exception $e) {
                            $retryCount++;
                            $errMsg = $e->getMessage();
                            if (str_contains($errMsg, 'reduce the amount of data')) {
                                $logger?->info("Meta requested data reduction for page $pageId at limit=$limit. Trying smaller limit.");
                                break;
                            }
                            if ($retryCount >= $maxRetries) {
                                $logger?->error("Error fetching posts for page $pageId: $errMsg");
                            } else {
                                usleep(200000 * $retryCount);
                            }
                        }
                    }
                }
            }

            return new Response(json_encode(['message' => 'Posts synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger?->error("Error in FacebookEntitySync::syncPosts: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $channeledAccounts
     * @param callable|null $entityProcessor
     * @return Response
     */
    public static function syncInstagramMedia(
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $channeledAccounts = null,
        ?callable $entityProcessor = null
    ): Response {
        try {
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccount) {
                $igId = (string)$channeledAccount->getPlatformId();
                $logger?->info("DEBUG: FacebookEntitySync::syncInstagramMedia - START processing IG account " . $igId);
                
                Helpers::checkJobStatus($jobId);
                
                $fetched = false;
                $mediaLimits = [100, 50, 25, 10];

                $logger?->info(">>> INICIO: Sincronizando media para IG Account: $igId (Timeframe: $startDate a $endDate)");
                
                foreach ($mediaLimits as $limit) {
                    if ($fetched) break;
                    $maxRetries = 3;
                    $retryCount = 0;

                    while ($retryCount < $maxRetries && !$fetched) {
                        try {
                            // Find page ID from config if possible (some IG API calls need the page ID)
                            $pageCfg = array_values(array_filter($config['pages'] ?? [], fn ($p) => (string)($p['ig_account'] ?? '') === $igId))[0] ?? [];
                            if (!empty($pageCfg['id'])) {
                                $api->setPageId((string)$pageCfg['id']);
                                $api->setSampleBasedToken(\Anibalealvarezs\FacebookGraphApi\Enums\TokenSample::PAGE);
                            }
                            
                            $media = $api->getInstagramMedia(igUserId: $igId, limit: $limit);
                            
                            if (!empty($media['data'])) {
                                $includeFilter = self::getFacebookFilter($config, 'POST', 'cache_include');
                                $excludeFilter = self::getFacebookFilter($config, 'POST', 'cache_exclude');

                                $filteredMedia = [];
                                foreach ($media['data'] as $m) {
                                    $mCaption = $m['caption'] ?? '';
                                    $mId = (string)$m['id'];
                                    if (self::matchesFilter($mCaption, $includeFilter, $excludeFilter) || self::matchesFilter($mId, $includeFilter, $excludeFilter)) {
                                        $filteredMedia[] = $m;
                                    }
                                }

                                if (!empty($filteredMedia)) {
                                    $converted = FacebookOrganicConvert::media(
                                        $filteredMedia, 
                                        null, 
                                        null, // accountId resolved in host
                                        $channeledAccount->getId()
                                    );
                                    
                                    $saveCount = 0;
                                    foreach ($converted as $item) {
                                        if ($entityProcessor) {
                                            $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                            ($entityProcessor)($item, 'media');
                                            $saveCount++;
                                        }
                                    }
                                    $logger?->info("<<< EXITO: Se sincronizaron $saveCount items de media para IG Account: $igId");
                                } else {
                                    $logger?->info("--- INFO: No se encontró media que coincidan con los filtros para IG Account: $igId");
                                }
                            }
                            $fetched = true;
                        } catch (\Exception $e) {
                            $retryCount++;
                            $errMsg = $e->getMessage();
                            if (str_contains($errMsg, 'reduce the amount of data')) {
                                $logger?->info("Meta requested data reduction for IG $igId at limit=$limit. Trying smaller limit.");
                                break;
                            }
                            if ($retryCount >= $maxRetries) {
                                $logger?->error("Error fetching media for IG $igId: " . $errMsg);
                            } else {
                                usleep(200000 * $retryCount);
                            }
                        }
                    }
                }
                $logger?->info("DEBUG: FacebookEntitySync::syncInstagramMedia - END processing IG account " . $igId);
            }

            return new Response(json_encode(['message' => 'Instagram media synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger?->error("Error in FacebookEntitySync::syncInstagramMedia: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }
}
