<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Services;

use Anibalealvarezs\MetaHubDriver\Conversions\FacebookMarketingConvert;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicConvert;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\MetaHubDriver\Enums\MetaFeature;
use Anibalealvarezs\MetaHubDriver\Enums\MetaEntityType;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Response;

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

    public static function matchesFilter(string $value, ?string $include, ?string $exclude): bool
    {
        if (empty($include) && empty($exclude)) {
            return true;
        }

        if ($include) {
            $matchedInclude = false;
            $includes = array_map('trim', explode(',', $include));
            foreach ($includes as $pattern) {
                if (empty($pattern)) continue;
                if (str_starts_with($pattern, '/') && str_ends_with($pattern, '/')) {
                    if (preg_match($pattern, $value)) {
                        $matchedInclude = true;
                        break;
                    }
                } elseif (stripos($value, $pattern) !== false) {
                    $matchedInclude = true;
                    break;
                }
            }
            if (!$matchedInclude) {
                return false;
            }
        }

        if ($exclude) {
            $excludes = array_map('trim', explode(',', $exclude));
            foreach ($excludes as $pattern) {
                if (empty($pattern)) continue;
                if (str_starts_with($pattern, '/') && str_ends_with($pattern, '/')) {
                    if (preg_match($pattern, $value)) {
                        return false;
                    }
                } elseif (stripos($value, $pattern) !== false) {
                    return false;
                }
            }
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
        ?callable $entityProcessor = null,
        ?callable $jobStatusChecker = null
    ): Response {
        try {
            $authorizedIdsMap = [];
            
            if (empty($channeledAccounts)) {
                $logger?->info("No channeled accounts provided for campaigns sync.");
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $adAccount) {
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }
                $accId = (string)$adAccount->getPlatformId();
                $adAccountCfg = $config['ad_accounts'][$accId] ?? ($config['ad_accounts']['act_' . $accId] ?? []);
                
                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg[MetaFeature::CAMPAIGNS->value])) {
                    $logger?->info("Skipping Campaign sync for account $accId (disabled in config)");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && !$fetched) {
                    try {
                        $includeFilter = self::getFacebookFilter($config, 'CAMPAIGN', 'cache_include');
                        $excludeFilter = self::getFacebookFilter($config, 'CAMPAIGN', 'cache_exclude');

                        $api->getCampaignsAndProcess(
                            callback: function ($pageData) use (&$authorizedIdsMap, $accId, $adAccount, $includeFilter, $excludeFilter, $entityProcessor, $logger) {
                                $filteredCampaigns = [];
                                foreach ($pageData as $c) {
                                    $cName = $c['name'] ?? '';
                                    $cId = (string)$c['id'];
                                    $matches = self::matchesFilter($cName, $includeFilter, $excludeFilter) || self::matchesFilter($cId, $includeFilter, $excludeFilter);
                                    if ($matches) {
                                        $authorizedIdsMap[$accId][] = $cId;
                                        $filteredCampaigns[] = $c;
                                    } else {
                                        $logger?->info("Skipping campaign $cId ($cName) - filtered out by extraction patterns.");
                                    }
                                }

                                if (!empty($filteredCampaigns)) {
                                    $converted = FacebookMarketingConvert::campaigns($filteredCampaigns, $adAccount->getId());
                                    foreach ($converted as $item) {
                                        if ($entityProcessor) {
                                            $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $adAccount]));
                                            ($entityProcessor)($item, MetaEntityType::CAMPAIGN->value);
                                        }
                                    }
                                }
                            },
                            adAccountId: $accId,
                            limit: $currentLimit,
                            additionalParams: [
                                'filtering' => json_encode([[
                                    'field' => 'effective_status',
                                    'operator' => 'IN',
                                    'value' => ["ACTIVE", "PAUSED", "ARCHIVED", "DELETED", "IN_PROCESS", "WITH_ISSUES"]
                                ]])
                            ]
                        );
                        $fetched = true;
                    } catch (\Exception $e) {
                        if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                            $currentLimit = max(10, (int) floor($currentLimit / 2));
                        }
                        $logger?->error("Error in syncCampaigns loop internal: " . $e->getMessage());
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
        ?callable $entityProcessor = null,
        ?callable $jobStatusChecker = null
    ): Response {
        try {
            $authorizedIdsMap = [];
            
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $adAccount) {
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }
                $accId = (string)$adAccount->getPlatformId();
                $adAccountCfg = $config['ad_accounts'][$accId] ?? ($config['ad_accounts']['act_' . $accId] ?? []);

                $logger?->info("DEBUG: FacebookEntitySync::syncAdGroups - Account: $accId | Enabled: " . ($adAccountCfg['enabled'] ?? 'N/A') . " | Feature: " . ($adAccountCfg[MetaFeature::ADSETS->value] ?? 'N/A'));
                
                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg[MetaFeature::ADSETS->value])) {
                    $logger?->info("Skipping AdSet sync for account $accId (disabled in config)");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && !$fetched) {
                    try {
                        $includeFilter = self::getFacebookFilter($config, 'ADSET', 'cache_include');
                        $excludeFilter = self::getFacebookFilter($config, 'ADSET', 'cache_exclude');

                        $additionalParams = [];
                        if ($parentIdsMap && isset($parentIdsMap[$accId])) {
                            $additionalParams['filtering'] = json_encode([
                                [
                                    'field' => 'campaign.id',
                                    'operator' => 'IN',
                                    'value' => $parentIdsMap[$accId],
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

                        $api->getAdsetsAndProcess(
                            callback: function ($pageData) use (&$authorizedIdsMap, $accId, $adAccount, $includeFilter, $excludeFilter, $entityProcessor, $logger) {
                                $filteredAdsets = [];
                                foreach ($pageData as $a) {
                                    $aName = $a['name'] ?? '';
                                    $aId = (string)$a['id'];
                                    if (self::matchesFilter($aName, $includeFilter, $excludeFilter) || self::matchesFilter($aId, $includeFilter, $excludeFilter)) {
                                        $authorizedIdsMap[$accId][] = $aId;
                                        $filteredAdsets[] = $a;
                                    } else {
                                        $logger?->info("Skipping adset $aId ($aName) - filtered out by extraction patterns");
                                    }
                                }

                                if (!empty($filteredAdsets)) {
                                    $converted = FacebookMarketingConvert::adsets($filteredAdsets, $adAccount->getId());
                                    foreach ($converted as $item) {
                                        if ($entityProcessor) {
                                            $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $adAccount]));
                                            ($entityProcessor)($item, MetaEntityType::AD_GROUP->value);
                                        }
                                    }
                                }
                            },
                            adAccountId: $accId, 
                            limit: $currentLimit, 
                            additionalParams: $additionalParams
                        );
                        $fetched = true;
                    } catch (\Exception $e) {
                        if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                            $currentLimit = max(10, (int) floor($currentLimit / 2));
                            $logger?->warning("Data limit error for $accId in syncAdGroups (AdSets): Reducing limit to $currentLimit");
                        }
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger?->error("Error syncing adsets for $accId: " . $e->getMessage());
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
        ?callable $entityProcessor = null,
        ?callable $jobStatusChecker = null
    ): Response {
        try {
            $authorizedIdsMap = [];
            
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccountItem) {
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }
                $adAccountId = (string)$channeledAccountItem->getPlatformId();
                $adAccountCfg = $config['ad_accounts'][$adAccountId] ?? ($config['ad_accounts']['act_' . $adAccountId] ?? []);
                
                $logger?->info("DEBUG: FacebookEntitySync::syncAds - Account: $adAccountId | Enabled: " . ($adAccountCfg['enabled'] ?? 'N/A') . " | Feature: " . ($adAccountCfg[MetaFeature::ADS->value] ?? 'N/A'));
                
                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg[MetaFeature::ADS->value])) {
                    $logger?->info("Skipping Ad sync for account $adAccountId (disabled in config)");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && !$fetched) {
                    try {
                        $includeFilter = self::getFacebookFilter($config, 'AD', 'cache_include');
                        $excludeFilter = self::getFacebookFilter($config, 'AD', 'cache_exclude');

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

                        $api->getAdsAndProcess(
                            callback: function ($pageData) use (&$authorizedIdsMap, $adAccountId, $channeledAccountItem, $includeFilter, $excludeFilter, $entityProcessor, $logger) {
                                $filteredAds = [];
                                foreach ($pageData as $a) {
                                    $aName = $a['name'] ?? '';
                                    $aId = (string)$a['id'];
                                    if (self::matchesFilter($aName, $includeFilter, $excludeFilter) || self::matchesFilter($aId, $includeFilter, $excludeFilter)) {
                                        $authorizedIdsMap[$adAccountId][] = $aId;
                                        $filteredAds[] = $a;
                                    } else {
                                        $logger?->info("Skipping ad $aId ($aName) - filtered out by extraction patterns");
                                    }
                                }

                                if (!empty($filteredAds)) {
                                    $converted = FacebookMarketingConvert::ads($filteredAds, $channeledAccountItem->getId());
                                    foreach ($converted as $item) {
                                        if ($entityProcessor) {
                                            $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccountItem]));
                                            ($entityProcessor)($item, MetaEntityType::AD->value);
                                        }
                                    }
                                }
                            },
                            adAccountId: $adAccountId, 
                            limit: $currentLimit, 
                            additionalParams: $additionalParams
                        );
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
        ?callable $entityProcessor = null,
        ?callable $jobStatusChecker = null
    ): Response {
        try {
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccount) {
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }
                $adAccountId = (string)$channeledAccount->getPlatformId();
                $adAccountCfg = $config['ad_accounts'][$adAccountId] ?? ($config['ad_accounts']['act_' . $adAccountId] ?? []);
                
                if (empty($adAccountCfg['enabled']) || empty($adAccountCfg[MetaFeature::CREATIVES->value])) {
                    $logger?->info("Skipping Creative sync for account $adAccountId (disabled in config)");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && !$fetched) {
                    try {
                        $includeFilter = self::getFacebookFilter($config, 'CREATIVE', 'cache_include');
                        $excludeFilter = self::getFacebookFilter($config, 'CREATIVE', 'cache_exclude');

                        $api->getCreativesAndProcess(
                            callback: function ($pageData) use ($adAccountId, $channeledAccount, $includeFilter, $excludeFilter, $entityProcessor, $logger) {
                                $filteredCreatives = [];
                                foreach ($pageData as $c) {
                                    $cName = $c['name'] ?? $c['title'] ?? '';
                                    $cId = (string)$c['id'];
                                    if (self::matchesFilter($cName, $includeFilter, $excludeFilter) || self::matchesFilter($cId, $includeFilter, $excludeFilter)) {
                                        $filteredCreatives[] = $c;
                                    } else {
                                        $logger?->info("Skipping creative $cId ($cName) - filtered out by extraction patterns");
                                    }
                                }

                                if (!empty($filteredCreatives)) {
                                    $converted = FacebookMarketingConvert::creatives($filteredCreatives, $channeledAccount->getId());
                                    foreach ($converted as $item) {
                                        if ($entityProcessor) {
                                            $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                            ($entityProcessor)($item, MetaEntityType::CREATIVE->value);
                                        }
                                    }
                                }
                            },
                            adAccountId: $adAccountId, 
                            limit: $currentLimit
                        );
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
        ?callable $entityProcessor = null,
        ?callable $jobStatusChecker = null
    ): Response {
        try {
            $pagesFromConfig = $config['pages'] ?? [];

            // Case 1: Organic pages from config (manually configured)
            if (empty($channeledAccounts) && !empty($pagesFromConfig)) {
                foreach ($pagesFromConfig as $pageCfg) {
                    if (empty($pageCfg['enabled'])) continue;
                    $pId = (string)($pageCfg['id'] ?? '');
                    if (!$pId) continue;
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }

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
                                ($entityProcessor)($item, MetaEntityType::PAGE->value);
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
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }
                    $adAccountId = (string)$channeledAccount->getPlatformId();
                    $logger?->info("DEBUG: FacebookEntitySync::syncPages - Processing discovery for Ad Account $adAccountId");

                    // Check if discovery is enabled for this account in config
                    $adAccountCfg = $config['ad_accounts'][$adAccountId] ?? [];
                    if (empty($adAccountCfg['enabled']) || empty($adAccountCfg['pages'])) {
                        continue;
                    }

                    $maxRetries = 3;
                    $retryCount = 0;
                    $fetched = false;

                    while ($retryCount < $maxRetries && !$fetched) {
                        try {
                            $includeFilter = self::getFacebookFilter($config, 'PAGE', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'PAGE', 'cache_exclude');

                            $api->getPagesAndProcess(
                                callback: function ($pageData) use ($adAccountId, $channeledAccount, $includeFilter, $excludeFilter, $entityProcessor, $logger) {
                                    $filteredPages = [];
                                    foreach ($pageData as $p) {
                                        $pName = $p['name'] ?? '';
                                        $pId = (string)$p['id'];
                                        if (self::matchesFilter($pName, $includeFilter, $excludeFilter) || self::matchesFilter($pId, $includeFilter, $excludeFilter)) {
                                            $filteredPages[] = $p;
                                        } else {
                                            $logger?->info("Skipping page $pId ($pName) - filtered out by extraction patterns");
                                        }
                                    }

                                    if (!empty($filteredPages)) {
                                        $converted = FacebookOrganicConvert::pages($filteredPages, $channeledAccount->getId());
                                        foreach ($converted as $item) {
                                            if ($entityProcessor) {
                                                $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                                ($entityProcessor)($item, MetaEntityType::PAGE->value);
                                            }
                                        }
                                    }
                                },
                                userId: $adAccountId
                            );
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
     * @param array|int|string|null $channeledAccountId
     * @param int|string|null $accountId
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
        array|int|string|null $channeledAccountId = null,
        int|string|null $accountId = null,
        ?callable $jobStatusChecker = null
    ): Response {
        try {
            $logger?->info("DEBUG: FacebookEntitySync::syncPosts - IDs received: Account: $accountId | ChanneledAccount: $channeledAccountId");
            if (empty($channeledPages)) {
                return new Response(json_encode(['message' => 'No pages to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledPages as $channeledPage) {
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }
                $pageId = (string)$channeledPage->getPlatformId();
 
                $pageCfg = $config['pages'][$pageId] ?? [];
                if ((isset($pageCfg['enabled']) && !$pageCfg['enabled']) || (isset($pageCfg[MetaFeature::POSTS->value]) && !$pageCfg[MetaFeature::POSTS->value])) {
                    $logger?->info("Skipping Post sync for page $pageId (disabled in config)");
                    continue;
                }

                $logger?->info(">>> INICIO: Sincronizando posts para FB Page: $pageId (Timeframe: $startDate a $endDate)");
                
                $fetched = false;
                $postLimits = [100, 50, 25, 10];

                foreach ($postLimits as $limit) {
                    if ($fetched) break;
                    $maxRetries = 3;
                    $retryCount = 0;
                    while ($retryCount < $maxRetries && !$fetched) {
                        try {
                            $api->setPageId($pageId);
                            $api->setSampleBasedToken(\Anibalealvarezs\FacebookGraphApi\Enums\TokenSample::PAGE);

                            $includeFilter = self::getFacebookFilter($config, 'POST', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'POST', 'cache_exclude');

                            $api->getFacebookPostsAndProcess(
                                callback: function ($pageData) use ($pageId, $channeledPage, $includeFilter, $excludeFilter, $entityProcessor, $logger, $accountId, $channeledAccountId) {
                                    $filteredPosts = [];
                                    foreach ($pageData as $p) {
                                        $pName = $p['message'] ?? $p['story'] ?? '';
                                        $pId = (string)$p['id'];
                                        if (self::matchesFilter($pName, $includeFilter, $excludeFilter) || self::matchesFilter($pId, $includeFilter, $excludeFilter)) {
                                            $filteredPosts[] = $p;
                                        }
                                    }

                                    if (!empty($filteredPosts)) {
                                        $pageAccountId = $accountId ?: ((method_exists($channeledPage, 'getAccount') && $channeledPage->getAccount()) ? $channeledPage->getAccount()->getId() : null);

                                        // Resolve specific ChanneledAccount ID for this page if a map is provided
                                        $specificChanneledAccountId = is_array($channeledAccountId)
                                            ? ($channeledAccountId[$pageId]?->getId() ?? null)
                                            : $channeledAccountId;

                                        $converted = FacebookOrganicConvert::posts(
                                            posts: $filteredPosts, 
                                            pageId: $channeledPage->getId(),
                                            accountId: $pageAccountId,
                                            channeledAccountId: $specificChanneledAccountId
                                        );
                                        $saveCount = 0;
                                        foreach ($converted as $item) {
                                            if ($entityProcessor) {
                                                $item->setContext(array_merge($item->getContext(), ['facebookPage' => $channeledPage]));
                                                ($entityProcessor)($item, MetaEntityType::POST->value);
                                                $saveCount++;
                                            }
                                        }
                                        $logger?->info("<<< EXITO: Se sincronizaron $saveCount posts para FB Page: $pageId");
                                    } else {
                                        $logger?->info("--- INFO: No se encontraron posts que coincidan con los filtros para FB Page: $pageId");
                                    }
                                },
                                pageId: $pageId, 
                                limit: $limit
                            );
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
        ?callable $entityProcessor = null,
        ?array $channeledPages = null,
        ?callable $jobStatusChecker = null
    ): Response {
        try {
            if (empty($channeledAccounts)) {
                return new Response(json_encode(['message' => 'No accounts to sync']), 200, ['Content-Type' => 'application/json']);
            }

            foreach ($channeledAccounts as $channeledAccount) {
                $igId = (string)$channeledAccount->getPlatformId();
 
                $pageCfg = array_values(array_filter($config['pages'] ?? [], fn ($p) => (string)($p['ig_account'] ?? '') === $igId))[0] ?? [];
                if ((isset($pageCfg['enabled']) && !$pageCfg['enabled']) || (isset($pageCfg[MetaFeature::IG_ACCOUNT_MEDIA->value]) && !$pageCfg[MetaFeature::IG_ACCOUNT_MEDIA->value])) {
                    $logger?->info("Skipping Instagram media sync for IG account $igId (disabled in config)");
                    continue;
                }

                $logger?->info("DEBUG: FacebookEntitySync::syncInstagramMedia - START processing IG account " . $igId);
                
                if ($jobStatusChecker && ($jobStatusChecker)($jobId) === false) {
                    throw new \Exception("Job cancelled or should not continue.");
                }
                
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
                            if (!empty($pageCfg['id'])) {
                                $api->setPageId((string)$pageCfg['id']);
                                $api->setSampleBasedToken(\Anibalealvarezs\FacebookGraphApi\Enums\TokenSample::PAGE);
                            }
                            
                            $includeFilter = self::getFacebookFilter($config, 'POST', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'POST', 'cache_exclude');

                            $api->getInstagramMediaAndProcess(
                                callback: function ($pageData) use ($igId, $channeledAccount, $includeFilter, $excludeFilter, $entityProcessor, $logger, $pageCfg, $channeledPages) {
                                    $filteredMedia = [];
                                    foreach ($pageData as $m) {
                                        $mCaption = $m['caption'] ?? '';
                                        $mId = (string)$m['id'];
                                        if (self::matchesFilter($mCaption, $includeFilter, $excludeFilter) || self::matchesFilter($mId, $includeFilter, $excludeFilter)) {
                                            $filteredMedia[] = $m;
                                        }
                                    }

                                    if (!empty($filteredMedia)) {
                                        $fbPagePlatformId = (string)($pageCfg['id'] ?? '');
                                        $fbPageEntity = array_values(array_filter($channeledPages ?? [], fn ($p) => (string)$p->getPlatformId() === $fbPagePlatformId))[0] ?? null;
                                        $internalPageId = $fbPageEntity ? $fbPageEntity->getId() : $fbPagePlatformId;
                                        
                                        $igAccountId = (method_exists($channeledAccount, 'getAccount') && $channeledAccount->getAccount()) ? $channeledAccount->getAccount()->getId() : null;

                                        $converted = FacebookOrganicConvert::media(
                                            $filteredMedia, 
                                            $internalPageId, 
                                            $igAccountId, 
                                            $channeledAccount->getId()
                                        );
                                        
                                        $saveCount = 0;
                                        foreach ($converted as $item) {
                                            if ($entityProcessor) {
                                                $item->setContext(array_merge($item->getContext(), ['channeledAccount' => $channeledAccount]));
                                                ($entityProcessor)($item, MetaEntityType::IG_MEDIA->value);
                                                $saveCount++;
                                            }
                                        }
                                        $logger?->info("<<< EXITO: Se sincronizaron $saveCount items de media para IG Account: $igId");
                                    } else {
                                        $logger?->info("--- INFO: No se encontró media que coincidan con los filtros para IG Account: $igId");
                                    }
                                },
                                igUserId: $igId, 
                                limit: $limit
                            );
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
