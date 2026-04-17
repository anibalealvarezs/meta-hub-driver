<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Services;

use Anibalealvarezs\ApiSkeleton\Enums\Channel;
use Anibalealvarezs\ApiDriverCore\Interfaces\SeederInterface;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookMarketingConvert;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicConvert;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Response;
use DateTime;
use Enums\CampaignObjective;
use Enums\CampaignStatus;
use Enums\CampaignBuyingType;
use Enums\OptimizationGoal;
use Enums\BillingEvent;
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
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $adAccountIds
     * @param FacebookGraphApi|null $api
     * @return Response
     */
    public static function syncCampaigns(
        SeederInterface $seeder,
        EntityManagerInterface $manager,
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null
    ): Response {
        Helpers::reconnectIfNeeded($manager);
        $logger?->info("DEBUG: FacebookEntitySync::syncCampaigns - ENTRY. Manager ID: " . spl_object_id($manager) . " | Open: " . ($manager->isOpen() ? 'YES' : 'NO'));

        try {
            $authorizedIdsMap = [];
            $hasErrors = false;
            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            $channeledSyncErrorClass = $seeder->getEntityClass('ChanneledSyncError');
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $campaignClass = $seeder->getEntityClass('Campaign');
            $channeledCampaignClass = $seeder->getEntityClass('ChanneledCampaign');

            $syncErrorRepo = $manager->getRepository($channeledSyncErrorClass);

            foreach ($adAccounts as $adAccount) {
                $adAccountId = (string)$adAccount['id'];
                $logger?->info("DEBUG: FacebookEntitySync::syncCampaigns - Processing ad account: $adAccountId");

                if (empty($adAccount['enabled']) || empty($adAccount['campaigns'])) {
                    $logger?->info("Skipping campaigns sync for ad account: $adAccountId (disabled in config)");
                    continue;
                }
                $channel = Channel::facebook_marketing;
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy([
                    'platformId' => $adAccountId,
                    'channel' => $channel->value,
                ]);

                if (! $channeledAccount) {
                    $logger?->error("DEBUG: FacebookEntitySync::syncCampaigns - ChanneledAccount NOT FOUND for platformId: $adAccountId");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    Helpers::reconnectIfNeeded($manager);
                    try {
                        $logger?->info("DEBUG: FacebookEntitySync::syncCampaigns - Phase 1. Manager ID: " . spl_object_id($manager) . " | Open: " . ($manager->isOpen() ? 'YES' : 'NO'));
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
                        $logger?->info("DEBUG: FacebookEntitySync::syncCampaigns - Phase 2. Manager ID: " . spl_object_id($manager) . " | Open: " . ($manager->isOpen() ? 'YES' : 'NO'));
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
                                foreach ($converted as $data) {
                                    $campaign = $manager->getRepository($campaignClass)->findOneBy(['campaignId' => $data->platformId]) ?? new $campaignClass();
                                    $campaign->addName($data->name);
                                    $campaign->addCampaignId($data->platformId);
                                    $manager->persist($campaign);
                                    
                                    $channeledCampaign = $manager->getRepository($channeledCampaignClass)->findOneBy([
                                        'platformId' => $data->platformId,
                                        'channeledAccount' => $channeledAccount
                                    ]) ?? new $channeledCampaignClass();
                                    $channeledCampaign->addPlatformId($data->platformId);
                                    $channeledCampaign->addChanneledAccount($channeledAccount);
                                    $channeledCampaign->addCampaign($campaign);
                                    $channeledCampaign->addChannel($channeledAccount->getChannel());
                                    
                                    // Handle Budget (daily_budget or lifetime_budget)
                                    $budget = (float)($data->budget ?? $data->lifetimeBudget ?? 0.0);
                                    $channeledCampaign->addBudget($budget);
                                     
                                     if (!empty($data->platformCreatedAt)) {
                                         $channeledCampaign->addPlatformCreatedAt($data->platformCreatedAt);
                                     }
                                    
                                    // Handle Enums with tryFrom for safety
                                    if (!empty($data->objective)) {
                                        $channeledCampaign->addObjective(CampaignObjective::tryFrom($data->objective));
                                    }
                                    if (!empty($data->status)) {
                                        $channeledCampaign->addStatus(CampaignStatus::tryFrom($data->status));
                                    }
                                    if (!empty($data->buyingType)) {
                                        $channeledCampaign->addBuyingType(CampaignBuyingType::tryFrom($data->buyingType));
                                    }
                                    
                                    $channeledCampaign->addData($data->data ?? []);
                                    
                                    $manager->persist($channeledCampaign);
                                }
                                $manager->flush();
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                            $currentLimit = max(10, (int) floor($currentLimit / 2));
                            $logger?->warning("Data limit error for $adAccountId in syncCampaigns: Reducing limit to $currentLimit");
                        }
                        $logger?->error("CRITICAL ERROR in syncCampaigns loop: " . $e->getMessage(), [
                            'exception' => get_class($e),
                            'manager_open' => ($manager->isOpen() ? 'YES' : 'NO')
                        ]);
                        $retryCount++;
                        if ($retryCount >= $maxRetries || !$manager->isOpen()) {
                            $hasErrors = true;
                            $logger?->error("Sync aborted or retries exhausted for $adAccountId. Manager closed: " . (!$manager->isOpen() ? 'YES' : 'NO'));
                            break; 
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
                $manager->clear();
            }

            if ($hasErrors) {
                throw new \Exception("Finished with partial errors. Check logs for details.");
            }

            return new Response(json_encode([
                'message' => 'Campaigns synchronized',
                'authorized_ids_map' => $authorizedIdsMap,
            ]), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncCampaigns: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $adAccountIds
     * @param FacebookGraphApi|null $api
     * @param array|null $parentIdsMap
     * @return Response
     */
    public static function syncAdGroups(
        SeederInterface $seeder,
        EntityManagerInterface $manager,
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null,
        ?array $parentIdsMap = null
    ): Response {
        Helpers::reconnectIfNeeded($manager);
        $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
        $channeledAdGroupClass = $seeder->getEntityClass('ChanneledAdGroup');
        $campaignClass = $seeder->getEntityClass('Campaign');
        $channeledCampaignClass = $seeder->getEntityClass('ChanneledCampaign');

        try {
            $authorizedIdsMap = [];
            $hasErrors = false;
            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            foreach ($adAccounts as $adAccount) {
                $adAccountId = (string)$adAccount['id'];
                $logger?->info("DEBUG: FacebookEntitySync::syncAdGroups - Processing ad account: $adAccountId");

                if (empty($adAccount['enabled']) || empty($adAccount['adsets'])) {
                    $logger?->info("Skipping adsets sync for ad account: $adAccountId (disabled in config)");
                    continue;
                }

                $channel = Channel::facebook_marketing;
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy([
                    'platformId' => $adAccountId,
                    'channel' => $channel->value,
                ]);

                if (! $channeledAccount) {
                    $logger?->error("DEBUG: FacebookEntitySync::syncAdGroups - ChanneledAccount NOT FOUND for platformId: $adAccountId");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    Helpers::reconnectIfNeeded($manager);
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
                                foreach ($converted as $data) {
                                    $campaign = $manager->getRepository($campaignClass)->findOneBy(['campaignId' => $data->channeledCampaignId]);
                                    if (!$campaign) continue;

                                     $channeledAdGroup = $manager->getRepository($channeledAdGroupClass)->findOneBy([
                                         'platformId' => $data->platformId,
                                         'channeledAccount' => $channeledAccount
                                     ]) ?? new $channeledAdGroupClass();
                                     $channeledAdGroup->addPlatformId($data->platformId);
                                     $channeledAdGroup->addName($data->name);
                                     $channeledAdGroup->addChanneledAccount($channeledAccount);
                                     $channeledAdGroup->addCampaign($campaign);
                                     $channeledAdGroup->addChannel($channeledAccount->getChannel());
                                     
                                     // Set relationships and fields
                                     $channeledCampaign = $manager->getRepository($channeledCampaignClass)->findOneBy([
                                         'platformId' => $data->channeledCampaignId,
                                         'channeledAccount' => $channeledAccount
                                     ]);
                                     if ($channeledCampaign) {
                                         $channeledAdGroup->addChanneledCampaign($channeledCampaign);
                                     }
                                     
                                     if (!empty($data->startDate)) $channeledAdGroup->addStartDate(new DateTime($data->startDate));
                                     if (!empty($data->endDate)) $channeledAdGroup->addEndDate(new DateTime($data->endDate));
                                     if (!empty($data->targeting)) $channeledAdGroup->addTargeting($data->targeting);
                                     
                                     // Handle Enums with tryFrom for safety
                                     if (!empty($data->status)) {
                                         $channeledAdGroup->addStatus(CampaignStatus::tryFrom($data->status));
                                     }
                                     if (!empty($data->optimizationGoal)) {
                                         $channeledAdGroup->addOptimizationGoal(OptimizationGoal::tryFrom($data->optimizationGoal));
                                     }
                                     if (!empty($data->billingEvent)) {
                                         $channeledAdGroup->addBillingEvent(BillingEvent::tryFrom($data->billingEvent));
                                     }
                                     
                                     $channeledAdGroup->addData($data->data ?? []);
                                     
                                     if (!empty($data->platformCreatedAt)) {
                                         $channeledAdGroup->addPlatformCreatedAt($data->platformCreatedAt);
                                     }
                                     
                                     $manager->persist($channeledAdGroup);
                                }
                                $manager->flush();
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
                            $hasErrors = true;
                            $logger?->error("Error syncing adsets for $adAccountId: " . $e->getMessage());
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
                $manager->clear();
            }

            return new Response(json_encode([
                'message' => 'AdGroups synchronized',
                'authorized_ids_map' => $authorizedIdsMap,
            ]), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncAdGroups: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $adAccountIds
     * @param FacebookGraphApi|null $api
     * @param array|null $parentIdsMap
     * @return Response
     */
    public static function syncAds(
        SeederInterface $seeder,
        EntityManagerInterface $manager,
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null,
        ?array $parentIdsMap = null
    ): Response {
        Helpers::reconnectIfNeeded($manager);
        $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
        $channeledAdClass = $seeder->getEntityClass('ChanneledAd');
        $channeledAdGroupClass = $seeder->getEntityClass('ChanneledAdGroup');
        $creativeClass = $seeder->getEntityClass('Creative');

        try {
            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            foreach ($adAccounts as $adAccount) {
                $adAccountId = (string)$adAccount['id'];
                $logger?->info("DEBUG: FacebookEntitySync::syncAds - Processing ad account: $adAccountId");

                if (empty($adAccount['enabled']) || empty($adAccount['ads'])) {
                    continue;
                }

                $channel = Channel::facebook_marketing;
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy([
                    'platformId' => $adAccountId,
                    'channel' => $channel->value,
                ]);

                if (! $channeledAccount) {
                    $logger?->error("DEBUG: FacebookEntitySync::syncAds - ChanneledAccount NOT FOUND for platformId: $adAccountId");
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    Helpers::reconnectIfNeeded($manager);
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
                                    $filteredAds[] = $a;
                                } else {
                                    $logger?->info("Skipping ad $aId ($aName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredAds)) {
                                $converted = FacebookMarketingConvert::ads($filteredAds, $channeledAccount->getId());
                                foreach ($converted as $data) {
                                     $channeledAdGroup = $manager->getRepository($channeledAdGroupClass)->findOneBy([
                                         'platformId' => $data->channeledAdGroupId,
                                         'channeledAccount' => $channeledAccount
                                     ]);
                                    if (!$channeledAdGroup) continue;

                                     $channeledAd = $manager->getRepository($channeledAdClass)->findOneBy([
                                         'platformId' => $data->platformId,
                                         'channeledAccount' => $channeledAccount
                                     ]) ?? new $channeledAdClass();
                                     $channeledAd->addPlatformId($data->platformId);
                                     $channeledAd->addName($data->name);
                                     $channeledAd->addChanneledAccount($channeledAccount);
                                     $channeledAd->addChanneledAdGroup($channeledAdGroup);
                                     $channeledAd->addChannel($channeledAccount->getChannel());
                                     
                                     if ($channeledAdGroup->getChanneledCampaign()) {
                                         $channeledAd->addChanneledCampaign($channeledAdGroup->getChanneledCampaign());
                                     }
                                     
                                     if (!empty($data->status)) {
                                         $channeledAd->addStatus(CampaignStatus::tryFrom($data->status));
                                     }
                                    
                                     // Preserve raw data
                                     $adData = $channeledAd->getData() ?? [];
                                     $adData = array_merge($adData, $data->data ?? []);
                                     
                                     if (!empty($data->channeledCreativeId)) {
                                         $creative = $manager->getRepository($creativeClass)->findOneBy(['creativeId' => $data->channeledCreativeId]);
                                        if ($creative) {
                                            $channeledAd->addCreative($creative);
                                        }
                                        // Store for retrospective linking
                                        $adData["facebook_creative_id"] = $data->channeledCreativeId;
                                    }
                                    $channeledAd->addData($adData);

                                    if (!empty($data->platformCreatedAt)) {
                                         $channeledAd->addPlatformCreatedAt($data->platformCreatedAt);
                                     }

                                     $manager->persist($channeledAd);
                                }
                                $manager->flush();
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
                $manager->clear();
            }

            return new Response(json_encode(['message' => 'Ads synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncAds: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param array|null $adAccountIds
     * @param FacebookGraphApi|null $api
     * @return Response
     */
    public static function syncCreatives(
        SeederInterface $seeder,
        EntityManagerInterface $manager,
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null
    ): Response {

        try {
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $creativeClass = $seeder->getEntityClass('Creative');

            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            foreach ($adAccounts as $adAccount) {
                if (empty($adAccount['enabled']) || empty($adAccount['creatives'])) {
                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy(['platformId' => $adAccountId]);
                if (! $channeledAccount) {
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;
                $currentLimit = 100;

                while ($retryCount < $maxRetries && ! $fetched) {
                    Helpers::reconnectIfNeeded($manager);
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
                                
                                // Fetch all ads for this account once to perform retrospective linking
                                $channeledAdClass = $seeder->getEntityClass("ChanneledAd");
                                $allAds = $manager->getRepository($channeledAdClass)->findBy(["channeledAccount" => $channeledAccount]);
                                foreach ($converted as $data) {
                                    $creative = $manager->getRepository($creativeClass)->findOneBy(['creativeId' => $data->platformId]) ?? new $creativeClass();
                                    $creative->addName($data->name);
                                    $creative->addCreativeId($data->platformId);
                                    $creative->addData($data->data ?? []);
                                    $manager->persist($creative);

                                    // Retrospective linking for Ads
                                    foreach ($allAds as $ad) {
                                        $adData = $ad->getData();
                                        if (($adData["facebook_creative_id"] ?? null) === $data->platformId) {
                                            $ad->addCreative($creative);
                                            $manager->persist($ad);
                                        }
                                    }
                                }
                                $manager->flush();
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
                $manager->clear();
            }

            return new Response(json_encode(['message' => 'Creatives synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncCreatives: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param FacebookGraphApi|null $api
     * @return Response
     */
    public static function syncPages(
        SeederInterface $seeder,
        EntityManagerInterface $manager,
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null
    ): Response {
        Helpers::reconnectIfNeeded($manager);

        try {
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $facebookPageClass = $seeder->getEntityClass('FacebookPage');

            $adAccounts = $config['ad_accounts'] ?? [];
            $pagesFromConfig = $config['pages'] ?? [];

            if (empty($adAccounts) && !empty($pagesFromConfig)) {
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy(['platformId' => $config['user_id'] ?? 'system']);
                
                foreach ($pagesFromConfig as $pageCfg) {
                    if (empty($pageCfg['enabled'])) continue;
                    $pId = (string)($pageCfg['id'] ?? '');
                    if (!$pId) continue;
                    Helpers::checkJobStatus($jobId);

                    $logger?->info("DEBUG: FacebookEntitySync::syncPages - Syncing Page $pId from config");
                    
                    try {
                        $api->setPageId($pId);

                        // Build all required page fields from config
                        $pageTitle    = $pageCfg['title'] ?? $pageCfg['name'] ?? null;
                        $pageUrl      = $pageCfg['url'] ?? ('https://www.facebook.com/' . $pId);
                        $pageHostname = $pageCfg['hostname'] ?? 'facebook.com';
                        $canonicalId  = \Helpers\Helpers::getCanonicalPageId(
                            url: $pageUrl,
                            platformId: $pId,
                            hostname: $pageHostname
                        );

                        $page = $manager->getRepository($facebookPageClass)->findOneBy(['platformId' => $pId])
                            ?? $manager->getRepository($facebookPageClass)->findOneBy(['canonicalId' => $canonicalId])
                            ?? new $facebookPageClass();

                        $page->addPlatformId($pId);
                        $page->addUrl($pageUrl);
                        $page->addTitle($pageTitle);
                        $page->addHostname($pageHostname);
                        $page->addCanonicalId($canonicalId);
                        
                        if ($channeledAccount) {
                            try {
                                $page->addAccount($channeledAccount->getAccount());
                            } catch (\Error $e) {
                                // Account property might be uninitialized
                            }
                            if (method_exists($channeledAccount, 'addPage')) {
                                $channeledAccount->addPage($page);
                                $manager->persist($channeledAccount);
                            }
                        }

                        $manager->persist($page);
                        $manager->flush();
                    } catch (\Exception $e) {
                        $logger?->error("Error syncing organic page $pId: " . $e->getMessage());
                    }
                }
            }

            foreach ($adAccounts as $adAccount) {
                Helpers::checkJobStatus($jobId);
                $logger?->info("DEBUG: FacebookEntitySync::syncPages - Processing Ad Account " . ($adAccount['id'] ?? 'unknown'));
                if (empty($adAccount['enabled']) || empty($adAccount['pages'])) {
                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy(['platformId' => $adAccountId]);
                if (! $channeledAccount) {
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;

                while ($retryCount < $maxRetries && ! $fetched) {
                    Helpers::reconnectIfNeeded($manager);
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
                                foreach ($converted as $data) {
                                    $page = $manager->getRepository($facebookPageClass)->findOneBy(['platformId' => $data->platformId]) ?? new $facebookPageClass();
                                    $page->addTitle($data->name);
                                    $page->addPlatformId($data->platformId);
                                    $page->addAccount($channeledAccount->getAccount());
                                    $page->addData($data->data ?? []);
                                    $manager->persist($page);
                                }
                                $manager->flush();
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger?->error("Error fetching pages for $adAccountId: " . $e->getMessage());
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
            }

            return new Response(json_encode(['message' => 'Pages synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncPages: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @param FacebookGraphApi|null $api
     * @return Response
     */
    public static function syncPosts(
        SeederInterface $seeder,
        EntityManagerInterface $manager,
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null
    ): Response {
        Helpers::reconnectIfNeeded($manager);

        try {
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $facebookPageClass = $seeder->getEntityClass('FacebookPage');
            $postClass = $seeder->getEntityClass('Post');

            $adAccounts = $config['ad_accounts'] ?? [];
            $pagesFromConfig = $config['pages'] ?? [];

            $pagesToProcess = [];
            
            if (!empty($adAccounts)) {
                foreach ($adAccounts as $adAccount) {
                    $adAccountId = (string)$adAccount['id'];
                    $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy(['platformId' => $adAccountId]);
                    if (! $channeledAccount) {
                        continue;
                    }
                    $dbPages = $manager->getRepository($facebookPageClass)->findBy(['channeledAccount' => $channeledAccount]);
                    foreach ($dbPages as $p) $pagesToProcess[] = $p;
                }
            } elseif (!empty($pagesFromConfig)) {
                foreach ($pagesFromConfig as $pageCfg) {
                    if (empty($pageCfg['enabled']) || empty($pageCfg['posts'])) continue;
                    $pId = (string)($pageCfg['id'] ?? '');
                    if (!$pId) continue;
                    $page = $manager->getRepository($facebookPageClass)->findOneBy(['platformId' => $pId]);
                    if ($page) $pagesToProcess[] = $page;
                }
            }

            foreach ($pagesToProcess as $page) {
                Helpers::checkJobStatus($jobId);
                $logger?->info(">>> INICIO: Sincronizando posts para FB Page: {$page->getPlatformId()} (Timeframe: $startDate a $endDate)");
                $fetched = false;
                $postLimits = [100, 50, 25, 10];

                foreach ($postLimits as $limit) {
                    if ($fetched) break;
                    $maxRetries = 3;
                    $retryCount = 0;

                    while ($retryCount < $maxRetries && ! $fetched) {
                        Helpers::reconnectIfNeeded($manager);
                        try {
                            $api->setPageId($page->getPlatformId());
                            $posts = $api->getFacebookPosts(pageId: $page->getPlatformId(), limit: $limit);
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
                                    $converted = FacebookOrganicConvert::posts($filteredPosts, $page->getId());
                                    $saveCount = 0;
                                    foreach ($converted as $pData) {
                                        $post = $manager->getRepository($postClass)->findOneBy(['postId' => $pData->platformId]) ?? new $postClass();
                                        $post->addPostId($pData->platformId);
                                        $post->addPage($page);
                                        try {
                                            $post->addAccount($page->getAccount());
                                        } catch (\Error $e) {
                                            $logger?->warning("Page {$page->getPlatformId()} has no account initialized. Skipping account assignment for post.");
                                        }
                                        // Try to find the ChanneledAccount associated with this Page
                                        $ca = $manager->getRepository($channeledAccountClass)->findOneBy([
                                            'platformId' => $page->getPlatformId(),
                                            'channel' => Channel::facebook_organic->value
                                        ]);
                                        if ($ca) {
                                            $post->addChanneledAccount($ca);
                                        }
                                        $post->addData($pData->data ?? []);
                                        $manager->persist($post);

                                        $saveCount++;
                                    }
                                    $manager->flush();
                                    $logger?->info("<<< EXITO: Se sincronizaron $saveCount posts para FB Page: {$page->getPlatformId()}");
                                } else {
                                    $logger?->info("--- INFO: No se encontraron posts que coincidan con los filtros para FB Page: {$page->getPlatformId()}");
                                }
                            }
                            $fetched = true;
                        } catch (\Exception $e) {
                            $retryCount++;
                            $errMsg = $e->getMessage();
                            // If Meta asks to reduce data, break the retry loop and try a smaller limit
                            if (str_contains($errMsg, 'reduce the amount of data')) {
                                $logger?->info("Meta requested data reduction for page " . $page->getPlatformId() . " at limit=$limit. Trying smaller limit.");
                                break; // break while, move to next limit
                            }
                            if ($retryCount >= $maxRetries) {
                                $logger?->error("Error fetching posts for page " . $page->getPlatformId() . ": " . $errMsg);
                            } else {
                                usleep(200000 * $retryCount);
                            }
                        }
                    } // end while
                } // end foreach postLimits
                $manager->clear();
                $logger?->info("DEBUG: FacebookEntitySync::syncPosts - END processing page " . $page->getPlatformId());
            } // end foreach pagesToProcess

            return new Response(json_encode(['message' => 'Posts synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncPosts: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }

    /**
     * @param SeederInterface $seeder
     * @param EntityManagerInterface $manager
     * @param FacebookGraphApi $api
     * @param array $config
     * @param string|null $startDate
     * @param string|null $endDate
     * @param LoggerInterface|null $logger
     * @param int|null $jobId
     * @return Response
     */
    public static function syncInstagramMedia(
        SeederInterface $seeder,
        EntityManagerInterface $manager,
        FacebookGraphApi $api,
        array $config,
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null
    ): Response {
        Helpers::reconnectIfNeeded($manager);

        try {
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $postClass = $seeder->getEntityClass('Post');

            $pagesFromConfig = $config['pages'] ?? [];

            foreach ($pagesFromConfig as $pageCfg) {
                if (empty($pageCfg['enabled']) || empty($pageCfg['ig_account']) || empty($pageCfg['ig_account_media'])) {
                    continue;
                }
                
                $igId = (string)$pageCfg['ig_account'];
                $logger?->info("DEBUG: FacebookEntitySync::syncInstagramMedia - START processing IG account " . $igId);
                
                Helpers::checkJobStatus($jobId);
                
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy(['platformId' => $igId]);
                if (!$channeledAccount) {
                    $logger?->warning("ChanneledAccount not found for IG ID $igId. Skipping media sync.");
                    continue;
                }

                $fetched = false;
                $mediaLimits = [100, 50, 25, 10];

                $logger?->info(">>> INICIO: Sincronizando media para IG Account: $igId (Timeframe: $startDate a $endDate)");
                
                foreach ($mediaLimits as $limit) {
                    if ($fetched) break;
                    $maxRetries = 3;
                    $retryCount = 0;

                    while ($retryCount < $maxRetries && !$fetched) {
                        Helpers::reconnectIfNeeded($manager);
                        try {
                            if (!empty($pageCfg['id'])) {
                                $api->setPageId((string)$pageCfg['id']);
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
                                    $accountId = null;
                                    try {
                                        if ($channeledAccount) {
                                            $accountId = $channeledAccount->getAccount() ? $channeledAccount->getAccount()->getId() : null;
                                        }
                                    } catch (\Error $e) {
                                        // Account property might be uninitialized
                                    }
                                    $converted = FacebookOrganicConvert::media(
                                        $filteredMedia, 
                                        null, 
                                        $accountId,
                                        $channeledAccount->getId()
                                    );
                                    
                                    // Resolve IG Page entity for the post
                                    $igPage = $manager->getRepository($seeder->getEntityClass('Page'))->findOneBy(['platformId' => $igId])
                                              ?? $manager->getRepository($seeder->getEntityClass('Page'))->findOneBy(['canonicalId' => 'ig:' . $igId]);

                                    $saveCount = 0;
                                    foreach ($converted as $mData) {
                                        $post = $manager->getRepository($postClass)->findOneBy(['postId' => $mData->platformId]) ?? new $postClass();
                                        $post->addPostId($mData->platformId);
                                        $post->addChanneledAccount($channeledAccount);
                                        try {
                                            $post->addAccount($channeledAccount->getAccount());
                                        } catch (\Error $e) {
                                            // ChanneledAccount account property might be uninitialized
                                        }
                                        if ($igPage) {
                                            $post->addPage($igPage);
                                        }
                                        $post->addData($mData->data ?? []);
                                        $manager->persist($post);

                                        $saveCount++;
                                    }
                                    $manager->flush();
                                    $logger?->info("<<< EXITO: Se sincronizaron $saveCount items de media para IG Account: $igId");
                                } else {
                                    $logger?->info("--- INFO: No se encontró media que coincida con los filtros para IG Account: $igId");
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
                $manager->clear();
                $logger?->info("DEBUG: FacebookEntitySync::syncInstagramMedia - END processing IG account " . $igId);
            }

            return new Response(json_encode(['message' => 'Instagram media synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncInstagramMedia: " . $e->getMessage());
            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }
}
