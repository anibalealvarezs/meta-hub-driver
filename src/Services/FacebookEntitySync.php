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
                // Job status check removed for host-agnosticism

                if (empty($adAccount['enabled']) || empty($adAccount['campaigns'])) {
                    $logger?->info("Skipping campaigns sync for ad account: " . $adAccount['id'] . " (disabled in config)");
                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy([
                    'platformId' => $adAccountId,
                ]);

                if (! $channeledAccount) {
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;

                while ($retryCount < $maxRetries && ! $fetched) {
                    try {
                        $campaigns = $api->getCampaigns(adAccountId: $adAccountId);
                        if (! empty($campaigns['data'])) {
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
                                $converted = FacebookMarketingConvert::campaigns($filteredCampaigns, $channeledAccount->getId());
                                foreach ($converted as $data) {
                                    $campaign = $manager->getRepository($campaignClass)->findOneBy(['campaignId' => $data->platformId]) ?? new $campaignClass();
                                    $campaign->setName($data->name);
                                    $campaign->addCampaignId($data->platformId);
                                    $manager->persist($campaign);
                                    
                                    $channeledCampaign = $manager->getRepository($channeledCampaignClass)->findOneBy([
                                        'platformId' => $data->platformId,
                                        'channeledAccount' => $channeledAccount
                                    ]) ?? new $channeledCampaignClass();
                                    $channeledCampaign->setPlatformId($data->platformId);
                                    $channeledCampaign->setChanneledAccount($channeledAccount);
                                    $channeledCampaign->setCampaign($campaign);
                                    $manager->persist($channeledCampaign);
                                }
                                $manager->flush();
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $hasErrors = true;
                            // logError is problematic because it's usually dynamic. 
                            // For now, we'll just throw the exception or log to PHP logger.
                            $logger?->error("Error syncing campaigns for $adAccountId: " . $e->getMessage());
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
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

        try {
            $authorizedIdsMap = [];
            $hasErrors = false;
            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $adGroupClass = $seeder->getEntityClass('AdGroup');
            $channeledAdGroupClass = $seeder->getEntityClass('ChanneledAdGroup');
            $campaignClass = $seeder->getEntityClass('Campaign');

            foreach ($adAccounts as $adAccount) {
                if (empty($adAccount['enabled']) || empty($adAccount['adsets'])) {
                    $logger?->info("Skipping adsets sync for ad account: " . $adAccount['id'] . " (disabled in config)");
                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy([
                    'platformId' => $adAccountId,
                ]);

                if (! $channeledAccount) {
                    continue;
                }

                $maxRetries = 3;
                $retryCount = 0;
                $fetched = false;

                while ($retryCount < $maxRetries && ! $fetched) {
                    try {
                        $additionalParams = [];
                        if ($parentIdsMap && isset($parentIdsMap[$adAccountId])) {
                            $additionalParams['filtering'] = [[
                                'field' => 'campaign.id',
                                'operator' => 'IN',
                                'value' => $parentIdsMap[$adAccountId],
                            ]];
                        }

                        $adsets = $api->getAdsets(adAccountId: $adAccountId, additionalParams: $additionalParams);
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
                                    $campaign = $manager->getRepository($campaignClass)->findOneBy(['campaignId' => $data->campaignPlatformId]);
                                    if (!$campaign) continue;

                                    $channeledAdGroup = $manager->getRepository($channeledAdGroupClass)->findOneBy([
                                        'platformId' => $data->platformId,
                                        'channeledAccount' => $channeledAccount
                                    ]) ?? new $channeledAdGroupClass();
                                    $channeledAdGroup->setPlatformId($data->platformId);
                                    $channeledAdGroup->addName($data->name);
                                    $channeledAdGroup->addChanneledAccount($channeledAccount);
                                    $channeledAdGroup->addCampaign($campaign);
                                    $manager->persist($channeledAdGroup);
                                }
                                $manager->flush();
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $hasErrors = true;
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

        try {
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $adClass = $seeder->getEntityClass('Ad');
            $channeledAdClass = $seeder->getEntityClass('ChanneledAd');
            $adGroupClass = $seeder->getEntityClass('AdGroup');

            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            foreach ($adAccounts as $adAccount) {
                if (empty($adAccount['enabled']) || empty($adAccount['ads'])) {
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
                    try {
                        $additionalParams = [];
                        if ($parentIdsMap && isset($parentIdsMap[$adAccountId])) {
                            $additionalParams['filtering'] = [[
                                'field' => 'adset.id',
                                'operator' => 'IN',
                                'value' => $parentIdsMap[$adAccountId],
                            ]];
                        }
                        $ads = $api->getAds(adAccountId: $adAccountId, additionalParams: $additionalParams);
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
                                        'platformId' => $data->adGroupPlatformId,
                                        'channeledAccount' => $channeledAccount
                                    ]);
                                    if (!$channeledAdGroup) continue;

                                    $channeledAd = $manager->getRepository($channeledAdClass)->findOneBy([
                                        'platformId' => $data->platformId,
                                        'channeledAccount' => $channeledAccount
                                    ]) ?? new $channeledAdClass();
                                    $channeledAd->setPlatformId($data->platformId);
                                    $channeledAd->addName($data->name);
                                    $channeledAd->addChanneledAccount($channeledAccount);
                                    $channeledAd->addChanneledAdGroup($channeledAdGroup);
                                    
                                    if (!empty($data->creativePlatformId)) {
                                        $creative = $manager->getRepository($creativeClass)->findOneBy(['creativeId' => $data->creativePlatformId]);
                                        if ($creative) {
                                            $channeledAd->addCreative($creative);
                                        }
                                    }

                                    $manager->persist($channeledAd);
                                }
                                $manager->flush();
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger?->error("Error fetching ads for $adAccountId: " . $e->getMessage());
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
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

                while ($retryCount < $maxRetries && ! $fetched) {
                    try {
                        $creatives = $api->getCreatives(adAccountId: $adAccountId);
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
                                foreach ($converted as $data) {
                                    $creative = $manager->getRepository($creativeClass)->findOneBy(['creativeId' => $data->platformId]) ?? new $creativeClass();
                                    $creative->addName($data->name);
                                    $creative->addCreativeId($data->platformId);
                                    $creative->addData($data->data ?? []);
                                    $manager->persist($creative);
                                }
                                $manager->flush();
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
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

        try {
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $facebookPageClass = $seeder->getEntityClass('FacebookPage');

            $adAccounts = $config['ad_accounts'] ?? [];
            foreach ($adAccounts as $adAccount) {
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

        try {
            $channeledAccountClass = $seeder->getEntityClass('ChanneledAccount');
            $facebookPageClass = $seeder->getEntityClass('FacebookPage');
            $postClass = $seeder->getEntityClass('Post');

            $adAccounts = $config['ad_accounts'] ?? [];
            foreach ($adAccounts as $adAccount) {
                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository($channeledAccountClass)->findOneBy(['platformId' => $adAccountId]);
                if (! $channeledAccount) {
                    continue;
                }

                $pages = $manager->getRepository($facebookPageClass)->findBy(['channeledAccount' => $channeledAccount]);
                foreach ($pages as $page) {
                    $maxRetries = 3;
                    $retryCount = 0;
                    $fetched = false;

                    while ($retryCount < $maxRetries && ! $fetched) {
                        try {
                            $posts = $api->getFacebookPosts(pageId: $page->getPlatformId(), limit: 100);
                            if (! empty($posts['data'])) {
                                $includeFilter = self::getFacebookFilter($config, 'POST', 'cache_include');
                                $excludeFilter = self::getFacebookFilter($config, 'POST', 'cache_exclude');

                                $filteredPosts = [];
                                foreach ($posts['data'] as $p) {
                                    $pName = $p['message'] ?? $p['story'] ?? '';
                                    $pId = (string)$p['id'];
                                    if (self::matchesFilter($pName, $includeFilter, $excludeFilter) || self::matchesFilter($pId, $includeFilter, $excludeFilter)) {
                                        $filteredPosts[] = $p;
                                    } else {
                                        $logger?->info("Skipping post $pId ($pName) - filtered out by extraction patterns");
                                    }
                                }

                                if (! empty($filteredPosts)) {
                                    $converted = FacebookOrganicConvert::posts($filteredPosts, $page->getId());
                                    foreach ($converted as $pData) {
                                        $post = $manager->getRepository($postClass)->findOneBy(['postId' => $pData->platformId]) ?? new $postClass();
                                        $post->addPostId($pData->platformId);
                                        $post->addPage($page);
                                        $post->addAccount($page->getAccount());
                                        $manager->persist($post);
                                    }
                                    $manager->flush();
                                }
                            }
                            $fetched = true;
                        } catch (\Exception $e) {
                            $retryCount++;
                            if ($retryCount >= $maxRetries) {
                                $logger?->error("Error fetching posts for page " . $page->getPlatformId() . ": " . $e->getMessage());
                            } else {
                                usleep(200000 * $retryCount);
                            }
                        }
                    }
                }
            }

            return new Response(json_encode(['message' => 'Posts synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncPosts: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }
}
