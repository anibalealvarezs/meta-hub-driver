<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Services;

use Anibalealvarezs\FacebookGraphApi\Conversions\FacebookMarketingConvert;
use Anibalealvarezs\FacebookGraphApi\Conversions\FacebookOrganicConvert;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Classes\DriverInitializer;
use Classes\MarketingProcessor;
use Classes\SocialProcessor;
use Entities\Analytics\Channeled\ChanneledAccount;
use Entities\Analytics\Channeled\ChanneledSyncError;
use Entities\Analytics\Page;
use Enums\Channel;
use Helpers\Helpers;
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
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null,
        ?FacebookGraphApi $api = null
    ): Response {
        if (! $logger) {
            $logger = Helpers::setLogger('facebook-entities.log');
        }

        try {
            $config = DriverInitializer::validateFacebookConfig($logger, Channel::facebook_marketing);
            if (! $api) {
                $api = DriverInitializer::initializeFacebookGraphApi($config, $logger);
            }
            $manager = Helpers::getManager();
            $authorizedIdsMap = [];

            $hasErrors = false;
            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            $syncErrorRepo = $manager->getRepository(ChanneledSyncError::class);

            foreach ($adAccounts as $adAccount) {
                Helpers::checkJobStatus($jobId);

                if (empty($adAccount['enabled']) || empty($adAccount['campaigns'])) {
                    $logger->info("Skipping campaigns sync for ad account: " . $adAccount['id'] . " (disabled in config)");

                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository(ChanneledAccount::class)->findOneBy([
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
                                if (Helpers::matchesFilter($cName, $includeFilter, $excludeFilter) || Helpers::matchesFilter($cId, $includeFilter, $excludeFilter)) {
                                    $authorizedIdsMap[$adAccountId][] = $cId;
                                    $filteredCampaigns[] = $c;
                                } else {
                                    $logger->info("Skipping campaign $cId ($cName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredCampaigns)) {
                                MarketingProcessor::processCampaigns(FacebookMarketingConvert::campaigns($filteredCampaigns, $channeledAccount->getId()), $manager);
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $hasErrors = true;
                            $syncErrorRepo->logError([
                                'platformId' => $adAccountId,
                                'channel' => Channel::facebook_marketing->value,
                                'syncType' => 'entity',
                                'entityType' => 'campaign',
                                'errorMessage' => $e->getMessage(),
                                'extraData' => ['jobId' => $jobId],
                            ]);
                        } else {
                            usleep(200000 * $retryCount);
                        }
                    }
                }
            }

            if ($hasErrors) {
                throw new \Exception("Finished with partial errors. Check channeled_sync_errors table or logs for details.");
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
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null,
        ?FacebookGraphApi $api = null,
        ?array $parentIdsMap = null
    ): Response {
        if (! $logger) {
            $logger = Helpers::setLogger('facebook-entities.log');
        }

        try {
            $config = DriverInitializer::validateFacebookConfig($logger, Channel::facebook_marketing);
            if (! $api) {
                $api = DriverInitializer::initializeFacebookGraphApi($config, $logger);
            }
            $manager = Helpers::getManager();
            $authorizedIdsMap = [];

            $hasErrors = false;
            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            $syncErrorRepo = $manager->getRepository(ChanneledSyncError::class);

            foreach ($adAccounts as $adAccount) {
                Helpers::checkJobStatus($jobId);

                if (empty($adAccount['enabled']) || empty($adAccount['adsets'])) {
                    $logger->info("Skipping adsets sync for ad account: " . $adAccount['id'] . " (disabled in config)");

                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository(ChanneledAccount::class)->findOneBy([
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
                            $includeFilter = self::getFacebookFilter($config, 'AD_SET', 'cache_include');
                            $excludeFilter = self::getFacebookFilter($config, 'AD_SET', 'cache_exclude');

                            $filteredAdsets = [];
                            foreach ($adsets['data'] as $a) {
                                $aName = $a['name'] ?? '';
                                $aId = (string)$a['id'];
                                if (Helpers::matchesFilter($aName, $includeFilter, $excludeFilter) || Helpers::matchesFilter($aId, $includeFilter, $excludeFilter)) {
                                    $authorizedIdsMap[$adAccountId][] = $aId;
                                    $filteredAdsets[] = $a;
                                } else {
                                    $logger->info("Skipping adset $aId ($aName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredAdsets)) {
                                MarketingProcessor::processAdGroups(FacebookMarketingConvert::adsets($filteredAdsets, $channeledAccount->getId()), $manager);
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $hasErrors = true;
                            $syncErrorRepo->logError([
                                'platformId' => $adAccountId,
                                'channel' => Channel::facebook_marketing->value,
                                'syncType' => 'entity',
                                'entityType' => 'adset',
                                'errorMessage' => $e->getMessage(),
                                'extraData' => ['jobId' => $jobId],
                            ]);
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
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null,
        ?FacebookGraphApi $api = null,
        ?array $parentIdsMap = null
    ): Response {
        if (! $logger) {
            $logger = Helpers::setLogger('facebook-entities.log');
        }

        try {
            $config = DriverInitializer::validateFacebookConfig($logger, Channel::facebook_marketing);
            if (! $api) {
                $api = DriverInitializer::initializeFacebookGraphApi($config, $logger);
            }
            $manager = Helpers::getManager();

            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            foreach ($adAccounts as $adAccount) {
                Helpers::checkJobStatus($jobId);
                if (empty($adAccount['enabled']) || empty($adAccount['ads'])) {
                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository(ChanneledAccount::class)->findOneBy(['platformId' => $adAccountId]);
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
                                if (Helpers::matchesFilter($aName, $includeFilter, $excludeFilter) || Helpers::matchesFilter($aId, $includeFilter, $excludeFilter)) {
                                    $filteredAds[] = $a;
                                } else {
                                    $logger->info("Skipping ad $aId ($aName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredAds)) {
                                MarketingProcessor::processAds(FacebookMarketingConvert::ads($filteredAds, $channeledAccount->getId()), $manager);
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger->error("Error fetching ads for $adAccountId: " . $e->getMessage());
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
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $adAccountIds = null,
        ?FacebookGraphApi $api = null
    ): Response {
        if (! $logger) {
            $logger = Helpers::setLogger('facebook-entities.log');
        }

        try {
            $config = DriverInitializer::validateFacebookConfig($logger, Channel::facebook_marketing);
            if (! $api) {
                $api = DriverInitializer::initializeFacebookGraphApi($config, $logger);
            }
            $manager = Helpers::getManager();

            $adAccounts = $config['ad_accounts'] ?? [];
            if ($adAccountIds) {
                $adAccounts = array_filter($adAccounts, fn ($acc) => in_array($acc['id'], $adAccountIds));
            }

            foreach ($adAccounts as $adAccount) {
                Helpers::checkJobStatus($jobId);
                if (empty($adAccount['enabled']) || empty($adAccount['creatives'])) {
                    continue;
                }

                $adAccountId = (string)$adAccount['id'];
                $channeledAccount = $manager->getRepository(ChanneledAccount::class)->findOneBy(['platformId' => $adAccountId]);
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
                                if (Helpers::matchesFilter($cName, $includeFilter, $excludeFilter) || Helpers::matchesFilter($cId, $includeFilter, $excludeFilter)) {
                                    $filteredCreatives[] = $c;
                                } else {
                                    $logger->info("Skipping creative $cId ($cName) - filtered out by extraction patterns");
                                }
                            }

                            if (! empty($filteredCreatives)) {
                                MarketingProcessor::processCreatives(FacebookMarketingConvert::creatives($filteredCreatives, $channeledAccount->getId()), $manager);
                            }
                        }
                        $fetched = true;
                    } catch (\Exception $e) {
                        $retryCount++;
                        if ($retryCount >= $maxRetries) {
                            $logger->error("Error fetching creatives for $adAccountId: " . $e->getMessage());
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
     * @param array|null $pageIds
     * @param FacebookGraphApi|null $api
     * @return Response
     */
    public static function syncPages(
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $pageIds = null,
        ?FacebookGraphApi $api = null
    ): Response {
        if (! $logger) {
            $logger = Helpers::setLogger('facebook-entities.log');
        }

        try {
            $config = DriverInitializer::validateFacebookConfig($logger, Channel::facebook_organic);
            if (! $api) {
                $api = DriverInitializer::initializeFacebookGraphApi($config, $logger);
            }
            $manager = Helpers::getManager();
            $accountRepo = $manager->getRepository(\Entities\Analytics\Account::class);

            $pagesResult = $config['pages'] ?? [];
            if ($pageIds) {
                $pagesResult = array_filter($pagesResult, fn ($p) => in_array($p['id'], $pageIds));
            }

            foreach ($pagesResult as $pageCfg) {
                Helpers::checkJobStatus($jobId);
                if (empty($pageCfg['enabled'])) {
                    continue;
                }

                $pageId = (string)$pageCfg['id'];
                $accountName = $pageCfg['account'] ?? $config['accounts_group_name'] ?? null;
                $account = $accountName ? $accountRepo->findOneBy(['name' => $accountName]) : null;
                if (! $account) {
                    continue;
                }

                $channeledAccount = $manager->getRepository(ChanneledAccount::class)->findOneBy(['platformId' => $pageId, 'channel' => Channel::facebook_organic->value]);
                if (! $channeledAccount) {
                    continue;
                }

                try {
                    $pageData = $api->getMyPages(fields: 'id,name,username,access_token,instagram_business_account{id,name,username}');
                    $targetPage = array_filter($pageData['data'] ?? [], fn ($p) => (string)$p['id'] === $pageId);
                    if (! empty($targetPage)) {
                        $includeFilter = self::getFacebookFilter($config, 'PAGE', 'cache_include');
                        $excludeFilter = self::getFacebookFilter($config, 'PAGE', 'cache_exclude');

                        $filteredPages = [];
                        foreach ($targetPage as $p) {
                            $pName = $p['name'] ?? '';
                            $pId = (string)$p['id'];
                            if (Helpers::matchesFilter($pName, $includeFilter, $excludeFilter) || Helpers::matchesFilter($pId, $includeFilter, $excludeFilter)) {
                                $filteredPages[] = $p;
                            } else {
                                $logger->info("Skipping page $pId ($pName) - filtered out by extraction patterns");
                            }
                        }

                        if (! empty($filteredPages)) {
                            SocialProcessor::processPages(FacebookOrganicConvert::pages(array_values($filteredPages), $channeledAccount->getId()), $manager);
                        }
                    } else {
                        SocialProcessor::processPages(FacebookOrganicConvert::pages([$pageCfg], $channeledAccount->getId()), $manager);
                    }
                } catch (\Exception $e) {
                    $logger->warning("Failed to fetch page $pageId from API, using config data: " . $e->getMessage());
                    SocialProcessor::processPages(FacebookOrganicConvert::pages([$pageCfg], $channeledAccount->getId()), $manager);
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
     * @param array|null $pageIds
     * @param FacebookGraphApi|null $api
     * @return Response
     */
    public static function syncPosts(
        ?string $startDate = null,
        ?string $endDate = null,
        ?LoggerInterface $logger = null,
        ?int $jobId = null,
        ?array $pageIds = null,
        ?FacebookGraphApi $api = null
    ): Response {
        if (! $logger) {
            $logger = Helpers::setLogger('facebook-entities.log');
        }

        try {
            $config = DriverInitializer::validateFacebookConfig($logger, Channel::facebook_organic);
            if (! $api) {
                $api = DriverInitializer::initializeFacebookGraphApi($config, $logger);
            }
            $manager = Helpers::getManager();
            $pageRepo = $manager->getRepository(Page::class);
            $channeledAccountRepo = $manager->getRepository(ChanneledAccount::class);

            $pagesToProcess = $config['pages'] ?? [];
            if ($pageIds) {
                $pagesToProcess = array_filter($pagesToProcess, fn ($p) => in_array($p['id'], $pageIds));
            }

            $cacheChunkSize = $config['cache_chunk_size'] ?? '1 month';
            $chunks = Helpers::getDateChunks($startDate ?: '-30 days', $endDate ?: 'now', $cacheChunkSize);

            foreach ($pagesToProcess as $pageCfg) {
                try {
                    Helpers::checkJobStatus($jobId);
                    if (empty($pageCfg['enabled']) || (empty($pageCfg['posts']) && empty($pageCfg['ig_account_media']))) {
                        continue;
                    }

                    $pageEntity = $pageRepo->findOneBy(['platformId' => $pageCfg['id']]);
                    if (! $pageEntity) {
                        continue;
                    }

                    $accountEntity = $pageEntity->getAccount();

                    foreach ($chunks as $chunk) {
                        Helpers::checkJobStatus($jobId);
                        $additionalParams = [
                            'since' => $chunk['start'],
                            'until' => $chunk['end'] . (strlen($chunk['end']) === 10 ? ' 23:59:59' : ''),
                        ];

                        if (! empty($pageCfg['posts'])) {
                            $api->setPageId((string)$pageCfg['id']);
                            $fbPosts = $api->getFacebookPosts(pageId: (string)$pageCfg['id'], limit: 100, additionalParams: $additionalParams);
                            if (! empty($fbPosts['data'])) {
                                $includeFilter = self::getFacebookFilter($config, 'POST', 'cache_include');
                                $excludeFilter = self::getFacebookFilter($config, 'POST', 'cache_exclude');

                                $filteredPosts = [];
                                foreach ($fbPosts['data'] as $p) {
                                    $pMessage = $p['message'] ?? $p['story'] ?? '';
                                    $pId = (string)$p['id'];
                                    if (Helpers::matchesFilter($pMessage, $includeFilter, $excludeFilter) || Helpers::matchesFilter($pId, $includeFilter, $excludeFilter)) {
                                        $filteredPosts[] = $p;
                                    } else {
                                        $logger->info("Skipping FB post $pId - filtered out by extraction patterns");
                                    }
                                }

                                if (! empty($filteredPosts)) {
                                    $fbChanneledAccount = $channeledAccountRepo->findOneBy(['platformId' => (string)$pageCfg['id'], 'channel' => Channel::facebook_organic->value]);
                                    SocialProcessor::processPosts(FacebookOrganicConvert::posts($filteredPosts, $pageEntity->getId(), $accountEntity->getId(), $fbChanneledAccount?->getId()), $manager);
                                }
                            }
                        }

                        if (! empty($pageCfg['ig_account']) && ! empty($pageCfg['ig_account_media'])) {
                            $igMedia = $api->getInstagramMedia(igUserId: (string)$pageCfg['ig_account'], limit: 100, additionalParams: $additionalParams);
                            if (! empty($igMedia['data'])) {
                                $includeFilter = self::getFacebookFilter($config, 'POST', 'cache_include');
                                $excludeFilter = self::getFacebookFilter($config, 'POST', 'cache_exclude');

                                $filteredMedia = [];
                                foreach ($igMedia['data'] as $m) {
                                    $mCaption = $m['caption'] ?? '';
                                    $mId = (string)$m['id'];
                                    if (Helpers::matchesFilter($mCaption, $includeFilter, $excludeFilter) || Helpers::matchesFilter($mId, $includeFilter, $excludeFilter)) {
                                        $filteredMedia[] = $m;
                                    } else {
                                        $logger->info("Skipping IG media $mId - filtered out by extraction patterns");
                                    }
                                }

                                if (! empty($filteredMedia)) {
                                    $igChanneledAccount = $channeledAccountRepo->findOneBy(['platformId' => (string)$pageCfg['ig_account'], 'channel' => Channel::facebook_organic->value]);
                                    SocialProcessor::processPosts(FacebookOrganicConvert::posts($filteredMedia, $pageEntity->getId(), $accountEntity->getId(), $igChanneledAccount?->getId()), $manager);
                                }
                            }
                        }
                    }
                } catch (\Exception $e) {
                    $logger->error("Error processing page posts " . $pageCfg['id'] . ": " . $e->getMessage());
                }
            }

            return new Response(json_encode(['message' => 'Posts synchronized']), 200, ['Content-Type' => 'application/json']);
        } catch (\Exception $e) {
            $logger->error("Error in FacebookEntitySync::syncPosts: " . $e->getMessage());

            return new Response(json_encode(['error' => $e->getMessage()]), 500);
        }
    }
}
