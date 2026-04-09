<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\FacebookGraphApi\Enums\MediaType;
use Anibalealvarezs\FacebookGraphApi\Conversions\FacebookOrganicMetricConvert;
use Anibalealvarezs\ApiSkeleton\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiSkeleton\Interfaces\AuthProviderInterface;
use Anibalealvarezs\ApiSkeleton\Traits\HasUpdatableCredentials;
use Anibalealvarezs\ApiSkeleton\Enums\Period;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;
use Carbon\Carbon;
use Doctrine\Common\Collections\ArrayCollection;

class FacebookOrganicDriver implements SyncDriverInterface
{
    use HasUpdatableCredentials;

    public array $updatableCredentials = [
        'FACEBOOK_USER_TOKEN',
        'FACEBOOK_USER_ID',
        'FACEBOOK_ACCOUNTS_GROUP',
        'FACEBOOK_APP_ID',
        'FACEBOOK_APP_SECRET'
    ];

    private ?AuthProviderInterface $authProvider = null;
    private ?LoggerInterface $logger = null;
    /** @var callable|null */
    private $dataProcessor = null;

    public function __construct(?AuthProviderInterface $authProvider = null, ?LoggerInterface $logger = null)
    {
        $this->authProvider = $authProvider;
        $this->logger = $logger;
    }

    public function getChannel(): string
    {
        return 'facebook_organic';
    }

    public function setAuthProvider(AuthProviderInterface $provider): void
    {
        $this->authProvider = $provider;
    }

    public function getAuthProvider(): ?AuthProviderInterface
    {
        return $this->authProvider;
    }

    public function setDataProcessor(callable $processor): void
    {
        $this->dataProcessor = $processor;
    }

    public function sync(DateTime $startDate, DateTime $endDate, array $config = []): Response
    {
        if (!$this->authProvider) {
            throw new Exception("AuthProvider not set for FacebookOrganicDriver");
        }
        
        if (!$this->dataProcessor) {
            throw new Exception("DataProcessor not set for FacebookOrganicDriver.");
        }

        if ($this->logger) {
            $this->logger->info("Starting FacebookOrganicDriver sync (Modular)...");
        }

        $api = $this->initializeApi($config);
        $entity = $config['entity'] ?? 'metrics';

        if ($entity !== 'metrics') {
            return $this->syncEntities($entity, $startDate, $endDate, $config, $api);
        }

        $pagesToProcess = $config['pages'] ?? [];
        $chunkSize = $config['cache_chunk_size'] ?? '1 week';
        
        $totalStats = ['metrics' => 0, 'rows' => 0, 'duplicates' => 0];

        foreach ($pagesToProcess as $page) {
            $pageId = (string)($page['id'] ?? '');
            if (!$pageId) continue;

            $api->setPageId($pageId);

            $chunks = \Anibalealvarezs\ApiSkeleton\Helpers\DateHelper::getDateChunks(
                $startDate->format('Y-m-d'),
                $endDate->format('Y-m-d'),
                $chunkSize
            );

            foreach ($chunks as $chunk) {
                $pageData = $this->fetchPageData($api, $page, $chunk['start'], $chunk['end'], $config);
                
                $collection = new ArrayCollection();

                // 1. Process Page Insights
                if (!empty($pageData['insights'])) {
                    $pageCollection = FacebookOrganicMetricConvert::pageMetrics(
                        rows: $pageData['insights'],
                        pagePlatformId: $pageId,
                        logger: $this->logger,
                        page: $pageId // Pass string ID; host resolves entity
                    );
                    foreach ($pageCollection as $m) $collection->add($m);
                }

                // 2. Process IG Account Insights
                if (!empty($pageData['ig_insights'])) {
                    foreach ($pageData['ig_insights'] as $insight) {
                        $igCollection = FacebookOrganicMetricConvert::igAccountMetrics(
                            rows: $insight['data'],
                            date: $chunk['start'],
                            page: $pageId,
                            account: $config['accounts_group_name'] ?? 'Default',
                            channeledAccount: $page['ig_account'] ?? null,
                            logger: $this->logger,
                            period: Period::Daily
                        );
                        foreach ($igCollection as $m) $collection->add($m);
                    }
                }

                // 3. Process IG Media
                if (!empty($pageData['ig_media'])) {
                    // Logic for iterating and converting media insights would go here
                    // For now, we delegate the conversion to the SDK if it has a way to handle it
                }

                // Persist converted collection in the host
                if ($this->dataProcessor && $collection->count() > 0) {
                    $result = ($this->dataProcessor)($collection, $this->logger);
                    
                    $totalStats['metrics'] += $result['metrics'] ?? $collection->count();
                    $totalStats['rows'] += $result['rows'] ?? 0;
                    $totalStats['duplicates'] += $result['duplicates'] ?? 0;
                }
            }
        }

        return new Response(json_encode(['status' => 'success', 'data' => $totalStats]));
    }

    private function syncEntities(string $entity, DateTime $startDate, DateTime $endDate, array $config, FacebookGraphApi $api): Response
    {
        $startDateStr = $startDate->format('Y-m-d');
        $endDateStr = $endDate->format('Y-m-d');
        $jobId = $config['jobId'] ?? null;
        $filters = $config['filters'] ?? null;

        switch ($entity) {
            case 'pages':
                return \Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync::syncPages(
                    startDate: $startDateStr,
                    endDate: $endDateStr,
                    logger: $this->logger,
                    jobId: $jobId,
                    pageIds: $filters->pageIds ?? null,
                    api: $api
                );
            case 'posts':
                return \Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync::syncPosts(
                    startDate: $startDateStr,
                    endDate: $endDateStr,
                    logger: $this->logger,
                    jobId: $jobId,
                    pageIds: $filters->pageIds ?? null,
                    api: $api
                );
            default:
                throw new Exception("Entity sync for '{$entity}' not implemented in FacebookOrganicDriver");
        }
    }

    public function getApi(array $config = []): FacebookGraphApi
    {
        return $this->initializeApi($config);
    }

    protected function initializeApi(array $config): FacebookGraphApi
    {
        return new FacebookGraphApi(
            userId: $config['facebook']['user_id'] ?? $_ENV['FACEBOOK_USER_ID'] ?? 'system',
            appId: $config['facebook']['app_id'] ?? $_ENV['FACEBOOK_APP_ID'] ?? '',
            appSecret: $config['facebook']['app_secret'] ?? $_ENV['FACEBOOK_APP_SECRET'] ?? '',
            redirectUrl: $config['facebook']['redirect_uri'] ?? $_ENV['FACEBOOK_REDIRECT_URI'] ?? '',
            userAccessToken: $this->authProvider->getAccessToken(),
            apiVersion: $config['facebook']['api_version'] ?? 'v18.0'
        );
    }

    private function fetchPageData(FacebookGraphApi $api, array $page, string $start, string $end, array $config): array
    {
        $data = [
            'insights' => [],
            'ig_media' => [],
            'fb_posts' => [],
            'ig_insights' => [],
            'ig_media_insights' => []
        ];

        $pageId = (string)$page['id'];

        // 1. Page Insights
        if ($page['page_metrics'] ?? false) {
            $data['insights'] = $api->getFacebookPageInsights(
                pageId: $pageId,
                since: $start,
                until: $end
            );
        }

        // 2. Instagram Account Insights
        if (!empty($page['ig_account']) && !empty($page['ig_account_metrics'])) {
            foreach ([1, 2, 3, 4, 5] as $option) {
                try {
                    $insights = $api->getDailyInstagramAccountTotalValueInsights(
                        instagramAccountId: (string)$page['ig_account'],
                        since: $start,
                        option: $option
                    );
                    if (!empty($insights['data'])) {
                        $data['ig_insights'][] = ['option' => $option, 'data' => $insights['data']];
                    }
                } catch (Exception $e) {
                    if ($this->logger) $this->logger->warning("IG Insight option $option failed: " . $e->getMessage());
                }
            }
        }

        return $data;
    }
}
