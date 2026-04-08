<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\FacebookGraphApi\Enums\MediaType;
use Anibalealvarezs\ApiSkeleton\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiSkeleton\Interfaces\AuthProviderInterface;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;
use Carbon\Carbon;

class FacebookOrganicDriver implements SyncDriverInterface
{
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
                
                $result = ($this->dataProcessor)(
                    data: $pageData,
                    startDate: $chunk['start'],
                    endDate: $chunk['end'],
                    page: $page,
                    config: $config
                );

                $totalStats['metrics'] += $result['metrics'] ?? 0;
                $totalStats['rows'] += $result['rows'] ?? 0;
                $totalStats['duplicates'] += $result['duplicates'] ?? 0;
            }
        }

        return new Response(json_encode(['status' => 'success', 'data' => $totalStats]));
    }

    private function initializeApi(array $config): FacebookGraphApi
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

        // 2. Instagram Media
        if (!empty($page['ig_account'])) {
            $syncSince = Carbon::parse($start)->startOfYear()->timestamp;
            $data['ig_media'] = $api->getInstagramMedia(
                igUserId: (string)$page['ig_account'],
                additionalParams: ['since' => $syncSince]
            );
        }

        // 3. Facebook Posts
        if ($page['posts'] ?? false) {
            $syncSince = Carbon::parse($start)->startOfYear()->timestamp;
            $data['fb_posts'] = $api->getFacebookPosts(
                pageId: $pageId,
                additionalParams: ['since' => $syncSince]
            );
        }

        // 4. IG Account Insights (Iterative process in host, moving here)
        if (!empty($page['ig_account']) && !empty($page['ig_account_metrics'])) {
            // Options 1-5 as per legacy code
            foreach ([1, 2, 3, 4, 5] as $option) {
                try {
                    $insights = $api->getDailyInstagramAccountTotalValueInsights(
                        instagramAccountId: (string)$page['ig_account'],
                        since: $start, // Note: legacy uses startDate object, we use $start string
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
