<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricBreakdown;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
use Anibalealvarezs\ApiSkeleton\Helpers\DateHelper;
use Anibalealvarezs\ApiSkeleton\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiSkeleton\Interfaces\AuthProviderInterface;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;

class FacebookMarketingDriver implements SyncDriverInterface
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
        return 'facebook_marketing';
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
            throw new Exception("AuthProvider not set for FacebookMarketingDriver");
        }
        
        if (!$this->dataProcessor) {
            throw new Exception("DataProcessor not set for FacebookMarketingDriver.");
        }

        if ($this->logger) {
            $this->logger->info("Starting FacebookMarketingDriver sync...");
        }

        $api = $this->initializeApi($config);
        $accountsToProcess = $config['ad_accounts'] ?? [];
        $chunkSize = $config['cache_chunk_size'] ?? '1 week';
        
        $totalStats = ['metrics' => 0, 'rows' => 0, 'duplicates' => 0];

        foreach ($accountsToProcess as $account) {
            $accountId = (string)($account['id'] ?? '');
            if (!$accountId) continue;

            $chunks = DateHelper::getDateChunks(
                $startDate->format('Y-m-d'),
                $endDate->format('Y-m-d'),
                $chunkSize
            );

            foreach ($chunks as $chunk) {
                $rows = $this->fetchInsights($api, $accountId, $chunk['start'], $chunk['end'], $config);
                
                $result = ($this->dataProcessor)(
                    data: $rows,
                    startDate: $chunk['start'],
                    endDate: $chunk['end'],
                    account: $account,
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

    private function fetchInsights(FacebookGraphApi $api, string $accountId, string $start, string $end, array $config): array
    {
        $metricConfig = $this->getMetricsConfig($config);
        $params = [
            'time_range' => json_encode(['since' => $start, 'until' => $end]),
            'fields' => $metricConfig['fields']
        ];

        $maxRetries = 3;
        $retryCount = 0;
        
        while ($retryCount < $maxRetries) {
            try {
                return $api->getAdAccountInsights(
                    adAccountId: $accountId,
                    metricBreakdown: $metricConfig['breakdowns'],
                    additionalParams: $params,
                    metricSet: $metricConfig['metricSet'],
                    customMetrics: $metricConfig['metrics']
                );
            } catch (Exception $e) {
                $retryCount++;
                if ($retryCount >= $maxRetries || $this->isFatal($e)) {
                    throw $e;
                }
                usleep(500000 * $retryCount);
            }
        }
        return ['data' => []];
    }

    private function getMetricsConfig(array $config): array
    {
        return [
            'metricSet' => MetricSet::basic,
            'breakdowns' => [MetricBreakdown::action_type, MetricBreakdown::publisher_platform],
            'fields' => 'account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,actions,action_values',
            'metrics' => []
        ];
    }

    private function isFatal(Exception $e): bool
    {
        $msg = $e->getMessage();
        return (stripos($msg, '(#100)') !== false || stripos($msg, 'valid insights metric') !== false || stripos($msg, 'permissions') !== false);
    }
}
