<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

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

        if ($this->logger) {
            $this->logger->info("Starting FacebookMarketingDriver sync (Modular)...");
        }
        
        if (!$this->dataProcessor) {
            throw new Exception("DataProcessor not set for FacebookMarketingDriver.");
        }

        $accountsToProcess = $config['ad_accounts'] ?? [];
        $chunkSize = $config['cache_chunk_size'] ?? '1 week';
        
        if (empty($accountsToProcess)) {
            $this->logger->warning("No ad accounts provided in config for FacebookMarketingDriver");
            return new Response(json_encode(['status' => 'success', 'message' => 'No accounts to process']), 200);
        }

        $totalStats = ['metrics' => 0, 'rows' => 0, 'duplicates' => 0];

        foreach ($accountsToProcess as $account) {
            $accountId = $account['id'] ?? 'unknown';
            if ($this->logger) {
                $this->logger->info("FacebookMarketingDriver: Processing ad account {$accountId}");
            }

            $chunks = \Anibalealvarezs\ApiSkeleton\Helpers\DateHelper::getDateChunks(
                $startDate->format('Y-m-d'),
                $endDate->format('Y-m-d'),
                $chunkSize
            );

            foreach ($chunks as $chunk) {
                if ($this->logger) {
                    $this->logger->info("Processing ad account {$accountId} chunk: {$chunk['start']} to {$chunk['end']}");
                }
                $result = ($this->dataProcessor)(
                    startDate: $chunk['start'],
                    endDate: $chunk['end'],
                    resume: $config['resume'] ?? false,
                    logger: $this->logger,
                    jobId: $config['jobId'] ?? null,
                    account: $account,
                    config: $config
                );

                $totalStats['metrics'] += $result['metrics'] ?? 0;
                $totalStats['rows'] += $result['rows'] ?? 0;
                $totalStats['duplicates'] += $result['duplicates'] ?? 0;
            }
        }

        return new Response(json_encode([
            'status' => 'success', 
            'data' => $totalStats
        ]));
    }
}
