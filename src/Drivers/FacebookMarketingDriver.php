<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricBreakdown;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
use Anibalealvarezs\FacebookGraphApi\Conversions\FacebookMarketingMetricConvert;
use Anibalealvarezs\ApiSkeleton\Helpers\DateHelper;
use Anibalealvarezs\ApiSkeleton\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiSkeleton\Interfaces\AuthProviderInterface;
use Anibalealvarezs\ApiSkeleton\Traits\HasUpdatableCredentials;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;
use Carbon\Carbon;
use Doctrine\Common\Collections\ArrayCollection;
use Anibalealvarezs\ApiSkeleton\Interfaces\SeederInterface;
use Doctrine\ORM\EntityManagerInterface;

class FacebookMarketingDriver implements SyncDriverInterface
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
        return 'facebook_marketing';
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
            throw new Exception("AuthProvider not set for FacebookMarketingDriver");
        }
        
        if (!$this->dataProcessor) {
            throw new Exception("DataProcessor not set for FacebookMarketingDriver.");
        }

        if ($this->logger) {
            $this->logger->info("Starting FacebookMarketingDriver sync (Modular)...");
        }

        $api = $this->initializeApi($config);
        $entity = $config['entity'] ?? 'metrics';

        if ($entity !== 'metrics') {
            return $this->syncEntities($entity, $startDate, $endDate, $config, $api);
        }

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
                
                if (empty($rows['data'])) continue;

                // Convert raw data into metrics using the SDK
                $collection = FacebookMarketingMetricConvert::adAccountMetrics(
                    rows: $rows['data'] ?? [],
                    logger: $this->logger,
                    account: $config['accounts_group_name'] ?? 'Default', // Passes name string; host will resolve entity
                    channeledAccountPlatformId: $accountId,
                    period: \Anibalealvarezs\ApiSkeleton\Enums\Period::Daily
                );

                // Persist converted collection in the host
                if ($this->dataProcessor && $collection->count() > 0) {
                    $result = ($this->dataProcessor)($collection, $this->logger);
                    
                    $totalStats['metrics'] += $result['metrics'] ?? $collection->count();
                    $totalStats['rows'] += $result['rows'] ?? count($rows['data']);
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

        $syncService = \Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync::class;

        switch ($entity) {
            case 'campaigns':
                if (class_exists($syncService)) {
                    return $syncService::syncCampaigns(
                        seeder: $config['seeder'],
                        manager: $config['manager'],
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        adAccountIds: $filters->adAccountIds ?? null
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case 'ad_groups':
                if (class_exists($syncService)) {
                    return $syncService::syncAdGroups(
                        seeder: $config['seeder'],
                        manager: $config['manager'],
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        adAccountIds: $filters->adAccountIds ?? null,
                        parentIdsMap: $filters->parentIdsMap ?? null
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case 'ads':
                if (class_exists($syncService)) {
                    return $syncService::syncAds(
                        seeder: $config['seeder'],
                        manager: $config['manager'],
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        adAccountIds: $filters->adAccountIds ?? null,
                        parentIdsMap: $filters->parentIdsMap ?? null
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case 'creatives':
                if (class_exists($syncService)) {
                    return $syncService::syncCreatives(
                        seeder: $config['seeder'],
                        manager: $config['manager'],
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        adAccountIds: $filters->adAccountIds ?? null
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            default:
                throw new Exception("Entity sync for '{$entity}' not implemented in FacebookMarketingDriver");
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
            'metricSet' => MetricSet::BASIC,
            'breakdowns' => [MetricBreakdown::AGE, MetricBreakdown::GENDER],
            'fields' => 'account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,actions,action_values',
            'metrics' => []
        ];
    }

    private function isFatal(Exception $e): bool
    {
        $msg = $e->getMessage();
        return (stripos($msg, '(#100)') !== false || stripos($msg, 'valid insights metric') !== false || stripos($msg, 'permissions') !== false);
    }
    /**
     * @inheritdoc
     */
    public function getConfigSchema(): array
    {
        return [
            'global' => [
                'enabled' => true,
                'cache_history_range' => '2 years',
                'cache_aggregations' => false,
                'metrics_strategy' => 'default',
            ],
            'entity' => [
                'id' => '',
                'name' => '',
                'enabled' => true,
                'exclude_from_caching' => false,
            ],
            'metrics' => [
                'spend' => ['enabled' => false, 'format' => 'currency', 'precision' => 2],
                'clicks' => ['enabled' => false, 'format' => 'number', 'precision' => 0],
                'impressions' => ['enabled' => false, 'format' => 'number', 'precision' => 0],
                'reach' => ['enabled' => false, 'format' => 'number', 'precision' => 0],
                'frequency' => ['enabled' => false, 'format' => 'number', 'precision' => 2],
                'ctr' => ['enabled' => false, 'format' => 'percent', 'precision' => 2],
                'cpc' => ['enabled' => false, 'format' => 'currency', 'precision' => 2, 'sparkline_direction' => 'inverted'],
                'cpm' => ['enabled' => false, 'format' => 'currency', 'precision' => 2, 'sparkline_direction' => 'inverted'],
                'results' => ['enabled' => false, 'format' => 'number', 'precision' => 0],
                'cost_per_result' => ['enabled' => false, 'format' => 'currency', 'precision' => 2, 'sparkline_direction' => 'inverted'],
                'result_rate' => ['enabled' => false, 'format' => 'percent', 'precision' => 2],
                'purchase_roas' => ['enabled' => false, 'format' => 'number', 'precision' => 2],
                'actions' => ['enabled' => false, 'format' => 'number', 'precision' => 0],
            ]
        ];
    }

    /**
     * @inheritdoc
     */
    /**
     * @inheritdoc
     */
    public function validateConfig(array $config): array
    {
        $globalExclude = $config['exclude_from_caching'] ?? [];
        if (!is_array($globalExclude)) {
            $globalExclude = [$globalExclude];
        }

        if (isset($config['AD_ACCOUNT'])) {
            $globalAdAccountDefaults = $config['AD_ACCOUNT'];
            $config['ad_accounts'] = array_map(function ($adAccount) use ($globalAdAccountDefaults, $globalExclude) {
                $merged = array_merge($globalAdAccountDefaults, $adAccount);
                if (in_array((string)($merged['id'] ?? ''), array_map('strval', $globalExclude))) {
                    $merged['exclude_from_caching'] = true;
                }
                return $merged;
            }, $config['ad_accounts'] ?? []);
        }
        return $config;
    }

    /**
     * @inheritdoc
     */

    public function seedDemoData(SeederInterface $seeder, array $config = []): void
    {
        $output = $config['output'] ?? null;
        if ($output) $output->writeln("📊 FB Marketing (Massive Simulation, JSON Source Logic)...");

        $em = $seeder->getEntityManager();
        
        $channelClass = $seeder->getEnumClass('channel');
        $fbChan = $channelClass::facebook_marketing;
        
        $accCount = 30; 
        $dates = $seeder->getDates(30);
        $statuses = ['ACTIVE', 'PAUSED', 'ARCHIVED'];
        $objectives = ['OUTCOME_SALES', 'OUTCOME_AWARENESS', 'OUTCOME_LEADS', 'OUTCOME_TRAFFIC'];

        $accountClass = $seeder->getEntityClass('account');
        $chanAccountClass = $seeder->getEntityClass('channeled_account');
        $campaignClass = $seeder->getEntityClass('campaign');
        $chanCampaignClass = $seeder->getEntityClass('channeled_campaign');
        $chanAdGroupClass = $seeder->getEntityClass('channeled_ad_group');
        $chanAdClass = $seeder->getEntityClass('channeled_ad');
        $accTypeEnumClass = $seeder->getEnumClass('account_type');

        $faker = \Faker\Factory::create('en_US');

        // Parent Account (Client)
        $fbParent = $em->getRepository($accountClass)->findOneBy(['name' => "Marketing Demo Client"]);
        if (!$fbParent) {
            $fbParent = (new $accountClass())->addName("Marketing Demo Client");
            $em->persist($fbParent);
            $em->flush();
        }
        $gId = $fbParent->getId();

        $progress = $config['progress'] ?? null;
        if ($progress) {
            $progress->setMaxSteps($accCount);
            $progress->start();
        }

        for ($i = 0; $i < $accCount; $i++) {
            $caPId = 'act_' . $this->generateSeedingPlatformId();
            $ca = (new $chanAccountClass())
                ->addPlatformId($caPId)
                ->addAccount($fbParent)
                ->addType($accTypeEnumClass::META_AD_ACCOUNT)
                ->addChannel($fbChan->value)
                ->addName($faker->company())
                ->addData([
                    'id' => $caPId,
                    'account_status' => 1,
                    'currency' => 'USD',
                    'timezone_name' => 'America/New_York',
                    'business_name' => $faker->company(),
                ]);
            $em->persist($ca);
            $em->flush();

            $campCount = rand(5, 10);
            for ($c = 0; $c < $campCount; $c++) {
                $gCpPId = $this->generateSeedingPlatformId();
                $campG = (new $campaignClass())->addCampaignId($gCpPId)->addName($faker->catchPhrase());
                $em->persist($campG);

                $cp = (new $chanCampaignClass())
                    ->addPlatformId($gCpPId)
                    ->addChanneledAccount($ca)
                    ->addCampaign($campG)
                    ->addChannel($fbChan->value)
                    ->addBudget(rand(100, 500))
                    ->addData([
                        'id' => $gCpPId,
                        'name' => $campG->getName(),
                        'objective' => $objectives[array_rand($objectives)],
                        'status' => $statuses[array_rand($statuses)],
                        'buying_type' => 'AUCTION',
                        'daily_budget' => rand(5000, 20000), 
                    ]);
                $em->persist($cp);
                $em->flush();

                $agCount = rand(2, 4);
                for ($s = 0; $s < $agCount; $s++) {
                    $agPId = $this->generateSeedingPlatformId();
                    $agName = "AdSet: " . $faker->words(3, true);
                    $ag = (new $chanAdGroupClass())
                        ->addPlatformId($agPId)
                        ->addChanneledAccount($ca)
                        ->addChannel($fbChan->value)
                        ->addName($agName)
                        ->addChanneledCampaign($cp)
                        ->addData([
                            'id' => $agPId,
                            'name' => $agName,
                            'status' => $statuses[array_rand($statuses)],
                            'billing_event' => 'IMPRESSIONS',
                            'optimization_goal' => 'REACH',
                            'targeting' => ['geo_locations' => ['countries' => ['US']]],
                        ]);
                    $em->persist($ag);
                    $em->flush();

                    $adCount = rand(2, 5);
                    for ($a = 0; $a < $adCount; $a++) {
                        $adPId = $this->generateSeedingPlatformId();
                        $adName = "Ad: " . $faker->words(2, true);
                        $ad = (new $chanAdClass())
                            ->addPlatformId($adPId)
                            ->addChanneledAccount($ca)
                            ->addChannel($fbChan->value)
                            ->addName($adName)
                            ->addChanneledAdGroup($ag)
                            ->addData([
                                'id' => $adPId,
                                'name' => $adName,
                                'status' => $statuses[array_rand($statuses)],
                                'creative' => ['id' => 'cre_' . rand(1000, 9999)],
                                'preview_shareable_link' => "https://fb.com/ads/preview/$adPId",
                            ]);
                        $em->persist($ad);
                        $em->flush();

                        $this->seedRealisticAdDaily($seeder, $dates, $fbChan, $gId, $ca->getId(), $campG->getId(), $cp->getId(), $ag->getId(), $ad->getId(), $fbParent->getName(), $caPId, $gCpPId, $gCpPId, $agPId, $adPId);
                    }
                }
            }
            if ($progress) $progress->advance();
            $em->clear();
            $fbParent = $em->getRepository($accountClass)->findOneBy(['id' => $gId]);
        }
        if ($progress) $progress->finish();
    }

    private function generateSeedingPlatformId(): string
    {
        return substr(str_shuffle(str_repeat('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', 5)), 0, 15);
    }

    private function seedRealisticAdDaily($seeder, $dates, $fbChan, $gId, $caId, $gCpId, $cpId, $agId, $adId, $accName, $caPId, $gCpPId, $cpPId, $agPId, $adPId): void
    {
        $ages = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+'];
        $genders = ['Female', 'Male', 'Unknown'];
        
        foreach ($dates as $date) {
            $used = [];
            for ($b = 0; $b < rand(1, 2); $b++) { 
                $age = $ages[array_rand($ages)];
                $gen = $genders[array_rand($genders)];
                if (isset($used["$age|$gen"])) {
                    continue;
                } 
                $used["$age|$gen"] = true;
                
                // We assume the seeder provides dimension hash resolution
                $setInfo = $seeder->getDimensionSetInfo($age, $gen);
                $setId = $setInfo['id'];
                $setHash = $setInfo['hash'];

                $imps = rand(100, 2000);
                $reach = (int)($imps * rand(70, 95) / 100);
                $spend = (float)($imps * rand(5, 15) / 1000);
                $clicks = (int)($imps * rand(1, 5) / 100);

                $data = [
                    'impressions' => $imps,
                    'spend' => $spend,
                    'reach' => $reach,
                    'clicks' => $clicks,
                    'ctr' => $imps > 0 ? $clicks / $imps : 0,
                    'cpc' => $clicks > 0 ? $spend / $clicks : 0,
                    'results' => (int)($clicks * rand(5, 15) / 100),
                ];

                foreach ($data as $name => $val) {
                    $seeder->queueMetric(
                        channel: $fbChan,
                        name: $name,
                        date: $date,
                        value: $val,
                        setId: $setId,
                        setHash: $setHash,
                        caId: $caId,
                        gAccId: $gId,
                        gCpId: $gCpId,
                        cpId: $cpId,
                        agId: $agId,
                        adId: $adId,
                        accName: $accName,
                        caPId: $caPId,
                        gCpPId: $gCpPId,
                        cpPId: $cpPId,
                        agPId: $agPId,
                        adPId: $adPId,
                        data: json_encode($data)
                    );
            }
        }
    }
}

    public function boot(): void
    {
        \Repositories\BaseRepository::registerRelations([
            'linked_fb_page_id' => ['table' => 'channeled_accounts', 'fk' => 'channeled_account_id', 'field' => 'data', 'alias' => 'rca', 'isJSON' => true, 'jsonPath' => 'facebook_page_id', 'isAttribute' => true],
        ]);
    }
}
