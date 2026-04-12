<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricBreakdown;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookMarketingMetricConvert;
use Anibalealvarezs\ApiDriverCore\Helpers\DateHelper;
use Anibalealvarezs\ApiDriverCore\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface;
use Anibalealvarezs\ApiDriverCore\Traits\HasUpdatableCredentials;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;
use Anibalealvarezs\ApiDriverCore\Interfaces\SeederInterface;
use Doctrine\ORM\EntityManagerInterface;
use Anibalealvarezs\MetaHubDriver\Services\MetaInitializerService;

class FacebookMarketingDriver implements SyncDriverInterface
{

    public static function getCommonConfigKey(): ?string
    {
        return 'facebook';
    }

    /**
     * Store credentials for this driver.
     * 
     * @param array $credentials
     * @return void
     */
    public static function storeCredentials(array $credentials): void
    {
        $tokenPath = $_ENV['FACEBOOK_TOKEN_PATH'] ?? getcwd() . '/storage/tokens/facebook_tokens.json';
        $tokenKey = 'facebook_auth';
        
        if (!is_dir(dirname($tokenPath))) {
            mkdir(dirname($tokenPath), 0755, true);
        }

        $tokens = file_exists($tokenPath) ? (json_decode(file_get_contents($tokenPath), true) ?? []) : [];
        
        $tokens[$tokenKey] = [
            'access_token' => $credentials['access_token'] ?? null,
            'refresh_token' => $credentials['refresh_token'] ?? null,
            'user_id' => $credentials['user_id'] ?? null,
            'scopes' => $credentials['scopes'] ?? [],
            'updated_at' => date('Y-m-d H:i:s'),
            'expires_at' => date('Y-m-d H:i:s', strtotime('+60 days'))
        ];
        
        file_put_contents($tokenPath, json_encode($tokens, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
    }

    /**
     * Get the public resources exposed by this driver.
     * 
     * @return array
     */
    public static function getPublicResources(): array
    {
        return ['metrics' => 'fb_metrics', 'campaigns' => 'fb_campaigns'];
    }

    /**
     * Get the display label for the channel.
     * 
     * @return string
     */
    public static function getChannelLabel(): string
    {
        return 'FacebookMarketing';
    }

    /**
     * Get the display icon for the channel.
     * 
     * @return string
     */
    public static function getChannelIcon(): string
    {
        return 'M';
    }

    /**
     * Get the routes served by this driver.
     * 
     * @return array
     */
    public static function getRoutes(): array
    {
        return array_merge(\Anibalealvarezs\ApiDriverCore\Routes\AssetRoutes::get(), [
            '/fb-login' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new \Anibalealvarezs\MetaHubDriver\Controllers\FacebookAuthController())->login(),
                'public' => true,
                'admin' => false
            ],
            '/fb-auth-start' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new \Anibalealvarezs\MetaHubDriver\Controllers\FacebookAuthController())->start(),
                'public' => true,
                'admin' => false
            ],
            '/fb-callback' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new \Anibalealvarezs\MetaHubDriver\Controllers\FacebookAuthController())->callback($args['request'] ?? \Symfony\Component\HttpFoundation\Request::createFromGlobals()),
                'public' => true,
                'admin' => false
            ],
            '/fb-reports' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new \Anibalealvarezs\MetaHubDriver\Controllers\ReportController())->marketing($args),
                'public' => ($_ENV['APP_ENV'] ?? '') === 'testing' || str_contains(strtolower($_ENV['PROJECT_NAME'] ?? ''), 'demo'),
                'admin' => false,
                'html' => true
            ]
        ]);
    }

    /**
     * @inheritdoc
     */
    public function fetchAvailableAssets(): array
    {
        if (!$this->authProvider) {
            return [];
        }

        try {
            $api = $this->getApi();
            $userId = $api->getUserId();
            
            $pagesData = $api->getPages(
                userId: $userId,
                permissions: [], 
                limit: 100, 
                fields: 'id,name,instagram_business_account{id,name,username}'
            );

            $assets = [
                'facebook_pages' => [],
                'facebook_ad_accounts' => []
            ];

            if (!empty($pagesData['data'])) {
                foreach ($pagesData['data'] as $page) {
                    $assets['facebook_pages'][] = [
                        'id' => $page['id'],
                        'title' => $page['name'],
                        'ig_account' => $page['instagram_business_account']['id'] ?? null,
                        'ig_account_name' => $page['instagram_business_account']['username'] ?? $page['instagram_business_account']['name'] ?? null,
                    ];
                }
            }

            $adAccountsData = $api->getAdAccounts(
                userId: $userId,
                limit: 100, 
                fields: 'id,name,account_id,account_status,currency'
            );

            if (!empty($adAccountsData['data'])) {
                foreach ($adAccountsData['data'] as $acc) {
                    $assets['facebook_ad_accounts'][] = [
                        'id' => $acc['id'],
                        'name' => $acc['name'] ?? ('Ad Account ' . $acc['id']),
                    ];
                }
            }

            return $assets;
        } catch (\Exception $e) {
            return [];
        }
    }

    /**
     * @inheritdoc
     */
    public function updateConfiguration(array $newData, array $currentConfig): array
    {
        $selectedAssets = $newData['assets']['ad_accounts'] ?? [];
        $channelEnabled = (bool) ($newData['enabled'] ?? true);
        $historyRange = $newData['cache_history_range'] ?? $newData['marketing_history_range'] ?? null;
        $featureToggles = $newData['feature_toggles'] ?? [];
        $metricsStrategy = $newData['metrics_strategy'] ?? null;
        $metricsConfig = $newData['metrics_config'] ?? null;
        $entityFilters = $newData['entity_filters'] ?? [];

        if (!isset($currentConfig['channels']['facebook_marketing'])) {
            $currentConfig['channels']['facebook_marketing'] = [];
        }
        
        $chanCfg = &$currentConfig['channels']['facebook_marketing'];

        if ($historyRange) {
            $chanCfg['cache_history_range'] = $historyRange;
        }
        
        // Cron settings
        foreach (['cron_entities_hour', 'cron_entities_minute', 'cron_recent_hour', 'cron_recent_minute'] as $key) {
            if (isset($featureToggles[$key])) {
                $chanCfg[$key] = (int)$featureToggles[$key];
            }
        }
        
        $chanCfg['enabled'] = $channelEnabled;

        // Redis cache toggle
        if (isset($featureToggles['cache_aggregations'])) {
            $prevValue = (bool)($chanCfg['cache_aggregations'] ?? false);
            $newValue = (bool)$featureToggles['cache_aggregations'];
            $chanCfg['cache_aggregations'] = $newValue;
            
            if ($prevValue && !$newValue && class_exists('\Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService')) {
                \Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService::clearChannel('facebook_marketing');
            }
        }

        if ($metricsStrategy) {
            $chanCfg['metrics_strategy'] = $metricsStrategy;
        }
        if ($metricsConfig !== null) {
            $chanCfg['metrics_config'] = $metricsConfig;
        }

        $marketingEntities = ['CAMPAIGN', 'ADSET', 'AD', 'CREATIVE'];
        foreach ($marketingEntities as $e) {
            if (isset($entityFilters[$e])) {
                $chanCfg[$e]['cache_include'] = $entityFilters[$e];
            }
        }

        $fbMarketingFeatures = ['ad_account_metrics', 'campaigns', 'campaign_metrics', 'adsets', 'adset_metrics', 'ads', 'ad_metrics', 'creatives', 'creative_metrics'];
        foreach ($fbMarketingFeatures as $f) {
            if (isset($featureToggles[$f])) {
                $chanCfg['AD_ACCOUNT'][$f] = (bool)$featureToggles[$f];
            }
        }

        // Ad Accounts management
        $currentAccs = $chanCfg['ad_accounts'] ?? [];
        $newAccsList = [];
        $selectedIds = array_map('strval', array_column($selectedAssets, 'id'));

        foreach ($currentAccs as $acc) {
            if (in_array((string)$acc['id'], $selectedIds)) {
                $newAccsList[] = $acc;
            }
        }

        $existingIds = array_map('strval', array_column($currentAccs, 'id'));
        foreach ($selectedAssets as $newAcc) {
            $accId = (string) $newAcc['id'];
            if (!in_array($accId, $existingIds)) {
                if (class_exists('\Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService')) {
                    $newAccsList[] = \Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService::getEntitySchema('facebook_marketing', [
                        'id' => $accId,
                        'name' => $newAcc['name'] ?? ("Ad Account " . $accId),
                    ]);
                } else {
                    $newAccsList[] = ['id' => $accId, 'name' => $newAcc['name'] ?? ("Ad Account " . $accId)];
                }
            }
        }

        $chanCfg['ad_accounts'] = $newAccsList;

        return $currentConfig;
    }

    /**
     * @inheritdoc
     */
    public function validateAuthentication(): array
    {
        try {
            $api = $this->getApi();
            $api->performRequest('GET', 'me', ['fields' => 'id,name']);
            return [
                'success' => true,
                'message' => 'Authentication is valid.',
                'details' => [
                    'user_id' => $api->getUserId()
                ]
            ];
        } catch (\Exception $e) {
            return [
                'success' => false,
                'message' => $e->getMessage(),
                'details' => []
            ];
        }
    }
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

                $collection = FacebookMarketingMetricConvert::adAccountMetrics(
                    rows: $rows['data'] ?? [],
                    logger: $this->logger,
                    account: $config['accounts_group_name'] ?? 'Default',
                    channeledAccountPlatformId: $accountId,
                    period: \Anibalealvarezs\ApiSkeleton\Enums\Period::Daily
                );

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
    public function validateConfig(array $config): array
    {
        $envOverrides = [
            'FACEBOOK_APP_ID' => 'app_id',
            'FACEBOOK_APP_SECRET' => 'app_secret',
            'FACEBOOK_REDIRECT_URI' => 'app_redirect_uri',
            'FACEBOOK_USER_TOKEN' => 'graph_user_access_token',
            'FACEBOOK_PAGE_TOKEN' => 'graph_page_access_token',
            'FACEBOOK_TOKEN_PATH' => 'graph_token_path',
            'FACEBOOK_USER_ID' => 'user_id',
            'FACEBOOK_ACCOUNTS_GROUP' => 'accounts_group_name',
        ];

        foreach ($envOverrides as $envKey => $configPath) {
            $val = getenv($envKey);
            if ($val !== false && $val !== '') {
                $config[$configPath] = $val;
            }
        }

        $tokenPath = $_ENV['FACEBOOK_TOKEN_PATH'] ?? $config['graph_token_path'] ?? './storage/tokens/facebook_tokens.json';
        if (file_exists($tokenPath)) {
            $tokens = json_decode(file_get_contents($tokenPath), true);
            $marketingToken = $tokens['facebook_marketing']['access_token'] ?? null;
            $marketingUserId = $tokens['facebook_marketing']['user_id'] ?? null;
            
            if ($marketingToken) {
                $config['graph_user_access_token'] = $marketingToken;
                $config['access_token'] = $marketingToken;
            }
            if ($marketingUserId) {
                $config['user_id'] = $marketingUserId;
            }
        }

        if (isset($config['AD_ACCOUNT'])) {
            $globalAdAccountDefaults = $config['AD_ACCOUNT'];
            $config['ad_accounts'] = array_map(function ($adAccount) use ($globalAdAccountDefaults) {
                return array_merge($globalAdAccountDefaults, $adAccount);
            }, $config['ad_accounts'] ?? []);
        }
        return $config;
    }

    public function seedDemoData(SeederInterface $seeder, array $config = []): void
    {
        $output = $config['output'] ?? null;
        if ($output) $output->writeln("🚀 Facebook Marketing (5 Campaigns, 180 Days)...");

        $em = $seeder->getEntityManager();
        $faker = \Faker\Factory::create('en_US');
        $dates = $seeder->getDates(180);

        $chanEnumClass = $seeder->getEnumClass('channel');
        $accTypeEnumClass = $seeder->getEnumClass('account_type');
        $fbChan = $chanEnumClass::facebook_marketing;

        $accClass = $seeder->getEntityClass('account');
        $chanAccountClass = $seeder->getEntityClass('channeled_account');
        $campaignClass = $seeder->getEntityClass('campaign');
        $chanCampaignClass = $seeder->getEntityClass('channeled_campaign');
        $chanAdGroupClass = $seeder->getEntityClass('channeled_ad_group');
        $chanAdClass = $seeder->getEntityClass('channeled_ad');

        $accRepo = $em->getRepository($accClass);
        $demoAccount = $accRepo->findOneBy(['name' => 'Demo Agency Marketing']) ?? (new $accClass())->addName('Demo Agency Marketing');
        $em->persist($demoAccount);
        $em->flush();

        $adAccountId = "act_" . $faker->numerify('################');
        $ca = $em->getRepository($chanAccountClass)->findOneBy(['platformId' => $adAccountId]) ?? (new $chanAccountClass());
        $ca->addPlatformId($adAccountId)
            ->addAccount($demoAccount)
            ->addType($accTypeEnumClass::META_AD_ACCOUNT)
            ->addChannel($fbChan->value)
            ->addName("Demo Ad Account");
        $em->persist($ca);
        $em->flush();

        $campaigns = [];
        for ($i = 1; $i <= 5; $i++) {
            $cId = $faker->numerify('##########');
            $cName = "Demo " . $faker->catchPhrase() . " Campaign";
            
            $campaign = $em->getRepository($campaignClass)->findOneBy(['campaignId' => $cId]) ?? new $campaignClass();
            $campaign->addCampaignId($cId)->addName($cName);
            $em->persist($campaign);

            $chanCampaign = $em->getRepository($chanCampaignClass)->findOneBy(['platformId' => $cId, 'channeledAccount' => $ca]) ?? new $chanCampaignClass();
            $chanCampaign->addPlatformId($cId)
                ->addChannel($fbChan->value)
                ->addChanneledAccount($ca)
                ->addCampaign($campaign)
                ->addBudget($faker->randomFloat(2, 50, 500));
            $em->persist($chanCampaign);
            
            $adGroups = [];
            for ($j = 1; $j <= 2; $j++) {
                $agId = $faker->numerify('##########');
                $agName = "AdSet $j - " . $faker->word();
                
                $chanAdGroup = $em->getRepository($chanAdGroupClass)->findOneBy(['platformId' => $agId, 'channeledAccount' => $ca]) ?? new $chanAdGroupClass();
                $chanAdGroup->addPlatformId($agId)
                    ->addChannel($fbChan->value)
                    ->addName($agName)
                    ->addChanneledAccount($ca)
                    ->addCampaign($campaign)
                    ->addChanneledCampaign($chanCampaign);
                $em->persist($chanAdGroup);
                
                for ($k = 1; $k <= 2; $k++) {
                    $adId = $faker->numerify('##########');
                    $adName = "Ad $k (" . $faker->colorName() . ")";
                    
                    $chanAd = $em->getRepository($chanAdClass)->findOneBy(['platformId' => $adId, 'channeledAccount' => $ca]) ?? new $chanAdClass();
                    $chanAd->addPlatformId($adId)
                        ->addChannel($fbChan->value)
                        ->addName($adName)
                        ->addChanneledAccount($ca)
                        ->addChanneledAdGroup($chanAdGroup)
                        ->addChanneledCampaign($chanCampaign);
                    $em->persist($chanAd);
                    
                    $adGroups[] = ['chanAd' => $chanAd, 'chanAdGroup' => $chanAdGroup, 'chanCampaign' => $chanCampaign];
                }
            }
            $campaigns[] = ['campaign' => $campaign, 'chanCampaign' => $chanCampaign, 'adGroups' => $adGroups];
        }
        $em->flush();

        $dimManager = $seeder->getDimensionManager();
        $countryEnumValues = ['USA', 'ESP', 'MEX', 'COL'];
        $deviceEnumValues = ['desktop', 'mobile', 'tablet'];

        $countries = [];
        foreach ($countryEnumValues as $code) {
            $enumClass = $seeder->getEnumClass('country');
            $countryClass = $seeder->getEntityClass('country');
            $enum = $enumClass::from($code);
            $c = $em->getRepository($countryClass)->findOneBy(['code' => $enum]);
            if (!$c) {
                $c = (new $countryClass())->addCode($enum)->addName($code);
                $em->persist($c);
            }
            $countries[$code] = $c;
        }
        $devices = [];
        foreach ($deviceEnumValues as $type) {
            $enumClass = $seeder->getEnumClass('device');
            $deviceClass = $seeder->getEntityClass('device');
            $enum = $enumClass::from($type);
            $d = $em->getRepository($deviceClass)->findOneBy(['type' => $enum]);
            if (!$d) {
                $d = (new $deviceClass())->addType($enum);
                $em->persist($d);
            }
            $devices[$type] = $d;
        }
        $em->flush();

        foreach ($dates as $date) {
            foreach ($campaigns as $cpData) {
                foreach ($cpData['adGroups'] as $agData) {
                    foreach ($seeder->getAges() as $age) {
                        foreach ($seeder->getGenders() as $gender) {
                            $code = $countryEnumValues[array_rand($countryEnumValues)];
                            $type = $deviceEnumValues[array_rand($deviceEnumValues)];
                            $country = $countries[$code];
                            $device = $devices[$type];

                            $dimSet = $dimManager->resolveDimensionSet([
                                ['dimensionKey' => 'age', 'dimensionValue' => $age],
                                ['dimensionKey' => 'gender', 'dimensionValue' => $gender],
                            ]);

                            $imps = rand(10, 100);
                            $clicks = (int)($imps * rand(1, 5) / 100);
                            $spend = (float)($clicks * rand(5, 20) / 10);
                            
                            $metrics = ['impressions' => $imps, 'clicks' => $clicks, 'spend' => $spend];
                            
                            foreach ($metrics as $name => $val) {
                                if ($val <= 0) continue;
                                $seeder->queueMetric(
                                    channel: $fbChan,
                                    name: $name,
                                    date: $date,
                                    value: $val,
                                    setId: $dimSet->getId(),
                                    caId: $ca->getId(),
                                    cpId: $agData['chanCampaign']->getId(),
                                    agId: $agData['chanAdGroup']->getId(),
                                    adId: $agData['chanAd']->getId(),
                                    countryId: $country->getId(),
                                    deviceId: $device->getId(),
                                    data: json_encode(['raw' => $val]),
                                    setHash: $dimSet->getHash(),
                                    channeledAccountPlatformId: $ca->getPlatformId(),
                                    countryPId: $code,
                                    devicePId: $type
                                );
                            }
                        }
                    }
                }
            }
            if ($output) $output->write(".");
        }
        $em->clear();
        if ($output) $output->writeln("\n   - Facebook Marketing complete.");
    }

    public function boot(): void
    {
        \Anibalealvarezs\ApiDriverCore\Classes\RepositoryRegistry::registerRelations([
            'linked_fb_page_id' => ['table' => 'channeled_accounts', 'fk' => 'channeled_account_id', 'field' => 'data', 'alias' => 'rca', 'isJSON' => true, 'jsonPath' => 'facebook_page_id', 'isAttribute' => true],
        ]);
    }

    /**
     * @inheritdoc
     */
    public function getAssetPatterns(): array
    {
        return [
            'facebook_ad_accounts' => [
                'prefix' => 'fa:acc',
                'hostnames' => ['facebook.com'],
                'url_id_regex' => '/act_([0-9]+)/',
                'type' => 'meta_ad_account',
                'key' => 'ad_accounts'
            ]
        ];
    }

    /**
     * @inheritdoc
     */
    public static function getPageTypes(): array
    {
        return [
            'meta_ad_account' => 'Meta Ad Account'
        ];
    }

    /**
     * @inheritdoc
     */
    public static function getAccountTypes(): array
    {
        return [
            'meta_ad_account' => 'Meta Ad Account'
        ];
    }

    /**
     * @inheritdoc
     */
    public static function getEntityPaths(): array
    {
        return [];
    }

    /**
     * @inheritdoc
     */
    public function prepareUiConfig(array $channelConfig): array
    {
        $ui = [];
        if (class_exists('\Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService')) {
            $ui['fb_metrics_config'] = \Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService::hydrate('facebook_marketing', 'metrics', $channelConfig['metrics_config'] ?? []);
        } else {
            $ui['fb_metrics_config'] = $channelConfig['metrics_config'] ?? [];
        }

        $ui['fb_cache_chunk_size'] = $channelConfig['cache_chunk_size'] ?? '1 week';
        $ui['fb_ad_account_ids'] = [];
        foreach (($channelConfig['ad_accounts'] ?? []) as $a) {
            $ui['fb_ad_account_ids'][] = (string)$a['id'];
        }

        $features = ['ad_account_metrics', 'campaigns', 'campaign_metrics', 'adsets', 'adset_metrics', 'ads', 'ad_metrics', 'creatives', 'creative_metrics'];
        foreach ($features as $f) {
            $ui['fb_feature_toggles'][$f] = $channelConfig['AD_ACCOUNT'][$f] ?? false; 
        }
        
        $entities = ['CAMPAIGN', 'ADSET', 'AD', 'CREATIVE'];
        foreach ($entities as $e) {
            $ui['fb_entity_filters'][$e] = $channelConfig[$e]['cache_include'] ?? '';
        }

        return $ui;
    }

    /**
     * @inheritdoc
     */
    public function initializeEntities(mixed $entityManager, array $config = []): array
    {
        if (!$entityManager instanceof EntityManagerInterface) {
            throw new Exception("EntityManagerInterface required for FacebookMarketingDriver entity initialization.");
        }

        $assets = $this->fetchAvailableAssets();
        $initializer = new MetaInitializerService($entityManager, $this->logger);
        
        return $initializer->initialize($this->getChannel(), $config, ['ad_accounts' => $assets['facebook_ad_accounts'] ?? []]);
    }

    /**
     * @inheritdoc
     */
    public function reset(mixed $entityManager, string $mode = 'all', array $config = []): array
    {
        if (!$entityManager instanceof EntityManagerInterface) {
            throw new Exception("EntityManagerInterface required for FacebookMarketingDriver reset.");
        }

        $resetter = new \Anibalealvarezs\MetaHubDriver\Services\MetaResetService($entityManager);
        return $resetter->reset($this->getChannel(), $mode);
    }
    /**
     * @inheritdoc
     */
    public function getDateFilterMapping(): array
    {
        return [];
    }
}
