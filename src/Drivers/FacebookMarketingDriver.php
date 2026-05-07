<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\ApiDriverCore\Classes\RepositoryRegistry;
use Anibalealvarezs\ApiDriverCore\Classes\AggregationProfileTemplates;
use Anibalealvarezs\ApiDriverCore\Classes\MetricProfileTemplates;
use Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity;
use Anibalealvarezs\ApiDriverCore\Interfaces\AggregationProfileProviderInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\CanonicalMetricDictionaryProviderInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\MetricProfileProviderInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\PageableInterface;
use Anibalealvarezs\ApiDriverCore\Routes\AssetRoutes;
use Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService;
use Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService;
use Anibalealvarezs\ApiDriverCore\Traits\HasHierarchicalValidationTrait;
use Anibalealvarezs\MetaHubDriver\Controllers\FacebookAuthController;
use Anibalealvarezs\MetaHubDriver\Enums\MetaFeature;
use Anibalealvarezs\MetaHubDriver\Enums\MetaEntityType;
use Anibalealvarezs\MetaHubDriver\Enums\MetaSyncScope;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\ApiDriverCore\Enums\AssetCategory;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricBreakdown;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
use Anibalealvarezs\FacebookGraphApi\Enums\AdAccountPermission;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookMarketingMetricConvert;
use Anibalealvarezs\ApiDriverCore\Helpers\DateHelper;
use Anibalealvarezs\ApiDriverCore\Interfaces\ChanneledAccountableInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface;
use Anibalealvarezs\ApiDriverCore\Traits\HasUpdatableCredentials;
use Anibalealvarezs\ApiDriverCore\Traits\SyncDriverTrait;
use GuzzleHttp\Exception\GuzzleException;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;
use Anibalealvarezs\ApiDriverCore\Interfaces\SeederInterface;
use Anibalealvarezs\ApiDriverCore\Enums\HierarchyType;
use Anibalealvarezs\ApiDriverCore\Helpers\FieldsNormalizerHelper;
use Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync;

class FacebookMarketingDriver implements SyncDriverInterface, ChanneledAccountableInterface, MetricProfileProviderInterface, AggregationProfileProviderInterface, CanonicalMetricDictionaryProviderInterface
{
    use HasHierarchicalValidationTrait;
    use SyncDriverTrait;

    public array $updatableCredentials = [
        'FACEBOOK_USER_TOKEN',
        'FACEBOOK_USER_ID',
        'FACEBOOK_ACCOUNTS_GROUP',
        'FACEBOOK_APP_ID',
        'FACEBOOK_APP_SECRET'
    ];

    public function getUpdatableCredentials(): array
    {
        return $this->updatableCredentials;
    }

    public static function getCommonConfigKey(): ?string
    {
        return 'facebook';
    }

    public static function getMetricProfiles(): array
    {
        return [
            MetricProfileTemplates::campaignBreakdown(
                channel: 'facebook_marketing',
                key: 'facebook_marketing_campaign',
                label: 'Facebook Marketing Campaign'
            ),
            MetricProfileTemplates::adGroupBreakdown(
                channel: 'facebook_marketing',
                key: 'facebook_marketing_ad_group',
                label: 'Facebook Marketing Ad Group'
            ),
            MetricProfileTemplates::adCreativeBreakdown(
                channel: 'facebook_marketing',
                key: 'facebook_marketing_ad',
                label: 'Facebook Marketing Ad'
            ),
        ];
    }

    public static function getAggregationProfiles(): array
    {
        return [
            AggregationProfileTemplates::adsHierarchyProfile(
                channel: 'facebook_marketing',
                key: 'facebook_marketing_ads_hierarchy',
                label: 'Facebook Marketing Ads Hierarchy'
            ),
        ];
    }

    public static function getCanonicalMetricDictionary(): array
    {
        return [
            'spend' => ['spend', 'spend_daily'],
            'clicks' => ['clicks', 'clicks_daily'],
            'impressions' => ['impressions', 'impressions_daily'],
            'reach' => ['reach', 'reach_daily'],
            'frequency' => ['frequency', 'frequency_daily'],
            'conversions' => ['results', 'results_daily'],
            'cost_per_conversion' => ['cost_per_result'],
            'conversion_rate' => ['result_rate'],
            'roas_purchase' => ['purchase_roas', 'purchase_roas_daily', 'website_purchase_roas', 'website_purchase_roas_daily'],
            'actions' => ['actions', 'actions_daily'],
        ];
    }

    public static function getPlatformEntityIdField(): string
    {
        return 'account_id';
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
        return array_merge(AssetRoutes::get(), [
            '/fb-login' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new FacebookAuthController())->login(),
                'public' => true,
                'admin' => false
            ],
            '/fb-auth-start' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new FacebookAuthController())->start(),
                'public' => true,
                'admin' => false
            ],
            '/fb-callback' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new FacebookAuthController())->callback($args['request'] ?? \Symfony\Component\HttpFoundation\Request::createFromGlobals()),
                'public' => true,
                'admin' => false
            ],
            '/fb-reports' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new \Anibalealvarezs\MetaHubDriver\Controllers\ReportController())->marketing($args),
                'public' => true,
                'admin' => false,
                'html' => true
            ]
        ]);
    }

    /**
     * @inheritdoc
     * @throws Exception|GuzzleException
     */
    public function fetchAvailableAssets(bool $throwOnError = false): array
    {
        if (!$this->authProvider) {
            return [];
        }

        try {
            $api = $this->getApi();
            $userId = $api->getUserId();
            
            $pagesData = $api->getPages(
                userId: $userId,
                fields: 'id,name,website,created_time,instagram_business_account{id,name,username,website}'
            );

            $assets = [
                'ad_accounts' => []
            ];

            if (!empty($pagesData['data'])) {
                foreach ($pagesData['data'] as $page) {
                    $assets['facebook_pages'][] = [
                        'id' => $page['id'],
                        'title' => $page['name'],
                        'hostname' => $page['website'] ?? null,
                        'created_time' => $page['created_time'] ?? null,
                        'data' => $page,
                        'ig_account' => $page['instagram_business_account']['id'] ?? null,
                        'ig_account_name' => $page['instagram_business_account']['username'] ?? $page['instagram_business_account']['name'] ?? null,
                        'ig_hostname' => $page['instagram_business_account']['website'] ?? null,
                        'ig_data' => $page['instagram_business_account'] ?? null,
                    ];
                }
            }

            $adAccountsData = $api->getAdAccounts(
                userId: $userId,
                fields: 'id,name,account_id,account_status,currency,created_time'
            );

            if (!empty($adAccountsData['data'])) {
                foreach ($adAccountsData['data'] as $acc) {
                    $assets['ad_accounts'][] = [
                        'id' => $acc['id'],
                        'name' => $acc['name'] ?? ('Ad Account ' . $acc['id']),
                        'created_time' => $acc['created_time'] ?? null,
                        'data' => $acc,
                    ];
                }
            }

            return $assets;
        } catch (Exception $e) {
            $this->logger?->error("FacebookMarketingDriver: Error fetching available assets: " . $e->getMessage());
            if ($throwOnError) {
                throw $e;
            }
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
        if (isset($newData['granular_sync'])) {
            $chanCfg['granular_sync'] = filter_var($newData['granular_sync'], FILTER_VALIDATE_BOOLEAN);
        }

        // Redis cache toggle
        if (isset($featureToggles['cache_aggregations'])) {
            $prevValue = (bool)($chanCfg['cache_aggregations'] ?? false);
            $newValue = (bool)$featureToggles['cache_aggregations'];
            $chanCfg['cache_aggregations'] = $newValue;
            
            if ($prevValue && !$newValue && class_exists('\Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService')) {
                CacheStrategyService::clearChannel('facebook_marketing');
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

        $fbMarketingFeatures = array_map(fn($f) => $f->value, MetaFeature::marketing());
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
                $lostAccess = false;
                $matchingSelected = null;
                foreach ($selectedAssets as $sa) {
                    if ((string)$sa['id'] === (string)$acc['id']) {
                        $lostAccess = filter_var($sa['lost_access'] ?? false, FILTER_VALIDATE_BOOLEAN);
                        $matchingSelected = $sa;
                        break;
                    }
                }
                $acc['lost_access'] = $lostAccess;
                if ($matchingSelected) {
                    $acc['enabled'] = filter_var($matchingSelected['enabled'] ?? true, FILTER_VALIDATE_BOOLEAN);
                    $acc['created_time'] = $matchingSelected['created_time'] ?? $acc['created_time'] ?? null;
                    $acc['data'] = $matchingSelected['data'] ?? $acc['data'] ?? [];
                }
                $newAccsList[] = $acc;
            } else {
                // Keep it in the list but disabled if it was there before (Status Toggling Support)
                $acc['enabled'] = false;
                $newAccsList[] = $acc;
            }
        }

        $existingIds = array_column($currentAccs, 'id');
        foreach ($selectedAssets as $newAcc) {
            $accId = $newAcc['id'];
            $isLostAccess = filter_var($newAcc['lost_access'] ?? false, FILTER_VALIDATE_BOOLEAN);
            
            if (!in_array($accId, $existingIds)) {
                $item = [
                    'id' => $accId,
                    'name' => $newAcc['name'] ?? ("Ad Account " . $accId),
                    'hostname' => $newAcc['hostname'] ?? null,
                    'enabled' => filter_var($newAcc['enabled'] ?? true, FILTER_VALIDATE_BOOLEAN),
                    'created_time' => $newAcc['created_time'] ?? null,
                    'data' => $newAcc['data'] ?? [],
                    'lost_access' => $isLostAccess
                ];

                if (class_exists('\Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService')) {
                    $schema = ConfigSchemaRegistryService::getEntitySchema('facebook_marketing', $item);
                    $newAccsList[] = $schema;
                } else {
                    $newAccsList[] = $item;
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
        } catch (Exception $e) {
            return [
                'success' => false,
                'message' => $e->getMessage(),
                'details' => []
            ];
        }
    }

    private ?AuthProviderInterface $authProvider;
    private ?LoggerInterface $logger;
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

    public static function getProviderLabel(): string
    {
        return 'Meta';
    }

    public static function getProviderName(): string
    {
        return 'facebook';
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

    /**
     * @throws GuzzleException
     * @throws Exception
     */
    public function sync(
        DateTime $startDate,
        DateTime $endDate,
        array $config = [],
        ?callable $shouldContinue = null,
        ?callable $identityMapper = null
    ): Response {
        if (!$this->authProvider) {
            throw new Exception("AuthProvider not set for FacebookMarketingDriver");
        }
        
        if (!$this->dataProcessor) {
            throw new Exception("DataProcessor not set for FacebookMarketingDriver.");
        }

        $api = $this->initializeApi($config);
        $rawEntity = $config['entity'] ?? MetaSyncScope::METRICS->value;
        $scope = MetaSyncScope::tryFrom($rawEntity);

        if ($scope === MetaSyncScope::ENTITIES) {
            return $this->syncEntities(MetaSyncScope::ENTITIES, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper);
        }

        $specificEntity = MetaEntityType::tryFrom($rawEntity);
        if ($specificEntity) {
            return $this->syncEntities($specificEntity, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper);
        }

        return $this->syncMetrics($startDate, $endDate, $config, $shouldContinue, $identityMapper);
    }

    /**
     * @throws GuzzleException
     * @throws Exception
     */
    private function syncMetrics(
        DateTime $startDate,
        DateTime $endDate,
        array $config = [],
        ?callable $shouldContinue = null,
        ?callable $identityMapper = null
    ): Response {
        $api = $this->initializeApi($config);
        $accountsToProcess = $config['ad_accounts'] ?? [];
        $chunkSize = $config['cache_chunk_size'] ?? '1 week';
        
        // 1. Batch Resolve Ad Accounts via Oracle
        $caMap = [];
        if ($identityMapper && !empty($accountsToProcess)) {
            $aIds = [];
            foreach ($accountsToProcess as $account) {
                $id = (string)($account['id'] ?? $account);
                if ($id) {
                    $cleanId = method_exists($this, 'getCleanId') ? $this->getCleanId($id) : $id;
                    $aIds[] = $cleanId;
                }
            }
            $caMap = $identityMapper('channeled_accounts', ['platform_ids' => $aIds]) ?? [];
        }

        $totalStats = ['metrics' => 0, 'rows' => 0, 'duplicates' => 0];

        $consecutiveFailures = 0;
        $maxConsecutiveFailures = 5;

        foreach ($accountsToProcess as $account) {
            $accountPlatformIdRaw = (string)($account['id'] ?? $account);
            if (!$accountPlatformIdRaw) continue;

            $accountPlatformId = method_exists($this, 'getCleanId') ? $this->getCleanId($accountPlatformIdRaw) : $accountPlatformIdRaw;

            try {

                $accCfg = $account['account_data'] ?? $account;
                if (isset($accCfg['enabled']) && !$accCfg['enabled']) {
                    $this->logger?->info("Skipping metrics sync for account $accountPlatformId (disabled in config)");
                    continue;
                }

                if (!empty($accCfg['lost_access'])) {
                    $this->logger?->info("Skipping metrics sync for account $accountPlatformId (lost access)");
                    continue;
                }

                // Resolve Internal Ad Account Entity object from pre-loaded map
                $caObject = $caMap[$accountPlatformId] ?? (new UniversalEntity())->setPlatformId($accountPlatformId);

                $levelsToFetch = $this->resolveLevelsToFetch($accCfg, $config);
                $chunks = DateHelper::getDateChunks($startDate->format('Y-m-d'), $endDate->format('Y-m-d'), $chunkSize);

                foreach ($chunks as $chunk) {
                    foreach ($levelsToFetch as $level) {
                        if ($shouldContinue && !$shouldContinue()) {
                            throw new Exception("Sync aborted by the orchestrator.");
                        }
                        $this->logger?->info(">>> INICIO: Sincronizando métricas de Marketing para Ad Account: $accountPlatformId (Level: $level | Timeframe: {$chunk['start']} a {$chunk['end']})");
                        $response = $this->fetchInsights($api, $accountPlatformId, $chunk['start'], $chunk['end'], $config, $level, $shouldContinue);
                        $rows = $response['data'] ?? [];
                        
                        if (!empty($rows)) {
                            $rows = $this->filterInsightRows($rows, $level, $config);
                        }

                        if (!empty($rows)) {
                            $collection = FacebookMarketingMetricConvert::metrics(
                                rows: $rows, 
                                channeledAccount: $caObject ?? $accountPlatformId, 
                                level: $level, 
                                logger: $this->logger,
                                account: (method_exists($caObject, 'getAccount')) ? $caObject->getAccount() : ($config['accounts_group_name'] ?? 'Default')
                            );
                            if ($this->dataProcessor && $collection->count() > 0) {
                                $this->validateHierarchicalIntegrity(collection: $collection, type: HierarchyType::MARKETING);

                                $result = ($this->dataProcessor)($collection, $this->logger);
                                
                                $metricsCount = $result['metrics'] ?? $collection->count();
                                $processedRows = $result['rows'] ?? count($rows);
                                $duplicates = $result['duplicates'] ?? 0;

                                $totalStats['metrics'] += $metricsCount;
                                $totalStats['rows'] += $processedRows;
                                $totalStats['duplicates'] += $duplicates;

                                $this->logger?->info("<<< EXITO: Sincronización completada para Ad Account: $accountPlatformId (Level: $level). Métricas: $metricsCount | Filas: $processedRows | Duplicados: $duplicates");
                            }
                        } else {
                            $this->logger?->info("--- INFO: No se encontraron datos de Marketing para Ad Account: $accountPlatformId (Level: $level)");
                        }
                    }
                }
                $consecutiveFailures = 0; // Reset on success
            } catch (Exception $e) {
                if (str_contains($e->getMessage(), 'Sync aborted')) throw $e;
                $this->logger?->error("Unhandled error syncing Ad Account $accountPlatformId: " . $e->getMessage());
                $consecutiveFailures++;
                if ($consecutiveFailures >= $maxConsecutiveFailures) {
                    throw new Exception("Terminating marketing sync due to $consecutiveFailures consecutive unhandled failures.");
                }
            }
        }

        return new Response(json_encode(['status' => 'success', 'data' => $totalStats]));
    }

    private function resolveLevelsToFetch(array $accCfg, array $config): array
    {
        if (!empty($accCfg[MetaFeature::AD_METRICS->value]) || !empty($config['AD_ACCOUNT'][MetaFeature::AD_METRICS->value])) {
            return ['ad'];
        } elseif (!empty($accCfg[MetaFeature::ADSET_METRICS->value]) || !empty($config['AD_ACCOUNT'][MetaFeature::ADSET_METRICS->value])) {
            return ['adset'];
        } elseif (!empty($accCfg[MetaFeature::CAMPAIGN_METRICS->value]) || !empty($config['AD_ACCOUNT'][MetaFeature::CAMPAIGN_METRICS->value])) {
            return ['campaign'];
        }
        return ['account'];
    }

    /**
     * @throws Exception
     */
    protected function syncEntities(
        MetaSyncScope|MetaEntityType $entity,
        DateTime $startDate,
        DateTime $endDate,
        array $config = [],
        ?FacebookGraphApi $api = null,
        ?callable $shouldContinue = null,
        ?callable $identityMapper = null
    ): Response {
        $api = $api ?? $this->initializeApi($config);
        $startDateStr = $startDate->format('Y-m-d');
        $endDateStr = $endDate->format('Y-m-d');
        $jobId = $config['jobId'] ?? null;
        $filters = $config['filters'] ?? (object)[];

        $syncService = FacebookEntitySync::class;

        $channeledAccounts = [];
        if ($identityMapper && !empty($config['ad_accounts'])) {
            $aIds = [];
            foreach ($config['ad_accounts'] as $account) {
                $id = (string)($account['id'] ?? $account);
                if ($id) {
                    $cleanId = method_exists($this, 'getCleanId') ? $this->getCleanId($id) : $id;
                    $aIds[] = $cleanId;
                }
            }
            if (!empty($aIds)) {
                $channeledAccounts = array_values($identityMapper('channeled_accounts', ['platform_ids' => $aIds]) ?? []);
            }
        }

        switch ($entity) {
            case MetaEntityType::CAMPAIGN:
                if (class_exists($syncService)) {
                    return $syncService::syncCampaigns(
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        channeledAccounts: $channeledAccounts,
                        entityProcessor: $this->dataProcessor,
                        jobStatusChecker: $shouldContinue
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case MetaEntityType::AD_GROUP:
                if (class_exists($syncService)) {
                    return $syncService::syncAdGroups(
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        channeledAccounts: $channeledAccounts,
                        parentIdsMap: $filters->parentIdsMap ?? null,
                        entityProcessor: $this->dataProcessor,
                        jobStatusChecker: $shouldContinue
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case MetaEntityType::AD:
                if (class_exists($syncService)) {
                    return $syncService::syncAds(
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        channeledAccounts: $channeledAccounts,
                        parentIdsMap: $filters->parentIdsMap ?? null,
                        entityProcessor: $this->dataProcessor,
                        jobStatusChecker: $shouldContinue
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case MetaEntityType::CREATIVE:
                if (class_exists($syncService)) {
                    return $syncService::syncCreatives(
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        channeledAccounts: $channeledAccounts,
                        entityProcessor: $this->dataProcessor,
                        jobStatusChecker: $shouldContinue
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case MetaSyncScope::ENTITIES:
                $results = [];
                $accCfg = $config['AD_ACCOUNT'] ?? [];
                $this->logger?->info("DEBUG: FacebookMarketingDriver::syncEntities (ENTITIES) - Features config: " . json_encode($accCfg));

                // 1. Campaigns
                if (!empty($accCfg[MetaFeature::CAMPAIGNS->value])) {
                    $campResponse = $this->syncEntities(MetaEntityType::CAMPAIGN, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper);
                    $results['campaigns'] = json_decode($campResponse->getContent(), true);
                }
                $campaignMap = $results['campaigns']['authorized_ids_map'] ?? null;

                // 2. Ad Groups
                if (!empty($accCfg[MetaFeature::ADSETS->value])) {
                    if ($campaignMap) {
                        $config['filters'] = (object) array_merge((array)($config['filters'] ?? []), ['parentIdsMap' => $campaignMap]);
                    }
                    $agResponse = $this->syncEntities(MetaEntityType::AD_GROUP, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper);
                    $results['ad_groups'] = json_decode($agResponse->getContent(), true);
                }
                $adSetMap = $results['ad_groups']['authorized_ids_map'] ?? null;

                // 3. Ads
                if (!empty($accCfg[MetaFeature::ADS->value])) {
                    if ($adSetMap) {
                        $config['filters'] = (object) array_merge((array)($config['filters'] ?? []), ['parentIdsMap' => $adSetMap]);
                    }
                    $results['ads'] = json_decode($this->syncEntities(MetaEntityType::AD, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper)->getContent(), true);
                }

                // 4. Creatives
                if (!empty($accCfg[MetaFeature::CREATIVES->value])) {
                    $results['creatives'] = json_decode($this->syncEntities(MetaEntityType::CREATIVE, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper)->getContent(), true);
                }

                return new Response(json_encode(['status' => 'success', 'results' => $results]), 200, ['Content-Type' => 'application/json']);
            default:
                throw new Exception("Entity sync for '{$entity}' not implemented in FacebookMarketingDriver");
        }
    }

    /**
     * @throws Exception
     */
    public function getApi(array $config = []): FacebookGraphApi
    {
        if (empty($config) && $this->authProvider instanceof \Anibalealvarezs\ApiDriverCore\Auth\BaseAuthProvider) {
            $config = $this->authProvider->getConfig();
        }
        return $this->initializeApi($config);
    }

    /**
     * @throws Exception
     */
    protected function initializeApi(array $config): FacebookGraphApi
    {
        $this->logger?->info("DEBUG: FacebookMarketingDriver::initializeApi - START");
        return new FacebookGraphApi(
            userId: $config['user_id'] ?? $config['facebook']['user_id'] ?? $this->authProvider->getUserId() ?: 'system',
            appId: $config['app_id'] ?? $config['facebook']['app_id'] ?? '',
            appSecret: $config['app_secret'] ?? $config['facebook']['app_secret'] ?? '',
            redirectUrl: $config['redirect_uri'] ?? $config['facebook']['redirect_uri'] ?? '',
            userAccessToken: $config['access_token'] ?? $config['graph_user_access_token'] ?? $this->authProvider->getAccessToken(),
            apiVersion: $config['api_version'] ?? $config['facebook']['api_version'] ?? 'v18.0'
        );
    }

    /**
     * @throws GuzzleException
     * @throws Exception
     */
    protected function fetchInsights(FacebookGraphApi $api, string $accountId, string $startDate, string $endDate, array $config, string $level = 'account', ?callable $shouldContinue = null): array
    {
        $this->logger?->info("DEBUG: FacebookMarketingDriver::fetchInsights - Requesting level '$level' for '$accountId' from $startDate to $endDate");
        $metricConfig = $this->getMetricsConfig($config);

        $params = [
            'time_range' => json_encode(['since' => $startDate, 'until' => $endDate]),
            'level' => $level,
            'fields' => $metricConfig['fields']
        ];

        $maxRetries = 3;
        $retryCount = 0;
        $currentLimit = 100;
        
        while ($retryCount < $maxRetries) {
            if ($shouldContinue && !$shouldContinue()) {
                throw new Exception("Sync aborted by the orchestrator.");
            }
            try {
                return $api->getAdAccountInsights(
                    adAccountId: $accountId,
                    limit: $currentLimit,
                    metricBreakdown: $metricConfig['breakdowns'],
                    metricSet: $metricConfig['metricSet'],
                    additionalParams: $params,
                    customMetrics: $metricConfig['metrics']
                );
            } catch (Exception $e) {
                if (str_contains($e->getMessage(), 'reduce the amount of data')) {
                    $currentLimit = max(10, (int) floor($currentLimit / 2));
                    $this->logger?->warning("Data limit error for $accountId in fetchInsights: Reducing limit to $currentLimit");
                }
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
        $customMetrics = $config['metrics'] ?? [];
        $metricSet = isset($config['metric_set']) ? (MetricSet::tryFrom($config['metric_set']) ?: MetricSet::BASIC) : (!empty($customMetrics) ? MetricSet::CUSTOM : MetricSet::BASIC);

        $baseFields = 'account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name';
        
        if ($metricSet === MetricSet::CUSTOM && !empty($customMetrics)) {
            $fields = $baseFields . ',' . (is_array($customMetrics) ? implode(',', $customMetrics) : $customMetrics);
        } else {
            $fields = $baseFields . ',' . AdAccountPermission::DEFAULT->insightsFields($metricSet) . ',cpm,action_values';
        }

        return [
            'metricSet' => $metricSet,
            'breakdowns' => $config['breakdowns'] ?? [MetricBreakdown::AGE, MetricBreakdown::GENDER],
            'fields' => $fields,
            'metrics' => is_array($customMetrics) ? $customMetrics : explode(',', (string)$customMetrics)
        ];
    }

    private function isFatal(Exception $e): bool
    {
        $msg = $e->getMessage();
        return (stripos($msg, '(#100)') !== false || stripos($msg, 'valid insights metric') !== false || stripos($msg, 'permissions') !== false);
    }

    private function filterInsightRows(array $rows, string $level, array $config): array
    {
        // 1. Get filters for all relevant levels to ensure hierarchical consistency
        $campaignInclude = FacebookEntitySync::getFacebookFilter($config, 'CAMPAIGN', 'cache_include');
        $campaignExclude = FacebookEntitySync::getFacebookFilter($config, 'CAMPAIGN', 'cache_exclude');

        $adsetInclude = FacebookEntitySync::getFacebookFilter($config, 'ADSET', 'cache_include');
        $adsetExclude = FacebookEntitySync::getFacebookFilter($config, 'ADSET', 'cache_exclude');

        $adInclude = FacebookEntitySync::getFacebookFilter($config, 'AD', 'cache_include');
        $adExclude = FacebookEntitySync::getFacebookFilter($config, 'AD', 'cache_exclude');

        $creativeInclude = FacebookEntitySync::getFacebookFilter($config, 'CREATIVE', 'cache_include');
        $creativeExclude = FacebookEntitySync::getFacebookFilter($config, 'CREATIVE', 'cache_exclude');

        // If no filters at all, just return everything
        if (!$campaignInclude && !$campaignExclude && !$adsetInclude && !$adsetExclude && !$adInclude && !$adExclude && !$creativeInclude && !$creativeExclude) {
            return $rows;
        }

        return array_filter($rows, function ($row) use (
            $level, $campaignInclude, $campaignExclude, $adsetInclude, $adsetExclude, 
            $adInclude, $adExclude, $creativeInclude, $creativeExclude
        ) {
            // 1. Strict Validation: Row must contain the ID for the requested level
            if ($level === 'ad' && (empty(trim((string)($row['ad_id'] ?? ''))) || trim((string)$row['ad_id']) === '0' || empty(trim((string)($row['adset_id'] ?? ''))))) {
                return false;
            }
            if ($level === 'adset' && (empty(trim((string)($row['adset_id'] ?? ''))) || trim((string)$row['adset_id']) === '0')) {
                return false;
            }
            if ($level === 'campaign' && (empty(trim((string)($row['campaign_id'] ?? ''))) || trim((string)$row['campaign_id']) === '0')) {
                return false;
            }

            // 2. Hierarchy Filters
            // A. Check Campaign level (available in most marketing insight levels)
            if (!empty($row['campaign_id']) && ($campaignInclude || $campaignExclude)) {
                $cId = (string)$row['campaign_id'];
                $cName = (string)($row['campaign_name'] ?? '');
                if (!FacebookEntitySync::matchesFilter($cId, $campaignInclude, $campaignExclude) && !FacebookEntitySync::matchesFilter($cName, $campaignInclude, $campaignExclude)) {
                    return false;
                }
            }

            // B. Check AdSet level
            if (!empty($row['adset_id']) && ($adsetInclude || $adsetExclude)) {
                $asId = (string)$row['adset_id'];
                $asName = (string)($row['adset_name'] ?? '');
                if (!FacebookEntitySync::matchesFilter($asId, $adsetInclude, $adsetExclude) && !FacebookEntitySync::matchesFilter($asName, $adsetInclude, $adsetExclude)) {
                    return false;
                }
            }

            // C. Check Ad level
            if (!empty($row['ad_id']) && ($adInclude || $adExclude)) {
                $adId = (string)$row['ad_id'];
                $adName = (string)($row['ad_name'] ?? '');
                if (!FacebookEntitySync::matchesFilter($adId, $adInclude, $adExclude) && !FacebookEntitySync::matchesFilter($adName, $adInclude, $adExclude)) {
                    return false;
                }
            }

            // D. Check Creative level
            if (!empty($row['creative_id']) && ($creativeInclude || $creativeExclude)) {
                $crId = (string)$row['creative_id'];
                if (!FacebookEntitySync::matchesFilter($crId, $creativeInclude, $creativeExclude)) {
                    return false;
                }
            }

            return true;
        });
    }

    /**
     * @inheritdoc
     */
    public function getConfigSchema(): array
    {
        return [
            'global' => [
                'enabled' => false,
                'cache_history_range' => '2 years',
                'cache_aggregations' => false,
                'metrics_strategy' => 'default',
            ],
            'entity' => [
                'id' => '',
                'name' => '',
                'enabled' => true,
                'exclude_from_caching' => false,
                'lost_access' => false,
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
        $config = ConfigSchemaRegistryService::hydrate(
            $this->getChannel(),
            'global',
            $config,
            $this->getConfigSchema()
        );

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
        if (is_string($tokenPath) && str_starts_with($tokenPath, './')) {
            $tokenPath = getcwd() . substr($tokenPath, 1);
        }

        if (file_exists($tokenPath)) {
            $tokens = json_decode(file_get_contents($tokenPath), true);
            $tokenData = $tokens['facebook_auth'] ?? $tokens['facebook_marketing'] ?? [];
            $marketingToken = $tokenData['access_token'] ?? null;
            $marketingUserId = $tokenData['user_id'] ?? null;
            
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
            $newAdAccounts = [];
            foreach ($config['ad_accounts'] ?? [] as $adAccount) {
                $merged = array_merge($globalAdAccountDefaults, $adAccount);
                $id = (string)($merged['id'] ?? '');
                if ($id) {
                    $newAdAccounts[$id] = $merged;
                } else {
                    $newAdAccounts[] = $merged;
                }
            }
            $config['ad_accounts'] = $newAdAccounts;
        }

        // 4. Handle Sync Window (History Range)
        if (empty($config['startDate']) && empty($config['start_date']) && !empty($config['cache_history_range'])) {
            $config['startDate'] = date('Y-m-d', strtotime('-' . $config['cache_history_range']));
        }

        return $config;
    }

    public function seedDemoData(SeederInterface $seeder, array $config = []): void
    {
        $output = $config['output'] ?? null;
        if ($output) $output->writeln("🚀 Facebook Marketing (5 Campaigns, 180 Days)...");

        $faker = \Faker\Factory::create('en_US');
        $dates = $seeder->getDates(180);

        $fbChan = \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_marketing;

        $demoAccount = $seeder->resolveEntity('account', ['name' => 'Demo Agency Marketing']);

        $adAccountId = "act_" . $faker->numerify('################');
        $ca = $seeder->resolveEntity('channeled_account', [
            'platformId' => $adAccountId,
            'account' => $demoAccount,
            'type' => 'meta_ad_account',
            'channel' => \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_marketing->value,
            'name' => "Demo Ad Account"
        ]);

        $campaigns = [];
        for ($i = 1; $i <= 5; $i++) {
            $cId = $faker->numerify('##########');
            $cName = "Demo " . $faker->catchPhrase() . " Campaign";
            
            $campaign = $seeder->resolveEntity('campaign', [
                'campaignId' => $cId,
                'name' => $cName
            ]);

            $chanCampaign = $seeder->resolveEntity('channeled_campaign', [
                'platformId' => $cId,
                'channel' => \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_marketing->value,
                'channeledAccount' => $ca,
                'campaign' => $campaign,
                'budget' => $faker->randomFloat(2, 50, 500)
            ]);
            
            $adGroups = [];
            for ($j = 1; $j <= 2; $j++) {
                $agId = $faker->numerify('##########');
                $agName = "AdSet $j - " . $faker->word();
                
                $chanAdGroup = $seeder->resolveEntity('channeled_ad_group', [
                    'platformId' => $agId,
                    'channel' => \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_marketing->value,
                    'name' => $agName,
                    'channeledAccount' => $ca,
                    'campaign' => $campaign,
                    'channeledCampaign' => $chanCampaign
                ]);
                
                for ($k = 1; $k <= 2; $k++) {
                    $adId = $faker->numerify('##########');
                    $adName = "Ad $k (" . $faker->colorName() . ")";
                    
                    $chanAd = $seeder->resolveEntity('channeled_ad', [
                        'platformId' => $adId,
                        'channel' => \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_marketing->value,
                        'name' => $adName,
                        'channeledAccount' => $ca,
                        'channeledAdGroup' => $chanAdGroup,
                        'channeledCampaign' => $chanCampaign
                    ]);
                    
                    $adGroups[] = ['chanAd' => $chanAd, 'chanAdGroup' => $chanAdGroup, 'chanCampaign' => $chanCampaign];
                }
            }
            $campaigns[] = ['campaign' => $campaign, 'chanCampaign' => $chanCampaign, 'adGroups' => $adGroups];
        }

        $dimManager = $seeder->getDimensionManager();
        $countryEnumValues = ['USA', 'ESP', 'MEX', 'COL'];
        $deviceEnumValues = ['desktop', 'mobile', 'tablet'];

        $countries = [];
        foreach ($countryEnumValues as $code) {
            $countries[$code] = $seeder->resolveEntity('country', ['name' => $code]);
        }

        $devices = [];
        foreach ($deviceEnumValues as $type) {
            $devices[$type] = $seeder->resolveEntity('device', ['type' => $type]);
        }

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
                                    setId: $dimSet->id,
                                    adId: $agData['chanAd']->id,
                                    agId: $agData['chanAdGroup']->id,
                                    cpId: $cpData['chanCampaign']->id,
                                    caId: $ca->id,
                                    gAccId: $demoAccount->id,
                                    gCpId: $cpData['campaign']->id,
                                    accName: $demoAccount->getTitle(),
                                    caPId: (string)$ca->getPlatformId(),
                                    gCpPId: (string)$cpData['campaign']->getPlatformId(),
                                    cpPId: (string)$cpData['chanCampaign']->getPlatformId(),
                                    data: json_encode($metrics)
                                );
                            }
                        }
                    }
                }
            }
            if ($output) $output->write(".");
        }
        if ($output) $output->writeln("\n   - Facebook Marketing complete.");
    }

    public function boot(): void
    {
        RepositoryRegistry::registerRelations([
            'linked_fb_page_id' => ['table' => 'channeled_accounts', 'fk' => 'channeled_account_id', 'field' => 'data', 'alias' => 'rca', 'isJSON' => true, 'jsonPath' => 'facebook_page_id', 'isAttribute' => true],
        ]);
    }

    public static function getAssetPatterns(): array
    {
        return [
            'facebook_ad_account' => [
                'category' => AssetCategory::IDENTITY,
                'key' => 'ad_accounts',
                'channeled_account' => [
                    'platform_id' => [
                        'type' => 'raw',
                        'key' => 'id'
                    ],
                    'platform_created_at_key' => 'created_time',
                    'name_key' => 'name',
                    'type' => 'facebook_ad_account',
                    'data_key' => 'data'
                ]
            ]
        ];
    }

    public static function getChanneledAccounts(array $asset): array {
        return [
            // Ad account
            [
                'platformId' => self::getPlatformId($asset, AssetCategory::IDENTITY, 'facebook_ad_account'),
                'platformCreatedAt' => self::getChanneledAccountPlatformCreatedAt(asset: $asset),
                'name' => self::getChanneledAccountName(asset: $asset),
                'type' => self::getChanneledAccountType(),
                'enabled' => filter_var($asset['enabled'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'data' => self::getChanneledAccountData(asset: $asset)
            ]
        ];
    }

    public static function getPlatformId(array $asset, AssetCategory $category, string $context): string
    {
        return match ($category) {
            AssetCategory::IDENTITY => self::deriveAdAccountId($asset, 'id'),
            AssetCategory::CAMPAIGN => self::deriveMarketingId($asset, 'id'),
            AssetCategory::GROUPING => self::deriveMarketingId($asset, 'id'),
            AssetCategory::UNIT => self::deriveMarketingId($asset, 'id'),
            default => (string) ($asset['id'] ?? '')
        };
    }

    public static function getCanonicalId(array $asset, AssetCategory $category, string $context): string
    {
        return self::getPlatformId($asset, $category, $context);
    }

    private static function deriveAdAccountId(array $asset, string $key): string
    {
        $pId = isset($asset[$key]) ? FieldsNormalizerHelper::getCleanString($asset[$key]) : '';
        return $pId ? str_replace('act_', '', $pId) : '';
    }

    private static function deriveMarketingId(array $asset, string $key): string
    {
        return isset($asset[$key]) ? FieldsNormalizerHelper::getCleanString($asset[$key]) : '';
    }

    // CHANNELED ACCOUNT FIELDS

    public static function getChanneledAccountPlatformId(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::META_AD_ACCOUNT): string {
        $idKey = $key ?: 'id';
        return isset($asset[$idKey]) && ($asset[$idKey]) ? self::getPlatformId($asset, AssetCategory::IDENTITY, 'facebook_ad_account') : '';
    }

    public static function getChanneledAccountPlatformCreatedAt(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::META_AD_ACCOUNT): string {
        $idKey = $key ?: 'created_time';
        return isset($asset[$idKey]) ? FieldsNormalizerHelper::getCleanString($asset[$idKey]) : '';
    }

    public static function getChanneledAccountName(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::META_AD_ACCOUNT): string {
        $idKey = $key ?: 'name';
        return isset($asset[$idKey]) ? FieldsNormalizerHelper::getCleanString($asset[$idKey]) : '';
    }

    public static function getChanneledAccountType(string|MetaEntityType $entityType = MetaEntityType::META_AD_ACCOUNT): string {
        return $entityType instanceof MetaEntityType ? $entityType->value : $entityType;
    }

    public static function getChanneledAccountData(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::META_AD_ACCOUNT): array {
        $idKey = $key ?: 'data';
        return isset($asset[$idKey]) ? FieldsNormalizerHelper::getCleanArray($asset[$idKey]) : [];
    }

    public function getCleanHostname(string $hostname): string
    {
        if (str_contains($hostname, 'sc-domain:')) {
            $hostname = str_replace('sc-domain:', '', $hostname);
        } else {
            $hostname = parse_url($hostname, PHP_URL_HOST);
        }
        return $hostname;
    }

    public function getCleanId(string $id): string
    {
        return str_replace('act_', '', $id);
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
            $ui['fb_metrics_config'] = ConfigSchemaRegistryService::hydrate('facebook_marketing', 'metrics', $channelConfig['metrics_config'] ?? []);
        } else {
            $ui['fb_metrics_config'] = $channelConfig['metrics_config'] ?? [];
        }

        $ui['fb_marketing_enabled'] = $channelConfig['enabled'] ?? false;
        $ui['fb_marketing_history_range'] = $channelConfig['cache_history_range'] ?? '2 years';
        $ui['fb_marketing_cron_entities_hour'] = $channelConfig['cron_entities_hour'] ?? 1;
        $ui['fb_marketing_cron_entities_minute'] = $channelConfig['cron_entities_minute'] ?? 0;
        $ui['fb_marketing_cron_recent_hour'] = $channelConfig['cron_recent_hour'] ?? 5;
        $ui['fb_marketing_cron_recent_minute'] = $channelConfig['cron_recent_minute'] ?? 0;
        $ui['fb_metrics_strategy'] = $channelConfig['metrics_strategy'] ?? 'default';
        $ui['fb_granular_sync'] = $channelConfig['granular_sync'] ?? false;

        $ui['fb_cache_chunk_size'] = $channelConfig['cache_chunk_size'] ?? '1 week';
        $ui['fb_ad_account_ids'] = [];
        $ui['fb_ad_accounts_full_config'] = $channelConfig['ad_accounts'] ?? [];
        foreach ($ui['fb_ad_accounts_full_config'] as $a) {
            if (!empty($a['enabled'])) {
                $ui['fb_ad_account_ids'][] = (string)$a['id'];
            }
        }

        $features = ['ad_account_metrics', 'campaigns', 'campaign_metrics', 'adsets', 'adset_metrics', 'ads', 'ad_metrics', 'creatives', 'creative_metrics'];
        foreach ($features as $f) {
            $ui['fb_feature_toggles'][$f] = $channelConfig['AD_ACCOUNT'][$f] ?? false;
        }
        $ui['fb_feature_toggles']['cache_aggregations'] = $channelConfig['cache_aggregations'] ?? false;

        $entities = ['CAMPAIGN', 'ADSET', 'AD', 'CREATIVE'];
        foreach ($entities as $e) {
            $ui['fb_entity_filters'][$e] = $channelConfig[$e]['cache_include'] ?? '';
        }

        return $ui;
    }

    /**
     * @inheritdoc
     * @throws Exception
     */
    public function initializeEntities(array $config = []): array
    {
        $assets = $this->fetchAvailableAssets(throwOnError: true);
        
        $initializerClass = '\\Anibalealvarezs\\MetaHubDriver\\Services\\MetaInitializerService';
        if (!class_exists($initializerClass)) {
            throw new Exception("MetaInitializerService not found.");
        }
        
        $initializer = new $initializerClass($this->logger);

        $identityMapper = $config['identityMapper'] ?? null;
        $dataProcessor = $config['dataProcessor'] ?? null;

        if (!$identityMapper || !$dataProcessor) {
            // Fallback for when called without callbacks (legacy or direct)
            return ['initialized' => 0, 'skipped' => 0, 'error' => 'Callbacks missing'];
        }

        return $initializer->initialize(
            $this->getChannel(), 
            $config, 
            ['ad_accounts' => $assets['facebook_ad_accounts'] ?? []],
            $identityMapper,
            $dataProcessor
        );
    }

    /**
     * @inheritdoc
     * @throws Exception
     */
    public function reset(string $mode = 'all', array $config = []): array
    {
        $resetCallback = $config['resetCallback'] ?? null;
        if ($resetCallback instanceof \Closure) {
            return $resetCallback($this->getChannel(), $mode);
        }

        throw new Exception("Reset callback not provided for " . $this->getChannel());
    }

    /**
     * @inheritdoc
     */
    public function getDateFilterMapping(): array
    {
        return [];
    }

    /**
     * @inheritdoc
     */
    public static function getInstanceRules(): array
    {
        return [
            'history_months' => 24,
            'entities_sync' => 'entities',
            'recent_cron_hour' => 5,
            'recent_cron_minute' => 30,
        ];
    }

    /**
     * @inheritdoc
     */
    public static function getEnvMapping(): array
    {
        return [
            'facebook' => [
                'FACEBOOK_APP_ID' => 'app_id',
                'FACEBOOK_APP_SECRET' => 'app_secret',
                'FACEBOOK_USER_ID' => 'user_id',
                'FACEBOOK_REDIRECT_URI' => 'redirect_uri',
                'FACEBOOK_TOKEN_PATH' => 'token_path',
            ]
        ];
    }

}
