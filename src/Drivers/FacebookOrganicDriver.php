<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\ApiDriverCore\Auth\BaseAuthProvider;
use Anibalealvarezs\ApiDriverCore\Classes\RepositoryRegistry;
use Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity;
use Anibalealvarezs\ApiDriverCore\Helpers\FieldsNormalizerHelper;
use Anibalealvarezs\ApiDriverCore\Interfaces\ChanneledAccountableInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\PageableInterface;
use Anibalealvarezs\ApiDriverCore\Routes\AssetRoutes;
use Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService;
use Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService;
use Anibalealvarezs\ApiDriverCore\Traits\HasHierarchicalValidationTrait;
use Anibalealvarezs\FacebookGraphApi\Enums\MediaProductType;
use Anibalealvarezs\FacebookGraphApi\Enums\MediaType;
use Anibalealvarezs\FacebookGraphApi\Enums\TokenSample;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
use Anibalealvarezs\MetaHubDriver\Controllers\FacebookAuthController;
use Anibalealvarezs\MetaHubDriver\Controllers\ReportController;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicMetricConvert;
use Anibalealvarezs\ApiDriverCore\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface;
use Anibalealvarezs\ApiDriverCore\Traits\HasUpdatableCredentials;
use Anibalealvarezs\ApiDriverCore\Traits\SyncDriverTrait;
use Anibalealvarezs\ApiSkeleton\Enums\Period;
use Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync;
use GuzzleHttp\Exception\GuzzleException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;
use Anibalealvarezs\ApiDriverCore\Interfaces\SeederInterface;
use Anibalealvarezs\ApiDriverCore\Helpers\DateHelper;
use Anibalealvarezs\ApiDriverCore\Enums\HierarchyType;
use Anibalealvarezs\MetaHubDriver\Enums\MetaFeature;
use Anibalealvarezs\MetaHubDriver\Enums\MetaEntityType;
use Anibalealvarezs\MetaHubDriver\Enums\MetaSyncScope;
use Doctrine\Common\Collections\ArrayCollection;

class FacebookOrganicDriver implements SyncDriverInterface, PageableInterface, ChanneledAccountableInterface
{
    use HasHierarchicalValidationTrait;
    use SyncDriverTrait;
    use HasUpdatableCredentials;

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
        return ['metrics' => 'fb_metrics'];
    }

    /**
     * Get the display label for the channel.
     * 
     * @return string
     */
    public static function getChannelLabel(): string
    {
        return 'FacebookOrganic';
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
                'callable' => fn(...$args) => (new FacebookAuthController())->callback($args['request'] ?? Request::createFromGlobals()),
                'public' => true,
                'admin' => false
            ],
            '/fb-organic-reports' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new ReportController())->organic($args),
                'public' => true,
                'admin' => false,
                'html' => true
            ]
        ]);
    }

    /**
     * @inheritdoc
     */
    public function updateConfiguration(array $newData, array $currentConfig): array
    {
        $selectedAssets = $newData['assets']['pages'] ?? [];
        $enabled = $newData['enabled'] ?? false;
        $historyRange = $newData['cache_history_range'] ?? $newData['organic_history_range'] ?? null;
        $featureToggles = $newData['feature_toggles'] ?? [];
        $entityFilters = $newData['entity_filters'] ?? [];

        if (!isset($currentConfig['channels']['facebook_organic'])) {
            $currentConfig['channels']['facebook_organic'] = [];
        }
        
        $chanCfg = &$currentConfig['channels']['facebook_organic'];

        if ($historyRange) {
            $chanCfg['cache_history_range'] = $historyRange;
        }
        
        // Cron settings
        foreach (['cron_entities_hour', 'cron_entities_minute', 'cron_recent_hour', 'cron_recent_minute'] as $key) {
            if (isset($featureToggles[$key])) {
                $chanCfg[$key] = (int)$featureToggles[$key];
            }
        }
        
        $chanCfg['enabled'] = $enabled;

        // Redis cache toggle
        if (isset($featureToggles['cache_aggregations'])) {
            $prevValue = (bool)($chanCfg['cache_aggregations'] ?? false);
            $newValue = (bool)$featureToggles['cache_aggregations'];
            $chanCfg['cache_aggregations'] = $newValue;
            
            if ($prevValue && !$newValue && class_exists('\Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService')) {
                CacheStrategyService::clearChannel('facebook_organic');
            }
        }

        $organicEntities = ['PAGE', 'POST', 'IG_ACCOUNT', 'IG_MEDIA'];
        foreach ($organicEntities as $e) {
            if (isset($entityFilters[$e])) {
                $chanCfg[$e]['cache_include'] = $entityFilters[$e];
            }
        }

        $fbOrganicFeatures = array_map(fn($f) => $f->value, MetaFeature::organic());
        foreach ($fbOrganicFeatures as $f) {
            if (isset($featureToggles[$f])) {
                $chanCfg['PAGE'][$f] = (bool)$featureToggles[$f];
            }
        }

        // Pages management
        $newPagesList = [];
        $this->logger?->info("DEBUG: updateConfiguration received assets",
            ['count' => count($selectedAssets), 'first_asset' => reset($selectedAssets)]);
        foreach ($selectedAssets as $pData) {
            $pageId = (string)$pData['id'];
            $item = [
                'id' => $pageId,
                'title' => $pData['title'] ?? null,
                'url' => $pData['url'] ?? null,
                'hostname' => $pData['hostname'] ?? null,
                'enabled' => filter_var($pData['enabled'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'exclude_from_caching' => filter_var($pData['exclude_from_caching'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_account' => $pData['ig_account'] ?? null,
                'ig_account_name' => $pData['ig_account_name'] ?? null,
                'ig_hostname' => $pData['ig_hostname'] ?? null,
                'created_time' => $pData['created_time'] ?? null,
                'ig_created_time' => $pData['ig_created_time'] ?? null,
                'data' => $pData['data'] ?? [],
                'ig_data' => $pData['ig_data'] ?? [],
                // Granularity Flags
                'page_metrics' => filter_var($pData['page_metrics'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'posts' => filter_var($pData['posts'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'post_metrics' => filter_var($pData['post_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_accounts' => filter_var($pData['ig_accounts'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_account_metrics' => filter_var($pData['ig_account_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_account_media' => filter_var($pData['ig_account_media'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_account_media_metrics' => filter_var($pData['ig_account_media_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'lost_access' => filter_var($pData['lost_access'] ?? false, FILTER_VALIDATE_BOOLEAN),
            ];
            
            if (class_exists('\Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService')) {
                $newPagesList[] = ConfigSchemaRegistryService::getEntitySchema('facebook_organic', $item);
            } else {
                $newPagesList[] = $item;
            }
        }
        $chanCfg['pages'] = $newPagesList;

        return $currentConfig;
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

            $this->logger?->info("DEBUG: Facebook Organic raw pages response",
                ['data_keys' => !empty($pagesData['data']) ? array_keys(reset($pagesData['data'])) : 'empty']);

            $assets = ['facebook_pages' => []];

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
            return $assets;
        } catch (Exception $e) {
            $this->logger?->error("FacebookOrganicDriver: Error fetching available assets: " . $e->getMessage());
            if ($throwOnError) {
                throw $e;
            }
            return [];
        }
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
        return 'facebook_organic';
    }

    public static function getProviderLabel(): string
    {
        return 'Meta';
    }

    public static function getProviderName(): string
    {
        return 'meta';
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
            throw new Exception("AuthProvider not set for FacebookOrganicDriver");
        }
        
        if (!$this->dataProcessor) {
            throw new Exception("DataProcessor not set for FacebookOrganicDriver.");
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
    public function syncMetrics(
        DateTime $startDate,
        DateTime $endDate,
        array $config = [],
        ?callable $shouldContinue = null,
        ?callable $identityMapper = null
    ): Response {
        $pagesToProcess = array_filter($config['pages'] ?? [], fn($p) => !isset($p['enabled']) || (bool)$p['enabled']);
        $api = $this->initializeApi($config);
        $chunkSize = $config['cache_chunk_size'] ?? '1 week';
        
        // 1. Batch Resolve Identities via Oracle (Facebook Pages & Instagram Accounts)
        $pageMap = [];
        $caMap = [];
        if ($identityMapper && !empty($pagesToProcess)) {
            $fbPIds = [];
            $igPIds = [];
            foreach ($pagesToProcess as $page) {
                if ($pId = (string)($page['id'] ?? $page)) $fbPIds[] = $pId;
                if ($igId = (string)($page['ig_account'] ?? null)) $igPIds[] = $igId;
            }
            $pageMap = $identityMapper('pages', ['platform_ids' => $fbPIds]) ?? [];
            // We resolve BOTH FB and IG accounts under 'channeled_accounts'
            $caMap = $identityMapper('channeled_accounts', ['platform_ids' => array_unique(array_merge($fbPIds, $igPIds))]) ?? [];
        }

        $totalStats = ['metrics' => 0, 'rows' => 0, 'duplicates' => 0];

        foreach ($pagesToProcess as $page) {
            $pagePlatformId = (string)($page['id'] ?? $page);
            $igPlatformId = (string)($page['ig_account'] ?? null);

            $this->logger?->info(">>> INICIO: Sincronizando métricas Orgánicas para FB Page: $pagePlatformId" . ($igPlatformId ? " e Instagram: $igPlatformId" : ""));
            
            // Resolve Internal Identities from pre-loaded maps
            $pageObj = $pageMap[$pagePlatformId] ?? (new UniversalEntity())->setPlatformId($pagePlatformId);
            $caObj = $caMap[$pagePlatformId] ?? (new UniversalEntity())->setPlatformId($pagePlatformId);
            $igCaObj = $igPlatformId ? ($caMap[$igPlatformId] ?? (new UniversalEntity())->setPlatformId($igPlatformId)) : null;

            $pageId = $pageObj->getPlatformId();
            $igCaId = $igCaObj ? $igCaObj->getPlatformId() : null;

            $api->setPageId($pagePlatformId);
            $api->setPageAccesstoken($page['access_token'] ?? $config['access_token'] ?? null);

            try {
                $api->setSampleBasedToken(TokenSample::PAGE);
            } catch (Exception $e) {
                $this->logger?->error("Failed to set page-based token for FB Page $pagePlatformId: " . $e->getMessage());
                continue; // Skip this page
            }
            
            $this->logger?->info("Syncing Organic metrics from {$startDate->format('Y-m-d')} to {$endDate->format('Y-m-d')}");

            $isRecent = $endDate->getTimestamp() >= (new \DateTime('yesterday'))->getTimestamp();

            $chunks = DateHelper::getDateChunks($startDate->format('Y-m-d'), $endDate->format('Y-m-d'), $chunkSize);
            foreach ($chunks as $idx => $chunk) {
                if ($shouldContinue && !$shouldContinue()) {
                    throw new Exception("Sync aborted by the orchestrator.");
                }
                $pageData = $this->fetchPageData(
                    $api, 
                    $page, 
                    $chunk['start'], 
                    $chunk['end'], 
                    $config, 
                    $shouldContinue, 
                    $identityMapper, 
                    $pageObj,
                    $igCaObj ?? $igCaId,
                    ($idx === 0 && $isRecent) // Only include lifetime metrics on the first chunk of a recent sync
                );

                $collection = new ArrayCollection();

                // 1. Process Page Insights
                if (!empty($pageData['insights'])) {
                    $pageCollection = FacebookOrganicMetricConvert::pageMetrics(
                        rows: $pageData['insights'],
                        pagePlatformId: (string)$pageId,
                        logger: $this->logger,
                        page: $pageObj,
                        channeledAccount: $caObj ?? $pagePlatformId,
                        account: ($caObj && method_exists($caObj, 'getAccount')) ? $caObj->getAccount() : ($config['accounts_group_name'] ?? 'Default')
                    );
                    foreach ($pageCollection as $m) $collection->add($m);
                }

                // 2. Process IG Account Insights
                if (!empty($pageData['ig_insights'])) {
                    foreach ($pageData['ig_insights'] as $insight) {
                        $igCollection = FacebookOrganicMetricConvert::igAccountMetrics(
                            rows: $insight['data'],
                            date: $chunk['start'],
                            page: $pageObj,
                            account: ($caObj && method_exists($caObj, 'getAccount')) ? $caObj->getAccount() : ($config['accounts_group_name'] ?? 'Default'),
                            channeledAccount: $igCaObj ?? $igPlatformId,
                            logger: $this->logger,
                            period: Period::Daily
                        );
                        foreach ($igCollection as $m) $collection->add($m);
                    }
                }

                // 3. Process Facebook Post Insights
                if (!empty($pageData['fb_post_insights'])) {
                    foreach ($pageData['fb_post_insights'] as $postInsight) {
                        $postCollection = FacebookOrganicMetricConvert::pageMetrics(
                            rows: $postInsight['data'],
                            postPlatformId: $postInsight['id'],
                            logger: $this->logger,
                            page: $pageObj,
                            post: $postInsight['instance'] ?? $postInsight['id'],
                            period: 'lifetime',
                            channeledAccount: $caObj ?? $pagePlatformId,
                            account: ($caObj && method_exists($caObj, 'getAccount')) ? $caObj->getAccount() : ($config['accounts_group_name'] ?? 'Default')
                        );
                        foreach ($postCollection as $m) $collection->add($m);
                    }
                }

                // 4. Process IG Media Insights
                if (!empty($pageData['ig_media_insights'])) {
                    foreach ($pageData['ig_media_insights'] as $mediaInsight) {
                        $igMediaCollection = FacebookOrganicMetricConvert::igMediaMetrics(
                            rows: $mediaInsight['data'],
                            date: date('Y-m-d'), // Lifetime metrics must be stamped with 'today'
                            page: $pageObj,
                            post: $mediaInsight['instance'] ?? $mediaInsight['id'],
                            account: ($caObj && method_exists($caObj, 'getAccount')) ? $caObj->getAccount() : ($config['accounts_group_name'] ?? 'Default'),
                            channeledAccount: $igCaObj ?? $igPlatformId,
                            logger: $this->logger
                        );
                        foreach ($igMediaCollection as $m) $collection->add($m);
                    }
                }

                // Persist converted collection 
                if ($this->dataProcessor && $collection->count() > 0) {
                    $this->validateHierarchicalIntegrity(collection: $collection, type: HierarchyType::POST);

                    $result = ($this->dataProcessor)($collection, $this->logger);
                    
                    $metricsCount = $result['metrics'] ?? $collection->count();
                    $rowsCount = $result['rows'] ?? 0;
                    $duplicatesCount = $result['duplicates'] ?? 0;

                    $totalStats['metrics'] += $metricsCount;
                    $totalStats['rows'] += $rowsCount;
                    $totalStats['duplicates'] += $duplicatesCount;

                    $this->logger?->info("<<< EXITO: Sincronización completada para FB Page: $pageId. Métricas: $metricsCount | Filas base: $rowsCount | Duplicados: $duplicatesCount");
                }
            }
        }

        return new Response(json_encode(['status' => 'success', 'data' => $totalStats]));
    }

    /**
     * @throws Exception
     */
    private function syncEntities(
        MetaSyncScope|MetaEntityType $entity,
        DateTime $startDate,
        DateTime $endDate,
        array $config = [],
        ?FacebookGraphApi $api = null,
        ?callable $shouldContinue = null,
        ?callable $identityMapper = null
    ): Response {
        $startDateStr = $startDate->format('Y-m-d');
        $endDateStr = $endDate->format('Y-m-d');
        $jobId = $config['jobId'] ?? null;

        $syncService = FacebookEntitySync::class;

        $pagesToProcess = array_filter($config['pages'] ?? [], fn($p) => !isset($p['enabled']) || (bool)$p['enabled']);
        $resolvedPages = [];
        $resolvedChanneledAccounts = [];
        
        if ($identityMapper && !empty($pagesToProcess)) {
            $fbPIds = [];
            $igPIds = [];
            foreach ($pagesToProcess as $page) {
                if ($pId = (string)($page['id'] ?? $page)) $fbPIds[] = $pId;
                if ($igId = (string)($page['ig_account'] ?? null)) $igPIds[] = $igId;
            }
            $resolvedPages = array_values($identityMapper('pages', ['platform_ids' => $fbPIds]) ?? []);
            $resolvedChanneledAccounts = $identityMapper('channeled_accounts', ['platform_ids' => array_unique(array_merge($fbPIds, $igPIds))]) ?? [];
        }

        switch ($entity) {
            case MetaEntityType::PAGE:
                if (class_exists($syncService)) {
                    // Discovery for marketing accounts
                    $adAccounts = [];
                    if ($identityMapper && !empty($config['ad_accounts'])) {
                         $aIds = array_map(fn($a) => (string)($a['id'] ?? $a), $config['ad_accounts']);
                         $adAccounts = array_values($identityMapper('channeled_accounts', ['platform_ids' => $aIds]) ?? []);
                    }
                    return $syncService::syncPages(
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        channeledAccounts: $adAccounts,
                        entityProcessor: $this->dataProcessor,
                        jobStatusChecker: $shouldContinue
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case MetaEntityType::POST:
                if (class_exists($syncService)) {
                    return $syncService::syncPosts(
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        channeledPages: $resolvedPages,
                        entityProcessor: $this->dataProcessor,
                        channeledAccountId: $resolvedChanneledAccounts,
                        accountId: (is_object($this->authProvider) && method_exists($this->authProvider, 'getAccount') && $this->authProvider->getAccount()) ? $this->authProvider->getAccount()->getId() : null,
                        jobStatusChecker: $shouldContinue
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case MetaEntityType::IG_MEDIA:
                if (class_exists($syncService)) {
                    // Filter resolved accounts to only include IG ones from config
                    $igPIds = array_filter(array_map(fn($p) => (string)($p['ig_account'] ?? null), $pagesToProcess));
                    $igAccounts = array_filter($resolvedChanneledAccounts, fn($ca, $pId) => in_array((string)$pId, $igPIds), ARRAY_FILTER_USE_BOTH);
                    
                    return $syncService::syncInstagramMedia(
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId,
                        channeledAccounts: array_values($igAccounts),
                        entityProcessor: $this->dataProcessor,
                        channeledPages: $resolvedPages,
                        jobStatusChecker: $shouldContinue
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case MetaSyncScope::ENTITIES:
                $results = [];
                $pageCfg = $config['PAGE'] ?? [];

                // 1. Pages
                $results['pages'] = json_decode($this->syncEntities(MetaEntityType::PAGE, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper)->getContent(), true);

                // 2. Posts
                if (!empty($pageCfg[MetaFeature::POSTS->value])) {
                    $results['posts'] = json_decode($this->syncEntities(MetaEntityType::POST, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper)->getContent(), true);
                }

                // 3. Instagram
                if (!empty($pageCfg[MetaFeature::IG_ACCOUNT_MEDIA->value])) {
                    $results['instagram'] = json_decode($this->syncEntities(MetaEntityType::IG_MEDIA, $startDate, $endDate, $config, $api, $shouldContinue, $identityMapper)->getContent(), true);
                }

                return new Response(json_encode(['status' => 'success', 'results' => $results]), 200, ['Content-Type' => 'application/json']);
            default:
                throw new Exception("Entity sync for '{$entity->value}' not implemented in FacebookOrganicDriver");
        }
    }

    public function getApi(array $config = []): FacebookGraphApi
    {
        if (empty($config) && $this->authProvider instanceof BaseAuthProvider) {
            $config = $this->authProvider->getConfig();
        }
        return $this->initializeApi($config);
    }

    /**
     * @throws Exception
     */
    protected function initializeApi(array $config): FacebookGraphApi
    {
        $this->logger?->info("DEBUG: FacebookOrganicDriver::initializeApi - START");
        return new FacebookGraphApi(
            userId: $config['user_id'] ?? $config['facebook']['user_id'] ?? 'system',
            appId: $config['app_id'] ?? $config['facebook']['app_id'] ?? '',
            appSecret: $config['app_secret'] ?? $config['facebook']['app_secret'] ?? '',
            redirectUrl: $config['redirect_uri'] ?? $config['facebook']['redirect_uri'] ?? '',
            userAccessToken: $config['access_token'] ?? $config['graph_user_access_token'] ?? $this->authProvider->getAccessToken(),
            apiVersion: $config['api_version'] ?? $config['facebook']['api_version'] ?? 'v18.0',
            logger: $this->logger
        );
    }

    /**
     * @throws GuzzleException
     * @throws Exception
     */
    private function fetchPageData(
        FacebookGraphApi $api,
        array $page,
        string $start,
        string $end,
        array $config,
        ?callable $shouldContinue = null,
        ?callable $identityMapper = null,
        $internalPageId = null,
        $igCaId = null,
        bool $includeLifetime = true
    ): array {
        $pagePlatformId = (string)($page['id'] ?? $page);
        
        $data = [
            'insights' => [],
            'ig_media' => [],
            'fb_posts' => [],
            'ig_insights' => [],
            'ig_media_insights' => [],
            'fb_post_insights' => [],
        ];

        // 1. Page Insights
        if ($page[MetaFeature::PAGE_METRICS->value] ?? false) {
            if ($shouldContinue && !$shouldContinue()) {
                throw new Exception("Sync aborted by the orchestrator.");
            }
            $metricSet = isset($config['metric_set']) ? (MetricSet::tryFrom($config['metric_set']) ?: MetricSet::BASIC) : MetricSet::BASIC;
            $customMetrics = $config['metrics'] ?? [];
            if (!is_array($customMetrics)) $customMetrics = explode(',', (string)$customMetrics);

            $data['insights'] = $api->getFacebookPageInsights(
                pageId: $pagePlatformId,
                since: $start,
                until: $end,
                metricSet: $metricSet,
                customMetrics: $customMetrics
            );
        }

        // 2. Instagram Account Insights
        if (!empty($page['ig_account']) && !empty($page[MetaFeature::IG_ACCOUNT_METRICS->value])) {
            if ($shouldContinue && !$shouldContinue()) {
                throw new Exception("Sync aborted by the orchestrator.");
            }
            $igTwoYearsAgo = (new DateTime())->modify('-2 years + 1 day')->format('Y-m-d');
            $igSince = max($start, $igTwoYearsAgo);

            foreach ([1, 2, 3, 4, 5] as $option) {
                if ($shouldContinue && !$shouldContinue()) {
                    throw new Exception("Sync aborted by the orchestrator.");
                }
                try {
                    $insights = $api->getDailyInstagramAccountTotalValueInsights(
                        instagramAccountId: (string)$page['ig_account'],
                        since: $igSince,
                        option: $option
                    );
                    if (!empty($insights['data'])) {
                        $data['ig_insights'][] = ['option' => $option, 'data' => $insights['data']];
                    }
                } catch (Exception $e) {
                    $this->logger?->warning("IG Insight option $option failed: " . $e->getMessage());
                }
            }
        }

        // 3. Facebook Post Insights (Dynamic Discovery via Mapper)
        if ($includeLifetime && ($page[MetaFeature::POST_METRICS->value] ?? false) && $identityMapper && $internalPageId) {
            $postsMap = $identityMapper('posts', ['page_id' => $internalPageId]);
            if ($postsMap) {
                foreach ($postsMap as $postId => $postInfo) {
                    if ($shouldContinue && !$shouldContinue()) {
                        throw new Exception("Sync aborted by the orchestrator.");
                    }
                    try {
                        $metricSet = isset($config['metric_set']) ? (MetricSet::tryFrom($config['metric_set']) ?: MetricSet::BASIC) : MetricSet::BASIC;
                        $customMetrics = $config['metrics'] ?? [];
                        if (!is_array($customMetrics)) $customMetrics = explode(',', (string)$customMetrics);

                        $postInsights = $api->getFacebookPostInsights(
                            postId: (string)$postId,
                            metricSet: $metricSet,
                            customMetrics: $customMetrics
                        );
                        if (!empty($postInsights['data'])) {
                            $data['fb_post_insights'][] = [
                                'id' => $postId,
                                'instance' => $postInfo,
                                'data' => $postInsights['data']
                            ];
                        }
                    } catch (Exception $e) {
                        $this->logger?->warning("Failed to fetch insights for Post $postId: " . $e->getMessage());
                    }
                }
            }
        }

        // 4. Instagram Media Insights (Dynamic Discovery via Mapper)
        if ($includeLifetime && !empty($page['ig_account']) && !empty($page[MetaFeature::IG_ACCOUNT_MEDIA_METRICS->value]) && $identityMapper && $igCaId) {
            // Instagram media are often synced under ChanneledAccount context
            $mediaMap = $identityMapper('posts', ['channeled_account_id' => $igCaId]);
            if ($mediaMap) {
                foreach ($mediaMap as $mediaId => $mediaInfo) {
                    if ($shouldContinue && !$shouldContinue()) {
                        throw new Exception("Sync aborted by the orchestrator.");
                    }
                    try {
                        // Resolve IG media type/product from Hub data
                        $rawInfo = is_object($mediaInfo) && method_exists($mediaInfo, 'getData') ? $mediaInfo->getData() : ($mediaInfo['data'] ?? []);
                        $mType = $rawInfo['media_type'] ?? null;
                        $pType = $rawInfo['media_product_type'] ?? null;

                        // Prioritize MediaProductType for more accurate metrics (Story/Reel/Feed)
                        $type = MediaProductType::tryFrom(strtoupper((string)$pType))
                                ?? MediaType::tryFrom(strtoupper((string)$mType))
                                ?? MediaType::CAROUSEL_ALBUM;
                        
                        $metricSet = isset($config['metric_set']) ? (MetricSet::tryFrom($config['metric_set']) ?: MetricSet::BASIC) : MetricSet::BASIC;
                        $customMetrics = $config['metrics'] ?? [];
                        if (!is_array($customMetrics)) $customMetrics = explode(',', (string)$customMetrics);

                        $mediaInsights = $api->getInstagramMediaInsights(
                            mediaId: (string)$mediaId, 
                            mediaType: $type,
                            metricSet: $metricSet,
                            customMetrics: $customMetrics
                        );
                        if (!empty($mediaInsights['data'])) {
                            $data['ig_media_insights'][] = [
                                'id' => $mediaId,
                                'instance' => $mediaInfo,
                                'data' => $mediaInsights['data']
                            ];
                        }
                    } catch (Exception $e) {
                        $this->logger?->warning("Failed to fetch insights for IG Media $mediaId: " . $e->getMessage());
                    }
                }
            }
        }

        return $data;
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
            ],
            'entity' => [
                'id' => '',
                'url' => '',
                'title' => '',
                'hostname' => '',
                'enabled' => true,
                'exclude_from_caching' => false,
                'ig_account' => null,
                'ig_account_name' => null,
                'ig_accounts' => false,
                'page_metrics' => true,
                'posts' => true,
                'post_metrics' => false,
                'ig_account_metrics' => false,
                'ig_account_media' => false,
                'ig_account_media_metrics' => false,
                'lost_access' => false,
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

        // 1. Explicit environment variable mappings (Agnostic version of Helpers' logic)
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

        // 2. --- 🛡️ SMART STORAGE AUTH MAPPING --- (Moved from Helpers)
        $tokenPath = $_ENV['FACEBOOK_TOKEN_PATH'] ?? $config['graph_token_path'] ?? './storage/tokens/facebook_tokens.json';
        if (is_string($tokenPath) && str_starts_with($tokenPath, './')) {
            $tokenPath = getcwd() . substr($tokenPath, 1);
        }
        
        if (file_exists($tokenPath)) {
            $tokens = json_decode(file_get_contents($tokenPath), true);
            $tokenData = $tokens['facebook_auth'] ?? $tokens['facebook_organic'] ?? [];
            $organicToken = $tokenData['access_token'] ?? null;
            $organicUserId = $tokenData['user_id'] ?? null;
            
            if ($organicToken) {
                $config['graph_user_access_token'] = $organicToken;
                $config['access_token'] = $organicToken;
            }
            if ($organicUserId) {
                $config['user_id'] = $organicUserId;
            }
        }

        // 3. Pages preparation
        $globalExclude = $config['exclude_from_caching'] ?? [];
        if (!is_array($globalExclude)) {
            $globalExclude = [$globalExclude];
        }

        if (isset($config['PAGE'])) {
            $globalPageDefaults = $config['PAGE'];
            $newPages = [];
            foreach ($config['pages'] ?? [] as $page) {
                $merged = array_merge($globalPageDefaults, $page);
                $id = (string)($merged['id'] ?? '');
                if (in_array($id, array_map('strval', $globalExclude))) {
                    $merged['exclude_from_caching'] = true;
                }
                if ($id) {
                    $newPages[$id] = $merged;
                } else {
                    $newPages[] = $merged;
                }
            }
            $config['pages'] = $newPages;
        }

        // 4. Handle Sync Window (History Range)
        if (empty($config['startDate']) && empty($config['start_date']) && !empty($config['cache_history_range'])) {
            $config['startDate'] = date('Y-m-d', strtotime('-' . $config['cache_history_range']));
        }

        return $config;
    }

    /**
     * @inheritdoc
     */
    public function seedDemoData(SeederInterface $seeder, array $config = []): void
    {
        $output = $config['output'] ?? null;
        if ($output) $output->writeln("🚀 Seeding Facebook Organic Realistic Demo Data...");

        $gscChan = \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_organic;
        
        $igMediaTypes = ['IMAGE', 'VIDEO', 'CAROUSEL_ALBUM', 'REEL'];
        $igProductTypes = ['FEED', 'REELS', 'STORY'];
        $dates = $seeder->getDates(30);

        $faker = \Faker\Factory::create('en_US');

        $pagesToSeed = 3;
        $seededPages = [];
        for ($i = 1; $i <= $pagesToSeed; $i++) {
            $name = "Demo Brand $i";
            
            $fbAcc = $seeder->resolveEntity('account', ['name' => "$name (FB)"]);

            $fbPId = "fb_page_$i";
            $page = $seeder->resolveEntity('page', [
                'platformId' => $fbPId,
                'account' => $fbAcc,
                'title' => "$name FB Page",
                'url' => "https://fb.com/$fbPId",
                'canonicalId' => $fbPId
            ]);

            $caFb = $seeder->resolveEntity('channeled_account', [
                'platformId' => $fbPId,
                'account' => $fbAcc,
                'type' => 'facebook_page',
                'channel' => $gscChan->value,
                'name' => "$name FB Page"
            ]);

            $igPId = "ig_acc_$i";
            $caIg = $seeder->resolveEntity('channeled_account', [
                'platformId' => $igPId,
                'account' => $fbAcc,
                'type' => 'instagram',
                'channel' => $gscChan->value,
                'name' => "$name IG Account",
                'data' => ['instagram_id' => $igPId, 'facebook_page_id' => $fbPId]
            ]);

            $seededPages[] = ['page' => $page, 'fbAcc' => $fbAcc, 'caIg' => $caIg, 'caFb' => $caFb];
        }

        $progress = $config['progress'] ?? null;
        if ($progress) {
            $progress->setMaxSteps(count($seededPages));
            $progress->start();
        }

        $now = date('Y-m-d H:i:s');

        foreach ($seededPages as $data) {
            $page = $data['page'];
            $fbParent = $data['fbAcc'];
            $caIg = $data['caIg'];
            $caFb = $data['caFb'];

            $postPIds = [];
            $igMediaCount = rand(50, 100);
            for ($m = 0; $m < $igMediaCount; $m++) {
                $mediaPId = 'ig_media_' . $page->getPlatformId() . '_' . $m;
                $itemDate = $dates[array_rand($dates)];
                $postPIds[] = $mediaPId;
                
                $seeder->queueMetric(
                    channel: \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_organic,
                    name: 'post_engagement',
                    date: $itemDate,
                    value: 0,
                    pageId: $page->id,
                    caId: $caIg->id,
                    gAccId: $fbParent->id,
                    postPId: $mediaPId,
                    data: json_encode([
                        'id' => $mediaPId,
                        'caption' => $faker->sentence(),
                        'media_type' => $igMediaTypes[array_rand($igMediaTypes)],
                        'media_product_type' => $igProductTypes[array_rand($igProductTypes)],
                        'timestamp' => $itemDate . 'T12:00:00+0000',
                        'permalink' => "https://www.instagram.com/p/demo_" . $mediaPId,
                    ])
                );
            }

            $fbPostCount = rand(30, 60);
            for ($p = 0; $p < $fbPostCount; $p++) {
                $postPId = 'fb_post_' . $page->getPlatformId() . '_' . $p;
                $itemDate = $dates[array_rand($dates)];
                $postPIds[] = $postPId;

                $seeder->queueMetric(
                    channel: \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_organic,
                    name: 'post_impressions',
                    date: $itemDate,
                    value: 0,
                    pageId: $page->id,
                    caId: $caFb->id,
                    gAccId: $fbParent->id,
                    postPId: $postPId,
                    data: json_encode([
                        'id' => $postPId,
                        'message' => $faker->sentence(),
                        'created_time' => $itemDate . 'T07:00:00+0000',
                        'permalink_url' => "https://www.facebook.com/posts/demo_" . $postPId,
                    ])
                );
            }

            // FB Simulation
            $gId = $fbParent->id;
            $gAccName = $fbParent->getTitle();
            $pId = $page->id;
            $pageUrl = $page->getUrl();
            $caFbId = $caFb->id;

            $this->seedDailyMetrics(
                seeder: $seeder, 
                dates: $dates, 
                chan: \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_organic,
                metricsCfg: [
                    'page_fans' => [0, 10, 'trend'],
                    'page_impressions' => [50, 500],
                    'page_post_engagements' => [10, 100],
                    'page_views_total' => [5, 40],
                ], 
                gId: $gId, 
                caId: $caFbId, 
                gCpId: null, 
                cpId: null, 
                postId: null, 
                pId: $pId, 
                gAccName: $gAccName, 
                caPId: (string)$caFb->getPlatformId(), 
                gCpPId: null, 
                cpPId: null, 
                pageUrl: $pageUrl, 
                postPId: null
            );

            foreach ($postPIds as $pstId) {
                if (str_starts_with($pstId, 'fb_post_')) {
                    $this->seedDailyMetrics(
                        seeder: $seeder, 
                        dates: $dates, 
                        chan: \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_organic,
                        metricsCfg: [
                            'post_impressions' => [10, 100],
                            'post_engagement' => [2, 20],
                            'post_reactions_by_type_total' => [1, 10],
                        ], 
                        gId: $gId, 
                        caId: $caFbId, 
                        gCpId: null, 
                        cpId: null, 
                        postId: null, 
                        pId: $pId, 
                        gAccName: $gAccName, 
                        caPId: (string)$caFb->getPlatformId(), 
                        gCpPId: null, 
                        cpPId: null, 
                        pageUrl: $pageUrl, 
                        postPId: $pstId
                    );
                } else {
                    $this->seedDailyMetrics(
                        seeder: $seeder, 
                        dates: $dates, 
                        chan: \Anibalealvarezs\ApiSkeleton\Enums\Channel::facebook_organic,
                        metricsCfg: [
                            'reach' => [10, 50],
                            'impressions' => [15, 60],
                            'likes' => [1, 10],
                            'comments' => [0, 5],
                        ], 
                        gId: $gId, 
                        caId: $caIg->id, 
                        gCpId: null, 
                        cpId: null, 
                        postId: null, 
                        pId: $page->id, 
                        gAccName: $gAccName, 
                        caPId: (string)$caIg->getPlatformId(), 
                        gCpPId: null, 
                        cpPId: null, 
                        pageUrl: $pageUrl, 
                        postPId: $pstId
                    );
                }
            }

            if ($progress) $progress->advance();
        }
        if ($progress) $progress->finish();
    }

    private function seedDailyMetrics($seeder, $dates, $chan, $metricsCfg, $gId, $caId, $gCpId, $cpId, $postId, $pId, $gAccName, $caPId, $gCpPId, $cpPId, $pageUrl, $postPId): void
    {
        $currentValues = array_map(function ($cfg) {
            return $cfg[0];
        }, $metricsCfg);

        foreach ($dates as $date) {
            $payload = [];
            foreach ($metricsCfg as $name => $cfg) {
                $inc = rand($cfg[0], $cfg[1]);
                if (isset($cfg[2]) && $cfg[2] === 'trend') {
                    $currentValues[$name] += $inc;
                    $val = $currentValues[$name];
                } else {
                    $val = $inc;
                }
                $payload[$name] = $val;

                $seeder->queueMetric(
                    channel: $chan,
                    name: $name,
                    date: $date,
                    value: $val,
                    setId: 0,
                    setHash: 'none',
                    caId: $caId,
                    gAccId: $gId,
                    gCpId: $gCpId,
                    cpId: $cpId,
                    postId: $postId,
                    pageId: $pId,
                    accName: $gAccName,
                    caPId: $caPId,
                    gCpPId: $gCpPId,
                    cpPId: $cpPId,
                    pageUrl: $pageUrl,
                    postPId: $postPId,
                    data: json_encode($payload)
                );
            }
        }
    }

    public function boot(): void
    {
        RepositoryRegistry::registerRelations([
            'post'              => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'post_id', 'alias' => 'rpo'],
            'post_id'           => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'post_id', 'alias' => 'rpo_id'],
            'permalink_url'     => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'data', 'alias' => 'rpo_pu', 'isJSON' => true, 'jsonPath' => 'permalink_url', 'isAttribute' => true],
            'permalink'         => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'data', 'alias' => 'rpo_pl', 'isJSON' => true, 'jsonPath' => 'permalink', 'isAttribute' => true],
            'timestamp'         => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'data', 'alias' => 'rpo_ts', 'isJSON' => true, 'jsonPath' => 'timestamp', 'isAttribute' => true],
            'created_time'      => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'data', 'alias' => 'rpo_ct', 'isJSON' => true, 'jsonPath' => 'created_time', 'isAttribute' => true],
            'media_type'        => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'data', 'alias' => 'rpo_mt', 'isJSON' => true, 'jsonPath' => 'media_type', 'isAttribute' => true],
            'message'           => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'data', 'alias' => 'rpo_msg', 'isJSON' => true, 'jsonPath' => 'message', 'isAttribute' => true],
            'caption'           => ['table' => 'posts', 'fk' => 'post_id', 'field' => 'data', 'alias' => 'rpo_cap', 'isJSON' => true, 'jsonPath' => 'caption', 'isAttribute' => true],
            'linked_fb_page'    => ['table' => 'channeled_accounts', 'fk' => 'channeled_account_id', 'field' => 'data', 'alias' => 'lfp', 'isJSON' => true, 'jsonPath' => 'facebook_page_id', 'isAttribute' => true],
            'linked_fb_page_id' => ['table' => 'channeled_accounts', 'fk' => 'channeled_account_id', 'field' => 'data', 'alias' => 'lfp', 'isJSON' => true, 'jsonPath' => 'facebook_page_id', 'isAttribute' => true],
        ]);
    }

    /**
     * @inheritdoc
     */
    public function getAssetPatterns(): array
    {
        return [
            'facebook_page' => [
                'key' => 'pages',
                'channeled_account' => [
                    'platform_id' => [
                        'type' => 'raw',
                        'key' => 'id'
                    ],
                    'platform_created_at_key' => 'created_time',
                    'name_key' => 'title',
                    'type' => MetaEntityType::PAGE->value,
                    'data_key' => 'data'
                ],
                'page' => [
                    'canonical_id' => [
                        'prefix' => 'fb',
                        'field' => 'platformId'
                    ],
                    'platform_id' => [
                        'type' => 'raw',
                        'key' => 'id'
                    ],
                    'title_key' => 'title',
                    'url' => [
                        'type' => 'custom',
                        'prefix' => 'https://facebook.com/',
                        'key' => 'id'
                    ],
                    'hostname_key' => 'hostname',
                    'data_key' => 'data'
                ]
            ],
            'instagram_account' => [
                'key' => 'pages',
                'channeled_account' => [
                    'platform_id' => [
                        'type' => 'raw',
                        'key' => 'ig_account'
                    ],
                    'platform_created_at_key' => 'ig_created_time',
                    'name_key' => 'ig_account_name',
                    'type' => MetaEntityType::INSTAGRAM_ACCOUNT->value,
                    'data_key' => 'ig_data'
                ],
                'page' => [
                    'canonical_id' => [
                        'prefix' => 'ig',
                        'field' => 'platformId'
                    ],
                    'platform_id' => [
                        'type' => 'raw',
                        'key' => 'ig_account'
                    ],
                    'title_key' => 'ig_account_name',
                    'url' => [
                        'type' => 'custom',
                        'prefix' => 'https://instagram.com/',
                        'key' => 'ig_account'
                    ],
                    'hostname_key' => 'ig_hostname',
                    'data_key' => 'ig_data'
                ]
            ]

        ];
    }

    public static function getPages(array $asset): array {
        return [
            // FB Page
            [
                'platformId' => self::getPagePlatformId(asset: $asset),
                'canonicalId' => self::getPageCanonicalId(asset: $asset),
                'hostname' => self::getPageHostname(asset: $asset),
                'title' => self::getPageTitle(asset: $asset),
                'url' => self::getPageUrl(asset: $asset),
                'data' => self::getPageData(asset: $asset)
            ],
            [
                'platformId' => self::getPagePlatformId(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'canonicalId' => self::getPageCanonicalId(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'hostname' => self::getPageHostname(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'title' => self::getPageTitle(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'url' => self::getPageUrl(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'data' => self::getPageData(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT)
            ]
            // IG Account
        ];
    }

    public static function getChanneledAccounts(array $asset): array {
        return [
            // FB Page
            [
                'platformId' => self::getChanneledAccountPlatformId(asset: $asset),
                'platformCreatedAt' => self::getChanneledAccountPlatformCreatedAt(asset: $asset),
                'name' => self::getChanneledAccountName(asset: $asset),
                'type' => self::getChanneledAccountType(),
                'data' => self::getChanneledAccountData(asset: $asset)
            ],
            [
                'platformId' => self::getChanneledAccountPlatformId(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'platformCreatedAt' => self::getChanneledAccountPlatformCreatedAt(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'name' => self::getChanneledAccountName(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'type' => self::getChanneledAccountType(entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                'data' => self::getChanneledAccountData(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT)
            ]
            // IG Account
        ];
    }

    // PAGE FIELDS

    public static function getPagePlatformId(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        $platformIdKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_account',
            default => 'id'
        };
        return FieldsNormalizerHelper::getCleanString($asset[$platformIdKey]);
    }

    public static function getPageCanonicalId(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        return match(self::getChanneledAccountType($entityType)){
                'instagram_account' => 'ig:',
                    default => 'fb:'
            }.self::getPagePlatformId($asset);
    }

    public static function getPageHostname(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        $hostnameKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_hostname',
            default => 'hostname'
        };
        return isset($asset[$hostnameKey]) && $asset[$hostnameKey] ? FieldsNormalizerHelper::getCleanString($asset[$hostnameKey]) : '';
    }

    public static function getPageTitle(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        $titleKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_account_name',
            default => 'title'
        };
        return FieldsNormalizerHelper::getCleanString($asset[$titleKey]);
    }

    public static function getPageUrl(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        return match(self::getChanneledAccountType($entityType)){
                'instagram_account' => 'https://instagram.com/',
                default => 'https://facebook.com/:'
            }.self::getPagePlatformId($asset);
    }

    public static function getPageData(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): array {
        $dataKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_data',
            default => 'data'
        };
        return FieldsNormalizerHelper::getCleanArray($asset[$dataKey]);
    }

    // CHANNELED ACCOUNT FIELDS

    public static function getChanneledAccountPlatformId(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        $platformIdKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_account',
            default => 'id'
        };
        return FieldsNormalizerHelper::getCleanString($asset[$platformIdKey]);
    }

    public static function getChanneledAccountPlatformCreatedAt(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        $platformCreatedAtKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_created_time',
            default => 'created_time'
        };
        return isset($asset[$platformCreatedAtKey]) && $asset[$platformCreatedAtKey] ? FieldsNormalizerHelper::getCleanString($asset[$platformCreatedAtKey]) : '';
    }

    public static function getChanneledAccountName(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        $nameKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_account_name',
            default => 'title'
        };
        return FieldsNormalizerHelper::getCleanString($asset[$nameKey]);
    }

    public static function getChanneledAccountType(string|MetaEntityType $entityType = MetaEntityType::PAGE): string {
        return $entityType instanceof MetaEntityType ? $entityType->value : $entityType;
    }

    public static function getChanneledAccountData(array $asset, string|MetaEntityType $entityType = MetaEntityType::PAGE): array {
        $dataKey = match(self::getChanneledAccountType($entityType)){
            'instagram_account' => 'ig_data',
            default => 'data'
        };
        return FieldsNormalizerHelper::getCleanArray($asset[$dataKey]);
    }

    /**
     * @inheritdoc
     */
    public static function getPageTypes(): array
    {
        return [
            'facebook_page' => 'Facebook Page',
            'instagram' => 'Instagram Account'
        ];
    }

    /**
     * @inheritdoc
     */
    public static function getAccountTypes(): array
    {
        return [
            'facebook_page' => 'Facebook Page',
            'instagram' => 'Instagram Account'
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
        $ui['fb_cache_chunk_size'] = $channelConfig['cache_chunk_size'] ?? '1 week';
        $ui['fb_organic_enabled'] = $channelConfig['enabled'] ?? false;
        $ui['fb_organic_history_range'] = $channelConfig['cache_history_range'] ?? '2 years';
        $ui['fb_organic_cron_entities_hour'] = $channelConfig['cron_entities_hour'] ?? 2;
        $ui['fb_organic_cron_entities_minute'] = $channelConfig['cron_entities_minute'] ?? 0;
        $ui['fb_organic_cron_recent_hour'] = $channelConfig['cron_recent_hour'] ?? 6;
        $ui['fb_organic_cron_recent_minute'] = $channelConfig['cron_recent_minute'] ?? 0;

        $ui['fb_page_ids'] = [];
        foreach (($channelConfig['pages'] ?? []) as $p) {
            if (!empty($p['enabled'])) {
                $ui['fb_page_ids'][] = (string)$p['id'];
            }
        }
        $ui['fb_pages_full_config'] = $channelConfig['pages'] ?? [];

        $features = ['page_metrics', 'posts', 'post_metrics', 'ig_accounts', 'ig_account_metrics', 'ig_account_media', 'ig_account_media_metrics'];
        foreach ($features as $f) {
            $ui['fb_feature_toggles'][$f] = $channelConfig['PAGE'][$f] ?? false;
        }
        $ui['fb_feature_toggles']['cache_aggregations'] = $channelConfig['cache_aggregations'] ?? false;
        
        $entities = ['PAGE', 'POST', 'IG_ACCOUNT', 'IG_MEDIA'];
        foreach ($entities as $e) {
            $ui['fb_entity_filters'][$e] = $channelConfig[$e]['cache_include'] ?? '';
        }

        return $ui;
    }

    /**
     * @inheritdoc
     * @throws Exception|GuzzleException
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
            ['pages' => $assets['facebook_pages'] ?? []],
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
            'recent_cron_hour' => 6,
            'recent_cron_minute' => 30,
        ];
    }
}
