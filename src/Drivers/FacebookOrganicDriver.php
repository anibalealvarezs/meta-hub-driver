<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicMetricConvert;
use Anibalealvarezs\ApiDriverCore\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface;
use Anibalealvarezs\ApiDriverCore\Traits\HasUpdatableCredentials;
use Anibalealvarezs\ApiSkeleton\Enums\Period;
use Symfony\Component\HttpFoundation\Response;
use Psr\Log\LoggerInterface;
use DateTime;
use Exception;
use Doctrine\Common\Collections\ArrayCollection;
use Anibalealvarezs\ApiDriverCore\Interfaces\SeederInterface;
use Doctrine\ORM\EntityManagerInterface;
use Anibalealvarezs\MetaHubDriver\Services\MetaInitializerService;

class FacebookOrganicDriver implements SyncDriverInterface
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
            '/fb-organic-reports' => [
                'httpMethod' => 'GET',
                'callable' => fn(...$args) => (new \Anibalealvarezs\MetaHubDriver\Controllers\ReportController())->organic($args),
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
                \Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService::clearChannel('facebook_organic');
            }
        }

        $organicEntities = ['PAGE', 'POST', 'IG_ACCOUNT', 'IG_MEDIA'];
        foreach ($organicEntities as $e) {
            if (isset($entityFilters[$e])) {
                $chanCfg[$e]['cache_include'] = $entityFilters[$e];
            }
        }

        $fbOrganicFeatures = ['page_metrics', 'posts', 'post_metrics', 'ig_accounts', 'ig_account_metrics', 'ig_account_media', 'ig_account_media_metrics'];
        foreach ($fbOrganicFeatures as $f) {
            if (isset($featureToggles[$f])) {
                $chanCfg['PAGE'][$f] = (bool)$featureToggles[$f];
            }
        }

        // Pages management
        $newPagesList = [];
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
                // Granularity Flags
                'page_metrics' => filter_var($pData['page_metrics'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'posts' => filter_var($pData['posts'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'post_metrics' => filter_var($pData['post_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_accounts' => filter_var($pData['ig_accounts'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_account_metrics' => filter_var($pData['ig_account_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_account_media' => filter_var($pData['ig_account_media'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'ig_account_media_metrics' => filter_var($pData['ig_account_media_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
            ];
            
            if (class_exists('\Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService')) {
                $newPagesList[] = \Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService::getEntitySchema('facebook_organic', $item);
            } else {
                $newPagesList[] = $item;
            }
        }
        $chanCfg['pages'] = $newPagesList;

        return $currentConfig;
    }

    /**
     * @inheritdoc
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
                permissions: [], 
                limit: 100, 
                fields: 'id,name,instagram_business_account{id,name,username}'
            );

            $assets = ['facebook_pages' => []];

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
            return $assets;
        } catch (\Exception $e) {
            if ($this->logger) $this->logger->error("FacebookOrganicDriver: Error fetching available assets: " . $e->getMessage());
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

        if ($entity !== 'metrics' && $entity !== 'metric') {
            return $this->syncEntities($entity, $startDate, $endDate, $config, $api);
        }

        $pagesToProcess = $config['pages'] ?? [];
        $chunkSize = $config['cache_chunk_size'] ?? '1 week';
        
        $totalStats = ['metrics' => 0, 'rows' => 0, 'duplicates' => 0];

        foreach ($pagesToProcess as $page) {
            $pageId = (string)($page['id'] ?? '');
            if (!$pageId) continue;

            $api->setPageId($pageId);

            $chunks = \Anibalealvarezs\ApiDriverCore\Helpers\DateHelper::getDateChunks(
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

        $syncService = \Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync::class;

        switch ($entity) {
            case 'pages':
                if (class_exists($syncService)) {
                    return $syncService::syncPages(
                        seeder: $config['seeder'],
                        manager: $config['manager'],
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case 'posts':
                if (class_exists($syncService)) {
                    return $syncService::syncPosts(
                        seeder: $config['seeder'],
                        manager: $config['manager'],
                        api: $api,
                        config: $config,
                        startDate: $startDateStr,
                        endDate: $endDateStr,
                        logger: $this->logger,
                        jobId: $jobId
                    );
                }
                throw new Exception("FacebookEntitySync service not found in host.");
            case 'entities':
                $results = [];
                $results['pages'] = json_decode($this->syncEntities('pages', $startDate, $endDate, $config, $api)->getContent(), true);
                $results['posts'] = json_decode($this->syncEntities('posts', $startDate, $endDate, $config, $api)->getContent(), true);
                return new Response(json_encode(['status' => 'success', 'results' => $results]), 200, ['Content-Type' => 'application/json']);
            default:
                throw new Exception("Entity sync for '{$entity}' not implemented in FacebookOrganicDriver");
        }
    }

    public function getApi(array $config = []): FacebookGraphApi
    {
        if (empty($config) && $this->authProvider instanceof \Anibalealvarezs\ApiDriverCore\Auth\BaseAuthProvider) {
            $config = $this->authProvider->getConfig();
        }
        return $this->initializeApi($config);
    }

    protected function initializeApi(array $config): FacebookGraphApi
    {
        return new FacebookGraphApi(
            userId: $config['user_id'] ?? $config['facebook']['user_id'] ?? 'system',
            appId: $config['app_id'] ?? $config['facebook']['app_id'] ?? '',
            appSecret: $config['app_secret'] ?? $config['facebook']['app_secret'] ?? '',
            redirectUrl: $config['redirect_uri'] ?? $config['facebook']['redirect_uri'] ?? '',
            userAccessToken: $config['access_token'] ?? $config['graph_user_access_token'] ?? $this->authProvider->getAccessToken(),
            apiVersion: $config['api_version'] ?? $config['facebook']['api_version'] ?? 'v18.0'
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
                'ig_account_media_insights' => false,
            ]
        ];
    }

    /**
     * @inheritdoc
     */
    public function validateConfig(array $config): array
    {
        $config = \Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService::hydrate(
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
            $config['pages'] = array_map(function ($page) use ($globalPageDefaults, $globalExclude) {
                $merged = array_merge($globalPageDefaults, $page);
                if (in_array((string)($merged['id'] ?? ''), array_map('strval', $globalExclude))) {
                    $merged['exclude_from_caching'] = true;
                }
                return $merged;
            }, $config['pages'] ?? []);
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

        $em = $seeder->getEntityManager();
        $channelClass = $seeder->getEnumClass('channel');
        $fbChan = $channelClass::facebook_organic;
        
        $igMediaTypes = ['IMAGE', 'VIDEO', 'CAROUSEL_ALBUM', 'REEL'];
        $igProductTypes = ['FEED', 'REELS', 'STORY'];
        $dates = $seeder->getDates(30);

        $accountClass = $seeder->getEntityClass('account');
        $pageClass = $seeder->getEntityClass('page');
        $chanAccountClass = $seeder->getEntityClass('channeled_account');
        $postClass = $seeder->getEntityClass('post');
        $accTypeEnumClass = $seeder->getEnumClass('account_type');
        $convertClass = "\\Anibalealvarezs\\MetaHubDriver\\Conversions\\FacebookOrganicMetricConvert";

        $faker = \Faker\Factory::create('en_US');

        $pagesToSeed = 3;
        $seededPages = [];
        for ($i = 1; $i <= $pagesToSeed; $i++) {
            $name = "Demo Brand $i";
            $fbAcc = $em->getRepository($accountClass)->findOneBy(['name' => "$name (FB)"]);
            if (!$fbAcc) {
                $fbAcc = (new $accountClass())->addName("$name (FB)");
                $em->persist($fbAcc);
            }

            $fbPId = "fb_page_$i";
            $page = $em->getRepository($pageClass)->findOneBy(['platformId' => $fbPId]);
            if (!$page) {
                $page = (new $pageClass())->addPlatformId($fbPId)->addAccount($fbAcc)->addTitle("$name FB Page")->addUrl("https://fb.com/$fbPId")->addCanonicalId($fbPId);
                $em->persist($page);
            }

            $caFb = $em->getRepository($chanAccountClass)->findOneBy(['platformId' => $fbPId]);
            if (!$caFb) {
                $caFb = (new $chanAccountClass())->addPlatformId($fbPId)->addAccount($fbAcc)->addType('facebook_page')->addChannel($fbChan->value)->addName("$name FB Page");
                $em->persist($caFb);
            }

            $igPId = "ig_acc_$i";
            $caIg = $em->getRepository($chanAccountClass)->findOneBy(['platformId' => $igPId, 'channel' => $fbChan->value]);
            if (!$caIg) {
                $caIg = $em->getRepository($chanAccountClass)->findOneBy(['name' => "$name IG Account", 'channel' => $fbChan->value]);
            }
            if (!$caIg) {
                $caIg = (new $chanAccountClass())->addPlatformId($igPId)->addAccount($fbAcc)->addType('instagram')->addChannel($fbChan->value)->addName("$name IG Account");
                $caIg->addData(['instagram_id' => $igPId, 'facebook_page_id' => $fbPId]); 
                $em->persist($caIg);
            } else {
                $caIg->addPlatformId($igPId)->addAccount($fbAcc)->addData(['instagram_id' => $igPId, 'facebook_page_id' => $fbPId]);
            }
            $em->flush();
            $seededPages[] = ['page' => $page, 'fbAcc' => $fbAcc, 'caIg' => $caIg, 'caFb' => $caFb];
        }

        $progress = $config['progress'] ?? null;
        if ($progress) {
            $progress->setMaxSteps(count($seededPages));
            $progress->start();
        }

        $now = date('Y-m-d H:i:s');
        $conn = $em->getConnection();

        foreach ($seededPages as $data) {
            $page = $data['page'];
            $fbParent = $data['fbAcc'];
            $caIg = $data['caIg'];
            $caFb = $data['caFb'];

            $postParams = [];
            $igMediaCount = rand(50, 100);
            for ($m = 0; $m < $igMediaCount; $m++) {
                $mediaPId = 'ig_media_' . $page->getId() . '_' . $m;
                $itemDate = $dates[array_rand($dates)];
                $postParams[] = [
                    'post_id' => $mediaPId,
                    'account_id' => $fbParent->getId(),
                    'page_id' => $page->getId(),
                    'channeled_account_id' => $caIg->getId(),
                    'data' => json_encode([
                        'id' => $mediaPId,
                        'caption' => $faker->sentence(),
                        'media_type' => $igMediaTypes[array_rand($igMediaTypes)],
                        'media_product_type' => $igProductTypes[array_rand($igProductTypes)],
                        'permalink' => "https://www.instagram.com/p/demo_" . $mediaPId,
                        'timestamp' => $itemDate . 'T07:00:00+0000',
                    ]),
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            }

            $fbPostCount = rand(30, 60);
            for ($p = 0; $p < $fbPostCount; $p++) {
                $postPId = 'fb_post_' . $page->getId() . '_' . $p;
                $itemDate = $dates[array_rand($dates)];
                $postParams[] = [
                    'post_id' => $postPId,
                    'account_id' => $fbParent->getId(),
                    'page_id' => $page->getId(),
                    'channeled_account_id' => $caFb->getId(),
                    'data' => json_encode([
                        'id' => $postPId,
                        'message' => $faker->sentence(),
                        'created_time' => $itemDate . 'T07:00:00+0000',
                        'permalink_url' => "https://www.facebook.com/posts/demo_" . $postPId,
                    ]),
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            }

            if (!empty($postParams)) {
                $cols = array_keys($postParams[0]);
                $plat = $conn->getDatabasePlatform();
                $isP = str_contains(strtolower(get_class($plat)), 'postgre');
                $ignore = $isP ? "" : "IGNORE";
                $suffix = $isP ? " ON CONFLICT DO NOTHING" : "";
                $sql = "INSERT $ignore INTO posts (" . implode(', ', $cols) . ") VALUES ";
                $values = [];
                $flatParams = [];
                foreach ($postParams as $row) {
                    $values[] = "(" . implode(',', array_fill(0, count($row), '?')) . ")";
                    foreach ($row as $val) $flatParams[] = $val;
                }
                $sql .= implode(', ', $values) . $suffix;
                $conn->executeStatement($sql, $flatParams);
            }

            $allPosts = $em->getRepository($postClass)->findBy(['page' => $page]);
            $fbPostEntities = [];
            $mediaEntities = [];
            $currentLifetimeValues = [];

            foreach ($allPosts as $pst) {
                if (str_starts_with($pst->getPostId(), 'fb_post_')) {
                    $fbPostEntities[] = $pst;
                } elseif (str_starts_with($pst->getPostId(), 'ig_media_')) {
                    $mediaEntities[] = $pst;
                    $currentLifetimeValues[$pst->getPostId()] = [
                        'reach' => rand(10, 50), 'impressions' => rand(15, 60), 'likes' => 0, 'comments' => 0,
                        'saved' => 0, 'shares' => 0, 'views' => 0, 'total_interactions' => 0,
                    ];
                }
            }

            // FB Simulation
            $gId = $fbParent->getId();
            $gAccName = $fbParent->getName();
            $pId = $page->getId();
            $pageUrl = $page->getUrl();
            $caFbId = $caFb->getId();

            $this->seedDailyMetrics($seeder, $dates, $fbChan, [
                'page_fans' => [0, 10, 'trend'],
                'page_impressions' => [50, 500],
                'page_post_engagements' => [10, 100],
                'page_views_total' => [5, 40],
            ], $gId, $caFbId, null, null, null, $pId, $gAccName, (string)$caFb->getPlatformId(), null, null, null, $pageUrl, null);

            foreach ($fbPostEntities as $fbPostEntity) {
                $this->seedDailyMetrics($seeder, $dates, $fbChan, [
                    'post_impressions' => [10, 100],
                    'post_engagement' => [2, 20],
                    'post_reactions_by_type_total' => [1, 10],
                ], $gId, $caFbId, null, null, $fbPostEntity->getId(), $pId, $gAccName, (string)$caFb->getPlatformId(), null, null, null, $pageUrl, $fbPostEntity->getPostId());
            }

            // IG Simulation
            $allIgMetrics = new ArrayCollection();
            foreach ($dates as $date) {
                $accountPayload = [
                    ['name' => 'reach', 'total_value' => ['value' => rand(100, 500)]],
                    ['name' => 'impressions', 'total_value' => ['value' => rand(150, 600)]],
                    ['name' => 'profile_views', 'total_value' => ['value' => rand(10, 100)]],
                    ['name' => 'website_clicks', 'total_value' => ['value' => rand(0, 5)]],
                    ['name' => 'profile_links_taps', 'total_value' => ['value' => rand(0, 5)]],
                    ['name' => 'follows_and_unfollows', 'total_value' => ['value' => rand(-2, 5)]],
                    ['name' => 'replies', 'total_value' => ['value' => rand(0, 5)]],
                    ['name' => 'accounts_engaged', 'total_value' => ['value' => rand(5, 40)]],
                ];
                $accountMetrics = $convertClass::igAccountMetrics($accountPayload, $date, $page, $fbParent, $caIg);
                foreach ($accountMetrics as $m) {
                    $m->date = new DateTime($date);
                    $m->page = $page;
                    $allIgMetrics->add($m);
                }

                foreach ($mediaEntities as $media) {
                    $mId = (string)$media->getPostId();
                    $currentLifetimeValues[$mId]['reach'] += rand(5, 50);
                    $currentLifetimeValues[$mId]['impressions'] += rand(10, 100);
                    $currentLifetimeValues[$mId]['likes'] += rand(0, 5);
                    $currentLifetimeValues[$mId]['comments'] += rand(0, 2);
                    $currentLifetimeValues[$mId]['saved'] += rand(0, 3);
                    $currentLifetimeValues[$mId]['shares'] += rand(0, 2);
                    $currentLifetimeValues[$mId]['views'] += rand(10, 40);
                    $currentLifetimeValues[$mId]['total_interactions'] = array_sum(array_slice($currentLifetimeValues[$mId], 2, 4));

                    $mediaPayload = [];
                    foreach ($currentLifetimeValues[$mId] as $n => $v) {
                        $mediaPayload[] = ['name' => $n, 'values' => [['value' => $v, 'end_time' => $date . 'T07:00:00+0000']]];
                    }

                    $metrics = $convertClass::igMediaMetrics($mediaPayload, $date, $page, $media, $fbParent, $caIg);
                    foreach ($metrics as $metric) {
                        $metric->post = $media;
                        $metric->page = $page;
                        $metric->account = $fbParent;
                        $metric->channeledAccount = $caIg;
                        $metric->gId = $gId;
                        $allIgMetrics->add($metric);
                    }
                }
            }

            if (!$allIgMetrics->isEmpty()) {
                $seeder->processMetricsMassive($allIgMetrics);
            }

            if ($progress) $progress->advance();
            $em->clear();
        }
        if ($progress) $progress->finish();
    }

    private function seedDailyMetrics($seeder, $dates, $chan, $metricsCfg, $gId, $caId, $gCpId, $cpId, $postId, $pId, $gAccName, $caPId, $gCpPId, $cpPId, $gPostPId, $pageUrl, $postPId): void
    {
        $currentValues = [];
        foreach ($metricsCfg as $name => $cfg) {
            $currentValues[$name] = $cfg[0];
        }

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
        \Anibalealvarezs\ApiDriverCore\Classes\RepositoryRegistry::registerRelations([
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
    /**
     * @inheritdoc
     */
    public function getAssetPatterns(): array
    {
        return [
            'facebook_pages' => [
                'prefix' => 'fb:page',
                'hostnames' => ['facebook.com'],
                'url_id_regex' => '~(\d+)/?$~',
                'type' => 'facebook_page',
                'key' => 'pages',
                'children' => [
                    'instagram_account' => [
                        'id_key' => 'ig_account',
                        'name_key' => 'ig_account_name',
                        'type' => 'instagram'
                    ]
                ]
            ]
        ];
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
            $ui['fb_page_ids'][] = (string)$p['id'];
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
     */
    public function initializeEntities(mixed $entityManager, array $config = []): array
    {
        if (!$entityManager instanceof EntityManagerInterface) {
            throw new Exception("EntityManagerInterface required for FacebookOrganicDriver entity initialization.");
        }

        $assets = $this->fetchAvailableAssets(throwOnError: true);
        $initializer = new MetaInitializerService($entityManager, $this->logger);
        
        return $initializer->initialize($this->getChannel(), $config, ['pages' => $assets['facebook_pages'] ?? []]);
    }

    /**
     * @inheritdoc
     */
    public function reset(mixed $entityManager, string $mode = 'all', array $config = []): array
    {
        if (!$entityManager instanceof EntityManagerInterface) {
            throw new Exception("EntityManagerInterface required for FacebookOrganicDriver reset.");
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
