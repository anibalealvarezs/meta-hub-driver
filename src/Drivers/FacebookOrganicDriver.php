<?php

    namespace Anibalealvarezs\MetaHubDriver\Drivers;

    use Anibalealvarezs\ApiDriverCore\Auth\BaseAuthProvider;
    use Anibalealvarezs\ApiDriverCore\Classes\AggregationProfileTemplates;
    use Anibalealvarezs\ApiDriverCore\Classes\RepositoryRegistry;
    use Anibalealvarezs\ApiDriverCore\Classes\MetricProfileTemplates;
    use Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity;
    use Anibalealvarezs\ApiDriverCore\Enums\AssetCategory;
    use Anibalealvarezs\ApiDriverCore\Helpers\FieldsNormalizerHelper;
    use Anibalealvarezs\ApiDriverCore\Interfaces\ChanneledAccountableInterface;
    use Anibalealvarezs\ApiDriverCore\Interfaces\AggregationProfileProviderInterface;
    use Anibalealvarezs\ApiDriverCore\Interfaces\CanonicalMetricDictionaryProviderInterface;
    use Anibalealvarezs\ApiDriverCore\Interfaces\MetricProfileProviderInterface;
    use Anibalealvarezs\ApiDriverCore\Interfaces\PageableInterface;
    use Anibalealvarezs\ApiDriverCore\Routes\AssetRoutes;
    use Anibalealvarezs\ApiDriverCore\Services\CacheStrategyService;
    use Anibalealvarezs\ApiDriverCore\Services\ConfigSchemaRegistryService;
    use Anibalealvarezs\ApiDriverCore\Traits\HasHierarchicalValidationTrait;
    use Anibalealvarezs\ApiSkeleton\Enums\Channel;
    use Anibalealvarezs\FacebookGraphApi\Enums\MediaProductType;
    use Anibalealvarezs\FacebookGraphApi\Enums\MediaType;
    use Anibalealvarezs\FacebookGraphApi\Enums\TokenSample;
    use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
    use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
    use Anibalealvarezs\FacebookGraphApi\Enums\FacebookPostPermission;
    use Anibalealvarezs\FacebookGraphApi\Support\FacebookInsightMetricGuard;
    use Anibalealvarezs\MetaHubDriver\Controllers\FacebookAuthController;
    use Anibalealvarezs\MetaHubDriver\Controllers\ReportController;
    use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicMetricConvert;
    use Anibalealvarezs\ApiDriverCore\Interfaces\SyncDriverInterface;
    use Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface;
    use Anibalealvarezs\ApiDriverCore\Traits\SyncDriverTrait;
    use Anibalealvarezs\ApiSkeleton\Enums\Period;
    use Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync;
    use Faker\Factory;
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

    class FacebookOrganicDriver implements SyncDriverInterface, PageableInterface, ChanneledAccountableInterface, MetricProfileProviderInterface, AggregationProfileProviderInterface, CanonicalMetricDictionaryProviderInterface
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
                MetricProfileTemplates::pageTotals(
                    channel: 'facebook_organic',
                    key: 'facebook_organic_page',
                    label: 'Facebook Organic Page'
                ),
                MetricProfileTemplates::pagePostBreakdown(
                    channel: 'facebook_organic',
                    key: 'facebook_organic_post',
                    label: 'Facebook Organic Post'
                ),
            ];
        }

        public static function getAggregationProfiles(): array
        {
            return [
                AggregationProfileTemplates::organicPageFlowProfile(
                    channel: 'facebook_organic',
                    key: 'facebook_organic_page_flow',
                    label: 'Facebook Organic Page Flow',
                    overrides: [
                        'asset_type' => 'page',
                        'group_patterns' => [
                            ['page', 'page_id', 'page_title'],
                            ['daily'],
                        ],
                        'filter_contract' => [
                            'channel' => ['eq'],
                            'account_type' => ['eq'],
                            'page_platform_id' => ['eq', 'in'],
                            'metricDate' => ['between', '>=', '<='],
                        ],
                        'reducer_strategies' => [
                            '*' => 'sum',
                        ],
                    ]
                ),
                AggregationProfileTemplates::organicPostMixedProfile(
                    channel: 'facebook_organic',
                    key: 'facebook_organic_post_snapshot',
                    label: 'Facebook Organic Post Snapshot',
                    overrides: [
                        'asset_type' => 'post',
                        'group_patterns' => [
                            ['caption', 'created_time', 'media_type', 'message', 'permalink', 'permalink_url', 'post', 'post_id', 'timestamp'],
                            ['post'],
                        ],
                        'filter_contract' => [
                            'channel' => ['eq'],
                            'account_type' => ['eq'],
                            'channeledAccount' => ['eq', 'in'],
                            'post' => ['eq', 'is_not_null'],
                            'period' => ['eq'],
                            'latest_snapshot' => ['eq'],
                            'metricDate' => ['between', '>=', '<='],
                        ],
                        'reducer_strategies' => [
                            '*' => 'latest_snapshot',
                        ],
                    ]
                ),
                AggregationProfileTemplates::organicPageFlowProfile(
                    channel: 'facebook_organic',
                    key: 'facebook_organic_linked_pages_flow',
                    label: 'Facebook Organic Linked Pages Flow',
                    overrides: [
                        'asset_type' => 'account',
                        'group_patterns' => [
                            ['channeledAccount', 'channeled_account_id', 'page_platform_id', 'linked_fb_page_id'],
                            ['channeledAccount', 'channeled_account_id', 'page_platform_id', 'linked_platform_entity_id'],
                        ],
                        'filter_contract' => [
                            'channel' => ['eq'],
                            'account_type' => ['eq'],
                            'channeledAccount' => ['eq', 'in'],
                            'metricDate' => ['between', '>=', '<='],
                        ],
                    ]
                ),

            ];
        }

        public static function getCanonicalMetricDictionary(): array
        {
            return [
                'likes' => ['likes', 'likes_daily', 'post_reactions_by_type_total', 'post_reactions_by_type_total_daily'],
                'comments' => ['comments', 'comments_daily', 'post_comments', 'post_comments_daily'],
                'reach' => ['reach', 'reach_daily', 'post_reach', 'post_reach_daily'],
                'views' => ['plays', 'plays_daily', 'video_views', 'video_views_daily', 'views', 'views_daily', 'post_video_views', 'post_video_views_daily', 'page_video_views', 'page_video_views_daily'],
                'profile_views' => ['profile_views', 'profile_views_daily'],
                'website_clicks' => ['website_clicks', 'website_clicks_daily'],
                'profile_links_taps' => ['profile_links_taps', 'profile_links_taps_daily'],
                'follows_and_unfollows' => ['follows_and_unfollows', 'follows_and_unfollows_daily'],
                'saves' => ['saves', 'saves_daily', 'saved', 'saved_daily'],
                'shares' => ['shares', 'shares_daily', 'post_shares', 'post_shares_daily'],
                'total_interactions' => ['total_interactions', 'total_interactions_daily', 'post_engagement', 'post_engagement_daily', 'post_engagements', 'post_engagements_daily', 'page_post_engagements', 'page_post_engagements_daily'],
                'replies' => ['replies', 'replies_daily'],
                'accounts_engaged' => ['accounts_engaged', 'accounts_engaged_daily'],
                'post_clicks' => ['post_clicks', 'post_clicks_daily'],
                'ig_reels_avg_watch_time' => ['ig_reels_avg_watch_time'],
                'ig_reels_video_view_total_time' => ['ig_reels_video_view_total_time'],
                'profile_activity' => ['profile_activity', 'profile_activity_daily'],
                'profile_visits' => ['profile_visits', 'profile_visits_daily'],
                'reposts' => ['reposts', 'reposts_daily'],
                'follows' => ['follows', 'follows_daily'],
            ];
        }

        public static function getPlatformEntityIdField(): string
        {
            return 'facebook_page_id';
        }

        /**
         * Store credentials for this driver.
         *
         * @param array $credentials
         * @return void
         */
        public static function storeCredentials(array $credentials): void
        {
            $tokenPath = $_ENV['FACEBOOK_TOKEN_PATH'] ?? getcwd().'/storage/tokens/facebook_tokens.json';
            $tokenKey = 'facebook_auth';

            if (!is_dir(dirname($tokenPath))) {
                mkdir(dirname($tokenPath), 0755, true);
            }

            $tokens = file_exists($tokenPath) ? (json_decode(file_get_contents($tokenPath), true) ?? []) : [];

            $tokens[$tokenKey] = [
                'access_token'  => $credentials['access_token'] ?? null,
                'refresh_token' => $credentials['refresh_token'] ?? null,
                'user_id'       => $credentials['user_id'] ?? null,
                'scopes'        => $credentials['scopes'] ?? [],
                'updated_at'    => date('Y-m-d H:i:s'),
                'expires_at'    => date('Y-m-d H:i:s', strtotime('+60 days'))
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
                '/fb-login'           => [
                    'httpMethod' => 'GET',
                    'callable'   => fn(...$args) => (new FacebookAuthController())->login(),
                    'public'     => true,
                    'admin'      => false
                ],
                '/fb-auth-start'      => [
                    'httpMethod' => 'GET',
                    'callable'   => fn(...$args) => (new FacebookAuthController())->start(),
                    'public'     => true,
                    'admin'      => false
                ],
                '/fb-callback'        => [
                    'httpMethod' => 'GET',
                    'callable'   => fn(...$args) => (new FacebookAuthController())->callback($args['request'] ?? Request::createFromGlobals()),
                    'public'     => true,
                    'admin'      => false
                ],
                '/fb-organic-reports' => [
                    'httpMethod' => 'GET',
                    'callable'   => fn(...$args) => (new ReportController())->organic($args),
                    'public'     => true,
                    'admin'      => false,
                    'html'       => true
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
            if (isset($newData['granular_sync'])) {
                $chanCfg['granular_sync'] = filter_var($newData['granular_sync'], FILTER_VALIDATE_BOOLEAN);
            }

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
                    'id'                       => $pageId,
                    'title'                    => $pData['title'] ?? null,
                    'url'                      => $pData['url'] ?? null,
                    'hostname'                 => $pData['hostname'] ?? null,
                    'link'                     => $pData['link'] ?? null,
                    'enabled'                  => filter_var($pData['enabled'] ?? false, FILTER_VALIDATE_BOOLEAN),
                    'exclude_from_caching'     => filter_var($pData['exclude_from_caching'] ?? false, FILTER_VALIDATE_BOOLEAN),
                    'ig_account'               => $pData['ig_account'] ?? null,
                    'ig_account_name'          => $pData['ig_account_name'] ?? null,
                    'ig_hostname'              => $pData['ig_hostname'] ?? null,
                    'created_time'             => $pData['created_time'] ?? null,
                    'ig_created_time'          => $pData['ig_created_time'] ?? null,
                    'data'                     => $pData['data'] ?? [],
                    'ig_data'                  => $pData['ig_data'] ?? [],
                    // Granularity Flags
                    'page_metrics'             => filter_var($pData['page_metrics'] ?? true, FILTER_VALIDATE_BOOLEAN),
                    'posts'                    => filter_var($pData['posts'] ?? true, FILTER_VALIDATE_BOOLEAN),
                    'post_metrics'             => filter_var($pData['post_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                    'ig_accounts'              => filter_var($pData['ig_accounts'] ?? false, FILTER_VALIDATE_BOOLEAN),
                    'ig_account_metrics'       => filter_var($pData['ig_account_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                    'ig_account_media'         => filter_var($pData['ig_account_media'] ?? false, FILTER_VALIDATE_BOOLEAN),
                    'ig_account_media_metrics' => filter_var($pData['ig_account_media_metrics'] ?? false, FILTER_VALIDATE_BOOLEAN),
                    'lost_access'              => filter_var($pData['lost_access'] ?? false, FILTER_VALIDATE_BOOLEAN),
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
                    fields: 'id,name,link,website,created_time,instagram_business_account{id,name,username,website}'
                );

                $this->logger?->info("DEBUG: Facebook Organic raw pages response",
                    ['data_keys' => !empty($pagesData['data']) ? array_keys(reset($pagesData['data'])) : 'empty']);

                $assets = ['pages' => []];

                if (!empty($pagesData['data'])) {
                    foreach ($pagesData['data'] as $page) {
                        $assets['pages'][] = [
                            'id'              => $page['id'],
                            'title'           => $page['name'],
                            'hostname'        => $page['website'] ?? null,
                            'url'             => $page['link'],
                            'link'            => $page['link'],
                            'created_time'    => $page['created_time'] ?? null,
                            'data'            => $page,
                            'ig_account'      => $page['instagram_business_account']['id'] ?? null,
                            'ig_account_name' => $page['instagram_business_account']['username'] ?? $page['instagram_business_account']['name'] ?? null,
                            'ig_hostname'     => $page['instagram_business_account']['website'] ?? null,
                            'ig_data'         => $page['instagram_business_account'] ?? null,
                        ];
                    }
                }

                return $assets;
            } catch (Exception $e) {
                $this->logger?->error("FacebookOrganicDriver: Error fetching available assets: ".$e->getMessage());
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
            DateTime  $startDate,
            DateTime  $endDate,
            array     $config = [],
            ?callable $shouldContinue = null,
            ?callable $identityMapper = null
        ): Response
        {
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
            DateTime  $startDate,
            DateTime  $endDate,
            array     $config = [],
            ?callable $shouldContinue = null,
            ?callable $identityMapper = null
        ): Response
        {
            $pagesToProcess = array_filter($config['pages'] ?? [], fn($p) => !isset($p['enabled']) || (bool)$p['enabled']);
            $api = $this->initializeApi($config);
            $chunkSize = $config['cache_chunk_size'] ?? '1 week';
            $targetAccountId = $config['account_id'] ?? $config['params']['account_id'] ?? null;

            // 1. Batch Resolve Identities via Oracle (Facebook Pages & Instagram Accounts)
            $pageMap = [];
            $caMap = [];
            if ($identityMapper && !empty($pagesToProcess)) {
                $fbPIds = [];
                $igPIds = [];
                foreach ($pagesToProcess as $page) {
                    // Use formal platform ID calculation
                    $pId = self::getPlatformId($page, AssetCategory::IDENTITY, MetaEntityType::PAGE->value);
                    $igId = isset($page['ig_account']) ? self::getPlatformId(['id' => $page['ig_account']], AssetCategory::IDENTITY, MetaEntityType::INSTAGRAM_ACCOUNT->value) : null;

                    if ($targetAccountId && $targetAccountId !== $pId && $targetAccountId !== $igId) {
                        continue;
                    }

                    if ($pId) $fbPIds[] = $pId;
                    if ($igId) $igPIds[] = $igId;
                }
                $pageMap = $identityMapper('pages', ['platform_ids' => array_unique(array_merge($fbPIds, $igPIds))]) ?? [];
                // We resolve BOTH FB and IG accounts under 'channeled_accounts'
                $caMap = $identityMapper('channeled_accounts', ['platform_ids' => array_unique(array_merge($fbPIds, $igPIds))]) ?? [];
            }

            $totalStats = ['metrics' => 0, 'rows' => 0, 'duplicates' => 0];

            foreach ($pagesToProcess as $page) {
                // Use formal platform ID calculation (same as Scheduler)
                $pagePlatformId = self::getPlatformId($page, AssetCategory::IDENTITY, MetaEntityType::PAGE->value);
                $igPlatformId = isset($page['ig_account']) ? self::getPlatformId(['id' => $page['ig_account']], AssetCategory::IDENTITY, MetaEntityType::INSTAGRAM_ACCOUNT->value) : null;

                if ($targetAccountId && $targetAccountId !== $pagePlatformId && $targetAccountId !== $igPlatformId) {
                    continue;
                }

                $this->logger?->info(">>> INICIO: Sincronizando métricas Orgánicas para FB Page: $pagePlatformId".($igPlatformId ? " e Instagram: $igPlatformId" : ""));

                // Resolve Internal Identities from pre-loaded maps
                $pageObj = $pageMap[$pagePlatformId] ?? (new UniversalEntity())->setPlatformId($pagePlatformId);
                $igPageObj = $igPlatformId ? ($pageMap[$igPlatformId] ?? (new UniversalEntity())->setPlatformId($igPlatformId)) : null;
                $caObj = $caMap[$pagePlatformId] ?? (new UniversalEntity())->setPlatformId($pagePlatformId);
                $igCaObj = $igPlatformId ? ($caMap[$igPlatformId] ?? (new UniversalEntity())->setPlatformId($igPlatformId)) : null;

                $pageId = $pageObj->getPlatformId();
                $igCaId = $igCaObj ? $igCaObj->getPlatformId() : null;

                $api->setPageId($pagePlatformId);
                $api->setPageAccesstoken($page['access_token'] ?? $config['access_token'] ?? null);

                try {
                    $api->setSampleBasedToken(TokenSample::PAGE);
                } catch (Exception $e) {
                    $this->logger?->error("Failed to set page-based token for FB Page $pagePlatformId: ".$e->getMessage());
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
                                page: $igPageObj ?? $pageObj,
                                account: ($igCaObj && method_exists($igCaObj, 'getAccount')) ? $igCaObj->getAccount() : (($caObj && method_exists($caObj, 'getAccount')) ? $caObj->getAccount() : ($config['accounts_group_name'] ?? 'Default')),
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
                                page: $igPageObj ?? $pageObj,
                                post: $mediaInsight['instance'] ?? $mediaInsight['id'],
                                account: ($igCaObj && method_exists($igCaObj, 'getAccount')) ? $igCaObj->getAccount() : (($caObj && method_exists($caObj, 'getAccount')) ? $caObj->getAccount() : ($config['accounts_group_name'] ?? 'Default')),
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
            DateTime                     $startDate,
            DateTime                     $endDate,
            array                        $config = [],
            ?FacebookGraphApi            $api = null,
            ?callable                    $shouldContinue = null,
            ?callable                    $identityMapper = null
        ): Response
        {
            $api = $api ?? $this->initializeApi($config);
            $startDateStr = $startDate->format('Y-m-d');
            $endDateStr = $endDate->format('Y-m-d');
            $jobId = $config['jobId'] ?? null;

            $syncService = FacebookEntitySync::class;

            $targetAccountId = $config['account_id'] ?? $config['params']['account_id'] ?? null;
            $pagesToProcess = array_filter($config['pages'] ?? [], fn($p) => !isset($p['enabled']) || (bool)$p['enabled']);
            $resolvedPages = [];
            $resolvedChanneledAccounts = [];

            if ($identityMapper && !empty($pagesToProcess)) {
                $fbPIds = [];
                $igPIds = [];
                foreach ($pagesToProcess as $page) {
                    $pId = self::getPlatformId($page, AssetCategory::IDENTITY, MetaEntityType::PAGE->value);
                    $igId = isset($page['ig_account']) ? self::getPlatformId(['id' => $page['ig_account']], AssetCategory::IDENTITY, MetaEntityType::INSTAGRAM_ACCOUNT->value) : null;

                    if ($targetAccountId && $targetAccountId !== $pId && $targetAccountId !== $igId) {
                        continue;
                    }

                    if ($pId) $fbPIds[] = $pId;
                    if ($igId) $igPIds[] = $igId;
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
                            accountId: $this->authProvider?->getUserId(),
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
            array            $page,
            string           $start,
            string           $end,
            array            $config,
            ?callable        $shouldContinue = null,
            ?callable        $identityMapper = null,
                             $internalPageId = null,
                             $igCaId = null,
            bool             $includeLifetime = true
        ): array
        {
            $pagePlatformId = (string)($page['id'] ?? $page);

            $data = [
                'insights'          => [],
                'ig_media'          => [],
                'fb_posts'          => [],
                'ig_insights'       => [],
                'ig_media_insights' => [],
                'fb_post_insights'  => [],
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
                        $this->logger?->warning("IG Insight option $option failed: ".$e->getMessage());
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
                            $rawPostInfo = is_object($postInfo) && method_exists($postInfo, 'getData')
                                ? (array)$postInfo->getData()
                                : (array)($postInfo['data'] ?? []);

                            $requestedMetrics = !empty($customMetrics)
                                ? array_values(array_filter(array_map('trim', $customMetrics)))
                                : array_values(array_filter(array_map(
                                    'trim',
                                    explode(',', FacebookPostPermission::DEFAULT->insightsFields($metricSet))
                                )));

                            $filteredMetrics = $this->filterFacebookPostMetricsForRawData(
                                metrics: $requestedMetrics,
                                postData: $rawPostInfo,
                                postId: (string)$postId,
                            );
                            if ($filteredMetrics === []) {
                                continue;
                            }

                            $postInsights = $api->getFacebookPostInsights(
                                postId: (string)$postId,
                                metricSet: MetricSet::CUSTOM,
                                customMetrics: $filteredMetrics,
                            );
                            if (!empty($postInsights['data'])) {
                                $data['fb_post_insights'][] = [
                                    'id'       => $postId,
                                    'instance' => $postInfo,
                                    'data'     => $postInsights['data']
                                ];
                            }
                        } catch (Exception $e) {
                            $this->logger?->warning("Failed to fetch insights for Post $postId: ".$e->getMessage());
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
                                customMetrics: $customMetrics,
                                mediaData: (array)$rawInfo,
                            );
                            if (!empty($mediaInsights['data'])) {
                                $data['ig_media_insights'][] = [
                                    'id'       => $mediaId,
                                    'instance' => $mediaInfo,
                                    'data'     => $mediaInsights['data']
                                ];
                            }
                        } catch (Exception $e) {
                            $this->logger?->warning("Failed to fetch insights for IG Media $mediaId: ".$e->getMessage());
                        }
                    }
                }
            }

            return $data;
        }

        /**
         * @param array<int, string> $metrics
         * @param array<string, mixed> $postData
         * @return array<int, string>
         */
        private function filterFacebookPostMetricsForRawData(array $metrics, array $postData, string $postId): array
        {
            $unsupported = FacebookInsightMetricGuard::resolveUnsupportedMetrics($postData);
            if ($unsupported === []) {
                return array_values(array_unique(array_filter(array_map('trim', $metrics))));
            }

            $mediaType = strtoupper(trim((string)($postData['media_type'] ?? $postData['type'] ?? 'unknown')));
            $mediaProductType = strtoupper(trim((string)($postData['media_product_type'] ?? 'unknown')));
            $filtered = [];
            foreach ($metrics as $metric) {
                $metric = trim((string)$metric);
                if ($metric === '') {
                    continue;
                }
                if (in_array($metric, $unsupported, true)) {
                    $this->logger?->info("FB API: Metric '$metric' prefiltered for Post $postId (media_type={$mediaType}, media_product_type={$mediaProductType}).");
                    continue;
                }
                $filtered[] = $metric;
            }

            return array_values(array_unique($filtered));
        }

        /**
         * @inheritdoc
         */
        public function getConfigSchema(): array
        {
            return [
                'global' => [
                    'enabled'             => false,
                    'cache_history_range' => '2 years',
                    'cache_aggregations'  => false,
                ],
                'entity' => [
                    'id'                       => '',
                    'url'                      => '',
                    'title'                    => '',
                    'hostname'                 => '',
                    'link'                     => '',
                    'enabled'                  => true,
                    'exclude_from_caching'     => false,
                    'ig_account'               => null,
                    'ig_account_name'          => null,
                    'ig_accounts'              => false,
                    'page_metrics'             => true,
                    'posts'                    => true,
                    'post_metrics'             => false,
                    'ig_account_metrics'       => false,
                    'ig_account_media'         => false,
                    'ig_account_media_metrics' => false,
                    'lost_access'              => false,
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
                'FACEBOOK_APP_ID'         => 'app_id',
                'FACEBOOK_APP_SECRET'     => 'app_secret',
                'FACEBOOK_REDIRECT_URI'   => 'app_redirect_uri',
                'FACEBOOK_USER_TOKEN'     => 'graph_user_access_token',
                'FACEBOOK_PAGE_TOKEN'     => 'graph_page_access_token',
                'FACEBOOK_TOKEN_PATH'     => 'graph_token_path',
                'FACEBOOK_USER_ID'        => 'user_id',
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
                $tokenPath = getcwd().substr($tokenPath, 1);
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
                $config['startDate'] = date('Y-m-d', strtotime('-'.$config['cache_history_range']));
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

            $gscChan = Channel::facebook_organic;

            $igMediaTypes = ['IMAGE', 'VIDEO', 'CAROUSEL_ALBUM', 'REEL'];
            $igProductTypes = ['FEED', 'REELS', 'STORY'];
            $dates = $seeder->getDates(30);

            $faker = Factory::create('en_US');

            $pagesToSeed = 3;
            $seededPages = [];
            for ($i = 1; $i <= $pagesToSeed; $i++) {
                $name = "Demo Brand $i";

                $fbAcc = $seeder->resolveEntity('account', ['name' => "$name (FB)"]);

                $fbPId = "fb_page_$i";
                $page = $seeder->resolveEntity('page', [
                    'platformId'  => $fbPId,
                    'account'     => $fbAcc,
                    'title'       => "$name FB Page",
                    'url'         => "https://fb.com/$fbPId",
                    'canonicalId' => $fbPId
                ]);

                $caFb = $seeder->resolveEntity('channeled_account', [
                    'platformId' => $fbPId,
                    'account'    => $fbAcc,
                    'type'       => MetaEntityType::PAGE->value,
                    'channel'    => $gscChan->value,
                    'name'       => "$name FB Page"
                ]);

                $igPId = "ig_acc_$i";
                $caIg = $seeder->resolveEntity('channeled_account', [
                    'platformId' => $igPId,
                    'account'    => $fbAcc,
                    'type'       => MetaEntityType::INSTAGRAM_ACCOUNT->value,
                    'channel'    => $gscChan->value,
                    'name'       => "$name IG Account",
                    'data'       => ['instagram_id' => $igPId, 'facebook_page_id' => $fbPId]
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
                    $mediaPId = 'ig_media_'.$page->getPlatformId().'_'.$m;
                    $itemDate = $dates[array_rand($dates)];
                    $postPIds[] = $mediaPId;

                    $seeder->queueMetric(
                        channel: Channel::facebook_organic,
                        name: 'post_engagement',
                        date: $itemDate,
                        value: 0,
                        pageId: $page->id,
                        caId: $caIg->id,
                        gAccId: $fbParent->id,
                        postPId: $mediaPId,
                        data: json_encode([
                            'id'                 => $mediaPId,
                            'caption'            => $faker->sentence(),
                            'media_type'         => $igMediaTypes[array_rand($igMediaTypes)],
                            'media_product_type' => $igProductTypes[array_rand($igProductTypes)],
                            'timestamp'          => $itemDate.'T12:00:00+0000',
                            'permalink'          => "https://www.instagram.com/p/demo_".$mediaPId,
                        ])
                    );
                }

                $fbPostCount = rand(30, 60);
                for ($p = 0; $p < $fbPostCount; $p++) {
                    $postPId = 'fb_post_'.$page->getPlatformId().'_'.$p;
                    $itemDate = $dates[array_rand($dates)];
                    $postPIds[] = $postPId;

                    $seeder->queueMetric(
                        channel: Channel::facebook_organic,
                        name: 'post_impressions',
                        date: $itemDate,
                        value: 0,
                        pageId: $page->id,
                        caId: $caFb->id,
                        gAccId: $fbParent->id,
                        postPId: $postPId,
                        data: json_encode([
                            'id'            => $postPId,
                            'message'       => $faker->sentence(),
                            'created_time'  => $itemDate.'T07:00:00+0000',
                            'permalink_url' => "https://www.facebook.com/posts/demo_".$postPId,
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
                    chan: Channel::facebook_organic,
                    metricsCfg: [
                        'page_fans'             => [0, 10, 'trend'],
                        'page_impressions'      => [50, 500],
                        'page_post_engagements' => [10, 100],
                        'page_views_total'      => [5, 40],
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
                            chan: Channel::facebook_organic,
                            metricsCfg: [
                                'post_impressions'             => [10, 100],
                                'post_engagement'              => [2, 20],
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
                            chan: Channel::facebook_organic,
                            metricsCfg: [
                                'reach'       => [10, 50],
                                'impressions' => [15, 60],
                                'likes'       => [1, 10],
                                'comments'    => [0, 5],
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
                'linked_fb_page'    => ['table' => 'channeled_accounts', 'fk' => 'channeled_account_id', 'field' => 'data', 'alias' => 'rca', 'isJSON' => true, 'jsonPath' => 'facebook_page_id', 'isAttribute' => true],
                'linked_fb_page_id' => ['table' => 'channeled_accounts', 'fk' => 'channeled_account_id', 'field' => 'data', 'alias' => 'rca', 'isJSON' => true, 'jsonPath' => 'facebook_page_id', 'isAttribute' => true],
            ]);
        }

        /**
         * @inheritdoc
         */
        public static function getAssetPatterns(): array
        {
            return [
                MetaEntityType::PAGE->value => [
                    'category'          => [AssetCategory::IDENTITY, AssetCategory::PAGEABLE],
                    'key'               => 'pages',
                    'channeled_account' => [
                        'platform_id'             => [
                            'type' => 'raw',
                            'key'  => 'id'
                        ],
                        'platform_created_at_key' => 'created_time',
                        'name_key'                => 'title',
                        'type'                    => MetaEntityType::PAGE->value,
                        'data_key'                => 'data'
                    ],
                    'page'              => [
                        'canonical_id' => [
                            'prefix' => 'fb',
                            'field'  => 'platformId'
                        ],
                        'platform_id'  => [
                            'type' => 'raw',
                            'key'  => 'id'
                        ],
                        'title_key'    => 'title',
                        'url'          => [
                            'type'   => 'custom',
                            'prefix' => 'https://facebook.com/',
                            'key'    => 'id'
                        ],
                        'hostname_key' => 'hostname',
                        'data_key'     => 'data'
                    ]
                ],
                MetaEntityType::INSTAGRAM_ACCOUNT->value => [
                    'category'          => [AssetCategory::IDENTITY, AssetCategory::PAGEABLE],
                    'key'               => 'pages',
                    'channeled_account' => [
                        'platform_id'             => [
                            'type' => 'raw',
                            'key'  => 'ig_account'
                        ],
                        'platform_created_at_key' => 'ig_created_time',
                        'name_key'                => 'ig_account_name',
                        'type'                    => MetaEntityType::INSTAGRAM_ACCOUNT->value,
                        'data_key'                => 'ig_data'
                    ],
                    'page'              => [
                        'canonical_id' => [
                            'prefix' => 'ig',
                            'field'  => 'platformId'
                        ],
                        'platform_id'  => [
                            'type' => 'raw',
                            'key'  => 'ig_account'
                        ],
                        'title_key'    => 'ig_account_name',
                        'url'          => [
                            'type'   => 'custom',
                            'prefix' => 'https://instagram.com/',
                            'key'    => 'ig_account'
                        ],
                        'hostname_key' => 'ig_hostname',
                        'data_key'     => 'ig_data'
                    ]
                ]

            ];
        }

        public static function getPages(array $asset): array
        {
            $list = [];
            $fbPageId = self::getPlatformId($asset, AssetCategory::PAGEABLE, MetaEntityType::PAGE->value);
            if (!empty($fbPageId)) {
                $fbPage = [
                    'platformId'  => $fbPageId,
                    'canonicalId' => self::getCanonicalId($asset, AssetCategory::PAGEABLE, MetaEntityType::PAGE->value),
                    'hostname'    => self::getPageHostname(asset: $asset),
                    'title'       => self::getPageTitle(asset: $asset),
                    'url'         => self::getPageUrl(asset: $asset),
                    'enabled'     => $asset['enabled'] ?? true,
                    'data'        => self::getPageData(asset: $asset)
                ];
                $list[] = $fbPage;
            }
            $igPlatformId = self::getPlatformId($asset, AssetCategory::PAGEABLE, MetaEntityType::INSTAGRAM_ACCOUNT->value);
            if ($igPlatformId) {
                $igAccount = [
                    'platformId'  => $igPlatformId,
                    'canonicalId' => self::getCanonicalId($asset, AssetCategory::PAGEABLE, MetaEntityType::INSTAGRAM_ACCOUNT->value),
                    'hostname'    => self::getPageHostname(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                    'title'       => self::getPageTitle(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                    'url'         => self::getPageUrl(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                    'enabled'     => $asset['ig_accounts'] ?? true,
                    'data'        => self::getPageData(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT)
                ];
                $list[] = $igAccount;
            }

            return $list;
        }

        public static function getChanneledAccounts(array $asset): array
        {
            $list = [];
            $fbPageId = self::getChanneledAccountPlatformId(asset: $asset);
            if (!empty($fbPageId)) {
                $fbPage = [
                    'platformId'        => $fbPageId,
                    'platformCreatedAt' => self::getChanneledAccountPlatformCreatedAt(asset: $asset),
                    'name'              => self::getChanneledAccountName(asset: $asset),
                    'type'              => self::getChanneledAccountType(),
                    'enabled'           => $asset['enabled'] ?? true,
                    'data'              => self::getChanneledAccountData(asset: $asset)
                ];
                $list[] = $fbPage;
            }
            $igPlatformId = self::getChanneledAccountPlatformId(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT);
            if ($igPlatformId) {
                $igAccount = [
                    'platformId'        => $igPlatformId,
                    'platformCreatedAt' => self::getChanneledAccountPlatformCreatedAt(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                    'name'              => self::getChanneledAccountName(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                    'type'              => self::getChanneledAccountType(entityType: MetaEntityType::INSTAGRAM_ACCOUNT),
                    'enabled'           => $asset['ig_accounts'] ?? true,
                    'data'              => self::getChanneledAccountData(asset: $asset, entityType: MetaEntityType::INSTAGRAM_ACCOUNT)
                ];
                $list[] = $igAccount;
            }

            return $list;
        }

        // PAGE FIELDS

        public static function getPlatformId(array $asset, AssetCategory $category, string $context = ''): string
        {
            return match ($category) {
                AssetCategory::IDENTITY, AssetCategory::PAGEABLE => match ($context) {
                    MetaEntityType::INSTAGRAM_ACCOUNT->value => self::deriveMetaId($asset, 'ig_account'),
                    MetaEntityType::PAGE->value => self::deriveMetaId($asset, 'id'),
                    default => ''
                },
                AssetCategory::UNIT => self::deriveMetaId($asset, 'id'),
                default => (string)($asset['id'] ?? '')
            };
        }

        public static function getCanonicalId(array $asset, AssetCategory $category, string $context): string
        {
            $pId = self::getPlatformId($asset, $category, $context);
            if (!$pId) return '';

            $patterns = self::getAssetPatterns();
            $prefix = $patterns[$context]['page']['canonical_id']['prefix'] ?? null;

            if ($category === AssetCategory::PAGEABLE && $prefix) {
                return str_starts_with($pId, $prefix.':') ? $pId : $prefix.':'.$pId;
            }

            return $pId;
        }

        private static function deriveMetaId(array $asset, string $key): string
        {
            return isset($asset[$key]) && $asset[$key] ? FieldsNormalizerHelper::getCleanString($asset[$key]) : '';
        }

        public static function getPagePlatformId(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            return self::getPlatformId($asset, AssetCategory::PAGEABLE, self::getChanneledAccountType($entityType));
        }

        public static function getPageCanonicalId(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            return self::getCanonicalId($asset, AssetCategory::PAGEABLE, self::getChanneledAccountType($entityType));
        }

        public static function getPageHostname(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            $hostnameKey = $key ?: match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'ig_hostname',
                default => 'hostname'
            };

            return isset($asset[$hostnameKey]) && ($asset[$hostnameKey]) ? FieldsNormalizerHelper::getCleanString($asset[$hostnameKey]) : '';
        }

        public static function getPageTitle(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            $titleKey = $key ?: match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'ig_account_name',
                default => 'title'
            };

            return isset($asset[$titleKey]) && ($asset[$titleKey]) ? FieldsNormalizerHelper::getCleanString($asset[$titleKey]) : '';
        }

        public static function getPageUrl(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            if ($key && isset($asset[$key])) {
                return FieldsNormalizerHelper::getCleanString($asset[$key]);
            }

            return match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => (isset($asset['ig_data']['username']) && $asset['ig_data']['username'] ?
                    'https://www.instagram.com/'.FieldsNormalizerHelper::getCleanString($asset['ig_data']['username']) : ''),
                default => isset($asset['link']) && ($asset['link']) ? FieldsNormalizerHelper::getCleanString($asset['link']) : ''
            };
        }

        public static function getPageData(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): array
        {
            $dataKey = $key ?: match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'ig_data',
                default => 'data'
            };

            return isset($asset[$dataKey]) && $asset[$dataKey] ? FieldsNormalizerHelper::getCleanArray($asset[$dataKey]) : [];
        }

        // CHANNELED ACCOUNT FIELDS

        public static function getChanneledAccountPlatformId(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            $platformIdKey = $key ?: match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'ig_account',
                default => 'id'
            };

            return isset($asset[$platformIdKey]) && ($asset[$platformIdKey]) ? FieldsNormalizerHelper::getCleanString($asset[$platformIdKey]) : '';
        }

        public static function getChanneledAccountPlatformCreatedAt(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            $platformCreatedAtKey = $key ?: match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'ig_created_time',
                default => 'created_time'
            };

            return isset($asset[$platformCreatedAtKey]) && $asset[$platformCreatedAtKey] ? FieldsNormalizerHelper::getCleanString($asset[$platformCreatedAtKey]) : '';
        }

        public static function getChanneledAccountName(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            $nameKey = $key ?: match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'ig_account_name',
                default => 'title'
            };

            return isset($asset[$nameKey]) && ($asset[$nameKey]) ? FieldsNormalizerHelper::getCleanString($asset[$nameKey]) : '';
        }

        public static function getChanneledAccountType(string|MetaEntityType $entityType = MetaEntityType::PAGE): string
        {
            return $entityType instanceof MetaEntityType ? $entityType->value : $entityType;
        }

        public static function getChanneledAccountData(array $asset, ?string $key = null, string|MetaEntityType $entityType = MetaEntityType::PAGE): array
        {
            $dataKey = $key ?: match (self::getChanneledAccountType($entityType)) {
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'ig_data',
                default => 'data'
            };

            return isset($asset[$dataKey]) && $asset[$dataKey] ? FieldsNormalizerHelper::getCleanArray($asset[$dataKey]) : [];
        }

        /**
         * @inheritdoc
         */
        public static function getPageTypes(): array
        {
            return [
                MetaEntityType::PAGE->value => 'Facebook Page',
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'Instagram Account'
            ];
        }

        /**
         * @inheritdoc
         */
        public static function getAccountTypes(): array
        {
            return [
                MetaEntityType::PAGE->value => 'Facebook Page',
                MetaEntityType::INSTAGRAM_ACCOUNT->value => 'Instagram Account'
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
            $ui['fb_organic_granular_sync'] = $channelConfig['granular_sync'] ?? false;

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

            throw new Exception("Reset callback not provided for ".$this->getChannel());
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
                'history_months'     => 24,
                'entities_sync'      => 'entities',
                'recent_cron_hour'   => 6,
                'recent_cron_minute' => 30,
            ];
        }

        /**
         * @inheritdoc
         */
        public function getConfigurationJs(): string
        {
            $jsPath = __DIR__ . '/js/FacebookOrganicConfigHandler.js';
            return file_exists($jsPath) ? file_get_contents($jsPath) : "";
        }
    }
