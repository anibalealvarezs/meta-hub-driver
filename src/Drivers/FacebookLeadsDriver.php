<?php

namespace Anibalealvarezs\MetaHubDriver\Drivers;

use Anibalealvarezs\ApiDriverCore\Enums\AssetCategory;
use Anibalealvarezs\ApiDriverCore\Enums\InstanceTier;
use Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\ChanneledAccountableInterface;
use Anibalealvarezs\ApiDriverCore\Interfaces\SyncDriverInterface;
use Anibalealvarezs\ApiDriverCore\Traits\SyncDriverTrait;
use Anibalealvarezs\MetaHubDriver\Traits\MetaSyncDriverTrait;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\ApiDriverCore\Helpers\FieldsNormalizerHelper;
use Anibalealvarezs\ApiDriverCore\Conversions\UniversalEntityConverter;
use Doctrine\Common\Collections\ArrayCollection;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Response;
use DateTime;
use Exception;

class FacebookLeadsDriver implements SyncDriverInterface, ChanneledAccountableInterface
{
    use SyncDriverTrait, MetaSyncDriverTrait {
        MetaSyncDriverTrait::storeCredentials insteadof SyncDriverTrait;
        MetaSyncDriverTrait::getEnvMapping insteadof SyncDriverTrait;
        MetaSyncDriverTrait::getCommonConfigKey insteadof SyncDriverTrait;
        MetaSyncDriverTrait::getUpdatableCredentials insteadof SyncDriverTrait;
        MetaSyncDriverTrait::getProviderLabel insteadof SyncDriverTrait;
        MetaSyncDriverTrait::getProviderName insteadof SyncDriverTrait;
        MetaSyncDriverTrait::reset insteadof SyncDriverTrait;
        MetaSyncDriverTrait::getDateFilterMapping insteadof SyncDriverTrait;
    }

    private ?AuthProviderInterface $authProvider;
    private ?LoggerInterface $logger;
    /** @var callable|null */
    private $dataProcessor = null;
    private const int DEFAULT_MAX_WORKERS = 1;

    public function __construct(?AuthProviderInterface $authProvider = null, ?LoggerInterface $logger = null)
    {
        $this->authProvider = $authProvider;
        $this->logger = $logger;
    }

    public static function getPlatformEntityIdField(): string
    {
        return 'facebook_page_id';
    }

    public static function getPublicResources(): array
    {
        return ['customers' => 'fb_leads'];
    }

    public static function getChannelLabel(): string
    {
        return 'FacebookLeads';
    }

    public static function getChannelIcon(): string
    {
        return 'L';
    }

    public static function getRoutes(): array
    {
        return [];
    }

    public static function getRateLimitWhitelist(): array
    {
        return [];
    }

    public function getChannel(): string
    {
        return 'facebook_leads';
    }

    public static function getChanneledAccountPlatformId(array $asset, ?string $key = null): string
    {
        return isset($asset['id']) ? FieldsNormalizerHelper::getCleanString($asset['id']) : '';
    }

    public static function getChanneledAccountPlatformCreatedAt(array $asset, ?string $key = null): string
    {
        return isset($asset['created_time']) ? FieldsNormalizerHelper::getCleanString($asset['created_time']) : '';
    }

    public static function getChanneledAccountName(array $asset, ?string $key = null): string
    {
        return isset($asset['title']) ? FieldsNormalizerHelper::getCleanString($asset['title']) : '';
    }

    public static function getChanneledAccountType(): string
    {
        return 'facebook_page';
    }

    public static function getChanneledAccountData(array $asset, ?string $key = null): array
    {
        return isset($asset['data']) ? FieldsNormalizerHelper::getCleanArray($asset['data']) : [];
    }

    public function fetchAvailableAssets(bool $throwOnError = false): array
    {
        if (!$this->authProvider || !$this->authProvider->hasCredentials()) {
            return [];
        }

        try {
            $api = $this->getApi();
            $userId = $api->getUserId();

            $pagesData = $api->getPages(
                userId: $userId,
                fields: 'id,name,link,website,created_time'
            );

            $assets = ['pages' => []];

            if (!empty($pagesData['data'])) {
                foreach ($pagesData['data'] as $page) {
                    $assets['pages'][] = [
                        'id'           => $page['id'],
                        'title'        => $page['name'],
                        'hostname'     => $page['website'] ?? null,
                        'url'          => $page['link'] ?? null,
                        'link'         => $page['link'] ?? null,
                        'created_time' => $page['created_time'] ?? null,
                        'data'         => $page,
                    ];
                }
            }

            return $assets;
        } catch (Exception $e) {
            $this->logger?->error("FacebookLeadsDriver: Error fetching available assets: " . $e->getMessage());
            if ($throwOnError) {
                throw $e;
            }

            return [];
        }
    }

    public function updateConfiguration(array $newData, array $currentConfig): array
    {
        $selectedPages = $newData['assets']['pages'] ?? [];
        if (!isset($currentConfig['channels']['facebook_leads'])) {
            $currentConfig['channels']['facebook_leads'] = [];
        }

        $chanCfg = &$currentConfig['channels']['facebook_leads'];
        $newPagesList = [];
        foreach ($selectedPages as $pData) {
            $newPagesList[] = [
                'id'           => $pData['id'],
                'title'        => $pData['title'] ?? null,
                'hostname'     => $pData['hostname'] ?? null,
                'url'          => $pData['url'] ?? null,
                'link'         => $pData['link'] ?? null,
                'created_time' => $pData['created_time'] ?? null,
                'data'         => $pData['data'] ?? [],
                'enabled'      => filter_var($pData['enabled'] ?? true, FILTER_VALIDATE_BOOLEAN),
            ];
        }
        $chanCfg['pages'] = $newPagesList;

        return $currentConfig;
    }

    public function sync(
        DateTime  $startDate,
        DateTime  $endDate,
        array     $config = [],
        ?callable $shouldContinue = null,
        ?callable $identityMapper = null
    ): Response {
        if (!$this->authProvider) {
            throw new Exception("AuthProvider not set for FacebookLeadsDriver");
        }

        if (!$this->dataProcessor) {
            throw new Exception("DataProcessor not set for FacebookLeadsDriver.");
        }

        $api = $this->getApi($config);
        $pagesToProcess = array_filter($config['pages'] ?? [], fn($p) => !isset($p['enabled']) || $p['enabled']);

        if (empty($pagesToProcess)) {
            return new Response(json_encode(['message' => 'No pages to process']), 200, ['Content-Type' => 'application/json']);
        }

        $totalLeadsProcessed = 0;

        foreach ($pagesToProcess as $page) {
            if ($shouldContinue && !$shouldContinue()) {
                break;
            }

            $pageId = (string)$page['id'];
            
            // Resolve page identity/channel_account via identityMapper
            $resolvedChannelAccount = null;
            if ($identityMapper) {
                $resolvedChannelAccount = $identityMapper($pageId, 'facebook_page');
            }

            try {
                // 1. Fetch leadgen forms for page
                $formsResponse = $api->getPageLeadgenForms($pageId);
                $forms = $formsResponse['data'] ?? [];

                foreach ($forms as $form) {
                    if ($shouldContinue && !$shouldContinue()) {
                        break;
                    }

                    $formId = (string)$form['id'];

                    // 2. Fetch leads for each form
                    $after = null;
                    do {
                        $leadsResponse = $api->getFormLeads($formId, 100, $after);
                        $leads = $leadsResponse['data'] ?? [];

                        if (empty($leads)) {
                            break;
                        }

                        $processedLeads = [];
                        foreach ($leads as $lead) {
                            $leadId = (string)$lead['id'];
                            
                            // Find email in field_data
                            $email = null;
                            $fieldData = $lead['field_data'] ?? [];
                            foreach ($fieldData as $field) {
                                if (($field['name'] ?? '') === 'email') {
                                    $email = !empty($field['values']) ? reset($field['values']) : null;
                                    break;
                                }
                            }

                            if (empty($email)) {
                                $email = $leadId . '@leads.facebook.com';
                            }

                            // Build lead_data interaction history
                            $leadData = [
                                [
                                    'origin'      => 'leadgen_form',
                                    'platform_id' => $formId,
                                    'timestamp'   => $lead['created_time'] ?? null,
                                ]
                            ];

                            $processedLeads[] = [
                                'id'                 => $leadId,
                                'created_time'       => $lead['created_time'] ?? null,
                                'email'              => $email,
                                'lead_data'          => $leadData,
                                'data'               => $lead,
                            ];
                        }

                        // Convert to UniversalEntity collection
                        $converted = UniversalEntityConverter::convert($processedLeads, [
                            'channel' => 'facebook_leads',
                            'platform_id_field' => 'id',
                            'date_field' => 'created_time',
                            'mapping' => [
                                'email' => 'email',
                                'lead_data' => 'lead_data',
                            ]
                        ]);

                        // Inject channeledAccount context
                        if ($resolvedChannelAccount) {
                            foreach ($converted as $item) {
                                $item->setContext(array_merge($item->getContext(), [
                                    'channeledAccount' => $resolvedChannelAccount
                                ]));
                            }
                        }

                        // Pass to dataProcessor
                        ($this->dataProcessor)($converted, 'channeled_customer');
                        $totalLeadsProcessed += count($processedLeads);

                        $after = $leadsResponse['paging']['cursors']['after'] ?? null;
                    } while ($after !== null);
                }
            } catch (Exception $e) {
                $this->logger?->error("FacebookLeadsDriver: Error syncing page {$pageId}: " . $e->getMessage());
            }
        }

        return new Response(json_encode([
            'message' => 'Facebook Leads Sync completed successfully',
            'processed' => $totalLeadsProcessed
        ]), 200, ['Content-Type' => 'application/json']);
    }

    public static function getPlatformId(array $asset, AssetCategory $category, string $context): string
    {
        return isset($asset['id']) ? FieldsNormalizerHelper::getCleanString($asset['id']) : '';
    }

    public static function getCanonicalId(array $asset, AssetCategory $category, string $context): string
    {
        return self::getPlatformId($asset, $category, $context);
    }

    public function prepareUiConfig(array $channelConfig): array
    {
        $ui = [];
        $ui['fb_leads_enabled'] = $channelConfig['enabled'] ?? false;
        $ui['fb_leads_pages_full_config'] = $channelConfig['pages'] ?? [];
        return $ui;
    }

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
            return ['initialized' => 0, 'skipped' => 0, 'error' => 'Callbacks missing'];
        }

        return $initializer->initialize(
            $this->getChannel(),
            $config,
            ['pages' => $assets['pages'] ?? []],
            $identityMapper,
            $dataProcessor
        );
    }

    public static function getInstanceRules(): array
    {
        return [
            'history_months'     => 1,
            'entities_sync'      => 'entities',
            'recent_cron_hour'   => 1,
            'recent_cron_minute' => 0,
        ];
    }

    public function getConfigurationJs(): string
    {
        return "";
    }

    public function getRequiredInstanceTier(): InstanceTier
    {
        return InstanceTier::TIER_2;
    }

    public static function getPages(array $asset): array
    {
        return [];
    }

    public static function getChanneledAccounts(array $asset): array
    {
        return [];
    }

    public static function getPageTypes(): array
    {
        return [
            'facebook_page' => 'Facebook Page'
        ];
    }

    public static function getAccountTypes(): array
    {
        return [
            'facebook_page' => 'Facebook Page'
        ];
    }

    public static function getEntityPaths(): array
    {
        return [];
    }

    public function validateConfig(array $config): array
    {
        return $config;
    }

    public function seedDemoData(\Anibalealvarezs\ApiDriverCore\Interfaces\SeederInterface $seeder, array $config = []): void
    {
    }

    protected function initializeApi(array $config): FacebookGraphApi
    {
        return new FacebookGraphApi(
            userId: $config['user_id'] ?? $config['facebook']['user_id'] ?? $this->authProvider->getUserId() ?: 'system',
            appId: $config['app_id'] ?? $config['facebook']['app_id'] ?? '',
            appSecret: $config['app_secret'] ?? $config['facebook']['app_secret'] ?? '',
            redirectUrl: $config['redirect_uri'] ?? $config['facebook']['redirect_uri'] ?? '',
            userAccessToken: $config['access_token'] ?? $config['graph_user_access_token'] ?? $this->authProvider->getAccessToken(),
            tokenIdentifier: $config['token_identifier'] ?? $config['facebook']['token_identifier'] ?? $_ENV['FACEBOOK_TOKEN_IDENTIFIER'] ?? getenv('FACEBOOK_TOKEN_IDENTIFIER') ?: "",
            apiVersion: $config['api_version'] ?? $config['facebook']['api_version'] ?? 'v18.0',
            logger: $this->logger,
            tokenRefresherCallback: $this->authProvider->getTokenRefresherCallback()
        );
    }

    public function boot(): void
    {
    }
}
