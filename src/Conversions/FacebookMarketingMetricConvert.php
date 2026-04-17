<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Conversions;

use Anibalealvarezs\ApiDriverCore\Conversions\UniversalMetricConverter;
use Anibalealvarezs\FacebookGraphApi\Enums\AdAccountPermission;
use Anibalealvarezs\FacebookGraphApi\Enums\AdPermission;
use Anibalealvarezs\FacebookGraphApi\Enums\AdsetPermission;
use Anibalealvarezs\FacebookGraphApi\Enums\CampaignPermission;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
use Doctrine\Common\Collections\ArrayCollection;
use Psr\Log\LoggerInterface;

/**
 * FacebookMarketingMetricConvert
 * 
 * Standardizes Facebook Marketing insights data into APIs Hub metric objects.
 * Refactored to be entity-agnostic for the standalone SDK.
 */
class FacebookMarketingMetricConvert
{
    private const METADATA_FIELDS = ['actions', 'cost_per_action_type', 'purchase_roas', 'website_purchase_roas'];

    /**
     * Converts Facebook Creative API rows into metrics.
     */
    public static function creativeMetrics(
        array $rows,
        ?LoggerInterface $logger = null,
        object|string|null $channeledAccount = null,
        object|string|null $creative = null,
        object|string|null $period = 'daily',
        MetricSet $metricSet = MetricSet::KEY,
        array $metricsToProcess = [],
        ?string $customFields = null,
        object|string|null $account = null,
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdPermission::DEFAULT->insightsFields($metricSet)));
        
        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string) $channeledAccount;
        $creativePlatformId = is_object($creative) && method_exists($creative, 'getCreativeId') ? $creative->getCreativeId() : (string) $creative;
        $periodValue = is_object($period) && isset($period->value) ? $period->value : (string) $period;

        return UniversalMetricConverter::convert($rows, [
            'channel' => 'facebook_marketing',
            'period' => $periodValue,
            'platform_id_field' => 'creative_id',
            'date_field' => 'date_start',
            'metrics' => array_combine($metricsList, $metricsList),
            'dimensions' => ['age', 'gender'],
            'metadata_fields' => self::METADATA_FIELDS,
            'context' => [
                'account' => $account,
                'channeledAccount' => $channeledAccount,
                'channeledAccountId' => $channeledAccountId,
                'creative' => $creative,
            ],
            'row_key_fields' => [
                'account_id' => ['account', 'channeledAccount'],
                'ad_id'      => 'channeledAd',
                'creative_id'=> 'creative',
            ],
            'fallback_platform_id' => $creativePlatformId
        ], $logger);
    }

    /**
     * Converts Facebook Ad Account API rows into metrics.
     */
    public static function adAccountMetrics(
        array $rows,
        ?LoggerInterface $logger = null,
        object|string|null $account = null,
        object|string|null $channeledAccount = null,
        object|string|null $period = 'daily',
        MetricSet $metricSet = MetricSet::KEY,
        array $metricsToProcess = [],
        ?string $customFields = null,
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdAccountPermission::DEFAULT->insightsFields($metricSet)));
        $periodValue = is_object($period) && isset($period->value) ? $period->value : (string) $period;
        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string) $channeledAccount;
        $channeledPlatformId = is_object($channeledAccount) && method_exists($channeledAccount, 'getPlatformId') ? $channeledAccount->getPlatformId() : (string) $channeledAccount;

        return UniversalMetricConverter::convert($rows, [
            'channel' => 'facebook_marketing',
            'period' => $periodValue,
            'platform_id_field' => 'account_id',
            'date_field' => 'date_start',
            'metrics' => array_combine($metricsList, $metricsList),
            'dimensions' => ['age', 'gender'],
            'metadata_fields' => self::METADATA_FIELDS,
            'context' => [
                'account' => $account,
                'channeledAccount' => $channeledAccount,
                'channeledAccountId' => $channeledAccountId,
            ],
            'row_key_fields' => [
                'account_id' => ['account', 'channeledAccount'],
            ],
            'fallback_platform_id' => $channeledPlatformId
        ], $logger);
    }

    /**
     * Converts Facebook Campaign API rows into metrics.
     */
    public static function campaignMetrics(
        array $rows,
        ?LoggerInterface $logger = null,
        object|string|null $channeledAccount = null,
        object|string|null $campaign = null,
        object|string|null $channeledCampaign = null,
        object|string|null $period = 'daily',
        MetricSet $metricSet = MetricSet::KEY,
        array $metricsToProcess = [],
        ?string $customFields = null,
        object|string|null $account = null,
    ): ArrayCollection {
        $metricsList = $customFields ? explode(',', $customFields) : explode(',', CampaignPermission::DEFAULT->insightsFields($metricSet));
        
        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string) $channeledAccount;
        $channeledCampaignId = is_object($channeledCampaign) && method_exists($channeledCampaign, 'getPlatformId') ? $channeledCampaign->getPlatformId() : (string) $channeledCampaign;
        $periodValue = is_object($period) && isset($period->value) ? $period->value : (string) $period;

        return UniversalMetricConverter::convert($rows, [
            'channel' => 'facebook_marketing',
            'period' => $periodValue,
            'platform_id_field' => 'campaign_id',
            'date_field' => 'date_start',
            'metrics' => array_combine($metricsList, $metricsList),
            'dimensions' => ['age', 'gender'],
            'metadata_fields' => self::METADATA_FIELDS,
            'context' => [
                'account' => $account,
                'channeledAccount' => $channeledAccount,
                'channeledAccountId' => $channeledAccountId,
                'campaign' => $campaign,
                'channeledCampaign' => $channeledCampaign,
            ],
            'row_key_fields' => [
                'account_id'  => ['account', 'channeledAccount'],
            ],
            'row_entity_fields' => [
                'campaign_id' => ['campaign', 'channeledCampaign'],
            ],
            'fallback_platform_id' => $channeledCampaignId
        ], $logger);
    }

    /**
     * Converts Facebook Adset API rows into metrics.
     */
    public static function adsetMetrics(
        array $rows,
        ?LoggerInterface $logger = null,
        object|string|null $channeledAccount = null,
        object|string|null $campaign = null,
        object|string|null $channeledCampaign = null,
        object|string|null $channeledAdGroup = null,
        object|string|null $period = 'daily',
        MetricSet $metricSet = MetricSet::KEY,
        array $metricsToProcess = [],
        ?string $customFields = null,
        object|string|null $account = null,
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdsetPermission::DEFAULT->insightsFields($metricSet)));

        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string) $channeledAccount;
        $channeledAdGroupId = is_object($channeledAdGroup) && method_exists($channeledAdGroup, 'getPlatformId') ? $channeledAdGroup->getPlatformId() : (string) $channeledAdGroup;
        $periodValue = is_object($period) && isset($period->value) ? $period->value : (string) $period;

        return UniversalMetricConverter::convert($rows, [
            'channel' => 'facebook_marketing',
            'period' => $periodValue,
            'platform_id_field' => 'adset_id',
            'date_field' => 'date_start',
            'metrics' => array_combine($metricsList, $metricsList),
            'dimensions' => ['age', 'gender'],
            'metadata_fields' => self::METADATA_FIELDS,
            'context' => [
                'account' => $account,
                'channeledAccount' => $channeledAccount,
                'channeledAccountId' => $channeledAccountId,
                'campaign' => $campaign,
                'channeledCampaign' => $channeledCampaign,
                'channeledAdGroup' => $channeledAdGroup,
            ],
            'row_key_fields' => [
                'account_id' => ['account', 'channeledAccount'],
            ],
            'row_entity_fields' => [
                'campaign_id' => ['campaign', 'channeledCampaign'],
                'adset_id'    => 'channeledAdGroup',
            ],
            'fallback_platform_id' => $channeledAdGroupId
        ], $logger);
    }

    /**
     * Converts Facebook Ad API rows into metrics.
     */
    public static function adMetrics(
        array $rows,
        ?LoggerInterface $logger = null,
        object|string|null $channeledAccount = null,
        object|string|null $campaign = null,
        object|string|null $channeledCampaign = null,
        object|string|null $channeledAdGroup = null,
        object|string|null $channeledAd = null,
        object|string|null $period = 'daily',
        MetricSet $metricSet = MetricSet::KEY,
        array $metricsToProcess = [],
        ?string $customFields = null,
        object|string|null $account = null,
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdPermission::DEFAULT->insightsFields($metricSet)));

        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string) $channeledAccount;
        $channeledAdId = is_object($channeledAd) && method_exists($channeledAd, 'getPlatformId') ? $channeledAd->getPlatformId() : (string) $channeledAd;
        $periodValue = is_object($period) && isset($period->value) ? $period->value : (string) $period;

        return UniversalMetricConverter::convert($rows, [
            'channel' => 'facebook_marketing',
            'period' => $periodValue,
            'platform_id_field' => 'ad_id',
            'date_field' => 'date_start',
            'metrics' => array_combine($metricsList, $metricsList),
            'dimensions' => ['age', 'gender'],
            'metadata_fields' => self::METADATA_FIELDS,
            'context' => [
                'account' => $account,
                'channeledAccount' => $channeledAccount,
                'channeledAccountId' => $channeledAccountId,
                'campaign' => $campaign,
                'channeledCampaign' => $channeledCampaign,
                'channeledAdGroup' => $channeledAdGroup,
                'channeledAd' => $channeledAd,
            ],
            'row_key_fields' => [
                'account_id' => ['account', 'channeledAccount'],
                'ad_id'      => 'channeledAd',
            ],
            'row_entity_fields' => [
                'campaign_id' => ['campaign', 'channeledCampaign'],
                'adset_id'    => 'channeledAdGroup',
            ],
            'fallback_platform_id' => $channeledAdId
        ], $logger);
    }

    /**
     * Metrics proxy for dynamic levels.
     */
    public static function metrics(
        array $rows,
        object|string $channeledAccount,
        string $level = 'account',
        ?LoggerInterface $logger = null,
        object|string|null $account = null,
    ): ArrayCollection {
        return match ($level) {
            'campaign' => self::campaignMetrics(rows: $rows, logger: $logger, channeledAccount: $channeledAccount, account: $account),
            'adset' => self::adsetMetrics(rows: $rows, logger: $logger, channeledAccount: $channeledAccount, account: $account),
            'ad' => self::adMetrics(rows: $rows, logger: $logger, channeledAccount: $channeledAccount, account: $account),
            default => self::adAccountMetrics(rows: $rows, logger: $logger, channeledAccount: $channeledAccount, account: $account),
        };
    }
}
