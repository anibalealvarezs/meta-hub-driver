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
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdPermission::DEFAULT->insightsFields($metricSet)));
        
        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getPlatformId') ? $channeledAccount->getPlatformId() : (string) $channeledAccount;
        $creativeId = is_object($creative) && method_exists($creative, 'getCreativeId') ? $creative->getCreativeId() : (string) $creative;
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
                'channeledAccount' => $channeledAccountId,
                'creative' => $creativeId,
            ],
            'fallback_platform_id' => $creativeId
        ], $logger);
    }

    /**
     * Converts Facebook Ad Account API rows into metrics.
     */
    public static function adAccountMetrics(
        array $rows,
        ?LoggerInterface $logger = null,
        object|string|null $account = null,
        ?string $channeledAccountPlatformId = null,
        object|string|null $period = 'daily',
        MetricSet $metricSet = MetricSet::KEY,
        array $metricsToProcess = [],
        ?string $customFields = null,
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdAccountPermission::DEFAULT->insightsFields($metricSet)));
        $periodValue = is_object($period) && isset($period->value) ? $period->value : (string) $period;

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
                'channeledAccount' => $channeledAccountPlatformId,
            ],
            'fallback_platform_id' => $channeledAccountPlatformId
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
    ): ArrayCollection {
        $metricsList = $customFields ? explode(',', $customFields) : explode(',', CampaignPermission::DEFAULT->insightsFields($metricSet));
        
        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getPlatformId') ? $channeledAccount->getPlatformId() : (string) $channeledAccount;
        $campaignId = is_object($campaign) && method_exists($campaign, 'getCampaignId') ? $campaign->getCampaignId() : (string) $campaign;
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
                'channeledAccount' => $channeledAccountId,
                'campaign' => $campaignId,
                'channeledCampaign' => $channeledCampaignId,
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
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdsetPermission::DEFAULT->insightsFields($metricSet)));

        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getPlatformId') ? $channeledAccount->getPlatformId() : (string) $channeledAccount;
        $campaignId = is_object($campaign) && method_exists($campaign, 'getCampaignId') ? $campaign->getCampaignId() : (string) $campaign;
        $channeledCampaignId = is_object($channeledCampaign) && method_exists($channeledCampaign, 'getPlatformId') ? $channeledCampaign->getPlatformId() : (string) $channeledCampaign;
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
                'channeledAccount' => $channeledAccountId,
                'campaign' => $campaignId,
                'channeledCampaign' => $channeledCampaignId,
                'channeledAdGroup' => $channeledAdGroupId,
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
    ): ArrayCollection {
        $metricsList = !empty($metricsToProcess) ? $metricsToProcess : ($customFields ? explode(',', $customFields) : explode(',', AdPermission::DEFAULT->insightsFields($metricSet)));

        $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getPlatformId') ? $channeledAccount->getPlatformId() : (string) $channeledAccount;
        $campaignId = is_object($campaign) && method_exists($campaign, 'getCampaignId') ? $campaign->getCampaignId() : (string) $campaign;
        $channeledCampaignId = is_object($channeledCampaign) && method_exists($channeledCampaign, 'getPlatformId') ? $channeledCampaign->getPlatformId() : (string) $channeledCampaign;
        $channeledAdGroupId = is_object($channeledAdGroup) && method_exists($channeledAdGroup, 'getPlatformId') ? $channeledAdGroup->getPlatformId() : (string) $channeledAdGroup;
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
                'channeledAccount' => $channeledAccountId,
                'campaign' => $campaignId,
                'channeledCampaign' => $channeledCampaignId,
                'channeledAdGroup' => $channeledAdGroupId,
                'channeledAd' => $channeledAdId,
            ],
            'fallback_platform_id' => $channeledAdId
        ], $logger);
    }
}
