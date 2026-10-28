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
        private const array METADATA_FIELDS = ['actions', 'cost_per_action_type', 'purchase_roas', 'website_purchase_roas'];
        private const array ACTION_NORMALIZED_METRICS = [
            'purchases',
            'purchase_value',
            'cost_per_purchase',
            'leads',
            'cost_per_lead',
        ];

        /**
         * Normalizes raw Meta action arrays into flat metric fields on the row.
         */
        public static function normalizeRowActions(array $row): array
        {
            $actions = [];
            if (!empty($row['actions']) && is_array($row['actions'])) {
                foreach ($row['actions'] as $act) {
                    if (isset($act['action_type']) && isset($act['value'])) {
                        $actions[$act['action_type']] = (float)$act['value'];
                    }
                }
            }

            $actionValues = [];
            if (!empty($row['action_values']) && is_array($row['action_values'])) {
                foreach ($row['action_values'] as $actVal) {
                    if (isset($actVal['action_type']) && isset($actVal['value'])) {
                        $actionValues[$actVal['action_type']] = (float)$actVal['value'];
                    }
                }
            }

            $cpa = [];
            if (!empty($row['cost_per_action_type']) && is_array($row['cost_per_action_type'])) {
                foreach ($row['cost_per_action_type'] as $cpaItem) {
                    if (isset($cpaItem['action_type']) && isset($cpaItem['value'])) {
                        $cpa[$cpaItem['action_type']] = (float)$cpaItem['value'];
                    }
                }
            }

            $hasActions = !empty($row['actions']) && is_array($row['actions']);
            $hasActionValues = !empty($row['action_values']) && is_array($row['action_values']);
            $hasCpa = !empty($row['cost_per_action_type']) && is_array($row['cost_per_action_type']);

            if (!$hasActions && !$hasActionValues && !$hasCpa) {
                return $row;
            }

            // 1. Purchases
            if ($hasActions) {
                $purchases = $actions['omni_purchase']
                    ?? ($actions['purchase']
                        ?? ($actions['offsite_conversion.fb_pixel_purchase']
                            ?? ($actions['onsite_conversion.purchase'] ?? 0.0)));
                $row['purchases'] = (float)$purchases;
            }

            // 2. Purchase Value
            if ($hasActionValues) {
                $purchaseValue = $actionValues['omni_purchase']
                    ?? ($actionValues['purchase']
                        ?? ($actionValues['offsite_conversion.fb_pixel_purchase']
                            ?? ($actionValues['onsite_conversion.purchase'] ?? 0.0)));
                $row['purchase_value'] = (float)$purchaseValue;
            }

            // 3. Cost Per Purchase
            if ($hasCpa || ($hasActions && isset($row['purchases']))) {
                $costPerPurchase = $cpa['omni_purchase']
                    ?? ($cpa['purchase']
                        ?? ($cpa['offsite_conversion.fb_pixel_purchase']
                            ?? ($cpa['onsite_conversion.purchase'] ?? null)));
                if ($costPerPurchase !== null) {
                    $row['cost_per_purchase'] = (float)$costPerPurchase;
                } elseif (!empty($row['purchases']) && $row['purchases'] > 0 && isset($row['spend'])) {
                    $row['cost_per_purchase'] = ((float)$row['spend']) / $row['purchases'];
                }
            }

            // 4. Leads
            if ($hasActions) {
                $leads = $actions['lead']
                    ?? ($actions['offsite_conversion.fb_pixel_lead']
                        ?? ($actions['onsite_conversion.lead'] ?? 0.0));
                $row['leads'] = (float)$leads;
            }

            // 5. Cost Per Lead
            if ($hasCpa || ($hasActions && isset($row['leads']))) {
                $costPerLead = $cpa['lead']
                    ?? ($cpa['offsite_conversion.fb_pixel_lead']
                        ?? ($cpa['onsite_conversion.lead'] ?? null));
                if ($costPerLead !== null) {
                    $row['cost_per_lead'] = (float)$costPerLead;
                } elseif (!empty($row['leads']) && $row['leads'] > 0 && isset($row['spend'])) {
                    $row['cost_per_lead'] = ((float)$row['spend']) / $row['leads'];
                }
            }

            return $row;
        }

        /**
         * Resolves the list of metrics to process, automatically appending normalized action metrics when evaluating default/custom field sets.
         */
        private static function resolveMetricsList(
            array $metricsToProcess,
            ?string $customFields,
            string $defaultFields,
        ): array {
            if (!empty($metricsToProcess)) {
                return array_values(array_unique(array_filter(array_map('trim', $metricsToProcess))));
            }

            $baseList = $customFields ? explode(',', $customFields) : explode(',', $defaultFields);
            $baseList = array_map('trim', $baseList);

            foreach (self::ACTION_NORMALIZED_METRICS as $normMetric) {
                if (!in_array($normMetric, $baseList, true)) {
                    $baseList[] = $normMetric;
                }
            }

            return array_values(array_unique(array_filter($baseList)));
        }

        /**
         * Converts Facebook Creative API rows into metrics.
         */
        public static function creativeMetrics(
            array              $rows,
            ?LoggerInterface   $logger = null,
            object|string|null $channeledAccount = null,
            object|string|null $creative = null,
            object|string|null $period = 'daily',
            MetricSet          $metricSet = MetricSet::KEY,
            array              $metricsToProcess = [],
            ?string            $customFields = null,
            object|string|null $account = null,
            array              $dimensions = ['age', 'gender'],
        ): ArrayCollection
        {
            $metricsList = self::resolveMetricsList($metricsToProcess, $customFields, AdPermission::DEFAULT->insightsFields($metricSet));
            $rows = array_map([self::class, 'normalizeRowActions'], $rows);

            $channeledAccountId = is_object($channeledAccount) ? (method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount) : (string)$channeledAccount;
            $creativePlatformId = is_object($creative) ? (method_exists($creative, 'getCreativeId') ? $creative->getCreativeId() : (method_exists($creative, 'getPlatformId') ? $creative->getPlatformId() : (string)$creative)) : (string)$creative;
            $periodValue = is_object($period) && isset($period->value) ? $period->value : (string)$period;

            return UniversalMetricConverter::convert($rows, [
                'channel'              => 'facebook_marketing',
                'period'               => $periodValue,
                'platform_id_field'    => 'creative_id',
                'date_field'           => 'date_start',
                'metrics'              => array_combine($metricsList, $metricsList),
                'dimensions'           => $dimensions,
                'metadata_fields'      => self::METADATA_FIELDS,
                'context'              => UniversalMetricConverter::getUniversalContext([
                    'account'            => $account,
                    'channeledAccount'   => $channeledAccount,
                    'channeledAccountId' => $channeledAccountId,
                    'creative'           => $creative,
                ]),
                'row_key_fields'       => [
                    'ad_id'       => 'channeledAd',
                    'creative_id' => 'creative',
                ],
                'fallback_platform_id' => $creativePlatformId
            ], $logger);
        }

        /**
         * Converts Facebook Ad Account API rows into metrics.
         */
        public static function adAccountMetrics(
            array              $rows,
            ?LoggerInterface   $logger = null,
            object|string|null $account = null,
            object|string|null $channeledAccount = null,
            object|string|null $period = 'daily',
            MetricSet          $metricSet = MetricSet::KEY,
            array              $metricsToProcess = [],
            ?string            $customFields = null,
            array              $dimensions = ['age', 'gender'],
        ): ArrayCollection
        {
            $metricsList = self::resolveMetricsList($metricsToProcess, $customFields, AdAccountPermission::DEFAULT->insightsFields($metricSet));
            $rows = array_map([self::class, 'normalizeRowActions'], $rows);

            $periodValue = is_object($period) && isset($period->value) ? $period->value : (string)$period;
            $channeledAccountId = is_object($channeledAccount) ? (method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount) : (string)$channeledAccount;
            $channeledPlatformId = is_object($channeledAccount) ? (method_exists($channeledAccount, 'getPlatformId') ? $channeledAccount->getPlatformId() : (string)$channeledAccount) : (string)$channeledAccount;

            return UniversalMetricConverter::convert($rows, [
                'channel'              => 'facebook_marketing',
                'period'               => $periodValue,
                'platform_id_field'    => 'account_id',
                'date_field'           => 'date_start',
                'metrics'              => array_combine($metricsList, $metricsList),
                'dimensions'           => $dimensions,
                'metadata_fields'      => self::METADATA_FIELDS,
                'context'              => UniversalMetricConverter::getUniversalContext([
                    'account'            => $account,
                    'channeledAccount'   => $channeledAccount,
                    'channeledAccountId' => $channeledAccountId,
                ]),
                'row_key_fields'       => [],
                'fallback_platform_id' => $channeledPlatformId
            ], $logger);
        }

        /**
         * Converts Facebook Campaign API rows into metrics.
         */
        public static function campaignMetrics(
            array              $rows,
            ?LoggerInterface   $logger = null,
            object|string|null $channeledAccount = null,
            object|string|null $campaign = null,
            object|string|null $channeledCampaign = null,
            object|string|null $period = 'daily',
            MetricSet          $metricSet = MetricSet::KEY,
            array              $metricsToProcess = [],
            ?string            $customFields = null,
            object|string|null $account = null,
            array              $dimensions = ['age', 'gender'],
        ): ArrayCollection
        {
            $metricsList = self::resolveMetricsList($metricsToProcess, $customFields, CampaignPermission::DEFAULT->insightsFields($metricSet));
            $rows = array_map([self::class, 'normalizeRowActions'], $rows);

            $channeledAccountId = is_object($channeledAccount) ? (method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount) : (string)$channeledAccount;
            $channeledCampaignId = is_object($channeledCampaign) ? (method_exists($channeledCampaign, 'getPlatformId') ? $channeledCampaign->getPlatformId() : (string)$channeledCampaign) : (string)$channeledCampaign;
            $periodValue = is_object($period) && isset($period->value) ? $period->value : (string)$period;

            return UniversalMetricConverter::convert($rows, [
                'channel'              => 'facebook_marketing',
                'period'               => $periodValue,
                'platform_id_field'    => 'campaign_id',
                'date_field'           => 'date_start',
                'metrics'              => array_combine($metricsList, $metricsList),
                'dimensions'           => $dimensions,
                'metadata_fields'      => self::METADATA_FIELDS,
                'context'              => UniversalMetricConverter::getUniversalContext([
                    'account'            => $account,
                    'channeledAccount'   => $channeledAccount,
                    'channeledAccountId' => $channeledAccountId,
                    'campaign'           => $campaign,
                    'channeledCampaign'  => $channeledCampaign,
                ]),
                'row_key_fields'       => [
                    'campaign_id' => ['campaign', 'channeledCampaign'],
                ],
                'row_entity_fields'    => [
                    'campaign_id' => 'channeledCampaign',
                ],
                'fallback_platform_id' => $channeledCampaignId
            ], $logger);
        }

        /**
         * Converts Facebook Adset API rows into metrics.
         */
        public static function adsetMetrics(
            array              $rows,
            ?LoggerInterface   $logger = null,
            object|string|null $channeledAccount = null,
            object|string|null $campaign = null,
            object|string|null $channeledCampaign = null,
            object|string|null $channeledAdGroup = null,
            object|string|null $period = 'daily',
            MetricSet          $metricSet = MetricSet::KEY,
            array              $metricsToProcess = [],
            ?string            $customFields = null,
            object|string|null $account = null,
            array              $dimensions = ['age', 'gender'],
        ): ArrayCollection
        {
            $metricsList = self::resolveMetricsList($metricsToProcess, $customFields, AdsetPermission::DEFAULT->insightsFields($metricSet));
            $rows = array_map([self::class, 'normalizeRowActions'], $rows);

            $channeledAccountId = is_object($channeledAccount) ? (method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount) : (string)$channeledAccount;
            $channeledAdGroupId = is_object($channeledAdGroup) ? (method_exists($channeledAdGroup, 'getPlatformId') ? $channeledAdGroup->getPlatformId() : (string)$channeledAdGroup) : (string)$channeledAdGroup;
            $periodValue = is_object($period) && isset($period->value) ? $period->value : (string)$period;

            return UniversalMetricConverter::convert($rows, [
                'channel'              => 'facebook_marketing',
                'period'               => $periodValue,
                'platform_id_field'    => 'adset_id',
                'date_field'           => 'date_start',
                'metrics'              => array_combine($metricsList, $metricsList),
                'dimensions'           => $dimensions,
                'metadata_fields'      => self::METADATA_FIELDS,
                'context'              => UniversalMetricConverter::getUniversalContext([
                    'account'            => $account,
                    'channeledAccount'   => $channeledAccount,
                    'channeledAccountId' => $channeledAccountId,
                    'campaign'           => $campaign,
                    'channeledCampaign'  => $channeledCampaign,
                    'channeledAdGroup'   => $channeledAdGroup,
                ]),
                'row_key_fields'       => [
                    'campaign_id' => ['campaign', 'channeledCampaign'],
                    'adset_id'    => ['channeledAdGroup'],
                ],
                'row_entity_fields'    => [
                    'campaign_id' => 'channeledCampaign',
                    'adset_id'    => 'channeledAdGroup',
                ],
                'fallback_platform_id' => $channeledAdGroupId
            ], $logger);
        }

        /**
         * Converts Facebook Ad API rows into metrics.
         */
        public static function adMetrics(
            array              $rows,
            ?LoggerInterface   $logger = null,
            object|string|null $channeledAccount = null,
            object|string|null $campaign = null,
            object|string|null $channeledCampaign = null,
            object|string|null $channeledAdGroup = null,
            object|string|null $channeledAd = null,
            object|string|null $period = 'daily',
            MetricSet          $metricSet = MetricSet::KEY,
            array              $metricsToProcess = [],
            ?string            $customFields = null,
            object|string|null $account = null,
            array              $dimensions = ['age', 'gender'],
        ): ArrayCollection
        {
            $metricsList = self::resolveMetricsList($metricsToProcess, $customFields, AdPermission::DEFAULT->insightsFields($metricSet));
            $rows = array_map([self::class, 'normalizeRowActions'], $rows);

            $channeledAccountId = is_object($channeledAccount) ? (method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount) : (string)$channeledAccount;
            $channeledAdId = is_object($channeledAd) ? (method_exists($channeledAd, 'getPlatformId') ? $channeledAd->getPlatformId() : (string)$channeledAd) : (string)$channeledAd;
            $periodValue = is_object($period) && isset($period->value) ? $period->value : (string)$period;

            return UniversalMetricConverter::convert($rows, [
                'channel'              => 'facebook_marketing',
                'period'               => $periodValue,
                'platform_id_field'    => 'ad_id',
                'date_field'           => 'date_start',
                'metrics'              => array_combine($metricsList, $metricsList),
                'dimensions'           => $dimensions,
                'metadata_fields'      => self::METADATA_FIELDS,
                'context'              => UniversalMetricConverter::getUniversalContext([
                    'account'            => $account,
                    'channeledAccount'   => $channeledAccount,
                    'channeledAccountId' => $channeledAccountId,
                    'campaign'           => $campaign,
                    'channeledCampaign'  => $channeledCampaign,
                    'channeledAdGroup'   => $channeledAdGroup,
                    'channeledAd'        => $channeledAd,
                ]),
                'row_key_fields'       => [
                    'campaign_id' => ['campaign', 'channeledCampaign'],
                    'adset_id'    => ['channeledAdGroup'],
                    'ad_id'       => ['channeledAd'],
                ],
                'row_entity_fields'    => [
                    'campaign_id' => 'channeledCampaign',
                    'adset_id'    => 'channeledAdGroup',
                    'ad_id'       => 'channeledAd',
                ],
                'fallback_platform_id' => $channeledAdId
            ], $logger);
        }

        /**
         * Metrics proxy for dynamic levels.
         */
        public static function metrics(
            array              $rows,
            object|string      $channeledAccount,
            string             $level = 'account',
            ?LoggerInterface   $logger = null,
            object|string|null $account = null,
            MetricSet          $metricSet = MetricSet::KEY,
            array              $metricsToProcess = [],
            ?string            $customFields = null,
            array              $dimensions = ['age', 'gender'],
        ): ArrayCollection
        {
            return match ($level) {
                'campaign' => self::campaignMetrics(
                    rows: $rows,
                    logger: $logger,
                    channeledAccount: $channeledAccount,
                    metricSet: $metricSet,
                    metricsToProcess: $metricsToProcess,
                    customFields: $customFields,
                    account: $account,
                    dimensions: $dimensions
                ),
                'adset' => self::adsetMetrics(
                    rows: $rows,
                    logger: $logger,
                    channeledAccount: $channeledAccount,
                    metricSet: $metricSet,
                    metricsToProcess: $metricsToProcess,
                    customFields: $customFields,
                    account: $account,
                    dimensions: $dimensions
                ),
                'ad' => self::adMetrics(
                    rows: $rows,
                    logger: $logger,
                    channeledAccount: $channeledAccount,
                    metricSet: $metricSet,
                    metricsToProcess: $metricsToProcess,
                    customFields: $customFields,
                    account: $account,
                    dimensions: $dimensions
                ),
                default => self::adAccountMetrics(
                    rows: $rows,
                    logger: $logger,
                    account: $account,
                    channeledAccount: $channeledAccount,
                    metricSet: $metricSet,
                    metricsToProcess: $metricsToProcess,
                    customFields: $customFields,
                    dimensions: $dimensions
                ),
            };
        }
    }
