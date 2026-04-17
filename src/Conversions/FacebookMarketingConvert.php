<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Conversions;

use Anibalealvarezs\ApiDriverCore\Conversions\UniversalEntityConverter;
use Doctrine\Common\Collections\ArrayCollection;

/**
 * FacebookMarketingConvert
 * 
 * Standardizes Facebook Marketing entity data (Campaigns, Ad Sets, Ads, Creatives)
 * into APIs Hub objects using the UniversalEntityConverter.
 */
class FacebookMarketingConvert
{
    public static function campaigns(array $campaigns, int|string|null $channeledAccountId = null): ArrayCollection
    {
        return UniversalEntityConverter::convert($campaigns, [
            'channel' => 'facebook_marketing',
            'platform_id_field' => 'id',
            'date_field' => 'start_time',
            'mapping' => [
                'name' => 'name',
                'startDate' => 'start_time',
                'endDate' => 'stop_time',
                'objective' => 'objective',
                'buyingType' => 'buying_type',
                'status' => 'status',
                'budget' => 'daily_budget',
                'lifetimeBudget' => 'lifetime_budget',
            ],
            'context' => [
                'channeledAccountId' => $channeledAccountId,
            ],
        ]);
    }

    public static function adsets(array $adsets, int|string|null $channeledAccountId = null): ArrayCollection
    {
        return UniversalEntityConverter::convert($adsets, [
            'channel' => 'facebook_marketing',
            'platform_id_field' => 'id',
            'date_field' => 'created_time',
            'mapping' => [
                'name' => 'name',
                'startDate' => 'start_time',
                'endDate' => 'stop_time',
                'status' => 'status',
                'optimizationGoal' => 'optimization_goal',
                'billingEvent' => 'billing_event',
                'targeting' => 'targeting',
                'channeledCampaignId' => 'campaign_id',
            ],
            'context' => [
                'channeledAccountId' => $channeledAccountId,
            ],
        ]);
    }

    public static function ads(array $ads, int|string|null $channeledAccountId = null): ArrayCollection
    {
        return UniversalEntityConverter::convert($ads, [
            'channel' => 'facebook_marketing',
            'platform_id_field' => 'id',
            'date_field' => 'created_time',
            'mapping' => [
                'name' => 'name',
                'status' => 'status',
                'channeledCampaignId' => 'campaign_id',
                'channeledAdGroupId' => 'adset_id',
                'channeledCreativeId' => 'creative.id',
            ],
            'context' => [
                'channeledAccountId' => $channeledAccountId,
            ],
        ]);
    }

    public static function creatives(array $creatives, int|string|null $channeledAccountId = null): ArrayCollection
    {
        return UniversalEntityConverter::convert($creatives, [
            'channel' => 'facebook_marketing',
            'platform_id_field' => 'id',
            'date_field' => 'created_time',
            'mapping' => [
                'name' => 'name',
            ],
            'context' => [
                'channeledAccountId' => $channeledAccountId,
            ],
        ]);
    }
}
