<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Enums;

/**
 * MetaFeature
 * 
 * Defines the standard configuration keys for Meta (Facebook/Instagram) features.
 */
enum MetaFeature: string
{
    // Organic Facebook
    case PAGE_METRICS = 'page_metrics';
    case POSTS = 'posts';
    case POST_METRICS = 'post_metrics';
    
    // Organic Instagram
    case IG_ACCOUNTS = 'ig_accounts';
    case IG_ACCOUNT_METRICS = 'ig_account_metrics';
    case IG_ACCOUNT_MEDIA = 'ig_account_media';
    case IG_ACCOUNT_MEDIA_METRICS = 'ig_account_media_metrics';
    
    // Marketing
    case AD_ACCOUNT_METRICS = 'ad_account_metrics';
    case CAMPAIGNS = 'campaigns';
    case CAMPAIGN_METRICS = 'campaign_metrics';
    case ADSETS = 'adsets';
    case ADSET_METRICS = 'adset_metrics';
    case ADS = 'ads';
    case AD_METRICS = 'ad_metrics';
    case CREATIVES = 'creatives';
    case CREATIVE_METRICS = 'creative_metrics';
    
    /**
     * Returns all organic features.
     */
    public static function organic(): array
    {
        return [
            self::PAGE_METRICS,
            self::POSTS,
            self::POST_METRICS,
            self::IG_ACCOUNTS,
            self::IG_ACCOUNT_METRICS,
            self::IG_ACCOUNT_MEDIA,
            self::IG_ACCOUNT_MEDIA_METRICS,
        ];
    }

    /**
     * Returns all marketing features.
     */
    public static function marketing(): array
    {
        return [
            self::AD_ACCOUNT_METRICS,
            self::CAMPAIGNS,
            self::CAMPAIGN_METRICS,
            self::ADSETS,
            self::ADSET_METRICS,
            self::ADS,
            self::AD_METRICS,
            self::CREATIVES,
            self::CREATIVE_METRICS,
        ];
    }
}
