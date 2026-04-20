<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Enums;

/**
 * MetaEntityType
 * 
 * Defines the specific standardized entity types for Meta (Facebook/Instagram).
 */
enum MetaEntityType: string
{
    case PAGE = 'facebook_page';
    case INSTAGRAM_ACCOUNT = 'instagram_account';
    case POST = 'post';
    case IG_MEDIA = 'ig_media';
    case CAMPAIGN = 'campaign';
    case AD_GROUP = 'ad_group';
    case AD = 'ad';
    case CREATIVE = 'creative';
}
