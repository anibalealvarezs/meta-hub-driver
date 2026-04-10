<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Conversions;

use Anibalealvarezs\ApiDriverCore\Conversions\UniversalEntityConverter;
use Doctrine\Common\Collections\ArrayCollection;

/**
 * FacebookOrganicConvert
 * 
 * Standardizes Facebook Page and Instagram Organic entity data (Posts, Pages)
 * into APIs Hub objects using the UniversalEntityConverter.
 */
class FacebookOrganicConvert
{
    /**
     * Converts raw Facebook posts into standardized objects.
     */
    public static function posts(
        array $posts,
        int|string|null $pageId = null,
        int|string|null $accountId = null,
        int|string|null $channeledAccountId = null
    ): ArrayCollection {
        return UniversalEntityConverter::convert($posts, [
            'channel' => 'facebook_organic',
            'platform_id_field' => 'id',
            'date_field' => 'created_time',
            'context' => [
                'pageId' => $pageId,
                'accountId' => $accountId,
                'channeledAccountId' => $channeledAccountId,
            ],
        ]);
    }

    /**
     * Converts raw Facebook pages into standardized objects.
     */
    public static function pages(array $pages, int|string|null $accountId = null): ArrayCollection
    {
        return UniversalEntityConverter::convert($pages, [
            'channel' => 'facebook_organic',
            'platform_id_field' => 'id',
            'date_field' => 'created_at',
            'mapping' => [
                'url' => fn ($r) => $r['url'] ?? $r['id'] ?? null,
                'title' => fn ($r) => $r['title'] ?? $r['name'] ?? $r['id'] ?? '',
                'hostname' => fn ($r) => $r['hostname'] ?? 'facebook.com',
                'canonicalId' => fn ($r) => $r['url'] ?? $r['id'] ?? null, // Simplified for SDK
            ],
            'context' => [
                'accountId' => $accountId,
            ],
        ]);
    }
}
