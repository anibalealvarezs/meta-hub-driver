<?php

namespace Tests\Unit\Conversions;

use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicConvert;
use PHPUnit\Framework\TestCase;

class FacebookOrganicConvertTest extends TestCase
{
    public function testConvertPosts()
    {
        $raw = [
            [
                'id' => 'post_1',
                'message' => 'Hello FB',
                'created_time' => '2026-01-01T10:00:00+0000'
            ]
        ];

        $result = FacebookOrganicConvert::posts($raw, 'page_1', 'acc_1', 'ca_1');
        
        $this->assertCount(1, $result);
        $entity = $result->first();
        $this->assertEquals('post_1', $entity->platformId);
        $this->assertEquals('page_1', $entity->getContext()['pageId']);
    }

    public function testConvertPages()
    {
        $raw = [
            [
                'id' => 'page_123',
                'name' => 'My Page',
                'url' => 'https://facebook.com/mypage'
            ]
        ];

        $result = FacebookOrganicConvert::pages($raw, 'acc_1');
        
        $this->assertCount(1, $result);
        $entity = $result->first();
        $this->assertEquals('page_123', $entity->platformId);
        $this->assertEquals('My Page', $entity->title);
    }

    public function testConvertMedia()
    {
        $raw = [
            [
                'id' => 'ig_123',
                'caption' => 'Hello IG',
                'timestamp' => '2026-01-01T15:00:00+0000'
            ]
        ];

        $result = FacebookOrganicConvert::media($raw, 'page_1', 'acc_1', 'ca_1');
        
        $this->assertCount(1, $result);
        $entity = $result->first();
        $this->assertEquals('ig_123', $entity->platformId);
    }
}
