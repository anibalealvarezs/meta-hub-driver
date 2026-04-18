<?php

namespace Tests\Unit\Conversions;

use Anibalealvarezs\MetaHubDriver\Conversions\FacebookMarketingConvert;
use PHPUnit\Framework\TestCase;
use Doctrine\Common\Collections\ArrayCollection;

class FacebookMarketingConvertTest extends TestCase
{
    public function testConvertCampaigns()
    {
        $raw = [
            [
                'id' => 'camp_1',
                'name' => 'Campaign 1',
                'start_time' => '2026-01-01T00:00:00+0000',
                'stop_time' => '2026-01-10T00:00:00+0000',
                'objective' => 'OUTCOME_SALES',
                'status' => 'ACTIVE',
                'daily_budget' => '1000'
            ]
        ];

        $result = FacebookMarketingConvert::campaigns($raw, 123);
        
        $this->assertInstanceOf(ArrayCollection::class, $result);
        $this->assertCount(1, $result);
        
        $entity = $result->first();
        $this->assertEquals('camp_1', $entity->platformId);
        $this->assertEquals('Campaign 1', $entity->name);
        $this->assertEquals('OUTCOME_SALES', $entity->objective);
        $this->assertEquals(123, $entity->getContext()['channeledAccountId']);
    }

    public function testConvertAdsets()
    {
        $raw = [
            [
                'id' => 'set_1',
                'name' => 'AdSet 1',
                'campaign_id' => 'camp_1',
                'status' => 'PAUSED'
            ]
        ];

        $result = FacebookMarketingConvert::adsets($raw, 123);
        
        $this->assertCount(1, $result);
        $entity = $result->first();
        $this->assertEquals('set_1', $entity->platformId);
        $this->assertEquals('camp_1', $entity->channeledCampaignId);
    }

    public function testConvertAds()
    {
        $raw = [
            [
                'id' => 'ad_1',
                'name' => 'Ad 1',
                'adset_id' => 'set_1',
                'creative' => ['id' => 'cr_1']
            ]
        ];

        $result = FacebookMarketingConvert::ads($raw, 123);
        
        $this->assertCount(1, $result);
        $entity = $result->first();
        $this->assertEquals('ad_1', $entity->platformId);
        $this->assertEquals('cr_1', $entity->channeledCreativeId);
    }
}
