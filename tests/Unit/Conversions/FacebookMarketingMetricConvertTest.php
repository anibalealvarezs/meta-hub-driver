<?php

namespace Tests\Unit\Conversions;

use Anibalealvarezs\MetaHubDriver\Conversions\FacebookMarketingMetricConvert;
use PHPUnit\Framework\TestCase;
use Doctrine\Common\Collections\ArrayCollection;

class FacebookMarketingMetricConvertTest extends TestCase
{
    public function testConvertAdAccountMetrics()
    {
        $raw = [
            [
                'account_id' => '123',
                'account_name' => 'My Account',
                'date_start' => '2026-01-01',
                'impressions' => '1000',
                'clicks' => '50',
                'spend' => '10.5'
            ]
        ];

        $result = FacebookMarketingMetricConvert::adAccountMetrics(
            rows: $raw,
            channeledAccount: 'ca_123'
        );

        $this->assertInstanceOf(ArrayCollection::class, $result);
        $this->assertCount(3, $result); // impressions, clicks, spend
        
        $metric = $result->first();
        $this->assertNotEmpty($metric->channel);
        $this->assertEquals('2026-01-01', $metric->metricDate);
    }

    public function testConvertCampaignMetrics()
    {
        $raw = [
            [
                'campaign_id' => 'camp_1',
                'campaign_name' => 'My Campaign',
                'date_start' => '2026-01-01',
                'impressions' => '500'
            ]
        ];

        $result = FacebookMarketingMetricConvert::campaignMetrics(
            rows: $raw,
            channeledAccount: 'ca_123',
            channeledCampaign: 'cc_123',
            metricsToProcess: ['impressions']
        );

        $this->assertCount(1, $result);
        $metric = $result->first();
        $this->assertEquals(500, $metric->value);
        $this->assertEquals('impressions', $metric->name);
    }
}
