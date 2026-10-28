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

    public function testNormalizeRowActionsAndConvert()
    {
        $raw = [
            [
                'campaign_id' => 'camp_1',
                'campaign_name' => 'My Campaign',
                'date_start' => '2026-01-01',
                'spend' => '100.0',
                'actions' => [
                    ['action_type' => 'omni_purchase', 'value' => '4'],
                    ['action_type' => 'lead', 'value' => '10'],
                ],
                'action_values' => [
                    ['action_type' => 'omni_purchase', 'value' => '450.0'],
                ],
                'cost_per_action_type' => [
                    ['action_type' => 'omni_purchase', 'value' => '25.0'],
                    ['action_type' => 'lead', 'value' => '10.0'],
                ],
            ]
        ];

        $result = FacebookMarketingMetricConvert::campaignMetrics(
            rows: $raw,
            channeledAccount: 'ca_123',
            channeledCampaign: 'cc_123',
            dimensions: [] // Pass 0 (Unbroken)
        );

        $metricsByName = [];
        foreach ($result as $m) {
            $metricsByName[$m->name] = $m;
        }

        $this->assertArrayHasKey('purchases', $metricsByName);
        $this->assertEquals(4.0, $metricsByName['purchases']->value);

        $this->assertArrayHasKey('purchase_value', $metricsByName);
        $this->assertEquals(450.0, $metricsByName['purchase_value']->value);

        $this->assertArrayHasKey('cost_per_purchase', $metricsByName);
        $this->assertEquals(25.0, $metricsByName['cost_per_purchase']->value);

        $this->assertArrayHasKey('leads', $metricsByName);
        $this->assertEquals(10.0, $metricsByName['leads']->value);

        $this->assertArrayHasKey('cost_per_lead', $metricsByName);
        $this->assertEquals(10.0, $metricsByName['cost_per_lead']->value);

        // Verify dimensions is empty array for Pass 0
        $this->assertEmpty($metricsByName['purchases']->dimensions);
    }
}
