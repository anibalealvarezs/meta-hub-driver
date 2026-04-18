<?php

namespace Tests\Unit\Conversions;

use Anibalealvarezs\MetaHubDriver\Conversions\FacebookOrganicMetricConvert;
use PHPUnit\Framework\TestCase;

class FacebookOrganicMetricConvertTest extends TestCase
{
    public function testConvertPageMetrics()
    {
        $raw = [
            [
                'name' => 'page_impressions',
                'period' => 'day',
                'values' => [
                    ['value' => 100, 'end_time' => '2026-01-01T08:00:00+0000']
                ]
            ]
        ];

        $result = FacebookOrganicMetricConvert::pageMetrics(
            rows: $raw,
            pagePlatformId: 'page_123',
            channeledAccount: 'ca_1'
        );

        $this->assertCount(1, $result);
        $metric = $result->first();
        $this->assertEquals(100, $metric->value);
        $this->assertEquals('page_impressions', $metric->name);
    }

    public function testConvertIgAccountMetrics()
    {
        $raw = [
            [
                'name' => 'impressions',
                'period' => 'day',
                'total_value' => ['value' => 500]
            ]
        ];

        $result = FacebookOrganicMetricConvert::igAccountMetrics(
            rows: $raw,
            date: '2026-01-01',
            channeledAccount: 'ca_1'
        );

        $this->assertCount(1, $result);
        $metric = $result->first();
        $this->assertEquals(500, $metric->value);
    }

    public function testConvertIgMediaMetrics()
    {
        $raw = [
            [
                'name' => 'engagement',
                'period' => 'lifetime',
                'values' => [['value' => 50]]
            ]
        ];

        $result = FacebookOrganicMetricConvert::igMediaMetrics(
            rows: $raw,
            date: '2026-01-01',
            post: 'media_123',
            channeledAccount: 'ca_1'
        );

        $this->assertCount(1, $result);
        $metric = $result->first();
        $this->assertEquals(50, $metric->value);
    }
}
