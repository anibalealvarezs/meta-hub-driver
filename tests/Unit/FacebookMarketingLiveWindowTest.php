<?php

declare(strict_types=1);

namespace Tests\Unit;

use Anibalealvarezs\FacebookGraphApi\Enums\MetricBreakdown;
use Anibalealvarezs\FacebookGraphApi\Enums\MetricSet;
use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\MetaHubDriver\Drivers\FacebookMarketingDriver;
use PHPUnit\Framework\TestCase;

class FacebookMarketingLiveWindowTest extends TestCase
{
    public function testFetchLiveWindowAppliesMonthlyGroupingAndCampaignKeywordFilter(): void
    {
        $fakeApi = new class extends FacebookGraphApi {
            public array $capturedCalls = [];

            public function __construct() {}

            public function getAdAccountInsights(
                string $adAccountId,
                int $limit = 100,
                MetricBreakdown|array|string|null $metricBreakdown = null,
                MetricSet $metricSet = MetricSet::BASIC,
                array $additionalParams = [],
                array $customMetrics = []
            ): array {
                $this->capturedCalls[] = compact('adAccountId', 'limit', 'additionalParams', 'customMetrics');

                return [
                    'data' => [
                        ['date_start' => '2026-09-01', 'date_stop' => '2026-09-30', 'reach' => '12500'],
                        ['date_start' => '2026-10-01', 'date_stop' => '2026-10-31', 'reach' => '18400'],
                    ],
                ];
            }
        };

        $driver = new class($fakeApi) extends FacebookMarketingDriver {
            public function __construct(private FacebookGraphApi $fakeApi) {}

            protected function initializeApi(array $config): FacebookGraphApi
            {
                return $this->fakeApi;
            }
        };

        $rows = $driver->fetchLiveWindow(
            config: ['access_token' => 'test'],
            platformId: '123456',
            metrics: ['reach'],
            startDate: '2026-09-01',
            endDate: '2026-10-31',
            dimensions: ['month'],
            filters: [
                'channeledAccount' => 1,
                'campaign_name' => ['operator' => 'contains', 'value' => 'promo'],
            ]
        );

        $this->assertCount(1, $fakeApi->capturedCalls);
        $params = $fakeApi->capturedCalls[0]['additionalParams'];
        $this->assertSame('monthly', $params['time_increment']);
        $this->assertSame('account', $params['level']);
        $this->assertSame(
            [['field' => 'campaign.name', 'operator' => 'CONTAIN', 'value' => 'promo']],
            json_decode($params['filtering'], true)
        );

        $this->assertSame([
            ['month' => '2026-09', 'reach' => 12500.0],
            ['month' => '2026-10', 'reach' => 18400.0],
        ], $rows);
    }
}
