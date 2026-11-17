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
    public function testFetchLiveWindowAppliesMonthlyGroupingAndNestedChanneledCampaignFilter(): void
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
            dimensions: ['monthly'],
            filters: [
                'channeledAccount' => 1,
                'channeledCampaign' => [['operator' => 'like', 'value' => 'sculpt']],
            ]
        );

        $this->assertCount(1, $fakeApi->capturedCalls);
        $params = $fakeApi->capturedCalls[0]['additionalParams'];
        $this->assertSame('monthly', $params['time_increment']);
        $this->assertSame('account', $params['level']);
        $this->assertSame(
            [['field' => 'campaign.name', 'operator' => 'CONTAIN', 'value' => 'sculpt']],
            json_decode($params['filtering'], true)
        );

        $this->assertSame([
            ['monthly' => '2026-09', 'reach' => 12500.0],
            ['monthly' => '2026-10', 'reach' => 18400.0],
        ], $rows);
    }

    public function testFetchLiveWindowSupportsWeeklyQuarterlySemiannualAndAnnuallyBuckets(): void
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
                $this->capturedCalls[] = $additionalParams;

                return ['data' => [['reach' => '5000']]];
            }
        };

        $driver = new class($fakeApi) extends FacebookMarketingDriver {
            public function __construct(private FacebookGraphApi $fakeApi) {}

            protected function initializeApi(array $config): FacebookGraphApi
            {
                return $this->fakeApi;
            }
        };

        $qRows = $driver->fetchLiveWindow([], '123', ['reach'], '2026-07-01', '2026-12-15', ['quarterly'], []);
        $this->assertCount(2, $fakeApi->capturedCalls);
        $this->assertSame('{"since":"2026-07-01","until":"2026-09-30"}', $fakeApi->capturedCalls[0]['time_range']);
        $this->assertSame('{"since":"2026-10-01","until":"2026-12-15"}', $fakeApi->capturedCalls[1]['time_range']);
        $this->assertSame('all_days', $fakeApi->capturedCalls[0]['time_increment']);
        $this->assertSame([
            ['quarterly' => '2026-Q3', 'reach' => 5000.0],
            ['quarterly' => '2026-Q4', 'reach' => 5000.0],
        ], $qRows);

        $fakeApi->capturedCalls = [];
        $sRows = $driver->fetchLiveWindow([], '123', ['reach'], '2026-01-01', '2026-12-31', ['semiannual'], []);
        $this->assertCount(2, $fakeApi->capturedCalls);
        $this->assertSame('{"since":"2026-01-01","until":"2026-06-30"}', $fakeApi->capturedCalls[0]['time_range']);
        $this->assertSame('{"since":"2026-07-01","until":"2026-12-31"}', $fakeApi->capturedCalls[1]['time_range']);
        $this->assertSame([
            ['semiannual' => '2026-S1', 'reach' => 5000.0],
            ['semiannual' => '2026-S2', 'reach' => 5000.0],
        ], $sRows);

        $fakeApi->capturedCalls = [];
        $aRows = $driver->fetchLiveWindow([], '123', ['reach'], '2025-06-01', '2026-06-01', ['annually'], []);
        $this->assertCount(2, $fakeApi->capturedCalls);
        $this->assertSame('{"since":"2025-06-01","until":"2025-12-31"}', $fakeApi->capturedCalls[0]['time_range']);
        $this->assertSame('{"since":"2026-01-01","until":"2026-06-01"}', $fakeApi->capturedCalls[1]['time_range']);
        $this->assertSame([
            ['annually' => '2025', 'reach' => 5000.0],
            ['annually' => '2026', 'reach' => 5000.0],
        ], $aRows);

        $fakeApi->capturedCalls = [];
        $wRows = $driver->fetchLiveWindow([], '123', ['reach'], '2026-11-02', '2026-11-15', ['weekly'], []);
        $this->assertCount(2, $fakeApi->capturedCalls);
        $this->assertSame('{"since":"2026-11-02","until":"2026-11-08"}', $fakeApi->capturedCalls[0]['time_range']);
        $this->assertSame('{"since":"2026-11-09","until":"2026-11-15"}', $fakeApi->capturedCalls[1]['time_range']);
        $this->assertSame([
            ['weekly' => '2026-W45', 'reach' => 5000.0],
            ['weekly' => '2026-W46', 'reach' => 5000.0],
        ], $wRows);

        $fakeApi->capturedCalls = [];
        $lRows = $driver->fetchLiveWindow([], '123', ['reach'], '2026-01-01', '2026-11-15', [], []);
        $this->assertCount(1, $fakeApi->capturedCalls);
        $this->assertSame('all_days', $fakeApi->capturedCalls[0]['time_increment']);
        $this->assertSame([['reach' => 5000.0]], $lRows);
    }
}
