<?php

namespace Tests\Unit;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\MetaHubDriver\Drivers\FacebookMarketingDriver;
use PHPUnit\Framework\TestCase;

class FacebookMarketingNoBreakdownTest extends TestCase
{
    /** @dataProvider baselineSelections */
    public function testExplicitDisableNeverReactivatesOptionalPasses(mixed $selection): void
    {
        $driver = new FacebookMarketingDriver();
        $method = new \ReflectionMethod($driver, 'resolveBreakdownPasses');
        $passes = $method->invoke($driver, [], ['breakdown_passes' => $selection]);
        $this->assertSame(['unbroken'], array_keys($passes));
        $this->assertSame([], $passes['unbroken']['breakdowns']);
        $this->assertSame([], $passes['unbroken']['dimension_keys']);
    }

    public static function baselineSelections(): array
    {
        return [['none'], [[]], [false], [''], [['none']], [['demographics' => false, 'placement' => false]], [['none', 'demographics']]];
    }

    public function testAccountDisableOverridesChannelDemographics(): void
    {
        $method = new \ReflectionMethod(FacebookMarketingDriver::class, 'resolveBreakdownPasses');
        $this->assertSame(['unbroken'], array_keys($method->invoke(new FacebookMarketingDriver(), ['breakdown_passes' => []], ['breakdown_passes' => 'demographics'])));
    }

    public function testBaselineReachesRealSdkWithoutAudienceBreakdownsAtEveryEntityLevel(): void
    {
        // Exercise real SDK query construction, replacing only the HTTP boundary.
        $api = new class extends FacebookGraphApi {
            public array $queries = [];
            public function __construct() {}
            protected function executeMarketingInsightsRequest(string $adAccountId, array $query): array
            {
                $this->queries[] = $query;
                return ['data' => []];
            }
        };
        $driver = new FacebookMarketingDriver();
        $resolver = new \ReflectionMethod($driver, 'resolveBreakdownPasses');
        $fetch = new \ReflectionMethod($driver, 'fetchInsights');
        $config = ['breakdown_passes' => 'none', 'breakdowns' => ['age', 'gender']];
        $passes = $resolver->invoke($driver, [], $config);
        foreach (['account', 'campaign', 'adset', 'ad'] as $level) {
            $fetch->invoke($driver, $api, 'act_test', '2026-10-01', '2026-10-02', $config, $level, null, $passes['unbroken']['breakdowns']);
        }
        $this->assertCount(4, $api->queries);
        foreach ($api->queries as $query) {
            $this->assertArrayNotHasKey('breakdowns', $query);
            $this->assertArrayHasKey('fields', $query);
            $this->assertSame('action_type', $query['action_breakdowns']);
        }
    }

    public function testConfigurationRoundTripKeepsNoneAndEmptyLists(): void
    {
        $driver = new FacebookMarketingDriver();
        foreach (['none', []] as $selection) {
            $updated = $driver->updateConfiguration(['type' => 'global', 'breakdown_passes' => $selection], ['channels' => ['facebook_marketing' => ['breakdown_passes' => 'demographics']]]);
            $this->assertSame($selection, $updated['channels']['facebook_marketing']['breakdown_passes']);
        }
    }
}
