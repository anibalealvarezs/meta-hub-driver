<?php

namespace Tests\Unit;

use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
use Anibalealvarezs\MetaHubDriver\Services\FacebookEntitySync;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Response;

class FacebookEntitySyncTest extends TestCase
{
    private $api;
    private $logger;

    protected function setUp(): void
    {
        $this->api = $this->createMock(FacebookGraphApi::class);
        $this->logger = $this->createMock(LoggerInterface::class);
    }

    public function testSyncCampaignsBasic()
    {
        $config = [
            'ad_accounts' => [
                'acc_123' => [
                    'enabled' => true,
                    \Anibalealvarezs\MetaHubDriver\Enums\MetaFeature::CAMPAIGNS->value => true
                ]
            ]
        ];
        $startDate = '2026-01-01';
        $endDate = '2026-01-07';
        
        // Mock getCampaignsAndProcess to just call the callback once
        $this->api->expects($this->once())
            ->method('getCampaignsAndProcess')
            ->willReturnCallback(function($callback, $adAccountId, $limit) {
                $callback([
                    [
                        'id' => '12345',
                        'name' => 'Test Campaign',
                        'status' => 'ACTIVE'
                    ]
                ]);
            });

        // Mock channeledAccount
        $channeledAccount = $this->getMockBuilder(\stdClass::class)
            ->addMethods(['getPlatformId', 'getId', 'getAccount'])
            ->getMock();
        $channeledAccount->method('getPlatformId')->willReturn('acc_123');
        $channeledAccount->method('getId')->willReturn(1);

        $response = FacebookEntitySync::syncCampaigns(
            api: $this->api,
            config: $config,
            startDate: $startDate,
            endDate: $endDate,
            logger: $this->logger,
            channeledAccounts: [$channeledAccount]
        );

        $this->assertInstanceOf(Response::class, $response);
        $this->assertEquals(200, $response->getStatusCode());
        $this->assertStringContainsString('Campaigns synchronized', $response->getContent());
    }

    public function testMatchesFilter()
    {
        // 1. Basic include
        $this->assertTrue(FacebookEntitySync::matchesFilter('Test Campaign', 'Test', null));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Other Campaign', 'Test', null));

        // 2. Multiple includes (comma-separated)
        $this->assertTrue(FacebookEntitySync::matchesFilter('X123', 'X, Y', null));
        $this->assertTrue(FacebookEntitySync::matchesFilter('Y456', 'X, Y', null));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Z789', 'X, Y', null));

        // 3. Basic exclude
        $this->assertTrue(FacebookEntitySync::matchesFilter('Campaign A', null, 'B'));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Campaign B', null, 'B'));

        // 4. Multiple excludes
        $this->assertFalse(FacebookEntitySync::matchesFilter('Campaign X', null, 'X, Y'));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Campaign Y', null, 'X, Y'));
        $this->assertTrue(FacebookEntitySync::matchesFilter('Campaign Z', null, 'X, Y'));

        // 5. Regex
        $this->assertTrue(FacebookEntitySync::matchesFilter('Campaign 123', '/\d+/', null));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Campaign ABC', '/\d+/', null));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Test 123', null, '/\d+/'));

        // 6. Both include and exclude
        $this->assertTrue(FacebookEntitySync::matchesFilter('Apple Pie', 'Apple', 'Orange'));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Apple Pie', 'Orange', null));
        $this->assertFalse(FacebookEntitySync::matchesFilter('Apple Pie', 'Apple', 'Pie'));
    }

    public function testSyncAdGroups()
    {
        $config = [
            'ad_accounts' => [
                'acc_123' => [
                    'enabled' => true,
                    \Anibalealvarezs\MetaHubDriver\Enums\MetaFeature::ADSETS->value => true
                ]
            ]
        ];
        
        $this->api->expects($this->once())
            ->method('getAdsetsAndProcess')
            ->willReturnCallback(function($callback) {
                $callback([['id' => 'ads_1', 'name' => 'AdSet 1']]);
            });

        $channeledAccount = $this->getMockBuilder(\stdClass::class)
            ->addMethods(['getPlatformId', 'getId'])
            ->getMock();
        $channeledAccount->method('getPlatformId')->willReturn('acc_123');
        $channeledAccount->method('getId')->willReturn(1);

        $response = FacebookEntitySync::syncAdGroups(
            api: $this->api,
            config: $config,
            channeledAccounts: [$channeledAccount]
        );

        $this->assertEquals(200, $response->getStatusCode());
        $this->assertStringContainsString('AdGroups synchronized', $response->getContent());
    }

    public function testJobCancellation()
    {
        $config = [
            'ad_accounts' => [
                'acc_123' => [
                    'enabled' => true,
                    \Anibalealvarezs\MetaHubDriver\Enums\MetaFeature::CAMPAIGNS->value => true
                ]
            ]
        ];

        $channeledAccount = $this->getMockBuilder(\stdClass::class)
            ->addMethods(['getPlatformId', 'getId'])
            ->getMock();
        $channeledAccount->method('getPlatformId')->willReturn('acc_123');

        $jobStatusChecker = function() {
            return false; // Stop immediately
        };

        $response = FacebookEntitySync::syncCampaigns(
            api: $this->api,
            config: $config,
            channeledAccounts: [$channeledAccount],
            jobStatusChecker: $jobStatusChecker
        );

        $this->assertEquals(500, $response->getStatusCode());
        $this->assertStringContainsString('Job cancelled', $response->getContent());
    }

    public function testSyncPosts()
    {
        $config = [
            'pages' => [
                'page_123' => ['enabled' => true, \Anibalealvarezs\MetaHubDriver\Enums\MetaFeature::POSTS->value => true]
            ]
        ];

        $this->api->expects($this->once())
            ->method('getFacebookPostsAndProcess')
            ->willReturnCallback(function($callback) {
                $callback([['id' => 'post_1', 'message' => 'Hello World', 'created_time' => '2026-01-01T12:00:00+0000']]);
            });

        $channeledPage = $this->getMockBuilder(\stdClass::class)
            ->addMethods(['getPlatformId', 'getId', 'getAccount'])
            ->getMock();
        $channeledPage->method('getPlatformId')->willReturn('page_123');
        $channeledPage->method('getId')->willReturn(10);

        $response = FacebookEntitySync::syncPosts(
            api: $this->api,
            config: $config,
            channeledPages: [$channeledPage]
        );

        $this->assertEquals(200, $response->getStatusCode());
    }

    public function testSyncInstagramMedia()
    {
        $config = [
            'pages' => [
                'page_123' => [
                    'id' => 'page_123',
                    'ig_account' => 'ig_999',
                    'enabled' => true,
                    \Anibalealvarezs\MetaHubDriver\Enums\MetaFeature::IG_ACCOUNT_MEDIA->value => true
                ]
            ]
        ];

        $this->api->expects($this->once())
            ->method('getInstagramMediaAndProcess')
            ->willReturnCallback(function($callback) {
                $callback([['id' => 'media_1', 'caption' => 'Insta Pic', 'timestamp' => '2026-01-01T12:00:00+0000']]);
            });

        $channeledAccount = $this->getMockBuilder(\stdClass::class)
            ->addMethods(['getPlatformId', 'getId'])
            ->getMock();
        $channeledAccount->method('getPlatformId')->willReturn('ig_999');
        $channeledAccount->method('getId')->willReturn(20);

        $response = FacebookEntitySync::syncInstagramMedia(
            api: $this->api,
            config: $config,
            channeledAccounts: [$channeledAccount]
        );

        $this->assertEquals(200, $response->getStatusCode());
    }
}

