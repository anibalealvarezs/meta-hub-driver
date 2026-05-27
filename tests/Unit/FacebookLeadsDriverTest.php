<?php

namespace Tests\Unit;

use Anibalealvarezs\MetaHubDriver\Drivers\FacebookLeadsDriver;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use DateTime;

class FacebookLeadsDriverTest extends TestCase
{
    private FacebookLeadsDriver $driver;

    protected function setUp(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $this->driver = new FacebookLeadsDriver(null, $logger);
    }

    public function testGetChannel()
    {
        $this->assertEquals('facebook_leads', $this->driver->getChannel());
    }

    public function testGetPlatformEntityIdField()
    {
        $this->assertEquals('facebook_page_id', FacebookLeadsDriver::getPlatformEntityIdField());
    }

    public function testChanneledAccountableInterfaceMethods()
    {
        $asset = [
            'id' => '12345',
            'created_time' => '2026-05-26T12:00:00+0000',
            'title' => 'Test Page',
            'data' => ['key' => 'val'],
        ];

        $this->assertEquals('12345', FacebookLeadsDriver::getChanneledAccountPlatformId($asset));
        $this->assertEquals('2026-05-26T12:00:00+0000', FacebookLeadsDriver::getChanneledAccountPlatformCreatedAt($asset));
        $this->assertEquals('Test Page', FacebookLeadsDriver::getChanneledAccountName($asset));
        $this->assertEquals('facebook_page', FacebookLeadsDriver::getChanneledAccountType());
        $this->assertEquals(['key' => 'val'], FacebookLeadsDriver::getChanneledAccountData($asset));
    }

    public function testSync()
    {
        // 1. Setup Auth and API Mocks
        $authMock = $this->createMock(\Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface::class);
        $authMock->method('getTokenRefresherCallback')->willReturn(null);
        $authMock->method('getAccessToken')->willReturn('fake_token');
        $authMock->method('getUserId')->willReturn('fake_user_id');
        $authMock->method('hasCredentials')->willReturn(true);

        $driver = $this->getMockBuilder(FacebookLeadsDriver::class)
            ->setConstructorArgs([$authMock, null])
            ->onlyMethods(['getApi'])
            ->getMock();

        $apiMock = $this->createMock(\Anibalealvarezs\FacebookGraphApi\FacebookGraphApi::class);
        $apiMock->method('getPageLeadgenForms')->willReturn([
            'data' => [
                ['id' => 'form_1', 'name' => 'Form 1']
            ]
        ]);
        $apiMock->method('getFormLeads')->willReturn([
            'data' => [
                [
                    'id' => 'lead_1',
                    'created_time' => '2026-05-26T12:00:00+0000',
                    'field_data' => [
                        [
                            'name' => 'email',
                            'values' => ['john@example.com']
                        ]
                    ]
                ]
            ],
            'paging' => []
        ]);

        $driver->method('getApi')->willReturn($apiMock);

        // 2. Setup Data Processor Mock
        $processedEntities = null;
        $processor = function($entities, $type) use (&$processedEntities) {
            $processedEntities = $entities;
        };
        $driver->setDataProcessor($processor);

        $response = $driver->sync(
            new DateTime('2026-05-25'),
            new DateTime('2026-05-26'),
            [
                'pages' => [
                    ['id' => 'page_123', 'enabled' => true]
                ]
            ]
        );

        $this->assertEquals(200, $response->getStatusCode());
        $this->assertNotNull($processedEntities);
        $this->assertCount(1, $processedEntities);

        $customer = $processedEntities->first();
        $this->assertEquals('lead_1', $customer->platformId);
        $this->assertEquals('john@example.com', $customer->email);
        $this->assertEquals([
            [
                'origin' => 'leadgen_form',
                'platform_id' => 'form_1',
                'timestamp' => '2026-05-26T12:00:00+0000',
            ]
        ], $customer->lead_data);
    }
}
