<?php

namespace Tests\Unit;

use Anibalealvarezs\MetaHubDriver\Drivers\FacebookMarketingDriver;
use Anibalealvarezs\MetaHubDriver\Enums\MetaSyncScope;
use Anibalealvarezs\MetaHubDriver\Enums\MetaEntityType;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use DateTime;

class FacebookMarketingDriverTest extends TestCase
{
    private $driver;
    private $logger;

    protected function setUp(): void
    {
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->driver = new FacebookMarketingDriver(null, $this->logger);
    }

    public function testGetConfigSchema()
    {
        $schema = $this->driver->getConfigSchema();
        $this->assertArrayHasKey('global', $schema);
        $this->assertArrayHasKey('entity', $schema);
    }

    public function testSyncFailsWithoutAuth()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('AuthProvider not set');
        
        $this->driver->sync(new DateTime(), new DateTime());
    }

    public function testGetCanonicalMetricDictionary()
    {
        $dictionary = FacebookMarketingDriver::getCanonicalMetricDictionary();

        $this->assertArrayHasKey('conversions', $dictionary);
        $this->assertArrayHasKey('roas_purchase', $dictionary);
        $this->assertContains('results', $dictionary['conversions']);
        $this->assertContains('purchase_roas', $dictionary['roas_purchase']);
    }

    public function testMetaSyncDriverTraitMethods()
    {
        // 1. Updatable credentials
        $creds = $this->driver->getUpdatableCredentials();
        $this->assertContains('FACEBOOK_USER_TOKEN', $creds);
        $this->assertContains('FACEBOOK_USER_ID', $creds);

        // 2. Common config keys & labels
        $this->assertEquals('facebook', FacebookMarketingDriver::getCommonConfigKey());
        $this->assertEquals('Meta', FacebookMarketingDriver::getProviderLabel());
        $this->assertEquals('facebook', FacebookMarketingDriver::getProviderName());

        // 3. Auth provider injection
        $authMock = $this->createMock(\Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface::class);
        $this->driver->setAuthProvider($authMock);
        $this->assertSame($authMock, $this->driver->getAuthProvider());

        // 4. Data processor injection
        $hasRun = false;
        $processor = function() use (&$hasRun) { $hasRun = true; };
        $this->driver->setDataProcessor($processor);
        $ref = new \ReflectionProperty($this->driver, 'dataProcessor');
        $ref->setAccessible(true);
        $injectedProcessor = $ref->getValue($this->driver);
        $this->assertSame($processor, $injectedProcessor);
        $injectedProcessor();
        $this->assertTrue($hasRun);

        // 5. Env mapping
        $mapping = FacebookMarketingDriver::getEnvMapping();
        $this->assertArrayHasKey('facebook', $mapping);
        $this->assertEquals('app_id', $mapping['facebook']['FACEBOOK_APP_ID']);
        $this->assertEquals('app_secret', $mapping['facebook']['FACEBOOK_APP_SECRET']);

        // 6. Date filter mapping
        $this->assertEquals([], $this->driver->getDateFilterMapping());
    }

    public function testResetThrowsExceptionWithoutCallback()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Reset callback not provided for facebook_marketing');
        
        $this->driver->reset();
    }

    public function testResetInvokesCallback()
    {
        $callbackCalled = false;
        $config = [
            'resetCallback' => function($channel, $mode) use (&$callbackCalled) {
                $callbackCalled = true;
                return ['channel' => $channel, 'mode' => $mode, 'success' => true];
            }
        ];

        $res = $this->driver->reset('custom_mode', $config);
        $this->assertTrue($callbackCalled);
        $this->assertEquals('facebook_marketing', $res['channel']);
        $this->assertEquals('custom_mode', $res['mode']);
        $this->assertTrue($res['success']);
    }

    public function testValidateConfig()
    {
        $inputConfig = [
            'metrics_strategy' => 'custom_strategy',
            'AD_ACCOUNT' => [
                'campaign_metrics' => true,
                'adset_metrics' => false
            ],
            'entity' => [
                'pages' => ['limit' => 50]
            ]
        ];

        $validated = $this->driver->validateConfig($inputConfig);

        $this->assertEquals('custom_strategy', $validated['metrics_strategy']);
        $this->assertTrue($validated['AD_ACCOUNT']['campaign_metrics']);
        $this->assertFalse($validated['AD_ACCOUNT']['adset_metrics']);
        $this->assertEquals(50, $validated['entity']['pages']['limit']);
    }

    public function testValidateAuthenticationWithoutCredentials()
    {
        // 1. Without AuthProvider set
        $res = $this->driver->validateAuthentication();
        $this->assertFalse($res['success']);
        $this->assertStringContainsString('Credentials not configured', $res['message']);

        // 2. With AuthProvider set but no credentials
        $authMock = $this->createMock(\Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface::class);
        $authMock->method('hasCredentials')->willReturn(false);
        $this->driver->setAuthProvider($authMock);

        $res = $this->driver->validateAuthentication();
        $this->assertFalse($res['success']);
    }

    public function testValidateAuthenticationSuccess()
    {
        $authMock = $this->createMock(\Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface::class);
        $authMock->method('hasCredentials')->willReturn(true);

        $apiMock = $this->createMock(\Anibalealvarezs\FacebookGraphApi\FacebookGraphApi::class);
        $apiMock->method('getUserId')->willReturn('fb_user_123');

        $driverMock = $this->getMockBuilder(FacebookMarketingDriver::class)
            ->setConstructorArgs([null, $this->logger])
            ->onlyMethods(['initializeApi'])
            ->getMock();

        $driverMock->method('initializeApi')->willReturn($apiMock);
        $driverMock->setAuthProvider($authMock);

        $res = $driverMock->validateAuthentication();
        $this->assertTrue($res['success']);
        $this->assertEquals('fb_user_123', $res['details']['user_id']);
    }

    public function testValidateAuthenticationException()
    {
        $authMock = $this->createMock(\Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface::class);
        $authMock->method('hasCredentials')->willReturn(true);

        $driverMock = $this->getMockBuilder(FacebookMarketingDriver::class)
            ->setConstructorArgs([null, $this->logger])
            ->onlyMethods(['initializeApi'])
            ->getMock();

        $driverMock->method('initializeApi')->willThrowException(new \Exception("API connection timeout"));
        $driverMock->setAuthProvider($authMock);

        $res = $driverMock->validateAuthentication();
        $this->assertFalse($res['success']);
        $this->assertEquals('API connection timeout', $res['message']);
    }

    public function testStoreCredentials()
    {
        $tempPath = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'fb_store_tokens_' . uniqid() . '.json';
        $_ENV['FACEBOOK_TOKEN_PATH'] = $tempPath;

        try {
            FacebookMarketingDriver::storeCredentials([
                'access_token' => 'custom_token',
                'user_id' => 'custom_user_id',
                'scopes' => ['ads_read', 'business_management']
            ]);

            $this->assertFileExists($tempPath);
            $content = json_decode(file_get_contents($tempPath), true);

            $this->assertArrayHasKey('facebook_auth', $content);
            $authData = $content['facebook_auth'];

            $this->assertEquals('custom_token', $authData['access_token']);
            $this->assertEquals('custom_user_id', $authData['user_id']);
            $this->assertEquals(['ads_read', 'business_management'], $authData['scopes']);
            $this->assertNotEmpty($authData['updated_at']);
            $this->assertNotEmpty($authData['expires_at']);

        } finally {
            if (file_exists($tempPath)) {
                unlink($tempPath);
            }
            unset($_ENV['FACEBOOK_TOKEN_PATH']);
        }
    }

    public function testInitializeApiPassesTokenRefresherCallback()
    {
        $authMock = $this->createMock(\Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface::class);
        $callback = function () { return 'new_token'; };
        $authMock->method('getTokenRefresherCallback')->willReturn($callback);
        $authMock->method('getAccessToken')->willReturn('fake_token');
        $authMock->method('getUserId')->willReturn('fake_user_id');
        $authMock->method('hasCredentials')->willReturn(true);

        $this->driver->setAuthProvider($authMock);

        $ref = new \ReflectionMethod($this->driver, 'initializeApi');
        $ref->setAccessible(true);

        /** @var \Anibalealvarezs\FacebookGraphApi\FacebookGraphApi $api */
        $api = $ref->invoke($this->driver, []);

        $apiRef = new \ReflectionProperty($api, 'tokenRefresherCallback');
        $apiRef->setAccessible(true);
        
        $this->assertSame($callback, $apiRef->getValue($api));
    }

    public function testInitializeApiWithNullTokenRefresherCallback()
    {
        $authMock = $this->createMock(\Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface::class);
        $authMock->method('getTokenRefresherCallback')->willReturn(null);
        $authMock->method('getAccessToken')->willReturn('fake_token');
        $authMock->method('getUserId')->willReturn('fake_user_id');
        $authMock->method('hasCredentials')->willReturn(true);

        $this->driver->setAuthProvider($authMock);

        $ref = new \ReflectionMethod($this->driver, 'initializeApi');
        $ref->setAccessible(true);

        /** @var \Anibalealvarezs\FacebookGraphApi\FacebookGraphApi $api */
        $api = $ref->invoke($this->driver, []);

        $apiRef = new \ReflectionProperty($api, 'tokenRefresherCallback');
        $apiRef->setAccessible(true);
        
        $this->assertNull($apiRef->getValue($api));
    }

    public function testFilterInsightRowsMetricExtraction()
    {
        $ref = new \ReflectionMethod($this->driver, 'filterInsightRows');
        $ref->setAccessible(true);

        // 1. Fully populated metrics scenario
        $populatedRow = [
            "impressions" => "595",
            "spend" => "4.34",
            "results" => [
                [
                    "indicator" => "conversions:offsite_conversion.fb_pixel_custom.TypeformSubmit",
                    "values" => [["value" => "1"]]
                ]
            ],
            "cost_per_result" => [
                [
                    "indicator" => "conversions:offsite_conversion.fb_pixel_custom.TypeformSubmit",
                    "values" => [["value" => "4.34"]]
                ]
            ],
            "result_rate" => [
                [
                    "indicator" => "conversions:offsite_conversion.fb_pixel_custom.TypeformSubmit",
                    "values" => [["value" => "0.16806723"]]
                ]
            ],
            "actions" => [
                ["action_type" => "offsite_conversion.fb_pixel_custom", "value" => "1"],
            ]
        ];

        $filteredPopulated = $ref->invoke($this->driver, [$populatedRow], 'AD', []);
        $resultPopulated = $filteredPopulated[0];

        $this->assertEquals(1.0, $resultPopulated['results']);
        $this->assertEquals(4.34, $resultPopulated['cost_per_result']);
        $this->assertEquals(0.16806723, $resultPopulated['result_rate']);

        // 2. Empty metrics scenario (fallback test)
        $emptyRow = [
            "impressions" => "5",
            "spend" => "0.03",
            "results" => [
                ["indicator" => "conversions:offsite_conversion.fb_pixel_custom.TypeformSubmit"]
            ],
            "cost_per_result" => [
                ["indicator" => "conversions:offsite_conversion.fb_pixel_custom.TypeformSubmit"]
            ],
            "result_rate" => [
                ["indicator" => "conversions:offsite_conversion.fb_pixel_custom.TypeformSubmit"]
            ],
            // In the empty case, actions might also not contain the target action, leading to 0 results
            "actions" => []
        ];

        $filteredEmpty = $ref->invoke($this->driver, [$emptyRow], 'AD', []);
        $resultEmpty = $filteredEmpty[0];

        $this->assertEquals(0, $resultEmpty['results']);
        $this->assertEquals(0, $resultEmpty['cost_per_result']);
        $this->assertEquals(0, $resultEmpty['result_rate']);
        $this->assertEquals(0, $resultEmpty['purchase_roas'] ?? 0); // Testing the missing key fallback

        // 3. Calculated fallback scenario
        $fallbackRow = [
            "impressions" => "1000",
            "spend" => "50",
            "results" => [],
            "cost_per_result" => [],
            "result_rate" => [],
            "actions" => [
                ["action_type" => "purchase", "value" => "5"],
            ]
        ];

        $filteredFallback = $ref->invoke($this->driver, [$fallbackRow], 'AD', []);
        $resultFallback = $filteredFallback[0];

        // results comes from actions fallback = 5
        $this->assertEquals(5.0, $resultFallback['results']);
        // cost_per_result = spend / results = 50 / 5 = 10
        $this->assertEquals(10.0, $resultFallback['cost_per_result']);
        // result_rate = (results / impressions) * 100 = (5 / 1000) * 100 = 0.5
        $this->assertEquals(0.5, $resultFallback['result_rate']);
    }
}
