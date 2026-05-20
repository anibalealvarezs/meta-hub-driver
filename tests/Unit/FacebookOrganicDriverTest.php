<?php

    namespace Tests\Unit;

    use Anibalealvarezs\MetaHubDriver\Drivers\FacebookOrganicDriver;
    use PHPUnit\Framework\TestCase;
    use Psr\Log\LoggerInterface;
    use DateTime;

    class FacebookOrganicDriverTest extends TestCase
    {
        private FacebookOrganicDriver $driver;

        protected function setUp(): void
        {
            $logger = $this->createMock(LoggerInterface::class);
            $this->driver = new FacebookOrganicDriver(null, $logger);
        }

        public function testGetChannel()
        {
            $this->assertEquals('facebook_organic', $this->driver->getChannel());
        }

        public function testGetConfigSchema()
        {
            $schema = $this->driver->getConfigSchema();
            $this->assertArrayHasKey('global', $schema);
            $this->assertArrayHasKey('entity', $schema);
        }

        public function testGetAggregationProfiles()
        {
            $profiles = FacebookOrganicDriver::getAggregationProfiles();

            $this->assertNotEmpty($profiles);
            $this->assertSame('facebook_organic', $profiles[0]['channel']);
            $this->assertArrayHasKey('group_patterns', $profiles[0]);
            $this->assertArrayHasKey('filter_contract', $profiles[0]);
            $this->assertArrayHasKey('reducer_strategies', $profiles[0]);
        }

        public function testGetCanonicalMetricDictionary()
        {
            $dictionary = FacebookOrganicDriver::getCanonicalMetricDictionary();

            $this->assertArrayHasKey('likes', $dictionary);
            $this->assertArrayHasKey('comments', $dictionary);
            $this->assertArrayHasKey('reach', $dictionary);
            $this->assertContains('likes_daily', $dictionary['likes']);
            $this->assertContains('post_reach', $dictionary['reach']);
        }

        public function testMetaSyncDriverTraitMethods()
        {
            // 1. Updatable credentials
            $creds = $this->driver->getUpdatableCredentials();
            $this->assertContains('FACEBOOK_USER_TOKEN', $creds);
            $this->assertContains('FACEBOOK_USER_ID', $creds);

            // 2. Common config keys & labels
            $this->assertEquals('facebook', FacebookOrganicDriver::getCommonConfigKey());
            $this->assertEquals('Meta', FacebookOrganicDriver::getProviderLabel());
            $this->assertEquals('facebook', FacebookOrganicDriver::getProviderName());

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
            $mapping = FacebookOrganicDriver::getEnvMapping();
            $this->assertArrayHasKey('facebook', $mapping);
            $this->assertEquals('app_id', $mapping['facebook']['FACEBOOK_APP_ID']);
            $this->assertEquals('app_secret', $mapping['facebook']['FACEBOOK_APP_SECRET']);

            // 6. Date filter mapping
            $this->assertEquals([], $this->driver->getDateFilterMapping());
        }

        public function testResetThrowsExceptionWithoutCallback()
        {
            $this->expectException(\Exception::class);
            $this->expectExceptionMessage('Reset callback not provided for facebook_organic');
            
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
            $this->assertEquals('facebook_organic', $res['channel']);
            $this->assertEquals('custom_mode', $res['mode']);
            $this->assertTrue($res['success']);
        }
    }
