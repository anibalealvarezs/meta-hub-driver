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

        /** @dataProvider followerSnapshotCases */
        public function testFollowerSnapshotsUseTheObservationDate(bool $recent, array $responses, array $expected, ?string $target = null, int $runs = 1): void
        {
            $api = $this->createMock(\Anibalealvarezs\FacebookGraphApi\FacebookGraphApi::class);
            $api->method('getFacebookPageInsights')->willReturn([]);
            $api->method('getDailyInstagramAccountTotalValueInsights')->willReturn([]);
            $api->expects($this->exactly($recent ? count($responses) * $runs : 0))->method('performRequest')
                ->willReturnCallback(function ($method, $endpoint, $query, ...$arguments) use ($responses) {
                    $this->assertSame('GET', $method);
                    $this->assertSame(['fields' => 'followers_count'], $query);
                    $this->assertArrayHasKey($endpoint, $responses);
                    $payload = $responses[$endpoint];
                    if ($payload instanceof \Exception) {
                        throw $payload;
                    }

                    return new \GuzzleHttp\Psr7\Response(200, [], json_encode($payload));
                });

            $driver = $this->getMockBuilder(FacebookOrganicDriver::class)
                ->setConstructorArgs([null, $this->createMock(LoggerInterface::class)])
                ->onlyMethods(['initializeApi'])
                ->getMock();
            $driver->method('initializeApi')->willReturn($api);
            $values = [];
            $driver->setDataProcessor(function ($metrics) use (&$values): array {
                foreach ($metrics as $metric) {
                    $this->assertSame('follower_total', $metric->name);
                    $this->assertSame('lifetime', $metric->period);
                    $this->assertSame(date('Y-m-d'), substr($metric->date, 0, 10));
                    $values[] = [(int) $metric->value, $metric->metricConfigKey, $metric->metricDate];
                }

                return ['metrics' => count($metrics)];
            });

            $config = ['pages' => [['id' => '123', 'ig_account' => '456', 'page_metrics' => true, 'ig_account_metrics' => true]]];
            if ($target !== null) {
                $config['account_id'] = $target;
            }
            for ($run = 0; $run < $runs; $run++) {
                $driver->syncMetrics(new DateTime('-2 weeks'), new DateTime($recent ? 'yesterday' : '-1 week'), $config);
            }

            $this->assertSame(array_merge(...array_fill(0, $runs, $expected)), array_column($values, 0));
            if ($runs > 1) {
                $this->assertSame(array_slice($values, 0, count($expected)), array_slice($values, count($expected)));
            }
        }

        public static function followerSnapshotCases(): array
        {
            return [
                'both accounts, including explicit zero' => [true, [123 => ['followers_count' => 0], 456 => ['followers_count' => '12']], [0, 12]],
                'missing and failed responses' => [true, [123 => [], 456 => new \RuntimeException('Unavailable')], []],
                'historical sync' => [false, [], []],
                'Instagram-only target and same-day retry' => [true, [456 => ['followers_count' => 12]], [12], '456', 2],
            ];
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
    }
