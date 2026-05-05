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
    }
