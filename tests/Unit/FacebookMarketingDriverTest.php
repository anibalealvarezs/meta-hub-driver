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
}
