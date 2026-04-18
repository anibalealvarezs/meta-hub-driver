<?php

namespace Tests\Unit;

use Anibalealvarezs\MetaHubDriver\Drivers\FacebookOrganicDriver;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use DateTime;

class FacebookOrganicDriverTest extends TestCase
{
    private $driver;
    private $logger;

    protected function setUp(): void
    {
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->driver = new FacebookOrganicDriver(null, $this->logger);
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
}
