<?php

// Stub the Core\Services\SyncService class if not already defined (i.e. outside host environment)
namespace Core\Services {
    if (!class_exists('Core\Services\SyncService')) {
        class SyncService
        {
            public static array $calls = [];
            private $logger;

            public function __construct($logger = null)
            {
                $this->logger = $logger;
            }

            public function execute(string $chanKey, ?string $startDate, ?string $endDate, array $options = []): void
            {
                self::$calls[] = [
                    'channel' => $chanKey,
                    'startDate' => $startDate,
                    'endDate' => $endDate,
                    'options' => $options
                ];
            }
        }
    }
}

namespace Tests\Unit\Commands {

    use Anibalealvarezs\MetaHubDriver\Commands\CacheMetaEntitiesCommand;
    use PHPUnit\Framework\TestCase;
    use Symfony\Component\Console\Application;
    use Symfony\Component\Console\Tester\CommandTester;
    use Core\Services\SyncService;

    class CacheMetaEntitiesCommandTest extends TestCase
    {
        protected function setUp(): void
        {
            if (class_exists('Core\Services\SyncService')) {
                SyncService::$calls = [];
            }
        }

        public function testConfigure()
        {
            $command = new CacheMetaEntitiesCommand();
            $this->assertEquals('meta:cache:entities', $command->getName());
            $this->assertTrue($command->getDefinition()->hasOption('jobId'));
            $this->assertTrue($command->getDefinition()->hasOption('startDate'));
            $this->assertTrue($command->getDefinition()->hasOption('endDate'));
            $this->assertTrue($command->getDefinition()->hasOption('channel'));
        }

        public function testExecuteSuccessfulWithBothChannels()
        {
            // If class SyncService wasn't loaded, this test will fail gracefully by checking class exists.
            if (!class_exists('Core\Services\SyncService')) {
                $this->markTestSkipped('SyncService stub not available.');
            }

            $application = new Application();
            $application->add(new CacheMetaEntitiesCommand());

            $command = $application->find('meta:cache:entities');
            $commandTester = new CommandTester($command);
            $commandTester->execute([
                '--startDate' => '2026-01-01',
                '--endDate' => '2026-01-10',
                '--jobId' => '42'
            ]);

            $output = $commandTester->getDisplay();
            $this->assertStringContainsString('Syncing Meta Entities for channel: facebook_organic...', $output);
            $this->assertStringContainsString('Syncing Meta Entities for channel: facebook_marketing...', $output);
            $this->assertStringContainsString('Meta entities cache sync completed successfully', $output);
            $this->assertEquals(0, $commandTester->getStatusCode());

            // 2 entities in organic (pages, posts) + 3 entities in marketing (campaigns, ad_groups, ads) = 5 sync calls
            $this->assertCount(5, SyncService::$calls);

            // First call should be facebook_organic pages
            $this->assertEquals('facebook_organic', SyncService::$calls[0]['channel']);
            $this->assertEquals('pages', SyncService::$calls[0]['options']['entity']);
            $this->assertEquals('2026-01-01', SyncService::$calls[0]['startDate']);
            $this->assertEquals('2026-01-10', SyncService::$calls[0]['endDate']);
            $this->assertEquals(42, SyncService::$calls[0]['options']['jobId']);

            // Last call should be facebook_marketing ads
            $this->assertEquals('facebook_marketing', SyncService::$calls[4]['channel']);
            $this->assertEquals('ads', SyncService::$calls[4]['options']['entity']);
        }

        public function testExecuteWithSingleChannel()
        {
            if (!class_exists('Core\Services\SyncService')) {
                $this->markTestSkipped('SyncService stub not available.');
            }

            $application = new Application();
            $application->add(new CacheMetaEntitiesCommand());

            $command = $application->find('meta:cache:entities');
            $commandTester = new CommandTester($command);
            $commandTester->execute([
                '--channel' => 'facebook_organic',
                '--jobId' => '100'
            ]);

            $output = $commandTester->getDisplay();
            $this->assertStringContainsString('Syncing Meta Entities for channel: facebook_organic...', $output);
            $this->assertStringNotContainsString('facebook_marketing', $output);
            $this->assertEquals(0, $commandTester->getStatusCode());

            // Only 2 calls (pages, posts)
            $this->assertCount(2, SyncService::$calls);
            $this->assertEquals('facebook_organic', SyncService::$calls[0]['channel']);
            $this->assertEquals('pages', SyncService::$calls[0]['options']['entity']);
            $this->assertEquals(100, SyncService::$calls[0]['options']['jobId']);
        }
    }
}
