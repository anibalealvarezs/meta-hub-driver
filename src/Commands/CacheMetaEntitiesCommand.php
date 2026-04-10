<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Commands;

use Anibalealvarezs\ApiSkeleton\Enums\Channel;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(
    name: 'meta:cache:entities',
    description: 'Syncs Meta entities (Facebook & Instagram) to database'
)]
class CacheMetaEntitiesCommand extends Command
{
    private $logger;

    public function __construct($logger = null)
    {
        $this->logger = $logger;
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('jobId', null, InputOption::VALUE_OPTIONAL, 'The ID of the job associating this execution')
             ->addOption('startDate', null, InputOption::VALUE_OPTIONAL, 'Start date for filtering (e.g. YYYY-MM-DD)')
             ->addOption('endDate', null, InputOption::VALUE_OPTIONAL, 'End date for filtering (e.g. YYYY-MM-DD)')
             ->addOption('channel', 'c', InputOption::VALUE_OPTIONAL, 'Specific channel (facebook_marketing or facebook_organic). If empty, syncs both.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $jobId = $input->getOption('jobId') ? (int) $input->getOption('jobId') : null;
        $startDate = $input->getOption('startDate');
        $endDate = $input->getOption('endDate');
        $requestedChannel = $input->getOption('channel');

        $channels = $requestedChannel ? [$requestedChannel] : ['facebook_organic', 'facebook_marketing'];

        try {
            // We use the host's sync service if available, or fallback to driver directly
            // For now, to keep it functional in apis-hub, we assume SyncService exists
            $syncServiceClass = '\Core\Services\SyncService';
            if (!class_exists($syncServiceClass)) {
                throw new \Exception("SyncService not found. This command currently requires the apis-hub host environment.");
            }

            $syncService = new $syncServiceClass($this->logger);

            foreach ($channels as $chanKey) {
                $output->writeln("<info>🚀 Syncing Meta Entities for channel: {$chanKey}...</info>");
                
                $entities = ($chanKey === 'facebook_organic') 
                    ? ['pages', 'posts'] 
                    : ['campaigns', 'ad_groups', 'ads'];

                foreach ($entities as $entity) {
                    $output->writeln("  - Syncing <comment>{$entity}</comment>...");
                    $syncService->execute($chanKey, $startDate, $endDate, [
                        'jobId' => $jobId,
                        'entity' => $entity,
                    ]);
                }
            }

            $output->writeln('<info>✅ Meta entities cache sync completed successfully</info>');
            return Command::SUCCESS;

        } catch (\Exception $e) {
            $output->writeln('<error>Error: ' . $e->getMessage() . '</error>');
            return Command::FAILURE;
        }
    }
}
