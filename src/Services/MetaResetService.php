<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Services;

use Doctrine\ORM\EntityManagerInterface;
use Anibalealvarezs\ApiSkeleton\Enums\Channel;

class MetaResetService
{
    private EntityManagerInterface $entityManager;

    public function __construct(EntityManagerInterface $entityManager)
    {
        $this->entityManager = $entityManager;
    }

    public function reset(string $channelName, string $mode = 'all'): array
    {
        $connection = $this->entityManager->getConnection();
        $enum = Channel::tryFromName($channelName);
        if (!$enum) {
            return ['error' => "Unknown channel: $channelName"];
        }

        $channelId = $enum->value;
        $channelSlug = $enum->name;

        $stats = ['cleared' => 0];

        if ($mode === 'all' || $mode === 'metrics') {
            $connection->executeStatement(
                "DELETE FROM jobs WHERE channel = ? AND entity = 'metric'",
                [$channelSlug],
                [\Doctrine\DBAL\ParameterType::STRING]
            );

            $connection->executeStatement("
                DELETE FROM channeled_metrics WHERE metric_id IN (
                    SELECT m.id FROM metrics m 
                    JOIN metric_configs mc ON m.metric_config_id = mc.id 
                    WHERE mc.channel = ?
                )", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);

            $connection->executeStatement("
                DELETE FROM metrics WHERE metric_config_id IN (
                    SELECT id FROM metric_configs WHERE channel = ?
                )", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);

            $connection->executeStatement("DELETE FROM metric_configs WHERE channel = ?", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);
        }

        if ($mode === 'all' || $mode === 'entities') {
            $connection->executeStatement(
                "DELETE FROM jobs WHERE channel = ? AND entity != 'metric'",
                [$channelSlug],
                [\Doctrine\DBAL\ParameterType::STRING]
            );

            $connection->executeStatement("DELETE FROM channeled_ads WHERE channel = ?", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);
            $connection->executeStatement("DELETE FROM channeled_ad_groups WHERE channel = ?", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);
            $connection->executeStatement("DELETE FROM channeled_campaigns WHERE channel = ?", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);

            $connection->executeStatement("
                DELETE FROM posts 
                WHERE channeled_account_id IN (SELECT id FROM channeled_accounts WHERE channel = ?)
            ", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);

            $connection->executeStatement("
                DELETE FROM pages 
                WHERE account_id IN (SELECT id FROM channeled_accounts WHERE channel = ?)
            ", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);

            $connection->executeStatement("DELETE FROM channeled_accounts WHERE channel = ?", [$channelId], [\Doctrine\DBAL\ParameterType::INTEGER]);
        }

        return $stats;
    }
}
