<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Services;

use Anibalealvarezs\ApiDriverCore\Classes\AssetRegistry;
use Anibalealvarezs\MetaHubDriver\Enums\MetaEntityType;
use Psr\Log\LoggerInterface;

class MetaInitializerService
{
    private ?LoggerInterface $logger;

    public function __construct(?LoggerInterface $logger = null)
    {
        $this->logger = $logger;
    }

    /**
     * Initializes Meta assets by resolving identities and persisting them via callbacks.
     */
    public function initialize(
        string $channel,
        array $config,
        array $apiData,
        callable $identityMapper,
        callable $dataProcessor
    ): array {
        $stats = ['initialized' => 0, 'skipped' => 0];

        if (isset($apiData['pages'])) {
             $res = $this->initializePages($channel, $config, $apiData['pages'], $identityMapper, $dataProcessor);
             $stats['initialized'] += $res['initialized'];
             $stats['skipped'] += $res['skipped'];
        }

        if (isset($apiData['ad_accounts'])) {
             $res = $this->initializeAdAccounts($channel, $config, $apiData['ad_accounts'], $identityMapper, $dataProcessor);
             $stats['initialized'] += $res['initialized'];
             $stats['skipped'] += $res['skipped'];
        }

        return $stats;
    }

    private function initializePages(
        string $channel,
        array $config,
        array $pages,
        callable $identityMapper,
        callable $dataProcessor
    ): array {
        $stats = ['initialized' => 0, 'skipped' => 0];
        $fbPIds = array_map(fn($p) => (string)$p['id'], $pages);
        $igPIds = array_filter(array_map(fn($p) => $p['instagram_business_account']['id'] ?? $p['ig_account'] ?? null, $pages));
        
        $urls = array_map(fn($p) => $p['url'] ?? "https://www.facebook.com/" . $p['id'], $pages);
        $igUrls = array_map(fn($id) => "https://www.instagram.com/" . $id, $igPIds);

        // 1. Batch Resolve Identities
        $pageMap = $identityMapper('pages', ['platform_ids' => array_unique(array_merge($fbPIds, $igPIds))]) ?? [];
        $caMap = $identityMapper('channeled_accounts', ['platform_ids' => array_unique(array_merge($fbPIds, $igPIds))]) ?? [];
        
        $fbGroupName = $config['accounts_group_name'] ?? 'Default Meta Group';
        $accountMap = $identityMapper('accounts', ['names' => [$fbGroupName]]) ?? [];
        $defaultAccount = $accountMap[$fbGroupName] ?? null;

        foreach ($pages as $page) {
            $platformId = (string)$page['id'];
            $title = $page['title'] ?? $page['name'] ?? "Page " . $platformId;

            if (
                \Anibalealvarezs\ApiDriverCore\Helpers\Helpers::isAssetFiltered($title, $config, 'PAGE')
                || !$this->isAssetEnabled($platformId, $config)
            ) {
                continue;
            }

            $pageUrl = $page['url'] ?? "https://www.facebook.com/" . $platformId;
            $hostname = $page['website'] ?? $page['hostname'] ?? 'facebook.com';
            $canonicalId = AssetRegistry::getCanonicalId($pageUrl, $platformId, MetaEntityType::PAGE->value, $hostname);

            $pageEntity = $pageMap[$platformId] ?? null;
            $caFb = $caMap[$platformId] ?? null;

            $isNew = false;
            $toPersist = new \Doctrine\Common\Collections\ArrayCollection();

            // 1. Resolve/Create FB Page
            if (!$pageEntity) {
                $pageEntity = new \Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity();
                $pageEntity->setPlatformId($platformId)
                    ->setCanonicalId($canonicalId)
                    ->setTitle($title)
                    ->setUrl($pageUrl)
                    ->setHostname($hostname)
                    ->setData($page);
                
                if ($defaultAccount) {
                    $pageEntity->setContext(['account' => $defaultAccount]);
                }
                $toPersist->add($pageEntity);
                $isNew = true;
            }

            // 2. Resolve/Create FB ChanneledAccount
            if (!$caFb) {
                $caFb = new \Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity();
                $caFb->setPlatformId($platformId)
                    ->setChannel($channel)
                    ->setType(MetaEntityType::PAGE->value)
                    ->setTitle($title)
                    ->setContext(['page' => $pageEntity, 'account' => $defaultAccount]);
                $toPersist->add($caFb);
            }

            // 3. Handle Instagram if present
            $igId = $page['instagram_business_account']['id'] ?? $page['ig_account'] ?? null;
            if ($igId && $this->isAssetEnabled((string)$igId, $config)) {
                $igId = (string)$igId;
                $igData = $page['instagram_business_account'] ?? ['id' => $igId];
                $igName = $igData['name'] ?? $igData['username'] ?? "IG " . $igId;
                $igUrl = "https://www.instagram.com/" . ($igData['username'] ?? $igId);
                $igCanonicalId = AssetRegistry::getCanonicalId($igUrl, $igId, MetaEntityType::INSTAGRAM_ACCOUNT->value, 'instagram.com');

                $igPage = $pageMap[$igId] ?? null;
                $caIg = $caMap[$igId] ?? null;

                if (!$igPage) {
                    $igPage = new \Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity();
                    $igPage->setPlatformId($igId)
                        ->setCanonicalId($igCanonicalId)
                        ->setTitle($igName)
                        ->setUrl($igUrl)
                        ->setHostname('instagram.com')
                        ->setData($igData);
                    if ($defaultAccount) {
                        $igPage->setContext(['account' => $defaultAccount]);
                    }
                    $toPersist->add($igPage);
                }

                if (!$caIg) {
                    $caIg = new \Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity();
                    $caIg->setPlatformId($igId)
                        ->setChannel($channel)
                        ->setType(MetaEntityType::INSTAGRAM_ACCOUNT->value)
                        ->setTitle($igName)
                        ->setContext(['page' => $igPage, 'account' => $defaultAccount]);
                    $toPersist->add($caIg);
                }
            }

            if ($toPersist->count() > 0) {
                $dataProcessor($toPersist, 'initialization');
            }

            if ($isNew) $stats['initialized']++; else $stats['skipped']++;
        }

        return $stats;
    }

    private function initializeAdAccounts(
        string $channel,
        array $config,
        array $adAccounts,
        callable $identityMapper,
        callable $dataProcessor
    ): array {
        $stats = ['initialized' => 0, 'skipped' => 0];
        $ids = array_map(fn($a) => (string)$a['id'], $adAccounts);

        // 1. Batch Resolve Identities
        $caMap = $identityMapper('channeled_accounts', ['platform_ids' => $ids]) ?? [];
        $fbGroupName = $config['accounts_group_name'] ?? 'Default Meta Group';
        $accountMap = $identityMapper('accounts', ['names' => [$fbGroupName]]) ?? [];
        $defaultAccount = $accountMap[$fbGroupName] ?? null;

        foreach ($adAccounts as $adAccount) {
            $adAccountId = (string)$adAccount['id'];
            $name = $adAccount['name'] ?? "Ad Account " . $adAccountId;

            if (
                \Anibalealvarezs\ApiDriverCore\Helpers\Helpers::isAssetFiltered($name, $config, 'AD_ACCOUNT')
                || !$this->isAssetEnabled($adAccountId, $config)
            ) {
                continue;
            }

            $ca = $caMap[$adAccountId] ?? null;

            if (!$ca) {
                $ca = new \Anibalealvarezs\ApiDriverCore\Classes\UniversalEntity();
                $ca->setPlatformId($adAccountId)
                    ->setChannel($channel)
                    ->setType(MetaEntityType::META_AD_ACCOUNT->value)
                    ->setTitle($name)
                    ->setData($adAccount);
                
                if ($defaultAccount) {
                    $ca->setContext(['account' => $defaultAccount]);
                }

                $collection = new \Doctrine\Common\Collections\ArrayCollection([$ca]);
                $dataProcessor($collection, 'initialization');
                $stats['initialized']++;
            } else {
                $stats['skipped']++;
            }
        }

        return $stats;
    }
    private function isAssetEnabled(string $platformId, array $config): bool
    {
        if (!isset($config['assets']) || !is_array($config['assets'])) {
            return true;
        }

        foreach ($config['assets'] as $asset) {
            if ((string)($asset['platformId'] ?? '') === $platformId) {
                return (bool)($asset['enabled'] ?? true);
            }
        }

        return false;
    }
}
