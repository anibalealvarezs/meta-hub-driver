<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Services;

use Anibalealvarezs\ApiDriverCore\Classes\AssetRegistry;
use Anibalealvarezs\ApiSkeleton\Enums\Channel;
use DateTime;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Log\LoggerInterface;

class MetaInitializerService
{
    private EntityManagerInterface $entityManager;
    private ?LoggerInterface $logger;

    public function __construct(EntityManagerInterface $entityManager, ?LoggerInterface $logger = null)
    {
        $this->entityManager = $entityManager;
        $this->logger = $logger;
    }

    public function initialize(string $channel, array $config, array $apiData): array
    {
        $stats = ['initialized' => 0, 'skipped' => 0];

        // We assume $apiData contains 'pages' if it's organic/general
        // and 'ad_accounts' if it's marketing

        if (isset($apiData['pages'])) {
             $res = $this->initializePages($channel, $config, $apiData['pages']);
             $stats['initialized'] += $res['initialized'];
             $stats['skipped'] += $res['skipped'];
        }

        if (isset($apiData['ad_accounts'])) {
             $res = $this->initializeAdAccounts($channel, $config, $apiData['ad_accounts']);
             $stats['initialized'] += $res['initialized'];
             $stats['skipped'] += $res['skipped'];
        }

        return $stats;
    }

    private function initializePages(string $channel, array $config, array $pages): array
    {
        $stats = ['initialized' => 0, 'skipped' => 0];
        
        // Resolve classes from host
        // In a real modular app, these should be interfaces or provided via a container
        // For now, we use the known host namespaces
        $pageClass = '\Entities\Analytics\Page';
        $accountClass = '\Entities\Analytics\Account';
        $channeledAccountClass = '\Entities\Analytics\Channeled\ChanneledAccount';
        $pageTypeClass = '\Enums\PageType';
        $accountEnumClass = '\Enums\Account';

        if (!class_exists($pageClass)) return $stats;

        $pageRepository = $this->entityManager->getRepository($pageClass);
        $accountRepository = $this->entityManager->getRepository($accountClass);
        $channeledAccountRepository = $this->entityManager->getRepository($channeledAccountClass);

        $fbGroupName = $config['accounts_group_name'] ?? 'Default Meta Group';
        $accountEntity = $accountRepository->findOneBy(['name' => $fbGroupName]);
        if (!$accountEntity) {
            $accountEntity = new $accountClass();
            $accountEntity->addName($fbGroupName);
            $this->entityManager->persist($accountEntity);
            $this->entityManager->flush();
        }

        foreach ($pages as $page) {
            $platformId = (string)$page['id'];
            $title = $page['name'] ?? "Page " . $platformId;

            if (\Anibalealvarezs\ApiDriverCore\Helpers\Helpers::isAssetFiltered($title, $config, 'PAGE')) {
                $this->logger?->info("Skipping filtered FB Page: $title");
                continue;
            }

            $pageUrl = $page['url'] ?? "https://www.facebook.com/" . $platformId;
            $hostname = $page['hostname'] ?? 'www.facebook.com';

            $typeEnum = defined("$pageTypeClass::FACEBOOK_PAGE") ? constant("$pageTypeClass::FACEBOOK_PAGE") : 'FACEBOOK_PAGE';
            $canonicalId = AssetRegistry::getCanonicalId($pageUrl, $platformId, $typeEnum);

            $pageEntity = $pageRepository->findOneBy(['canonicalId' => $canonicalId]);
            $isNew = false;
            if (!$pageEntity) {
                $pageEntity = new $pageClass();
                $pageEntity->addCanonicalId($canonicalId)
                    ->addPlatformId($platformId)
                    ->addAccount($accountEntity);
                $isNew = true;
            }

            $pageEntity->addUrl($pageUrl)
                ->addTitle($title)
                ->addHostname($hostname)
                ->addData($page)
                ->addUpdatedAt(new DateTime());

            $this->entityManager->persist($pageEntity);
            
            if ($isNew) $stats['initialized']++; else $stats['skipped']++;

            // Initialize ChanneledAccount for Page
            $fbChanneledAccount = $channeledAccountRepository->findOneBy([
                'platformId' => $platformId, 
                'channel' => Channel::facebook_organic->value
            ]);
            if (!$fbChanneledAccount) {
                $fbChanneledAccount = new $channeledAccountClass();
                $fbChanneledAccount->addPlatformId($platformId)
                    ->addAccount($accountEntity)
                    ->addType(defined("$accountEnumClass::FACEBOOK_PAGE") ? constant("$accountEnumClass::FACEBOOK_PAGE") : 'FACEBOOK_PAGE')
                    ->addChannel(Channel::facebook_organic->value)
                    ->addName($title)
                    ->addPlatformCreatedAt(new DateTime('2004-02-04'))
                    ->addData($page);
                $this->entityManager->persist($fbChanneledAccount);
            }

            // Initialize Instagram Account if present
            $igId = $page['instagram_business_account']['id'] ?? $page['ig_account'] ?? null;
            if ($igId) {
                $igChanneledAccount = $channeledAccountRepository->findOneBy([
                    'platformId' => (string)$igId, 
                    'channel' => Channel::facebook_organic->value
                ]);
                $igData = $page['instagram_business_account'] ?? ['id' => $igId];
                $igName = $igData['name'] ?? $igData['username'] ?? "IG " . $igId;
                
                if (!$igChanneledAccount) {
                    $igChanneledAccount = new $channeledAccountClass();
                    $igChanneledAccount->addPlatformId((string)$igId)
                        ->addAccount($accountEntity)
                        ->addType(defined("$accountEnumClass::INSTAGRAM") ? constant("$accountEnumClass::INSTAGRAM") : 'INSTAGRAM')
                        ->addChannel(Channel::facebook_organic->value)
                        ->addName($igName)
                        ->addPlatformCreatedAt(new DateTime('2010-10-06'))
                        ->addData($igData);
                    $this->entityManager->persist($igChanneledAccount);
                }
            }
        }

        $this->entityManager->flush();
        return $stats;
    }

    private function initializeAdAccounts(string $channel, array $config, array $adAccounts): array
    {
        $stats = ['initialized' => 0, 'skipped' => 0];
        
        $accountClass = '\Entities\Analytics\Account';
        $channeledAccountClass = '\Entities\Analytics\Channeled\ChanneledAccount';
        $accountEnumClass = '\Enums\Account';

        if (!class_exists($channeledAccountClass)) return $stats;

        $accountRepository = $this->entityManager->getRepository($accountClass);
        $channeledAccountRepository = $this->entityManager->getRepository($channeledAccountClass);

        $fbGroupName = $config['accounts_group_name'] ?? 'Default Meta Group';
        $accountEntity = $accountRepository->findOneBy(['name' => $fbGroupName]);
        if (!$accountEntity) {
            $accountEntity = new $accountClass();
            $accountEntity->addName($fbGroupName);
            $this->entityManager->persist($accountEntity);
            $this->entityManager->flush();
        }

        foreach ($adAccounts as $adAccount) {
            $adAccountId = (string)$adAccount['id'];
            $name = $adAccount['name'] ?? "Ad Account " . $adAccountId;

            if (\Anibalealvarezs\ApiDriverCore\Helpers\Helpers::isAssetFiltered($name, $config, 'AD_ACCOUNT')) {
                $this->logger?->info("Skipping filtered FB Ad Account: $name");
                continue;
            }
            
            $adAccEntity = $channeledAccountRepository->findOneBy([
                'platformId' => $adAccountId,
                'channel' => Channel::facebook_marketing->value
            ]);

            if (!$adAccEntity) {
                $adAccEntity = new $channeledAccountClass();
                $adAccEntity->addPlatformId($adAccountId)
                    ->addAccount($accountEntity)
                    ->addType(defined("$accountEnumClass::META_AD_ACCOUNT") ? constant("$accountEnumClass::META_AD_ACCOUNT") : 'META_AD_ACCOUNT')
                    ->addChannel(Channel::facebook_marketing->value)
                    ->addName($name)
                    ->addPlatformCreatedAt(new DateTime('2010-10-06'))
                    ->addData($adAccount);
                $this->entityManager->persist($adAccEntity);
                $stats['initialized']++;
            } else {
                $adAccEntity->addName($name)->addData($adAccount);
                $this->entityManager->persist($adAccEntity);
                $stats['skipped']++;
            }
        }

        $this->entityManager->flush();
        return $stats;
    }
}
