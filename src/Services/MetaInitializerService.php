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
            $title = $page['title'] ?? $page['name'] ?? "Page " . $platformId;

            if (\Anibalealvarezs\ApiDriverCore\Helpers\Helpers::isAssetFiltered($title, $config, 'PAGE')) {
                $this->logger?->info("Skipping filtered FB Page: $title");
                continue;
            }

            // Resolve specific account for this page, fallback to global
            $pageSpecificAccountName = $page['account'] ?? $config['accounts_group_name'] ?? 'Default Meta Group';
            $pageSpecificAccountEntity = $accountRepository->findOneBy(['name' => $pageSpecificAccountName]);
            if (!$pageSpecificAccountEntity) {
                $pageSpecificAccountEntity = new $accountClass();
                $pageSpecificAccountEntity->addName($pageSpecificAccountName);
                $this->entityManager->persist($pageSpecificAccountEntity);
                $this->entityManager->flush();
            }

            $pageUrl = $page['url'] ?? "https://www.facebook.com/" . $platformId;
            $hostname = $page['website'] ?? $page['hostname'] ?? null;
            if (!$hostname && $pageUrl) {
                $parsed = parse_url($pageUrl);
                $hostname = $parsed['host'] ?? null;
            }
            if ($hostname) {
                $hostname = preg_replace('~^https?://(?:www\.)?~i', '', $hostname);
                $hostname = strtolower(explode('/', $hostname)[0]);
            }

            $typeEnum = defined("$pageTypeClass::FACEBOOK_PAGE") ? constant("$pageTypeClass::FACEBOOK_PAGE") : 'facebook_page';
            $canonicalId = AssetRegistry::getCanonicalId($pageUrl, $platformId, $typeEnum, $hostname);

            $pageEntity = $pageRepository->findOneBy(['canonicalId' => $canonicalId]);
            $isNew = false;
            if (!$pageEntity) {
                $pageEntity = new $pageClass();
                $pageEntity->addCanonicalId($canonicalId)
                    ->addPlatformId($platformId)
                    ->addAccount($pageSpecificAccountEntity);
                $isNew = true;
            }

            $pageEntity->addUrl($pageUrl)
                ->addTitle($title)
                ->addHostname($hostname ?: 'facebook.com')
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
                    ->addAccount($pageSpecificAccountEntity)
                    ->addType(defined("$accountEnumClass::FACEBOOK_PAGE") ? constant("$accountEnumClass::FACEBOOK_PAGE") : 'FACEBOOK_PAGE')
                    ->addChannel(Channel::facebook_organic->value)
                    ->addName($title)
                    ->addPlatformCreatedAt(new DateTime('2004-02-04'))
                    ->addData($page);
            }
            if (method_exists($fbChanneledAccount, 'addPage')) {
                $fbChanneledAccount->addPage($pageEntity);
            }
            $this->entityManager->persist($fbChanneledAccount);

            // Initialize Instagram Account as Page if present
            $igId = $page['instagram_business_account']['id'] ?? $page['ig_account'] ?? null;
            if ($igId) {
                $igData = $page['instagram_business_account'] ?? ['id' => $igId];
                $igName = $igData['name'] ?? $igData['username'] ?? "IG " . $igId;
                $igUrl = "https://www.instagram.com/" . ($igData['username'] ?? $igId);
                
                $igTypeEnum = defined("$pageTypeClass::INSTAGRAM") ? constant("$pageTypeClass::INSTAGRAM") : 'instagram';
                $igCanonicalId = AssetRegistry::getCanonicalId($igUrl, (string)$igId, $igTypeEnum, 'instagram.com');

                $igPageEntity = $pageRepository->findOneBy(['canonicalId' => $igCanonicalId]);
                if (!$igPageEntity) {
                    $igPageEntity = new $pageClass();
                    $igPageEntity->addCanonicalId($igCanonicalId)
                        ->addPlatformId((string)$igId)
                        ->addAccount($pageSpecificAccountEntity);
                    $this->logger?->info("Initialized new IG Page: $igName");
                }

                $igPageEntity->addUrl($igUrl)
                    ->addTitle($igName)
                    ->addHostname($hostname ?: 'instagram.com')
                    ->addData($igData)
                    ->addUpdatedAt(new DateTime());

                $this->entityManager->persist($igPageEntity);

                $igChanneledAccount = $channeledAccountRepository->findOneBy([
                    'platformId' => (string)$igId, 
                    'channel' => Channel::facebook_organic->value
                ]);
                
                if (!$igChanneledAccount) {
                    $igChanneledAccount = new $channeledAccountClass();
                    $igChanneledAccount->addPlatformId((string)$igId)
                        ->addAccount($pageSpecificAccountEntity)
                        ->addType(defined("$accountEnumClass::INSTAGRAM") ? constant("$accountEnumClass::INSTAGRAM") : 'INSTAGRAM')
                        ->addChannel(Channel::facebook_organic->value)
                        ->addName($igName)
                        ->addPlatformCreatedAt(new DateTime('2010-10-06'))
                        ->addData($igData);
                }
                
                if (method_exists($igChanneledAccount, 'addPage')) {
                    $igChanneledAccount->addPage($igPageEntity);
                }
                $this->entityManager->persist($igChanneledAccount);
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

            // Resolve specific account for this ad account, fallback to global
            $accSpecificAccountName = $adAccount['account'] ?? $config['accounts_group_name'] ?? 'Default Meta Group';
            $accSpecificAccountEntity = $accountRepository->findOneBy(['name' => $accSpecificAccountName]);
            if (!$accSpecificAccountEntity) {
                $accSpecificAccountEntity = new $accountClass();
                $accSpecificAccountEntity->addName($accSpecificAccountName);
                $this->entityManager->persist($accSpecificAccountEntity);
                $this->entityManager->flush();
            }
            
            $adAccEntity = $channeledAccountRepository->findOneBy([
                'platformId' => $adAccountId,
                'channel' => Channel::facebook_marketing->value
            ]);

            if (!$adAccEntity) {
                $adAccEntity = new $channeledAccountClass();
                $adAccEntity->addPlatformId($adAccountId)
                    ->addAccount($accSpecificAccountEntity)
                    ->addType(defined("$accountEnumClass::META_AD_ACCOUNT") ? constant("$accountEnumClass::META_AD_ACCOUNT") : 'META_AD_ACCOUNT')
                    ->addChannel(Channel::facebook_marketing->value)
                    ->addName($name)
                    ->addPlatformCreatedAt(new DateTime('2010-10-06'))
                    ->addData($adAccount);
                $this->entityManager->persist($adAccEntity);
                $stats['initialized']++;
            } else {
                $adAccEntity->addName($name)
                    ->addAccount($accSpecificAccountEntity)
                    ->addData($adAccount);
                $this->entityManager->persist($adAccEntity);
                $stats['skipped']++;
            }
        }

        $this->entityManager->flush();
        return $stats;
    }
}
