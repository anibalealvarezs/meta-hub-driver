<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Auth;

use Anibalealvarezs\ApiSkeleton\Auth\BaseAuthProvider;
use Anibalealvarezs\ApiSkeleton\Interfaces\OAuthProviderInterface;
use Anibalealvarezs\FacebookGraphApi\Enums\UserPermission;
use Anibalealvarezs\FacebookGraphApi\Enums\PagePermission;

class FacebookAuthProvider extends BaseAuthProvider implements OAuthProviderInterface
{
    public function getAccessToken(): string
    {
        return $this->data['facebook_auth']['access_token'] 
            ?? $this->data['facebook_marketing']['access_token'] 
            ?? "";
    }

    public function getUserId(): string
    {
        return $this->data['facebook_auth']['user_id'] 
            ?? $this->data['facebook_marketing']['user_id'] 
            ?? "";
    }

    public function setAccessToken(string $token): void
    {
        if (!isset($this->data['facebook_auth'])) {
            $this->data['facebook_auth'] = [];
        }
        $this->data['facebook_auth']['access_token'] = $token;
        $this->save();
    }

    /**
     * @inheritdoc
     */
    public function getAuthUrl(string $redirectUri, array $config = []): string
    {
        $clientId = $this->data['app_id'] ?? $_ENV['FACEBOOK_APP_ID'] ?? '';
        
        $scopes = [
            UserPermission::PUBLIC_PROFILE->value,
            UserPermission::EMAIL->value,
        ];

        // Marketing Scopes
        if (!empty($config['ad_accounts']) || ($config['marketing_enabled'] ?? false)) {
            $scopes[] = UserPermission::ADS_READ->value;
            $scopes[] = PagePermission::BUSINESS_MANAGEMENT->value;
        }

        // Organic Pages and Instagram Scopes
        if (!empty($config['pages']) || ($config['organic_enabled'] ?? false)) {
            $scopes[] = PagePermission::PAGES_SHOW_LIST->value;
            $scopes[] = PagePermission::PAGES_READ_ENGAGEMENT->value;
            $scopes[] = PagePermission::PAGES_READ_USER_CONTENT->value;
            $scopes[] = UserPermission::READ_INSIGHTS->value;
            $scopes[] = UserPermission::INSTAGRAM_BASIC->value;
            $scopes[] = UserPermission::INSTAGRAM_MANAGE_INSIGHTS->value;
            $scopes[] = PagePermission::BUSINESS_MANAGEMENT->value;
        }

        $state = $config['state'] ?? bin2hex(random_bytes(16));

        return "https://www.facebook.com/v25.0/dialog/oauth?" . http_build_query([
            'client_id' => $clientId,
            'redirect_uri' => $redirectUri,
            'scope' => implode(',', array_unique($scopes)),
            'response_type' => 'code',
            'state' => $state
        ]);
    }

    /**
     * @inheritdoc
     */
    public function handleCallback(string $code, string $redirectUri): array
    {
        $clientId = $this->data['app_id'] ?? $_ENV['FACEBOOK_APP_ID'] ?? '';
        $clientSecret = $this->data['app_secret'] ?? $_ENV['FACEBOOK_APP_SECRET'] ?? '';

        // 1. Exchange code for Short-Lived User Token (2h)
        $tokenUrl = "https://graph.facebook.com/v25.0/oauth/access_token?" . http_build_query([
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'redirect_uri' => $redirectUri,
            'code' => $code
        ]);

        $response = json_decode(@file_get_contents($tokenUrl), true);
        $shortLivedToken = $response['access_token'] ?? null;

        if (!$shortLivedToken) {
            throw new \Exception("Failed to retrieve access token from Facebook.");
        }

        // 2. Exchange for Long-Lived User Token (60 days)
        $exchangeUrl = "https://graph.facebook.com/v25.0/oauth/access_token?" . http_build_query([
            'grant_type' => 'fb_exchange_token',
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'fb_exchange_token' => $shortLivedToken
        ]);

        $exchangeResponse = json_decode(@file_get_contents($exchangeUrl), true);
        $longLivedToken = $exchangeResponse['access_token'] ?? null;

        if (!$longLivedToken) {
            throw new \Exception("Failed to exchange long-lived token.");
        }

        // 3. Get User ID
        $meUrl = "https://graph.facebook.com/v25.0/me?" . http_build_query([
            'access_token' => $longLivedToken,
            'fields' => 'id'
        ]);
        $meResponse = json_decode(@file_get_contents($meUrl), true);
        $userId = $meResponse['id'] ?? null;

        return [
            'facebook_auth' => [
                'access_token' => $longLivedToken,
                'user_id' => $userId,
                'updated_at' => date('Y-m-d H:i:s'),
                'expires_at' => date('Y-m-d H:i:s', strtotime('+60 days'))
            ]
        ];
    }

    public function isExpired(): bool
    {
        $expiry = $this->data['facebook_auth']['expires_at'] ?? null;
        if (!$expiry) return false;
        return strtotime($expiry) < time();
    }
}
