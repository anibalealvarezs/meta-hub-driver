<?php

namespace Anibalealvarezs\MetaHubDriver\Auth;

use Anibalealvarezs\ApiSkeleton\Interfaces\AuthProviderInterface;

class FacebookAuthProvider implements AuthProviderInterface
{
    private array $credentials = [];
    private string $tokenPath;

    public function __construct(?string $tokenPath = null, array $config = [])
    {
        // $projectDir is now relative to the host project when using as a driver
        $this->tokenPath = $tokenPath ?? $_ENV['FACEBOOK_TOKEN_PATH'] ?? getcwd() . '/storage/tokens/facebook_tokens.json';
        $this->loadCredentials();
    }

    private function loadCredentials(): void
    {
        if (file_exists($this->tokenPath)) {
            $tokens = json_decode(file_get_contents($this->tokenPath), true) ?? [];
            // We use 'facebook_auth' as our unified key, but fallback to 'facebook_marketing' for BC
            $this->credentials = $tokens['facebook_auth'] ?? $tokens['facebook_marketing'] ?? [];
        }
    }

    public function getAccessToken(): string
    {
        return $this->credentials['access_token'] ?? '';
    }

    public function isValid(): bool
    {
        return !empty($this->credentials['access_token']);
    }

    public function isExpired(): bool
    {
        if (empty($this->credentials['expires_at'])) {
            return false; // Long lived tokens might not have an explicit expiry in some cases
        }

        return strtotime($this->credentials['expires_at']) <= (time() + 3600); // 1h buffer
    }

    public function refresh(): bool
    {
        // Facebook tokens are refreshed via long-lived exchange.
        // Usually, Facade handles the initial long-lived exchange.
        return false; 
    }

    public function getScopes(): array
    {
        return $this->credentials['scopes'] ?? [];
    }

    public function getUserId(): ?string
    {
        return $this->credentials['user_id'] ?? null;
    }

    public function setAccessToken(string $token): void
    {
        $this->credentials['access_token'] = $token;
        $this->saveCredentials();
    }

    private function saveCredentials(): void
    {
        $tokens = file_exists($this->tokenPath) ? (json_decode(file_get_contents($this->tokenPath), true) ?? []) : [];
        $tokens['facebook_auth'] = array_merge($tokens['facebook_auth'] ?? [], $this->credentials);
        $tokens['facebook_auth']['updated_at'] = date('Y-m-d H:i:s');
        
        file_put_contents($this->tokenPath, json_encode($tokens, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
    }
}
