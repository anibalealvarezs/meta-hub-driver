<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Auth;

use Anibalealvarezs\ApiSkeleton\Auth\BaseAuthProvider;

class FacebookAuthProvider extends BaseAuthProvider
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

    public function isExpired(): bool
    {
        $expiry = $this->data['facebook_auth']['expires_at'] ?? null;
        if (!$expiry) return false;
        return strtotime($expiry) < time();
    }
}
