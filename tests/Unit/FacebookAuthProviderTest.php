<?php

namespace Tests\Unit;

use Anibalealvarezs\MetaHubDriver\Auth\FacebookAuthProvider;
use PHPUnit\Framework\TestCase;

class FacebookAuthProviderTest extends TestCase
{
    private string $tempFile;

    protected function setUp(): void
    {
        $this->tempFile = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'fb_tokens_' . uniqid() . '.json';
    }

    protected function tearDown(): void
    {
        if (file_exists($this->tempFile)) {
            unlink($this->tempFile);
        }
    }

    public function testConstructorWithString()
    {
        $provider = new FacebookAuthProvider($this->tempFile);
        $this->assertFalse($provider->hasCredentials());
    }

    public function testConstructorWithArray()
    {
        $provider = new FacebookAuthProvider(['token_path' => $this->tempFile]);
        $this->assertFalse($provider->hasCredentials());
    }

    public function testGetAccessTokenAndUserId()
    {
        $data = [
            'facebook_auth' => [
                'access_token' => 'test_token',
                'user_id' => '123456789',
                'expires_at' => date('Y-m-d H:i:s', strtotime('+10 days'))
            ]
        ];
        file_put_contents($this->tempFile, json_encode($data));

        $provider = new FacebookAuthProvider($this->tempFile);
        $this->assertTrue($provider->hasCredentials());
        $this->assertEquals('test_token', $provider->getAccessToken());
        $this->assertEquals('123456789', $provider->getUserId());
    }

    public function testSetAccessToken()
    {
        $provider = new FacebookAuthProvider($this->tempFile);
        $provider->setAccessToken('new_token_value');

        $this->assertEquals('new_token_value', $provider->getAccessToken());

        $reloaded = new FacebookAuthProvider($this->tempFile);
        $this->assertEquals('new_token_value', $reloaded->getAccessToken());
    }

    public function testIsExpired()
    {
        $expiredData = [
            'facebook_auth' => [
                'access_token' => 'expired_token',
                'expires_at' => date('Y-m-d H:i:s', strtotime('-1 day'))
            ]
        ];
        file_put_contents($this->tempFile, json_encode($expiredData));

        $provider = new FacebookAuthProvider($this->tempFile);
        $this->assertTrue($provider->isExpired());

        $validData = [
            'facebook_auth' => [
                'access_token' => 'valid_token',
                'expires_at' => date('Y-m-d H:i:s', strtotime('+1 day'))
            ]
        ];
        file_put_contents($this->tempFile, json_encode($validData));

        $providerValid = new FacebookAuthProvider($this->tempFile);
        $this->assertFalse($providerValid->isExpired());
    }

    public function testGetAuthUrl()
    {
        $data = [
            'app_id' => '999999',
            'app_secret' => 'supersecret'
        ];
        file_put_contents($this->tempFile, json_encode($data));

        $provider = new FacebookAuthProvider($this->tempFile);
        $url = $provider->getAuthUrl('https://example.com/callback', [
            'organic_enabled' => true,
            'marketing_enabled' => true,
            'state' => 'xyz123'
        ]);

        $this->assertStringContainsString('client_id=999999', $url);
        $this->assertStringContainsString('redirect_uri=https%3A%2F%2Fexample.com%2Fcallback', $url);
        $this->assertStringContainsString('state=xyz123', $url);
        $this->assertStringContainsString('scope=', $url);
        $this->assertStringContainsString('public_profile', $url);
        $this->assertStringContainsString('ads_read', $url);
        $this->assertStringContainsString('business_management', $url);
    }
}
