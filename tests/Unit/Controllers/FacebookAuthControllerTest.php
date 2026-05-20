<?php

namespace Tests\Unit\Controllers;

use Anibalealvarezs\MetaHubDriver\Controllers\FacebookAuthController;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\RedirectResponse;
use Symfony\Component\HttpFoundation\Response;

class FacebookAuthControllerTest extends TestCase
{
    protected function setUp(): void
    {
        $_SERVER['HTTP_HOST'] = 'localhost:8000';
        $_SERVER['HTTPS'] = 'off';
        unset($_SERVER['HTTP_X_FORWARDED_PROTO']);
    }

    public function testConstructorDeduceRedirectUri()
    {
        $_ENV['FACEBOOK_APP_ID'] = '123';
        $_ENV['FACEBOOK_APP_SECRET'] = 'secret';
        $_ENV['FACEBOOK_REDIRECT_URI'] = 'https://custom.callback/url';

        $controller = new FacebookAuthController();
        
        $ref = new \ReflectionProperty($controller, 'redirectUri');
        $ref->setAccessible(true);
        $this->assertEquals('https://custom.callback/url', $ref->getValue($controller));
    }

    public function testLoginViewFallback()
    {
        $controller = new FacebookAuthController();
        $response = $controller->login();

        $this->assertInstanceOf(Response::class, $response);
        $this->assertEquals(200, $response->getStatusCode());
        $this->assertStringContainsString('text/html', $response->headers->get('Content-Type'));
        $this->assertStringContainsString('Meta', $response->getContent());
    }

    public function testStartWithoutClientIdRedirectsToLogin()
    {
        $_ENV['FACEBOOK_APP_ID'] = '';
        unset($_ENV['FACEBOOK_APP_ID']);

        // Use reflection to bypass config loading
        $controller = new FacebookAuthController();
        $refId = new \ReflectionProperty($controller, 'clientId');
        $refId->setAccessible(true);
        $refId->setValue($controller, '');

        $response = $controller->start();
        $this->assertInstanceOf(RedirectResponse::class, $response);
        $this->assertEquals('/fb-login?error=invalid_config', $response->getTargetUrl());
    }

    public function testStartWithClientIdRedirectsToFacebook()
    {
        $controller = new FacebookAuthController();
        $refId = new \ReflectionProperty($controller, 'clientId');
        $refId->setAccessible(true);
        $refId->setValue($controller, '999111');

        $refUri = new \ReflectionProperty($controller, 'redirectUri');
        $refUri->setAccessible(true);
        $refUri->setValue($controller, 'https://test.local/callback');

        $response = $controller->start();
        $this->assertInstanceOf(RedirectResponse::class, $response);
        $target = $response->getTargetUrl();

        $this->assertStringStartsWith('https://www.facebook.com/v25.0/dialog/oauth', $target);
        $this->assertStringContainsString('client_id=999111', $target);
        $this->assertStringContainsString('redirect_uri=https%3A%2F%2Ftest.local%2Fcallback', $target);
        $this->assertStringContainsString('response_type=code', $target);
        $this->assertStringContainsString('scope=', $target);
    }

    public function testCallbackWithoutCode()
    {
        $controller = new FacebookAuthController();
        $request = new Request();
        
        $response = $controller->callback($request);
        $this->assertInstanceOf(Response::class, $response);
        $this->assertEquals(400, $response->getStatusCode());
        $this->assertEquals('Authorization code missing.', $response->getContent());
    }
}
