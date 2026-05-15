<?php

    namespace Anibalealvarezs\MetaHubDriver\Traits;

    use Anibalealvarezs\ApiDriverCore\Auth\BaseAuthProvider;
    use Anibalealvarezs\ApiDriverCore\Interfaces\AuthProviderInterface;
    use Anibalealvarezs\FacebookGraphApi\FacebookGraphApi;
    use Closure;
    use Exception;
    use GuzzleHttp\Exception\GuzzleException;

    trait MetaSyncDriverTrait
    {
        public array $updatableCredentials = [
            'FACEBOOK_USER_TOKEN',
            'FACEBOOK_USER_ID',
            'FACEBOOK_ACCOUNTS_GROUP',
            'FACEBOOK_APP_ID',
            'FACEBOOK_APP_SECRET'
        ];

        public function getUpdatableCredentials(): array
        {
            return $this->updatableCredentials;
        }

        public static function getCommonConfigKey(): ?string
        {
            return 'facebook';
        }

        public static function getProviderLabel(): string
        {
            return 'Meta';
        }

        public static function getProviderName(): string
        {
            return 'facebook';
        }

        public function setAuthProvider(AuthProviderInterface $provider): void
        {
            $this->authProvider = $provider;
        }

        public function getAuthProvider(): ?AuthProviderInterface
        {
            return $this->authProvider;
        }

        public function setDataProcessor(callable $processor): void
        {
            $this->dataProcessor = $processor;
        }

        /**
         * @throws Exception
         */
        public function getApi(array $config = []): FacebookGraphApi
        {
            if (empty($config) && $this->authProvider instanceof BaseAuthProvider) {
                $config = $this->authProvider->getConfig();
            }

            return $this->initializeApi($config);
        }

        /*
         * @param array $credentials
         * @return void
         */
        public static function storeCredentials(array $credentials): void
        {
            $tokenPath = $_ENV['FACEBOOK_TOKEN_PATH'] ?? getcwd().'/storage/tokens/facebook_tokens.json';
            $tokenKey = 'facebook_auth';

            if (!is_dir(dirname($tokenPath))) {
                mkdir(dirname($tokenPath), 0755, true);
            }

            $tokens = file_exists($tokenPath) ? (json_decode(file_get_contents($tokenPath), true) ?? []) : [];

            $tokens[$tokenKey] = [
                'access_token'  => $credentials['access_token'] ?? null,
                'refresh_token' => $credentials['refresh_token'] ?? null,
                'user_id'       => $credentials['user_id'] ?? null,
                'scopes'        => $credentials['scopes'] ?? [],
                'updated_at'    => date('Y-m-d H:i:s'),
                'expires_at'    => date('Y-m-d H:i:s', strtotime('+60 days'))
            ];

            file_put_contents($tokenPath, json_encode($tokens, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        }

        /**
         * @inheritdoc
         */
        public static function getEnvMapping(): array
        {
            return [
                'FACEBOOK_APP_ID'         => 'app_id',
                'FACEBOOK_APP_SECRET'     => 'app_secret',
                'FACEBOOK_REDIRECT_URI'   => 'app_redirect_uri',
                'FACEBOOK_USER_TOKEN'     => 'graph_user_access_token',
                'FACEBOOK_PAGE_TOKEN'     => 'graph_page_access_token',
                'FACEBOOK_TOKEN_PATH'     => 'graph_token_path',
                'FACEBOOK_USER_ID'        => 'user_id',
                'FACEBOOK_ACCOUNTS_GROUP' => 'accounts_group_name',
            ];
        }

        /**
         * @inheritdoc
         */
        public function validateAuthentication(): array
        {
            try {
                $api = $this->getApi();
                $api->performRequest('GET', 'me', ['fields' => 'id,name']);

                return [
                    'success' => true,
                    'message' => 'Authentication is valid.',
                    'details' => [
                        'user_id' => $api->getUserId()
                    ]
                ];
            } catch (Exception|GuzzleException $e) {
                return [
                    'success' => false,
                    'message' => $e->getMessage(),
                    'details' => []
                ];
            }
        }

        /**
         * @inheritdoc
         * @throws Exception
         */
        public function reset(string $mode = 'all', array $config = []): array
        {
            $resetCallback = $config['resetCallback'] ?? null;
            if ($resetCallback instanceof Closure) {
                return $resetCallback($this->getChannel(), $mode);
            }

            throw new Exception("Reset callback not provided for ".$this->getChannel());
        }

        /**
         * @inheritdoc
         */
        public function getDateFilterMapping(): array
        {
            return [];
        }
    }