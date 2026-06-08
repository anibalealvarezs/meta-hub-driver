<?php

    declare(strict_types=1);

    namespace Anibalealvarezs\MetaHubDriver\Conversions;

    use Anibalealvarezs\ApiDriverCore\Conversions\UniversalMetricConverter;
    use Carbon\Carbon;
    use Doctrine\Common\Collections\ArrayCollection;
    use Psr\Log\LoggerInterface;

    /**
     * FacebookOrganicMetricConvert
     *
     * Standardizes Facebook Page and Instagram Organic data into APIs Hub metric objects.
     * Refactored to be entity-agnostic for the standalone SDK.
     */
    class FacebookOrganicMetricConvert
    {
        public static function pageMetrics(
            array              $rows,
            string             $pagePlatformId = '',
            string             $postPlatformId = '',
            ?LoggerInterface   $logger = null,
            object|string|null $page = null,
            object|string|null $post = null,
            object|string|null $period = 'daily',
            object|string|null $channeledAccount = null,
            object|string|null $account = null,
        ): ArrayCollection
        {
            $platformId = $pagePlatformId ?: $postPlatformId;
            $periodValue = is_object($period) && isset($period->value) ? $period->value : (string)$period;
            $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount;
            $context = UniversalMetricConverter::getUniversalContext([
                'page'               => $page,
                'post'               => $post,
                'account'            => $account,
                'channeledAccount'   => $channeledAccount,
                'channeledAccountId' => $channeledAccountId,
            ]);

            $collection = new ArrayCollection();
            // The input can be the raw API response with a 'data' key, or just the array of metrics.
            $metricRows = $rows['data'] ?? $rows;
            if (!is_array($metricRows)) {
                return $collection;
            }

            foreach ($metricRows as $metric) {
                $metricName = $metric['name'] ?? 'unknown';
                $metricPeriod = $metric['period'] ?? null;
                $resolvedPeriod = self::resolveMetricPeriod($metricName, is_string($metricPeriod) ? $metricPeriod : null, $periodValue);
                $isDailyMetric = self::isDailyMetric($metricName, $resolvedPeriod);
                $previousScalarValue = null;
                $previousBreakdownValues = [];

                foreach ($metric['values'] ?? [] as $dailyData) {
                    $dailyValue = $dailyData['value'] ?? null;
                    if (!isset($dailyData['end_time'])) {
                        if ($resolvedPeriod === 'lifetime') {
                            $date = Carbon::now()->toDateString();
                        } else {
                            throw new \Exception("Missing 'end_time' in metric values for non-lifetime metric: {$metricName} (Period: " . ($metricPeriod ?? $periodValue) . ")");
                        }
                    } else {
                        $date = $dailyData['end_time'];
                    }

                    // Case 1: The value is a simple scalar (e.g., page_impressions).
                    if (is_scalar($dailyValue) || is_null($dailyValue)) {
                        $convertedValue = self::normalizeDailySeriesValue($dailyValue, $previousScalarValue, $isDailyMetric);
                        if ($isDailyMetric && (is_numeric($dailyValue) || is_null($dailyValue))) {
                            $previousScalarValue = $dailyValue;
                        }
                        $rowMetrics = UniversalMetricConverter::convert([['date' => $date, 'value' => $dailyValue]], [
                            'channel'              => 'facebook_organic',
                            'period'               => $resolvedPeriod,
                            'fallback_platform_id' => $platformId,
                            'date_field'           => 'date',
                            'metrics'              => ['value' => $metricName],
                            'context'              => $context,
                        ], $logger);
                        if ($isDailyMetric) {
                            foreach ($rowMetrics as $rowMetric) {
                                $rowMetric->value = $convertedValue;
                                $rowMetric->period = $resolvedPeriod;
                            }
                        }
                        foreach ($rowMetrics as $m) $collection->add($m);
                    } // Case 2: The value is a breakdown object (e.g., page_actions_post_reactions_total).
                    // This also handles the API quirk where an empty breakdown is an empty array `[]`.
                    elseif (is_object($dailyValue) || is_array($dailyValue)) {
                        foreach ((array)$dailyValue as $dimensionName => $value) {
                            $dimensions = [];
                            if ($metricName === 'page_actions_post_reactions_total') {
                                $dimensions[] = ['dimensionKey' => 'reaction_type', 'dimensionValue' => (string)$dimensionName];
                            } else {
                                // Generic fallback for other potential breakdown metrics.
                                $dimensions[] = ['dimensionKey' => 'breakdown', 'dimensionValue' => (string)$dimensionName];
                            }

                            $convertedValue = self::normalizeDailySeriesValue($value, $previousBreakdownValues[(string)$dimensionName] ?? null, $isDailyMetric, (string)$dimensionName);
                            if ($isDailyMetric && (is_numeric($value) || is_null($value))) {
                                $previousBreakdownValues[(string)$dimensionName] = $value;
                            }

                            $rowMetrics = UniversalMetricConverter::convert([['date' => $date, 'value' => $value]], [
                                'channel'              => 'facebook_organic',
                                'period'               => $resolvedPeriod,
                                'fallback_platform_id' => $platformId,
                                'date_field'           => 'date',
                                'metrics'              => ['value' => $metricName],
                                'dimensions'           => $dimensions,
                                'context'              => $context,
                            ], $logger);
                            if ($isDailyMetric) {
                                foreach ($rowMetrics as $rowMetric) {
                                    $rowMetric->value = $convertedValue;
                                    $rowMetric->period = $resolvedPeriod;
                                }
                            }
                            foreach ($rowMetrics as $m) $collection->add($m);
                        }
                    }
                }
            }

            return $collection;
        }

        /**
         * Converts Instagram Account API rows into metrics.
         */
        public static function igAccountMetrics(
            array              $rows,
            string             $date,
            object|string|null $page = null,
            object|string|null $account = null,
            object|string|null $channeledAccount = null,
            ?LoggerInterface   $logger = null,
            object|string|null $period = 'daily',
        ): ArrayCollection
        {
            $collection = new ArrayCollection();
            $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount;
            $periodValue = is_object($period) && isset($period->value) ? $period->value : (string)$period;

            foreach ($rows as $row) {
                $row['date'] = $date;

                // 1. Handle standard total value or nulls
                $valuePath = 'value'; // Fallback
                if (isset($row['total_value'])) {
                    $valuePath = 'total_value.value';
                } elseif (isset($row['values']) && is_array($row['values']) && isset($row['values'][0]['value'])) {
                    $valuePath = 'values.0.value';
                }
                
                if (!isset($row['total_value']) && !isset($row['values']) && !isset($row['value'])) {
                    $row['value'] = null; // inject a mock value if completely missing
                }
                
                $config = [
                    'channel'              => 'facebook_organic',
                    'period'               => $periodValue,
                    'fallback_platform_id' => $channeledAccountId,
                    'date_field'           => 'date',
                    'context'              => [
                        'account'            => $account,
                        'channeledAccount'   => $channeledAccount,
                        'channeledAccountId' => $channeledAccountId,
                        'page'               => $page,
                    ],
                    'metrics'              => [$valuePath => $row['name'] ?? 'unknown'],
                    'include_nulls'        => true
                ];
                $rowMetrics = UniversalMetricConverter::convert([$row], $config, $logger);
                foreach ($rowMetrics as $m) $collection->add($m);

                // 2. Handle breakdowns
                if (isset($row['total_value']['breakdowns'])) {
                    foreach ($row['total_value']['breakdowns'] as $breakdown) {
                        $dimKeys = $breakdown['dimension_keys'] ?? [];
                        foreach ($breakdown['results'] ?? [] as $result) {
                            $dimValues = $result['dimension_values'] ?? [];
                            $dimensions = [];
                            foreach ($dimKeys as $idx => $key) {
                                $dimensions[] = ['dimensionKey' => $key, 'dimensionValue' => $dimValues[$idx] ?? 'unknown'];
                            }

                            $metricRow = array_merge($row, [
                                'breakdown_value' => $result['value'] ?? 0,
                                'dimensions'      => $dimensions
                            ]);

                            $rowMetrics = UniversalMetricConverter::convert([$metricRow], [
                                'channel'              => 'facebook_organic',
                                'period'               => $periodValue,
                                'fallback_platform_id' => $channeledAccountId,
                                'date_field'           => 'date',
                                'metrics'              => ['breakdown_value' => ($row['name'] ?? 'unknown')],
                                'dimensions'           => $dimensions,
                                'context'              => UniversalMetricConverter::getUniversalContext([
                                    'account'            => $account,
                                    'channeledAccount'   => $channeledAccount,
                                    'channeledAccountId' => $channeledAccountId,
                                    'page'               => $page,
                                ])
                            ], $logger);
                            foreach ($rowMetrics as $m) $collection->add($m);
                        }
                    }
                }
            }

            return $collection;
        }

        /**
         * Converts Instagram Media API rows into metrics.
         */
        public static function igMediaMetrics(
            array              $rows,
            string             $date,
            object|string|null $page = null,
            object|string|null $post = null,
            object|string|null $account = null,
            object|string|null $channeledAccount = null,
            ?LoggerInterface   $logger = null,
        ): ArrayCollection
        {
            $collection = new ArrayCollection();
            $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : (string)$channeledAccount;
            $postId = is_object($post) && method_exists($post, 'getPostId') ? $post->getPostId() : (string)$post;

            foreach ($rows as $row) {
                $row['date'] = $date;
                $metricName = (string)($row['name'] ?? 'unknown');
                $metricPeriod = is_string($row['period'] ?? null) ? $row['period'] : null;
                $resolvedPeriod = self::resolveMetricPeriod($metricName, $metricPeriod, 'lifetime');
                $rowMetrics = UniversalMetricConverter::convert([$row], [
                    'channel'              => 'facebook_organic',
                    'period'               => $resolvedPeriod,
                    'fallback_platform_id' => $postId,
                    'date_field'           => 'date',
                    'metrics'              => ['values' => $metricName],
                    'include_nulls'        => true,
                    'context'              => UniversalMetricConverter::getUniversalContext([
                        'account'            => $account,
                        'channeledAccount'   => $channeledAccount,
                        'channeledAccountId' => $channeledAccountId,
                        'page'               => $page,
                        'post'               => $post,
                    ])
                ], $logger);
                if (self::isDailyMetric($metricName, $resolvedPeriod)) {
                    foreach ($rowMetrics as $rowMetric) {
                        $rowMetric->period = $resolvedPeriod;
                    }
                }
                foreach ($rowMetrics as $m) $collection->add($m);
            }

            return $collection;
        }

        /**
         * Converts raw Facebook Page posts into a collection for SocialProcessor.
         */
        public static function toPostsCollection(
            array             $posts,
            object|int|string $page,
            object|int|string $account,
            int|string|null   $channeledAccountId = null
        ): ArrayCollection
        {
            $collection = new ArrayCollection();
            $pageId = is_object($page) && method_exists($page, 'getId') ? $page->getId() : $page;
            $accountId = is_object($account) && method_exists($account, 'getId') ? $account->getId() : $account;

            foreach ($posts as $post) {
                $p = (object)[
                    'platformId'         => $post['id'],
                    'pageId'             => $pageId,
                    'accountId'          => $accountId,
                    'channeledAccountId' => $channeledAccountId,
                    'data'               => $post
                ];
                $collection->add($p);
            }

            return $collection;
        }

        /**
         * Converts raw Instagram Media items into a collection for SocialProcessor.
         */
        public static function toInstagramMediaCollection(
            array             $mediaItems,
            object|int|string $page,
            object|int|string $account,
            object|int|string $channeledAccount,
        ): ArrayCollection
        {
            $collection = new ArrayCollection();
            $pageId = is_object($page) && method_exists($page, 'getId') ? $page->getId() : $page;
            $accountId = is_object($account) && method_exists($account, 'getId') ? $account->getId() : $account;
            $channeledAccountId = is_object($channeledAccount) && method_exists($channeledAccount, 'getId') ? $channeledAccount->getId() : $channeledAccount;

            foreach ($mediaItems as $item) {
                $p = (object)[
                    'platformId'         => $item['id'],
                    'pageId'             => $pageId,
                    'accountId'          => $accountId,
                    'channeledAccountId' => $channeledAccountId,
                    'data'               => $item
                ];
                $collection->add($p);
            }

            return $collection;
        }

        private static function resolveMetricPeriod(string $metricName, ?string $metricPeriod, string $fallbackPeriod): string
        {
            if (self::isDailyMetricName($metricName)) {
                return 'daily';
            }

            if ($metricPeriod !== null && $metricPeriod !== '') {
                return $metricPeriod;
            }

            return $fallbackPeriod;
        }

        private static function isDailyMetric(string $metricName, string $period): bool
        {
            return self::isDailyMetricName($metricName) || $period === 'daily';
        }

        private static function isDailyMetricName(string $metricName): bool
        {
            return str_ends_with($metricName, '_daily');
        }

        private static function normalizeDailySeriesValue(mixed $currentValue, mixed $previousValue, bool $isDailyMetric): mixed
        {
            if (!$isDailyMetric || !is_numeric($currentValue)) {
                return $currentValue;
            }

            if ($previousValue === null || !is_numeric($previousValue)) {
                return $currentValue;
            }

            return $currentValue - $previousValue;
        }
    }
