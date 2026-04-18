<?php

declare(strict_types=1);

namespace Anibalealvarezs\MetaHubDriver\Enums;

/**
 * MetaSyncScope
 * 
 * Defines the high-level synchronization orchestration modes.
 */
enum MetaSyncScope: string
{
    case METRICS = 'metrics';
    case ENTITIES = 'entities';
}
