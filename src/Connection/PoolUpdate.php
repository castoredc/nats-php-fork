<?php

declare(strict_types=1);

namespace Basis\Nats\Connection;

/**
 * What an INFO message changed about the server pool.
 */
class PoolUpdate
{
    /**
     * @param string[] $added addresses appended to the pool
     * @param string[] $removed addresses of discovered servers no longer advertised
     * @param bool $hasNew whether any added address had never been seen before
     */
    public function __construct(
        public readonly array $added = [],
        public readonly array $removed = [],
        public readonly bool $hasNew = false,
    ) {
    }
}
