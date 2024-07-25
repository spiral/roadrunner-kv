<?php

declare(strict_types=1);

namespace Spiral\RoadRunner\KeyValue\Tests;

use Spiral\RoadRunner\KeyValue\Cache;
use Spiral\RoadRunner\KeyValue\Serializer\DefaultSerializer;
use Spiral\RoadRunner\KeyValue\Serializer\SerializerInterface;
use Spiral\RoadRunner\KeyValue\Tests\Stub\FrozenDateCacheStub;

final class CacheTest extends CacheTestCase
{
    /**
     * @param array<string, mixed> $mapping
     */
    protected function cache(
        array $mapping = [],
        SerializerInterface $serializer = new DefaultSerializer()
    ): Cache {
        return new Cache($this->rpc($mapping), $this->name, $serializer);
    }

    /**
     * @param array<string, mixed> $mapping
     */
    protected function frozenDateCache(
        \DateTimeImmutable $date,
        array $mapping = [],
        SerializerInterface $serializer = new DefaultSerializer(),
    ): Cache {
        return new FrozenDateCacheStub($date, $this->rpc($mapping), $this->name, $serializer);
    }
}
