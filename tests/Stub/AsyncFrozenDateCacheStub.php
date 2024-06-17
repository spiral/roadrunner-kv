<?php

declare(strict_types=1);

namespace Spiral\RoadRunner\KeyValue\Tests\Stub;

use Spiral\Goridge\RPC\AsyncRPCInterface;
use Spiral\RoadRunner\KeyValue\AsyncCache;
use Spiral\RoadRunner\KeyValue\Serializer\DefaultSerializer;
use Spiral\RoadRunner\KeyValue\Serializer\SerializerInterface;

final class AsyncFrozenDateCacheStub extends AsyncCache
{
    private \DateTimeImmutable $date;

    public function __construct(
        \DateTimeImmutable $date,
        AsyncRPCInterface $rpc,
        string $name,
        SerializerInterface $serializer = new DefaultSerializer()
    ) {
        $this->date = $date;

        parent::__construct($rpc, $name, $serializer);
    }

    final protected function now(): \DateTimeImmutable
    {
        return $this->date;
    }
}
