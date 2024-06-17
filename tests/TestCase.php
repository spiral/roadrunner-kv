<?php

declare(strict_types=1);

namespace Spiral\RoadRunner\KeyValue\Tests;

use PHPUnit\Framework\TestCase as BaseTestCase;
use Spiral\RoadRunner\KeyValue\Tests\Stub\AsyncRPCConnectionStub;
use Spiral\RoadRunner\KeyValue\Tests\Stub\RPCConnectionStub;

abstract class TestCase extends BaseTestCase
{
    /**
     * @param array<string, mixed> $mapping
     * @return RPCConnectionStub
     */
    protected function rpc(array $mapping = []): RPCConnectionStub
    {
        return new RPCConnectionStub($mapping);
    }

    /**
     * @param array<string, mixed> $mapping
     * @return AsyncRPCConnectionStub
     */
    protected function asyncRPC(array $mapping = []): AsyncRPCConnectionStub
    {
        return new AsyncRPCConnectionStub($mapping);
    }

    public static function valuesDataProvider(): array
    {
        return [
            'null' => [null],
            'int' => [0xDEAD_BEEF],
            'zero int' => [0],
        ];
    }
}
