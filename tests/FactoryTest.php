<?php

declare(strict_types=1);

namespace Spiral\RoadRunner\KeyValue\Tests;

use Spiral\RoadRunner\KeyValue\Factory;
use Spiral\RoadRunner\KeyValue\FactoryInterface;
use Spiral\RoadRunner\KeyValue\Serializer\DefaultSerializer;
use Spiral\RoadRunner\KeyValue\Serializer\SerializerInterface;
use function random_bytes;

class FactoryTest extends TestCase
{
    /**
     * @param array<string, mixed> $mapping
     */
    private function factory(array $mapping = [], SerializerInterface $serializer = new DefaultSerializer()): FactoryInterface
    {
        return new Factory($this->rpc($mapping), $serializer);
    }

    /**
     * @param array<string, mixed> $mapping
     */
    private function asyncFactory(array $mapping = [], SerializerInterface $serializer = new DefaultSerializer()): FactoryInterface
    {
        return new Factory($this->asyncRPC($mapping), $serializer);
    }

    public function testFactoryCreation(): void
    {
        $this->expectNotToPerformAssertions();
        $this->factory();
    }

    public function testAsyncFactoryCreation(): void
    {
        $this->expectNotToPerformAssertions();
        $this->asyncFactory();
    }

    public function testSuccessSelectOfUnknownStorage(): void
    {
        $name = random_bytes(32);

        $driver = $this->factory()
            ->select($name);

        $this->assertSame($name, $driver->getName());
    }

    public function testSuccessSelectOfUnknownStorageWithAsync(): void
    {
        $name = random_bytes(32);

        $driver = $this->asyncFactory()
            ->select($name);

        $this->assertSame($name, $driver->getName());
    }
}
