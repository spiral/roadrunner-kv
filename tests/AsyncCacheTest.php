<?php

declare(strict_types=1);

namespace Spiral\RoadRunner\KeyValue\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use RoadRunner\KV\DTO\V1\Item;
use RoadRunner\KV\DTO\V1\Request;
use Spiral\Goridge\RPC\Exception\ServiceException;
use Spiral\RoadRunner\KeyValue\AsyncCache;
use Spiral\RoadRunner\KeyValue\Exception\InvalidArgumentException;
use Spiral\RoadRunner\KeyValue\Exception\KeyValueException;
use Spiral\RoadRunner\KeyValue\Serializer\DefaultSerializer;
use Spiral\RoadRunner\KeyValue\Serializer\SerializerInterface;
use Spiral\RoadRunner\KeyValue\Tests\Stub\AsyncFrozenDateCacheStub;

final class AsyncCacheTest extends CacheTestCase
{
    /**
     * @param array<string, mixed> $mapping
     */
    protected function cache(
        array $mapping = [],
        SerializerInterface $serializer = new DefaultSerializer()
    ): AsyncCache {
        return new AsyncCache($this->asyncRPC($mapping), $this->name, $serializer);
    }

    /**
     * @param array<string, mixed> $mapping
     */
    protected function frozenDateCache(
        \DateTimeImmutable $date,
        array $mapping = [],
        SerializerInterface $serializer = new DefaultSerializer(),
    ): AsyncCache {
        return new AsyncFrozenDateCacheStub($date, $this->asyncRPC($mapping), $this->name, $serializer);
    }

    #[DataProvider('serializersWithValuesDataProvider')]
    public function testSetAsync(SerializerInterface $serializer, mixed $expected): void
    {
        if (\is_float($expected) && \is_nan($expected)) {
            $this->markTestSkipped('Unable to execute test for NAN float value');
        }

        if (\is_resource($expected)) {
            $this->markTestSkipped('Unable to execute test for resource value');
        }

        $driver = $this->getAssertableCacheOnSet($serializer, ['key' => $expected]);

        $driver->setAsync('key', $expected);
        $driver->commitAsync();
    }

    #[DataProvider('serializersWithValuesDataProvider')]
    public function testMultipleSetAsync(SerializerInterface $serializer, mixed $value): void
    {
        if (\is_float($value) && \is_nan($value)) {
            $this->markTestSkipped('Unable to execute test for NAN float value');
        }

        if (\is_resource($value)) {
            $this->markTestSkipped('Unable to execute test for resource value');
        }

        $expected = ['key' => $value, 'key2' => $value];

        $driver = $this->getAssertableCacheOnSet($serializer, $expected);
        $driver->setMultipleAsync($expected);
        $driver->commitAsync();
    }

    public function testSetAsyncWithRelativeIntTTL(): void
    {
        $seconds = 0xDEAD_BEEF;

        // This is the current time for cache and relative date
        $now = new \DateTimeImmutable();
        // Relative date: [$now] + [$seconds]
        $expected = $now->add(new \DateInterval("PT{$seconds}S"))
            ->format(\DateTimeInterface::RFC3339);

        $driver = $this->frozenDateCache($now, [
            'kv.Set' => function (Request $request) use ($expected) {
                /** @var Item $item */
                $item = $request->getItems()[0];
                $this->assertSame($expected, $item->getTimeout());

                return $this->response();
            },
        ]);

        // Send relative date in $now + $seconds
        $driver->setAsync('key', 'value', $seconds);
        $driver->commitAsync();
    }

    public function testSetAsyncWithRelativeDateIntervalTTL(): void
    {
        $seconds = 0xDEAD_BEEF;
        $interval = new \DateInterval("PT{$seconds}S");

        // This is the current time for cache and relative date
        $now = new \DateTimeImmutable();

        // Add interval to frozen current time
        $expected = $now->add($interval)
            ->format(\DateTimeInterface::RFC3339);

        $driver = $this->frozenDateCache($now, [
            'kv.Set' => function (Request $request) use ($expected) {
                /** @var Item $item */
                $item = $request->getItems()[0];
                $this->assertSame($expected, $item->getTimeout());

                return $this->response();
            },
        ]);

        $driver->setAsync('key', 'value', $interval);
        $driver->commitAsync();
    }

    #[DataProvider('valuesDataProvider')]
    public function testSetAsyncWithInvalidTTL(mixed $invalidTTL): void
    {
        $type = \get_debug_type($invalidTTL);

        if ($invalidTTL === null || \is_int($invalidTTL) || $invalidTTL instanceof \DateTimeInterface) {
            $this->markTestSkipped('Can not complete negative test for valid TTL of type ' . $type);
        }

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Cache item ttl (expiration) must be of type int or \DateInterval, but ' . $type . ' passed',
        );

        $driver = $this->cache();

        // Send relative date in $now + $seconds
        $driver->setAsync('key', 'value', $invalidTTL);
        // Make sure not reachable
        $this->assertSame(true, false);
        $driver->commitAsync();
    }

    public function testDeleteAsync(): void
    {
        $driver = $this->cache(['kv.Delete' => $this->response([])]);
        $this->assertTrue($driver->deleteAsync('key'));
        $this->assertTrue($driver->commitAsync());
    }

    public function testDeleteAsyncWithError(): void
    {
        $driver = $this->cache([
            'kv.Delete' => function () {
                throw new ServiceException('Error: Can not delete something');
            },
        ]);

        $driver->deleteAsync('key');
        $this->expectException(KeyValueException::class);
        $driver->commitAsync();
    }

    public function testDeleteMultipleAsync(): void
    {
        $driver = $this->cache(['kv.Delete' => $this->response([])]);
        $this->assertTrue($driver->deleteMultipleAsync(['key', 'key2']));
        $this->assertTrue($driver->commitAsync());
    }

    public function testDeleteMultipleAsyncWithError(): void
    {
        $driver = $this->cache([
            'kv.Delete' => function () {
                throw new ServiceException('Error: Can not delete something');
            },
        ]);

        $driver->deleteMultipleAsync(['key', 'key2']);
        $this->expectException(KeyValueException::class);
        $driver->commitAsync();
    }

    public function testSetAsyncMultipleWithInvalidKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cache key must be a string, but int passed');

        $driver = $this->cache();
        $driver->setMultipleAsync([0 => 0xDEAD_BEEF]);
        // Make sure not reachable
        $this->assertSame(true, false);
        $driver->commitAsync();
    }

    public function testDeleteMultipleAsyncWithInvalidKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cache key must be a string, but int passed');

        $driver = $this->cache();
        $driver->deleteMultipleAsync([0 => 0xDEAD_BEEF]);
        // Make sure not reachable
        $this->assertSame(true, false);
        $driver->commitAsync();
    }

    /**
     * @return \Traversable<string, array{0: callable(Cache)}>
     */
    public static function methodsDataProvider(): \Traversable
    {
        yield from parent::methodsDataProvider();

        yield 'setAsync' => [fn (AsyncCache $c) => $c->setAsync('key', 'value') && $c->commitAsync()];
        yield 'setMultipleAsync' => [fn (AsyncCache $c) => $c->setMultiple(['key' => 'value']) && $c->commitAsync()];
        yield 'deleteMultipleAsync' => [fn (AsyncCache $c) => $c->deleteMultipleAsync(['key']) && $c->commitAsync()];
        yield 'deleteAsync' => [fn (AsyncCache $c) => $c->delete('key') && $c->commitAsync()];
    }
}
