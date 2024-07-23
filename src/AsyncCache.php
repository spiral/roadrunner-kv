<?php

declare(strict_types=1);

namespace Spiral\RoadRunner\KeyValue;

use RoadRunner\KV\DTO\V1\Response;
use Spiral\Goridge\RPC\AsyncRPCInterface;
use Spiral\Goridge\RPC\Exception\RPCException;
use Spiral\Goridge\RPC\Exception\ServiceException;
use Spiral\RoadRunner\KeyValue\Exception\KeyValueException;
use Spiral\RoadRunner\KeyValue\Exception\StorageException;
use Spiral\RoadRunner\KeyValue\Serializer\DefaultSerializer;
use Spiral\RoadRunner\KeyValue\Serializer\SerializerInterface;

/**
 * @psalm-suppress PropertyNotSetInConstructor
 * @property AsyncRPCInterface $rpc
 */
class AsyncCache extends Cache implements AsyncStorageInterface
{
    /**
     * @var positive-int[]
     */
    protected array $callsInFlight = [];

    /**
     * @param non-empty-string $name
     */
    public function __construct(
        AsyncRPCInterface $rpc,
        string $name,
        SerializerInterface $serializer = new DefaultSerializer()
    ) {
        parent::__construct($rpc, $name, $serializer);
    }

    /**
     * Note: The current PSR-16 implementation always returns true or
     * exception on error.
     *
     * @throws KeyValueException
     * @throws RPCException
     */
    public function deleteAsync(string $key): bool
    {
        return $this->deleteMultipleAsync([$key]);
    }

    /**
     * Note: The current PSR-16 implementation always returns true or
     * exception on error.
     *
     * @param iterable<string> $keys
     *
     * @throws KeyValueException
     * @throws RPCException
     */
    public function deleteMultipleAsync(iterable $keys): bool
    {
        // Handle someone never calling commitAsync()
        if (\count($this->callsInFlight) > 1000) {
            $this->commitAsync();
        }

        $this->callsInFlight[] = $this->rpc->callAsync('kv.Delete', $this->requestKeys($keys));

        return true;
    }

    /**
     * @param positive-int|\DateInterval|null $ttl
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @throws KeyValueException
     * @throws RPCException
     */
    public function setAsync(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool
    {
        return $this->setMultipleAsync([$key => $value], $ttl);
    }

    /**
     * @param iterable<string, mixed> $values
     * @param positive-int|\DateInterval|null $ttl
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @throws KeyValueException
     * @throws RPCException
     */
    public function setMultipleAsync(iterable $values, null|int|\DateInterval $ttl = null): bool
    {
        // Handle someone never calling commitAsync()
        if (\count($this->callsInFlight) > 1000) {
            $this->commitAsync();
        }

        $this->callsInFlight[] = $this->rpc->callAsync(
            'kv.Set',
            $this->requestValues($values, $this->ttlToRfc3339String($ttl))
        );

        return true;
    }

    /**
     * @throws KeyValueException
     * @throws RPCException
     */
    public function commitAsync(): bool
    {
        try {
            $this->rpc->getResponses($this->callsInFlight, Response::class);
        } catch (ServiceException $e) {
            $message = \str_replace(["\t", "\n"], ' ', $e->getMessage());

            if (\str_contains($message, 'no such storage')) {
                throw new StorageException(\sprintf(self::ERROR_INVALID_STORAGE, $this->name));
            }

            throw new KeyValueException($message, $e->getCode(), $e);
        } finally {
            $this->callsInFlight = [];
        }

        return true;
    }
}
