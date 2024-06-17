<?php

namespace Spiral\RoadRunner\KeyValue;

use DateInterval;
use RoadRunner\KV\DTO\V1\Response;
use Spiral\Goridge\RPC\AsyncRPCInterface;
use Spiral\Goridge\RPC\Exception\RPCException;
use Spiral\Goridge\RPC\Exception\ServiceException;
use Spiral\Goridge\RPC\RPCInterface;
use Spiral\RoadRunner\KeyValue\Exception\KeyValueException;
use Spiral\RoadRunner\KeyValue\Exception\StorageException;
use Spiral\RoadRunner\KeyValue\Serializer\DefaultSerializer;
use Spiral\RoadRunner\KeyValue\Serializer\SerializerInterface;
use function sprintf;
use function str_contains;
use function str_replace;

/**
 * @psalm-suppress PropertyNotSetInConstructor
 */
class AsyncCache extends Cache implements AsyncStorageInterface
{
    /**
     * @var positive-int[]
     */
    protected array $callsInFlight = [];

    /**
     * @param AsyncRPCInterface $rpc
     * @param non-empty-string $name
     */
    public function __construct(
        RPCInterface $rpc,
        string $name,
        SerializerInterface $serializer = new DefaultSerializer()
    ) {
        parent::__construct($rpc, $name, $serializer);

        // This should result in things like the Symfony ContainerBuilder throwing during build instead of runtime.
        assert($this->rpc instanceof AsyncRPCInterface);
    }

    /**
     * Note: The current PSR-16 implementation always returns true or
     * exception on error.
     *
     * {@inheritDoc}
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
     * {@inheritDoc}
     *
     * @psalm-param iterable<string> $keys
     *
     * @throws KeyValueException
     * @throws RPCException
     */
    public function deleteMultipleAsync(iterable $keys): bool
    {
        assert($this->rpc instanceof AsyncRPCInterface);

        // Handle someone never calling commitAsync()
        if (count($this->callsInFlight) > 1000) {
            $this->commitAsync();
        }

        $this->callsInFlight[] = $this->rpc->callAsync('kv.Delete', $this->requestKeys($keys));

        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @psalm-param positive-int|\DateInterval|null $ttl
     * @psalm-suppress MoreSpecificImplementedParamType
     * @throws KeyValueException
     * @throws RPCException
     */
    public function setAsync(string $key, mixed $value, null|int|DateInterval $ttl = null): bool
    {
        return $this->setMultipleAsync([$key => $value], $ttl);
    }

    /**
     * {@inheritDoc}
     *
     * @psalm-param iterable<string, mixed> $values
     * @psalm-param positive-int|\DateInterval|null $ttl
     * @psalm-suppress MoreSpecificImplementedParamType
     * @throws KeyValueException
     * @throws RPCException
     */
    public function setMultipleAsync(iterable $values, null|int|DateInterval $ttl = null): bool
    {
        assert($this->rpc instanceof AsyncRPCInterface);

        // Handle someone never calling commitAsync()
        if (count($this->callsInFlight) > 1000) {
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
        assert($this->rpc instanceof AsyncRPCInterface);

        try {
            $this->rpc->getResponses($this->callsInFlight, Response::class);
        } catch (ServiceException $e) {
            $message = str_replace(["\t", "\n"], ' ', $e->getMessage());

            if (str_contains($message, 'no such storage')) {
                throw new StorageException(sprintf(self::ERROR_INVALID_STORAGE, $this->name));
            }

            throw new KeyValueException($message, $e->getCode(), $e);
        } finally {
            $this->callsInFlight = [];
        }

        return true;
    }
}
