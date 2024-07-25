<?php

declare(strict_types=1);

namespace Spiral\RoadRunner\KeyValue\Tests\Stub;

use Spiral\Goridge\RPC\AsyncRPCInterface;

class AsyncRPCConnectionStub extends RPCConnectionStub implements AsyncRPCInterface
{
    /**
     * @var array<int, mixed>
     */
    private array $responses = [];
    private int $seq = 0;

    public function callIgnoreResponse(string $method, mixed $payload): void
    {
        $this->call($method, $payload);
    }

    public function callAsync(string $method, mixed $payload): int
    {
        $seq = $this->seq;
        $this->responses[$seq] = ['needsCall' => true, 'method' => $method, 'payload' => $payload];
        $this->seq++;
        return $seq;
    }

    public function hasResponse(int $seq): bool
    {
        return isset($this->responses[$seq]);
    }

    public function hasResponses(array $seqs): array
    {
        return array_filter($seqs, $this->hasResponse(...));
    }

    public function getResponse(int $seq, mixed $options = null): mixed
    {
        if (isset($this->responses[$seq]['needsCall'])) {
            $this->responses[$seq] = $this->call($this->responses[$seq]['method'], $this->responses[$seq]['payload'], $options);
        }

        return $this->responses[$seq];
    }

    public function getResponses(array $seqs, mixed $options = null): iterable
    {
        return array_map(fn($seq) => $this->getResponse($seq, $options), $seqs);
    }
}
