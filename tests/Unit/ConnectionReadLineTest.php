<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Connection;
use Basis\Nats\Message\Info;
use Basis\Nats\Message\Pong;
use InvalidArgumentException;
use LogicException;
use ReflectionMethod;
use ReflectionProperty;
use Tests\TestCase;

/**
 * A protocol line longer than a single read chunk used to be cut in half:
 * stream_get_line() returns the chunk without consuming the delimiter and hands the
 * remainder back on the next call, so a large clustered INFO decoded to nothing and
 * its leftovers reached Factory as unparsable garbage.
 */
class ConnectionReadLineTest extends TestCase
{
    /** @var resource[] */
    private array $sockets = [];

    public function tearDown(): void
    {
        foreach ($this->sockets as $socket) {
            if (is_resource($socket)) {
                fclose($socket);
            }
        }
        $this->sockets = [];
    }

    public function testInfoLongerThanOneChunkIsReadWhole(): void
    {
        // 40 nodes advertised with kubernetes style dns names: well past one chunk.
        $urls = [];
        for ($i = 0; $i < 40; $i++) {
            $urls[] = "nats-$i.nats-headless.production.svc.cluster.local:4222";
        }

        $info = [
            'server_id' => str_repeat('N', 56),
            'server_name' => str_repeat('N', 56),
            'version' => '2.11.4',
            'host' => '0.0.0.0',
            'port' => 4222,
            'headers' => true,
            'max_payload' => 1048576,
            'proto' => 1,
            'connect_urls' => $urls,
        ];

        $line = 'INFO ' . json_encode($info);
        $this->assertGreaterThan(1024, strlen($line), 'the fixture has to exceed one chunk');

        $info = $this->readInfo($line . "\r\n");

        $this->assertSame($urls, $info->connect_urls);
        $this->assertSame(1048576, $info->max_payload);
    }

    public function testLineOfExactlyOneChunkIsReadWhole(): void
    {
        $prefix = 'INFO {"server_name":"';
        $suffix = '","proto":1}';
        $padding = 1024 - strlen($prefix) - strlen($suffix);
        $name = str_repeat('x', $padding);
        $line = $prefix . $name . $suffix;

        $this->assertSame(1024, strlen($line));

        $info = $this->readInfo($line . "\r\n");

        $this->assertSame($name, $info->server_name);
    }

    /**
     * Truncated json used to leave every property uninitialized while the message
     * still satisfied an instanceof check.
     */
    public function testDamagedLineIsRejected(): void
    {
        $connection = $this->connectionReading('INFO {"server_name":"cut-off' . "\r\n");

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid payload for message');

        $connection->getMessage(0);
    }

    /**
     * One byte short of a chunk puts the carriage return in the last byte of the read
     * window and the line feed outside it, so the delimiter is not recognised and
     * the next protocol line used to be glued onto this one.
     *
     * @dataProvider lengthsAroundAChunkBoundary
     */
    public function testLineEndingAtAChunkBoundaryDoesNotSwallowTheNextLine(int $length): void
    {
        $prefix = 'INFO {"server_name":"';
        $suffix = '","proto":1}';
        $name = str_repeat('x', $length - strlen($prefix) - strlen($suffix));
        $line = $prefix . $name . $suffix;

        $this->assertSame($length, strlen($line));

        $connection = $this->connectionReading($line . "\r\nPONG\r\n");

        $this->assertInstanceOf(Pong::class, $connection->getMessage(1));
        $this->assertSame($name, $connection->getInfoMessage()->server_name);
    }

    public static function lengthsAroundAChunkBoundary(): array
    {
        return [
            'one short of a chunk' => [1023],
            'one chunk' => [1024],
            'one over a chunk' => [1025],
            'one short of two chunks' => [2047],
        ];
    }

    /**
     * A line that has only partly arrived is left where it is, so that the rest can
     * complete it later, instead of being consumed and left half read.
     */
    public function testPartialLineIsCompletedByALaterRead(): void
    {
        $connection = $this->connectionReading('INFO {"server_name":"par');

        $this->assertNull($connection->getMessage(0));

        fwrite($this->sockets[1], "tial\"}\r\nPONG\r\n");

        $this->assertInstanceOf(Pong::class, $connection->getMessage(1));
        $this->assertSame('partial', $connection->getInfoMessage()->server_name);
    }

    /**
     * A line with no end is not buffered for ever. What is left of it cannot be told
     * from the start of the next one, so the connection is dropped rather than read on
     * from the middle of it.
     */
    public function testLineBeyondTheLimitFailsAndDropsTheConnection(): void
    {
        $connection = $this->connectionReading('');

        $stream = fopen('php://temp', 'w+');
        fwrite($stream, 'INFO {"server_name":"' . str_repeat('x', 1_048_576 + 10));
        rewind($stream);
        (new ReflectionProperty(Connection::class, 'socket'))->setValue($connection, $stream);

        try {
            (new ReflectionMethod(Connection::class, 'readLine'))->invoke($connection);
            $this->fail('the line is over the limit');
        } catch (LogicException $e) {
            $this->assertStringContainsString('exceeds', $e->getMessage());
        }

        $socket = (new ReflectionProperty(Connection::class, 'socket'))->getValue($connection);
        $this->assertNull($socket, 'the connection is not left in the middle of a line');
    }

    /**
     * A peer that hung up is readable too, with nothing to read, which must still end
     * in the disconnect being handled rather than in an empty poll.
     */
    public function testClosedPeerIsStillReportedByANonBlockingRead(): void
    {
        $connection = $this->connectionReading('');
        fclose($this->sockets[1]);

        $this->expectException(LogicException::class);

        $connection->getMessage(0);
    }

    /**
     * Feeds a raw protocol line to a connection over a socket pair and returns the
     * INFO state it ended up with. An asynchronous INFO is not handed back to
     * callers, so the merged message is what there is to observe.
     */
    private function readInfo(string $wire): Info
    {
        $connection = $this->connectionReading($wire);

        // A zero timeout consumes whatever is buffered and returns, so nothing has
        // to be caught here and an unexpected failure is not swallowed.
        $connection->getMessage(0);

        return $connection->getInfoMessage();
    }

    private function connectionReading(string $wire): Connection
    {
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        $this->sockets[] = $clientEnd;
        $this->sockets[] = $serverEnd;

        stream_set_timeout($clientEnd, 0, 200_000);
        fwrite($serverEnd, $wire);

        $client = new Client(new Configuration(['reconnect' => false, 'timeout' => 1]));
        $property = new ReflectionProperty(Connection::class, 'socket');
        $property->setValue($client->connection, $clientEnd);

        return $client->connection;
    }
}
