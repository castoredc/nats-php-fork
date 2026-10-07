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
 * A protocol line has to be read whole however long it is. stream_get_line() only
 * recognises a two byte delimiter when both bytes fall inside the window it was
 * given, so a window that ends between the "\r" and the "\n" returns the "\r" as part
 * of the line and leaves the "\n" to be read as the start of the next one. A large
 * clustered INFO decoded to nothing, and its leftovers reached Factory as garbage.
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
        // 40 nodes advertised with kubernetes style dns names: well past a kilobyte.
        $urls = [];
        for ($i = 0; $i < 40; $i++) {
            $urls[] = "nats-$i.nats-headless.production.svc.cluster.local:4222";
        }

        $line = 'INFO ' . json_encode([
            'server_id' => str_repeat('N', 56),
            'server_name' => str_repeat('N', 56),
            'version' => '2.11.4',
            'host' => '0.0.0.0',
            'port' => 4222,
            'headers' => true,
            'max_payload' => 1048576,
            'proto' => 1,
            'connect_urls' => $urls,
        ]);

        $this->assertGreaterThan(1024, strlen($line), 'the fixture has to exceed a kilobyte');

        $info = $this->connectionReading($line . "\r\n")->getMessage(1);

        $this->assertInstanceOf(Info::class, $info);
        $this->assertSame($urls, $info->connect_urls);
        $this->assertSame(1048576, $info->max_payload);
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
     * One byte short of a window puts the carriage return in its last byte and the
     * line feed outside it, so the delimiter was not recognised and the next protocol
     * line was glued onto this one.
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

        $info = $connection->getMessage(1);
        $this->assertInstanceOf(Info::class, $info);
        $this->assertSame($name, $info->server_name);
        $this->assertInstanceOf(Pong::class, $connection->getMessage(1));
    }

    public static function lengthsAroundAChunkBoundary(): array
    {
        return [
            'one short of a kilobyte' => [1023],
            'a kilobyte' => [1024],
            'one over a kilobyte' => [1025],
            'one short of two kilobytes' => [2047],
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

        fwrite($this->sockets[1], "tial\"}\r\n");

        $info = $connection->getMessage(1);
        $this->assertInstanceOf(Info::class, $info);
        $this->assertSame('partial', $info->server_name);
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
