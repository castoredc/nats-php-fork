<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Connection;
use Basis\Nats\Message\Ping;
use Exception;
use ReflectionProperty;
use Tests\TestCase;

/**
 * A connection that failed part way through its handshake has not announced itself,
 * sent its CONNECT or restored its subscriptions, so nothing may be written to it as
 * though it had.
 */
class ConnectionHandshakeFailureTest extends TestCase
{
    /** @var resource|null */
    private $listener;

    public function tearDown(): void
    {
        if (is_resource($this->listener)) {
            fclose($this->listener);
        }
    }

    public function testFailedHandshakeLeavesNoSocketBehind(): void
    {
        $connection = $this->connectionToASilentServer();

        try {
            $connection->sendMessage(new Ping());
            $this->fail('the handshake should have timed out');
        } catch (Exception $e) {
            $this->assertStringContainsString('Timeout waiting for message', $e->getMessage());
        }

        $socket = (new ReflectionProperty(Connection::class, 'socket'))->getValue($connection);
        $this->assertFalse(is_resource($socket), 'the half established socket must be closed');
    }

    public function testNextWriteAfterAFailedHandshakeConnectsAgain(): void
    {
        $connection = $this->connectionToASilentServer();

        foreach ([1, 2] as $attempt) {
            try {
                $connection->sendMessage(new Ping());
                $this->fail("attempt $attempt wrote to a connection that never completed its handshake");
            } catch (Exception $e) {
                $this->assertStringContainsString('Timeout waiting for message', $e->getMessage());
            }
        }
    }

    /**
     * Accepts connections in the kernel and never answers, so the client connects and
     * then waits in vain for the INFO that opens the protocol.
     */
    private function connectionToASilentServer(): Connection
    {
        $this->listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        $this->assertNotFalse($this->listener, "could not listen: $error");

        $address = stream_socket_get_name($this->listener, false);

        $client = new Client(new Configuration([
            'servers' => [$address],
            'timeout' => 0.1,
        ]));

        return $client->connection;
    }
}
