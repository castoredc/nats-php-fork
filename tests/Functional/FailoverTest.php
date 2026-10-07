<?php

declare(strict_types=1);

namespace Tests\Functional;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Connection;
use Exception;
use ReflectionProperty;
use Tests\FunctionalTestCase;

class FailoverTest extends FunctionalTestCase
{
    /** Ports nothing is listening on. */
    private const DEAD = ['localhost:4291', 'localhost:4292'];

    private function live(): string
    {
        return getenv('NATS_HOST') . ':' . getenv('NATS_PORT');
    }

    public function testSingleServerEntry(): void
    {
        $client = $this->createClient(['servers' => [$this->live()]]);

        $this->assertTrue($client->ping());
    }

    /**
     * The very first connection has to walk the pool too, not only reconnects.
     */
    public function testInitialConnectionSkipsDeadServers(): void
    {
        $client = $this->createClient([
            'servers' => [...self::DEAD, $this->live()],
            'noRandomize' => true,
        ]);

        $this->assertTrue($client->ping());
        $this->assertSame($this->live(), $client->connection->getPool()->current()->getAddress());
    }

    public function testReconnectMovesToAnotherServer(): void
    {
        $client = $this->createClient([
            'servers' => [$this->live(), ...self::DEAD],
            'noRandomize' => true,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]);

        $client->ping();
        $this->assertSame($this->live(), $client->connection->getPool()->current()->getAddress());

        $socket = new ReflectionProperty(Connection::class, 'socket');
        fclose($socket->getValue($client->connection));

        // Everything else in the pool is dead, so recovery has to come back round
        // to the live server rather than give up on it.
        $this->assertTrue($client->ping());
        $this->assertSame($this->live(), $client->connection->getPool()->current()->getAddress());
    }

    /**
     * An unreachable single server still reports the socket error rather than
     * retrying forever, which is what an initial connection is expected to do.
     */
    public function testUnreachableServerReportsTheSocketError(): void
    {
        $client = $this->createClient(['servers' => ['localhost:4291']]);

        $this->expectExceptionMessageMatches('/^Connection refused$|^A connection attempt failed/');
        $client->ping();
    }

    public function testEveryServerUnreachable(): void
    {
        $client = $this->createClient([
            'servers' => self::DEAD,
            'noRandomize' => true,
        ]);

        $this->expectException(Exception::class);
        $client->ping();
    }

    /**
     * The budget is per server, so a pool of dead servers is retired rather than
     * swept forever.
     */
    public function testExhaustedPoolStopsRetrying(): void
    {
        $client = $this->createClient([
            'servers' => self::DEAD,
            'noRandomize' => true,
            'maxReconnectAttempts' => 1,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]);

        try {
            $client->ping();
            $this->fail('connecting to a pool of dead servers has to fail');
        } catch (Exception) {
            // expected
        }

        $this->assertSame(0, $client->connection->getPool()->count());
    }

    public function testCredentialsFromServerUrlAreUsed(): void
    {
        $host = getenv('NATS_HOST');
        $client = $this->createClient([
            'servers' => ["nats://ruser:T0pS3cr3t@$host:4224"],
        ]);

        $this->assertTrue($client->ping());
        $this->assertSame('ruser', $client->connection->getConnectMessage()->user);
    }

    /**
     * Rejected credentials are not retried indefinitely: the second failure in a
     * row retires the server.
     */
    public function testRejectedCredentialsRetireTheServer(): void
    {
        $host = getenv('NATS_HOST');
        $client = $this->createClient([
            'servers' => ["nats://ruser:wrong-password@$host:4224"],
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]);

        try {
            $client->ping();
            $this->fail('a rejected password has to fail');
        } catch (Exception) {
            // expected
        }

        $this->assertTrue($client->connection->getPool()->current()?->authenticationFailed ?? false);
    }

    public function testForceReconnectEstablishesANewConnection(): void
    {
        $client = $this->createClient([
            'servers' => [$this->live()],
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]);
        $client->ping();

        $socket = new ReflectionProperty(Connection::class, 'socket');
        $before = (int) $socket->getValue($client->connection);

        $client->forceReconnect();

        $this->assertTrue($client->ping());
        $this->assertNotSame($before, (int) $socket->getValue($client->connection));
    }

    /**
     * The documented way to leave a draining server: the lame duck handler asks for
     * a reconnect, which is applied at the next read or write rather than unwinding
     * the read that delivered the notification.
     */
    public function testLameDuckHandlerCanMigrateOffTheDrainingServer(): void
    {
        $client = $this->createClient([
            'servers' => [$this->live()],
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
            'lameDuckModeHandler' => fn (Client $client) => $client->forceReconnect(),
        ]);
        $client->ping();

        $socket = new ReflectionProperty(Connection::class, 'socket');
        $before = (int) $socket->getValue($client->connection);

        // Stand in for the draining server and announce it.
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        fwrite($serverEnd, 'INFO {"ldm":true}' . "\r\n");
        $socket->setValue($client->connection, $clientEnd);

        $client->connection->getMessage(0);

        $this->assertTrue(
            $client->connection->getPool()->current()->draining,
            'the server has to be recorded as draining'
        );

        // Applied here, on the next use of the connection.
        $this->assertTrue($client->ping());
        $this->assertNotSame($before, (int) $socket->getValue($client->connection));
        $this->assertFalse(
            $client->connection->getPool()->current()->draining,
            'a fresh connection is not draining'
        );

        fclose($serverEnd);
    }

    public function tearDown(): void
    {
        // The inherited teardown talks to the server, which these clients cannot.
    }
}
