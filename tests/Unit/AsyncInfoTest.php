<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Connection;
use Basis\Nats\Message\Pong;
use ReflectionProperty;
use Tests\TestCase;

/**
 * Cluster topology and lame duck mode arrive as asynchronous INFO messages. A
 * socket pair stands in for the server so both can be delivered exactly.
 */
class AsyncInfoTest extends TestCase
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

    public function testDiscoveredServersJoinThePool(): void
    {
        $client = $this->client(['servers' => ['a.example.com:4222'], 'noRandomize' => true]);

        $this->deliver($client, ['connect_urls' => ['a.example.com:4222', 'b.example.com:4222']]);

        $this->assertSame(['nats://b.example.com:4222'], $client->getDiscoveredServers());
        $this->assertCount(2, $client->getServers());
    }

    public function testDiscoveryNotifiesOnce(): void
    {
        $calls = 0;
        $client = $this->client([
            'servers' => ['a.example.com:4222'],
            'noRandomize' => true,
            'discoveredServersHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);
        $this->assertSame(1, $calls);

        // The same address again is not a new discovery.
        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);
        $this->assertSame(1, $calls);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222', 'c.example.com:4222']]);
        $this->assertSame(2, $calls);
    }

    /**
     * Lame duck mode is a notification. The go, python and javascript clients all
     * keep the connection and wait for the server to close it, leaving the decision
     * to migrate to the application.
     */
    public function testLameDuckModeNotifiesAndKeepsTheConnection(): void
    {
        $notified = [];
        $client = $this->client([
            'lameDuckModeHandler' => function (Client $client) use (&$notified) {
                $notified[] = $client->connection->getPool()->current()->getAddress();
            },
        ]);

        $this->deliver($client, ['ldm' => true]);

        $this->assertSame(['localhost:4222'], $notified);
        $this->assertTrue($client->connection->getPool()->current()->draining);

        $socket = new ReflectionProperty(Connection::class, 'socket');
        $this->assertTrue(is_resource($socket->getValue($client->connection)));
    }

    public function testLameDuckModeNotifiesOnlyOnTheTransition(): void
    {
        $calls = 0;
        $client = $this->client([
            'lameDuckModeHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['ldm' => true]);
        $this->deliver($client, ['ldm' => true]);

        $this->assertSame(1, $calls, 'the server repeats the flag on every later update');
    }

    public function testNoLameDuckNotificationWithoutTheFlag(): void
    {
        $calls = 0;
        $client = $this->client([
            'lameDuckModeHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);

        $this->assertSame(0, $calls);
    }

    /**
     * A handler runs inline on the read that delivered the update, so it must not be
     * able to take the connection down.
     */
    public function testThrowingHandlerIsContained(): void
    {
        $client = $this->client([
            'lameDuckModeHandler' => function () {
                throw new \RuntimeException('handler exploded');
            },
        ]);

        $warnings = $this->collectingWarnings(fn () => $this->deliver($client, ['ldm' => true]));

        $this->assertTrue($client->connection->getPool()->current()->draining);
        $this->assertCount(1, $warnings);
    }

    /**
     * Without a logger a failing handler used to leave no trace at all, so a lame
     * duck handler that threw before asking to migrate simply never migrated.
     */
    public function testThrowingHandlerIsReportedWhenThereIsNoLogger(): void
    {
        $client = $this->client([
            'discoveredServersHandler' => function () {
                throw new \RuntimeException('handler exploded');
            },
            'servers' => ['a.example.com:4222'],
        ]);

        $warnings = $this->collectingWarnings(
            fn () => $this->deliver($client, ['connect_urls' => ['b.example.com:4222']])
        );

        $this->assertCount(1, $warnings);
        $this->assertStringContainsString('handler exploded', $warnings[0]);
    }

    public function testThrowingHandlerGoesToTheLoggerWhenThereIsOne(): void
    {
        $logged = [];
        $client = $this->client([
            'lameDuckModeHandler' => function () {
                throw new \RuntimeException('handler exploded');
            },
        ]);
        $client->connection->setLogger(new class ($logged) extends \Psr\Log\AbstractLogger {
            public function __construct(private array &$logged)
            {
            }

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->logged[] = [(string) $level, (string) $message];
            }
        });

        $warnings = $this->collectingWarnings(fn () => $this->deliver($client, ['ldm' => true]));

        $this->assertSame([], $warnings, 'a logger is the place for it, no warning as well');
        $this->assertContains(['error', 'handler failed: handler exploded'], $logged);
    }

    public function testIgnoreDiscoveredServersKeepsThePoolAsConfigured(): void
    {
        $calls = 0;
        $client = $this->client([
            'servers' => ['a.example.com:4222'],
            'ignoreDiscoveredServers' => true,
            'discoveredServersHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);

        $this->assertSame(['nats://a.example.com:4222'], $client->getServers());
        $this->assertSame(0, $calls);
    }

    /**
     * The lame duck flag still gets through when discovery is switched off, since
     * the two are unrelated concerns carried on the same message.
     */
    public function testLameDuckModeStillNotifiesWhenDiscoveryIsIgnored(): void
    {
        $calls = 0;
        $client = $this->client([
            'ignoreDiscoveredServers' => true,
            'lameDuckModeHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['ldm' => true, 'connect_urls' => ['b.example.com:4222']]);

        $this->assertSame(1, $calls);
    }

    /**
     * Only the handshake waits for an INFO. A later one is handled by the client, so
     * the read that the caller made for its own reply must not be spent on it.
     */
    public function testAsynchronousInfoIsNotHandedToTheCaller(): void
    {
        $client = $this->client([]);
        $this->attach($client, "INFO {\"connect_urls\":[\"b:4222\"]}\r\nPONG\r\n");

        $message = $client->connection->getMessage(1);

        $this->assertInstanceOf(Pong::class, $message);
        $this->assertSame(['nats://b:4222'], $client->connection->getDiscoveredServers());
    }

    public function testAsynchronousInfoAloneLeavesTheCallerWithNothing(): void
    {
        $client = $this->client([]);
        $this->attach($client, "INFO {\"connect_urls\":[\"b:4222\"]}\r\n");

        $this->assertNull($client->connection->getMessage(0));
        $this->assertSame(['nats://b:4222'], $client->connection->getDiscoveredServers());
    }

    private function attach(Client $client, string $wire): void
    {
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        $this->sockets[] = $clientEnd;
        $this->sockets[] = $serverEnd;

        fwrite($serverEnd, $wire);

        (new ReflectionProperty(Connection::class, 'socket'))->setValue($client->connection, $clientEnd);
    }

    /**
     * Runs an action and returns the user level warnings it raised. PHPUnit would
     * otherwise turn the first one into an exception.
     *
     * @return string[]
     */
    private function collectingWarnings(callable $action): array
    {
        $warnings = [];
        set_error_handler(function (int $level, string $message) use (&$warnings): bool {
            $warnings[] = $message;

            return true;
        }, E_USER_WARNING);

        try {
            $action();
        } finally {
            restore_error_handler();
        }

        return $warnings;
    }

    /**
     * Later INFO messages repeat the tls flags. Acting on them again would hand an
     * already encrypted socket to the tls handshake a second time.
     */
    public function testAsynchronousInfoDoesNotStartTlsAgain(): void
    {
        $client = $this->client([]);
        (new ReflectionProperty(Connection::class, 'tlsEnabled'))->setValue($client->connection, true);

        $this->deliver($client, ['tls_required' => true, 'tls_verify' => true]);

        $this->assertTrue($client->connection->getInfoMessage()->tls_required);
    }

    /**
     * Shows the guard is what makes the test above pass: on a connection that has
     * not negotiated tls the same message does start the handshake.
     */
    public function testTlsIsStartedWhenItHasNotBeenNegotiated(): void
    {
        $client = $this->client([]);
        // Stands in for the context the handshake would have created.
        (new ReflectionProperty(Connection::class, 'context'))->setValue($client->connection, stream_context_create());

        // A socket pair cannot do tls, and being asked to is what shows the handshake
        // was started.
        $raised = [];
        set_error_handler(function (int $level, string $message) use (&$raised): bool {
            $raised[] = $message;

            return true;
        }, E_WARNING);

        try {
            $this->expectExceptionMessage('Error enabling TLS');
            $this->deliver($client, ['tls_required' => true]);
        } finally {
            restore_error_handler();
            $this->assertStringContainsString('does not support SSL/crypto', $raised[0] ?? '');
        }
    }

    private function client(array $options): Client
    {
        return new Client(new Configuration($options + ['reconnect' => false, 'timeout' => 1]));
    }

    /** Hands an INFO message to the client over a socket pair standing in for the server. */
    private function deliver(Client $client, array $info): void
    {
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        $this->sockets[] = $clientEnd;
        $this->sockets[] = $serverEnd;

        fwrite($serverEnd, 'INFO ' . json_encode($info) . "\r\n");

        $socket = new ReflectionProperty(Connection::class, 'socket');
        $socket->setValue($client->connection, $clientEnd);

        $client->connection->getMessage(0);
    }
}
