<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Message\Ping;
use Exception;
use Psr\Log\AbstractLogger;
use Tests\TestCase;
use Tests\Utils\ScriptedServer;

/**
 * Connection behaviour that depends on the exact order of protocol lines, played by
 * a server that follows a script.
 */
class ConnectionScriptedServerTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"X","server_name":"X","version":"2.10.0","host":"127.0.0.1",'
        . '"port":4222,"headers":true,"max_payload":1048576,"proto":1}' . "\r\n";

    /** A handshake that is accepted, followed by one ping from the application. */
    private const ACCEPTING = [
        ['send', self::INFO],
        ['read'], // CONNECT
        ['read'], // PING
        ['send', "PONG\r\n"],
        ['read'], // the application's PING
    ];

    /**
     * A server with something to say about its cluster may say it before answering
     * the PING. That used to fail an otherwise good connection.
     */
    public function testInfoArrivingBeforeThePongDoesNotFailTheHandshake(): void
    {
        $cluster = 'INFO {"server_id":"X","connect_urls":["127.0.0.1:4999"],"proto":1}' . "\r\n";
        $server = ScriptedServer::start([
            ['send', self::INFO],
            ['read'],
            ['read'],
            ['send', $cluster],
            ['send', "PONG\r\n"],
            ['read'],
        ]);

        $client = $this->client($server->address);
        $client->connection->sendMessage(new Ping());

        $this->assertSame(['nats://127.0.0.1:4999'], $client->connection->getDiscoveredServers());

        $client->connection->close();
        $this->assertCount(3, $server->finish());
    }

    /**
     * Two rejections in a row retire the server. Each attempt is a new connection, so
     * the server plays the rejection twice.
     */
    public function testServerThatRejectsTheCredentialsTwiceIsDropped(): void
    {
        $server = ScriptedServer::start([
            ['send', self::INFO],
            ['read'],
            ['read'],
            ['send', "-ERR 'Authorization Violation'\r\n"],
        ], 2);

        $client = $this->client($server->address);
        $pool = $client->connection->getPool();

        $this->assertFailsWith('Authorization Violation', fn () => $client->connection->sendMessage(new Ping()));
        $this->assertSame([$this->url($server->address)], $client->connection->getServers());
        $this->assertTrue($pool->current()->authenticationFailed);

        $this->assertFailsWith('Authorization Violation', fn () => $client->connection->sendMessage(new Ping()));
        $this->assertSame([], $client->connection->getServers());

        $server->finish();
    }

    public function testForceReconnectIsHonouredWithAutomaticReconnectionOffAndLogsNoError(): void
    {
        $server = ScriptedServer::start(self::ACCEPTING, 2);
        $logger = $this->recordingLogger();

        $client = $this->client($server->address, ['reconnect' => false]);
        $client->connection->setLogger($logger);

        $client->connection->sendMessage(new Ping());
        $client->forceReconnect();
        $client->connection->sendMessage(new Ping());
        $client->connection->close();

        $connects = array_filter(
            $server->finish(),
            fn (?string $line) => $line !== null && str_starts_with($line, 'CONNECT ')
        );

        $this->assertCount(2, $connects, 'the requested reconnection has to happen');
        $this->assertSame([], $logger->records('error'), 'a requested reconnection is not a failure');
    }

    /**
     * The failure that stops a requested reconnection is the connection error, not
     * something made up to stand for it.
     */
    public function testForceReconnectReportsTheConnectionErrorWhenNoServerAnswers(): void
    {
        $server = ScriptedServer::start(self::ACCEPTING, 1);

        $client = $this->client($server->address, ['maxReconnectAttempts' => 1, 'timeout' => 0.3]);
        $client->connection->sendMessage(new Ping());

        $client->forceReconnect();

        try {
            $client->connection->sendMessage(new Ping());
            $this->fail('nothing is left to reconnect to');
        } catch (Exception $e) {
            $this->assertSame(Exception::class, $e::class);
            $this->assertNotSame('No servers available', $e->getMessage());
        }

        $server->finish();
    }

    /**
     * There is nothing to move off before the first connection, so asking to is
     * answered by the connection the next write makes anyway, not by a second one.
     */
    public function testForceReconnectBeforeTheFirstConnectionConnectsOnce(): void
    {
        $server = ScriptedServer::start(self::ACCEPTING, 1);

        $client = $this->client($server->address, ['maxReconnectAttempts' => 1, 'timeout' => 0.5]);
        $client->forceReconnect();
        $client->connection->sendMessage(new Ping());
        $client->connection->close();

        $lines = $server->finish();

        $this->assertCount(
            1,
            array_filter($lines, fn (?string $line) => $line !== null && str_starts_with($line, 'CONNECT '))
        );
        $this->assertSame('PING', $lines[2], 'the application ping is the first thing sent after the handshake');
    }

    /**
     * With the default, unlimited budget a requested reconnect that finds nobody
     * used to keep sweeping the pool for ever, blocking the publish or process call
     * that triggered it. It is one pass: the caller asked for a move, and hearing
     * that none is possible is the answer.
     */
    public function testForceReconnectMakesOnePassWhenTheBudgetIsUnlimited(): void
    {
        $server = ScriptedServer::start(self::ACCEPTING, 1);

        $attempts = 0;
        $client = $this->client($server->address, ['timeout' => 0.3]);
        // Aborts a runaway loop with a failure instead of letting the test hang.
        $client->connection->setLogger(new class ($attempts) extends AbstractLogger {
            public function __construct(private int &$attempts)
            {
            }

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if (str_contains((string) $message, 'failed:') && ++$this->attempts > 3) {
                    throw new \RuntimeException('still sweeping after ' . $this->attempts . ' attempts');
                }
            }
        });

        $client->connection->sendMessage(new Ping());
        $client->forceReconnect();

        try {
            $client->connection->sendMessage(new Ping());
            $this->fail('nothing is left to reconnect to');
        } catch (Exception $e) {
            $this->assertSame(Exception::class, $e::class);
            $this->assertStringNotContainsString('still sweeping', $e->getMessage());
        }

        $this->assertSame(1, $attempts, 'one pass over a one server pool');

        $server->finish();
    }

    /**
     * Dropping a server explains every failure that follows, so it is logged where
     * it happens, with the address and the reason.
     */
    public function testDroppingAServerIsLoggedAsAWarning(): void
    {
        $server = ScriptedServer::start([
            ['send', self::INFO],
            ['read'],
            ['read'],
            ['send', "-ERR 'Authorization Violation'\r\n"],
        ], 2);

        $logger = $this->recordingLogger();
        $client = $this->client($server->address);
        $client->connection->setLogger($logger);

        $this->assertFailsWith('Authorization Violation', fn () => $client->connection->sendMessage(new Ping()));
        $this->assertFailsWith('Authorization Violation', fn () => $client->connection->sendMessage(new Ping()));

        $this->assertContains(
            'dropped ' . $server->address . ' from the pool: rejected the credentials twice running',
            $logger->records('warning')
        );

        $server->finish();
    }

    /**
     * Only the very first connection fills the pool silently, so that starting up
     * against an assembled cluster reports no discovery. A server met while
     * reconnecting that names a peer the client has never heard of is news.
     */
    public function testServerMetWhileReconnectingIsReportedToTheDiscoveryHandler(): void
    {
        $quiet = ScriptedServer::start(self::ACCEPTING);

        $withPeer = 'INFO {"server_id":"B","proto":1,"max_payload":1048576,"connect_urls":["127.0.0.1:4998"]}' . "\r\n";
        $joined = ScriptedServer::start([
            ['send', $withPeer],
            ['read'],
            ['read'],
            ['send', "PONG\r\n"],
            ['read'],
        ]);

        $notified = [];
        $client = $this->clusterClient([$quiet->address, $joined->address], [
            'discoveredServersHandler' => function () use (&$notified) {
                $notified[] = 'discovered';
            },
        ]);

        $client->connection->sendMessage(new Ping());
        $this->assertSame([], $notified, 'the first connection is silent');

        $client->forceReconnect();
        $client->connection->sendMessage(new Ping());

        $this->assertSame(['discovered'], $notified);
        $this->assertContains('nats://127.0.0.1:4998', $client->connection->getDiscoveredServers());

        $client->connection->close();
        $quiet->finish();
        $joined->finish();
    }

    /**
     * A lame duck notice between CONNECT and PONG used to be dropped, and the pool
     * forgot the server was draining once the connection was confirmed.
     */
    public function testLameDuckNoticeBeforeThePongIsReported(): void
    {
        $quiet = ScriptedServer::start(self::ACCEPTING);

        $draining = ScriptedServer::start([
            ['send', self::INFO],
            ['read'],
            ['read'],
            ['send', 'INFO {"server_id":"B","proto":1,"max_payload":2048,"ldm":true}' . "\r\n"],
            ['send', "PONG\r\n"],
            ['read'],
        ]);

        $notified = [];
        $client = $this->clusterClient([$quiet->address, $draining->address], [
            'lameDuckModeHandler' => function () use (&$notified) {
                $notified[] = 'draining';
            },
        ]);

        $client->connection->sendMessage(new Ping());
        $client->forceReconnect();
        $client->connection->sendMessage(new Ping());

        $this->assertSame(['draining'], $notified);
        $this->assertTrue($client->connection->getPool()->current()->draining);
        $this->assertSame(2048, $client->connection->getInfoMessage()->max_payload, 'the later INFO is merged');

        $client->connection->close();
        $quiet->finish();
        $draining->finish();
    }

    /**
     * The handler runs once the connection is up, not in the middle of the
     * handshake: a handler that publishes, as one asking to migrate might, would
     * otherwise write to a connection that has not sent CONNECT yet.
     */
    public function testNotificationsWaitForTheHandshakeToFinish(): void
    {
        $quiet = ScriptedServer::start(self::ACCEPTING);
        $draining = ScriptedServer::start([
            ['send', 'INFO {"server_id":"B","proto":1,"max_payload":1048576,"ldm":true}' . "\r\n"],
            ['read'],
            ['read'],
            ['send', "PONG\r\n"],
            ['read'],
        ]);

        $connecting = null;
        $client = $this->clusterClient([$quiet->address, $draining->address]);
        $connection = $client->connection;
        $client->configuration->lameDuckModeHandler = function () use (&$connecting, $connection) {
            $property = new \ReflectionProperty($connection, 'connecting');
            $connecting = $property->getValue($connection);
        };

        $connection->sendMessage(new Ping());
        $client->forceReconnect();
        $connection->sendMessage(new Ping());

        $this->assertFalse($connecting, 'the handler ran after the handshake, not during it');

        $connection->close();
        $quiet->finish();
        $draining->finish();
    }

    /**
     * An opening INFO that already says draining is not a transition on the very
     * first connection, as in the go and python clients.
     */
    public function testFirstConnectionToADrainingServerIsSilent(): void
    {
        $draining = ScriptedServer::start([
            ['send', 'INFO {"server_id":"B","proto":1,"max_payload":1048576,"ldm":true}' . "\r\n"],
            ['read'],
            ['read'],
            ['send', "PONG\r\n"],
            ['read'],
        ]);

        $notified = [];
        $client = $this->clusterClient([$draining->address], [
            'lameDuckModeHandler' => function () use (&$notified) {
                $notified[] = 'draining';
            },
        ]);

        $client->connection->sendMessage(new Ping());

        $this->assertSame([], $notified);
        $this->assertTrue($client->connection->getInfoMessage()->ldm, 'it is still visible on the INFO');

        $client->connection->close();
        $draining->finish();
    }

    /**
     * A line with no end is not buffered for ever. What is left of it cannot be told
     * from the start of the next one, so the connection is dropped rather than read
     * on from the middle of it.
     */
    public function testProtocolLineBeyondTheLimitFailsAndDropsTheConnection(): void
    {
        $server = ScriptedServer::start([
            ['flood', 'INFO {"server_name":"', 'x', 1_048_576 + 10],
            ['sleep', 0.3],
        ]);

        $client = $this->client($server->address);

        $this->assertFailsWith('exceeds', fn () => $client->connection->sendMessage(new Ping()));

        $socket = new \ReflectionProperty($client->connection, 'socket');
        $this->assertFalse(is_resource($socket->getValue($client->connection)), 'the socket is not left mid line');

        $server->finish();
    }

    /**
     * The wait is once per pass over the servers, not between attempts, so a healthy
     * peer further down the list is reached without an artificial delay. Three dead
     * servers and two passes are one wait; waiting after every attempt would be five.
     */
    public function testReconnectWaitsOncePerPassOverTheServers(): void
    {
        $live = ScriptedServer::start(self::ACCEPTING);

        $client = $this->clusterClient([$live->address, '127.0.0.1:1', '127.0.0.1:2'], [
            'maxReconnectAttempts' => 2,
            'reconnectWait' => 0.25,
            'timeout' => 0.3,
        ]);
        $client->connection->sendMessage(new Ping());

        // The server goes away and refuses new connections, as when it dies.
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        fclose($serverEnd);
        $socket = new \ReflectionProperty($client->connection, 'socket');
        fclose($socket->getValue($client->connection));
        $socket->setValue($client->connection, $clientEnd);
        $live->finish();

        $start = microtime(true);
        $this->assertFailsWith('', fn () => $client->connection->sendMessage(new Ping()));
        $elapsed = microtime(true) - $start;

        $this->assertGreaterThanOrEqual(0.2, $elapsed, 'it waited between the two passes');
        $this->assertLessThan(0.9, $elapsed, 'and not after every attempt');
    }

    private function clusterClient(array $servers, array $options = []): Client
    {
        return new Client(new Configuration($options + [
            'servers' => $servers,
            'noRandomize' => true,
            'timeout' => 1,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]));
    }

    private function client(string $address, array $options = []): Client
    {
        return new Client(new Configuration($options + [
            'servers' => [$address],
            'timeout' => 1,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]));
    }

    private function url(string $address): string
    {
        return 'nats://' . $address;
    }

    private function assertFailsWith(string $message, callable $action): void
    {
        try {
            $action();
        } catch (Exception $e) {
            $this->assertStringContainsStringIgnoringCase($message, $e->getMessage());

            return;
        }

        $this->fail("expected a failure mentioning '$message'");
    }

    private function recordingLogger(): object
    {
        return new class () extends AbstractLogger {
            /** @var array<int, array{string, string}> */
            private array $records = [];

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->records[] = [(string) $level, (string) $message];
            }

            /** @return string[] messages logged at the given level */
            public function records(string $level): array
            {
                return array_column(
                    array_filter($this->records, fn (array $record) => $record[0] === $level),
                    1
                );
            }
        };
    }
}
