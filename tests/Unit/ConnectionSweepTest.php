<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Message\Ping;
use Exception;
use Tests\TestCase;
use Tests\Utils\ScriptedServer;

/**
 * How a pass over the pool picks the next server after one fails.
 */
class ConnectionSweepTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"X","server_name":"X","version":"2.10.0","host":"127.0.0.1",'
        . '"port":4222,"headers":true,"max_payload":1048576,"proto":1}' . "\r\n";

    private const REJECTING = [
        ['send', self::INFO],
        ['read'], // CONNECT
        ['read'], // PING
        ['send', "-ERR 'Authorization Violation'\r\n"],
    ];

    private const ACCEPTING = [
        ['send', self::INFO],
        ['read'], // CONNECT
        ['read'], // PING
        ['send', "PONG\r\n"],
        ['read'], // the application's PING
    ];

    /**
     * A server tells us about its peers in the INFO that opens the connection, so a
     * seed that goes on to fail has already named the servers worth trying next.
     * The pass used to be sized before that INFO arrived and gave up without them.
     */
    public function testServersAdvertisedByAFailingSeedAreStillTried(): void
    {
        $peer = ScriptedServer::start(self::ACCEPTING);

        $seedInfo = 'INFO {"server_id":"X","server_name":"X","version":"2.10.0","host":"127.0.0.1",'
            . '"port":4222,"headers":true,"max_payload":1048576,"proto":1,'
            . '"connect_urls":["' . $peer->address . '"]}' . "\r\n";

        $seed = ScriptedServer::start([
            ['send', $seedInfo],
            ['read'],
            ['read'],
            ['send', "-ERR 'Authorization Violation'\r\n"],
        ]);

        $client = $this->client([$seed->address]);
        $client->connection->sendMessage(new Ping());

        $this->assertSame($peer->address, $client->connection->getPool()->current()->getAddress());

        $client->connection->close();
        $seed->finish();
        $peer->finish();
    }

    /**
     * A server that was dropped from the pool leaves the next one in line as the
     * current server. Rotating again on top of that handed the attempt to the one
     * after it.
     */
    public function testServerAfterADroppedOneIsTriedNext(): void
    {
        $rejecting = ScriptedServer::start(self::REJECTING);
        $first = ScriptedServer::start(self::ACCEPTING);
        $second = ScriptedServer::start(self::ACCEPTING);

        $client = $this->client([$rejecting->address, $first->address, $second->address]);

        // The server has already rejected us once, so one more rejection drops it.
        $client->connection->getPool()->current()->authenticationFailed = true;

        $client->connection->sendMessage(new Ping());

        $this->assertSame($first->address, $client->connection->getPool()->current()->getAddress());
        $this->assertNotContains(
            'nats://' . $rejecting->address,
            $client->connection->getServers(),
            'the rejecting server is gone'
        );

        $client->connection->close();
        $rejecting->finish();
        $first->finish();
        // Never connected to, so it is still waiting for a client.
        $second->stop();
    }

    /**
     * Two rejections only count against a server when nothing else went wrong in
     * between. A failure of another kind is no evidence the credentials are bad.
     */
    public function testAFailureOfAnotherKindClearsTheRejection(): void
    {
        $server = ScriptedServer::start([
            ['send', self::INFO],
            ['read'],
            ['read'],
            // ends without answering: the connection closes under the handshake
        ]);

        $client = $this->client([$server->address]);
        $current = $client->connection->getPool()->current();
        $current->authenticationFailed = true;

        try {
            $client->connection->sendMessage(new Ping());
            $this->fail('the handshake has to fail');
        } catch (Exception) {
            // expected
        }

        $this->assertFalse($current->authenticationFailed);

        $server->finish();
    }

    private function client(array $servers): Client
    {
        return new Client(new Configuration([
            'servers' => $servers,
            'noRandomize' => true,
            'timeout' => 1,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]));
    }
}
