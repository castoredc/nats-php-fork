<?php

declare(strict_types=1);

namespace Tests\Functional;

use Basis\Nats\Client;
use Basis\Nats\Connection;
use ReflectionProperty;
use Tests\FunctionalTestCase;

/**
 * Exercises discovery and failover against the three node cluster in
 * docker/docker-compose.yml. Each node advertises a client_advertise address that
 * is reachable from here, which is what makes connect_urls usable at all.
 */
class ClusterTest extends FunctionalTestCase
{
    /** Cached so only the first test pays for waiting on a cluster that is absent. */
    private static ?string $unavailable = null;
    private static bool $checked = false;

    private const NODES = [
        1 => ['client' => '127.0.0.1:4231', 'monitor' => 8231],
        2 => ['client' => '127.0.0.1:4232', 'monitor' => 8232],
        3 => ['client' => '127.0.0.1:4233', 'monitor' => 8233],
    ];

    public function setUp(): void
    {
        parent::setUp();

        $this->awaitCluster();
    }

    public function testTopologyIsDiscovered(): void
    {
        $client = $this->clusterClient([self::NODES[1]['client']]);
        $client->ping();

        $discovered = $this->awaitDiscovery($client, 2);

        $this->assertContains('nats://' . self::NODES[2]['client'], $discovered);
        $this->assertContains('nats://' . self::NODES[3]['client'], $discovered);
        $this->assertCount(3, $client->getServers(), 'one entry per node');
    }

    /**
     * An assembled cluster advertises itself in the INFO that opens the connection,
     * so the pool is already complete by the time the client is connected and there
     * is no discovery to report. The go and python clients both suppress the
     * notification for that first message, and python pins this exact case.
     *
     * The notification for a node that joins later is covered without a cluster in
     * Tests\Unit\AsyncInfoTest.
     */
    public function testAnAssembledClusterReportsNoDiscovery(): void
    {
        $calls = 0;
        $client = $this->clusterClient(
            [self::NODES[1]['client']],
            ['discoveredServersHandler' => function () use (&$calls) {
                $calls++;
            }]
        );
        $client->ping();
        $client->process(0.3);

        $this->assertCount(3, $client->getServers(), 'the pool is filled during the handshake');
        $this->assertSame(0, $calls);
    }

    /**
     * A configured host name is not resolved before being compared, so it does not
     * match the address the cluster advertises for that same node and both end up
     * in the pool. The go client behaves the same way; resolving would be wrong,
     * since a name can stand for several addresses.
     */
    public function testAHostNameDoesNotMatchAnAdvertisedAddress(): void
    {
        $client = $this->clusterClient(['localhost:4231']);
        $client->ping();

        $this->awaitDiscovery($client, 3);

        $this->assertContains('nats://localhost:4231', $client->getServers());
        $this->assertContains('nats://127.0.0.1:4231', $client->getServers());
        $this->assertCount(4, $client->getServers(), 'node one is present under both names');
    }

    /**
     * The point of discovery: a client configured with one node can fail over onto
     * a node it was never told about.
     */
    public function testFailoverOntoADiscoveredNode(): void
    {
        $client = $this->clusterClient([self::NODES[1]['client']]);
        $client->ping();
        $this->awaitDiscovery($client, 2);

        $before = $client->connection->getPool()->current()->getAddress();
        $this->assertSame(self::NODES[1]['client'], $before);

        $this->dropConnection($client);

        $this->assertTrue($client->ping());

        // Any of the three addresses would satisfy a membership check, node 1 among
        // them, so a client that never left would pass. It has to be on a node it was
        // never configured with.
        $current = $client->connection->getPool()->current();
        $this->assertNotSame($before, $current->getAddress());
        $this->assertTrue($current->isImplicit, 'the new server was learned from the cluster');
        $this->assertContains($current->getAddress(), array_column(self::NODES, 'client'));
    }

    /**
     * Messages still reach a subscriber on another node after the publisher has
     * been forced onto a different one, which is what makes the failover useful
     * rather than merely successful.
     */
    public function testMessagesStillRouteAfterFailover(): void
    {
        $subject = 'cluster.' . bin2hex(random_bytes(4));

        $subscriber = $this->clusterClient([self::NODES[2]['client']]);
        $received = [];
        $subscriber->subscribe($subject, function ($message) use (&$received) {
            $received[] = (string) $message;
        });
        $subscriber->process(1);

        $publisher = $this->clusterClient([self::NODES[1]['client']]);
        $publisher->ping();
        $this->awaitDiscovery($publisher, 2);

        $before = $publisher->connection->getPool()->current()->getAddress();

        $this->dropConnection($publisher);
        $publisher->publish($subject, 'after failover');

        // The cluster routes the message whichever node the publisher is on, so
        // delivery alone does not show it moved.
        $this->assertNotSame($before, $publisher->connection->getPool()->current()->getAddress());

        $threshold = microtime(true) + 3;
        while ($received === [] && microtime(true) < $threshold) {
            $subscriber->process(0.1);
        }

        $this->assertSame(['after failover'], $received);

        $subscriber->disconnect();
        $publisher->disconnect();
    }

    public function tearDown(): void
    {
        // The inherited teardown deletes every stream on the default server, which
        // has nothing to do with the cluster these tests use.
    }

    /**
     * Replaces the socket with one whose peer has gone, which is what the client sees
     * when a server dies. Closing the client's own socket is not the same: that is
     * noticed before any read or write, and reconnects to the server it was already
     * using rather than moving on.
     */
    private function dropConnection(Client $client): void
    {
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        fclose($serverEnd);

        $socket = new ReflectionProperty(Connection::class, 'socket');
        fclose($socket->getValue($client->connection));
        $socket->setValue($client->connection, $clientEnd);
    }

    private function clusterClient(array $servers, array $options = []): Client
    {
        return $this->createClient($options + [
            'servers' => $servers,
            'noRandomize' => true,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]);
    }

    /**
     * Discovery arrives on an asynchronous INFO some time after the connection is
     * up, so the pool has to be given a chance to fill rather than read at once.
     *
     * @return string[]
     */
    private function awaitDiscovery(Client $client, int $expected): array
    {
        $threshold = microtime(true) + 5;

        while (microtime(true) < $threshold) {
            if (count($client->getDiscoveredServers()) >= $expected) {
                break;
            }
            $client->process(0.1);
        }

        return $client->getDiscoveredServers();
    }

    /**
     * Waits for every node to be up and routed to the others.
     *
     * `docker compose up -d` returns once the containers have started, not once
     * they are ready, and a three node mesh takes appreciably longer to form than a
     * single server takes to boot. Waiting rather than skipping outright keeps this
     * from quietly passing on a machine where the cluster was merely slow.
     */
    private function awaitCluster(): void
    {
        if (self::$checked) {
            if (self::$unavailable !== null) {
                $this->unavailable(self::$unavailable);
            }

            return;
        }

        self::$checked = true;
        $threshold = microtime(true) + 30;
        $routes = [];

        while (microtime(true) < $threshold) {
            $routes = [];

            foreach (self::NODES as $number => $node) {
                $varz = @file_get_contents("http://127.0.0.1:{$node['monitor']}/varz");
                $routes[$number] = is_string($varz) ? (json_decode($varz, true)['routes'] ?? 0) : 0;
            }

            if (min($routes) >= 2) {
                return;
            }

            usleep(250_000);
        }

        self::$unavailable = 'The cluster is not available (routes per node: '
            . json_encode($routes) . '). Start it with: docker compose up -d in docker/';

        $this->unavailable(self::$unavailable);
    }

    /**
     * Skips on a machine where the cluster simply is not running, but fails on CI. CI
     * starts the cluster itself, so finding it absent there is a broken setup, and a
     * skip would leave the run green with no cluster coverage at all.
     */
    private function unavailable(string $reason): void
    {
        if (getenv('CI') !== false && getenv('CI') !== '') {
            $this->fail($reason);
        }

        $this->markTestSkipped($reason);
    }
}
