<?php

declare(strict_types=1);

namespace Tests\Unit\Connection;

use Basis\Nats\Configuration;
use Basis\Nats\Connection\Server;
use Basis\Nats\Connection\ServerPool;
use Basis\Nats\Message\Info;
use RuntimeException;
use Tests\TestCase;

class ServerPoolTest extends TestCase
{
    /** Deterministic stand in for shuffle(): reverses instead of randomising. */
    private static function reversingShuffle(): callable
    {
        return static fn (array $servers): array => array_reverse($servers);
    }

    private function pool(array $options = [], ?callable $shuffle = null): ServerPool
    {
        $options += ['noRandomize' => true];

        return new ServerPool(new Configuration($options), $shuffle ? \Closure::fromCallable($shuffle) : null);
    }

    private function info(array $values): Info
    {
        return new Info($values);
    }

    public function testFallsBackToHostAndPort()
    {
        $pool = $this->pool(['host' => 'nats.example.com', 'port' => 4333]);

        $this->assertSame(['nats://nats.example.com:4333'], $pool->getServers());
        $this->assertSame('nats.example.com', $pool->current()->host);
    }

    /**
     * host and port are ignored entirely once servers is given: they default to
     * localhost:4222, so there is no way to tell a deliberate value from an
     * untouched one, and merging would inject localhost into every cluster.
     */
    public function testServersReplaceHostAndPort()
    {
        $pool = $this->pool([
            'host' => 'ignored.example.com',
            'servers' => ['a.example.com:4222', 'b.example.com:4222'],
        ]);

        $this->assertSame(
            ['nats://a.example.com:4222', 'nats://b.example.com:4222'],
            $pool->getServers()
        );
    }

    /**
     * A negative port has to survive as far as the socket, which reports it as a
     * refused connection.
     */
    public function testHostAndPortAreNotParsed()
    {
        $pool = $this->pool(['port' => -1]);

        $this->assertSame('tcp://localhost:-1', $pool->current()->getDsn());
    }

    /**
     * Both forms worked when the address was built as "$host:$port". The first has
     * to be bracketed once, not twice.
     */
    public function testBracketedIpv6HostIsAccepted()
    {
        $pool = $this->pool(['host' => '[::1]']);

        $this->assertSame('tcp://[::1]:4222', $pool->current()->getDsn());
        $this->assertSame('[::1]:4222', $pool->current()->getAddress());
    }

    public function testBareIpv6HostIsAccepted()
    {
        $this->assertSame('tcp://[::1]:4222', $this->pool(['host' => '::1'])->current()->getDsn());
    }

    /**
     * A scheme in host used to make php open a tls stream, which is not how the
     * protocol upgrades. It is refused with the place to put it instead, rather than
     * becoming an address that cannot be connected to.
     */
    public function testSchemeInHostIsRefused()
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("servers: ['tls://nats.example.com:4222']");

        $this->pool(['host' => 'tls://nats.example.com']);
    }

    public function testExplicitServersAreShuffledWhenRandomizing()
    {
        $pool = $this->pool(
            ['noRandomize' => false, 'servers' => ['a:4222', 'b:4222', 'c:4222']],
            self::reversingShuffle()
        );

        $this->assertSame(['nats://c:4222', 'nats://b:4222', 'nats://a:4222'], $pool->getServers());
        $this->assertSame('c', $pool->current()->host);
    }

    public function testNoRandomizeKeepsConfiguredOrder()
    {
        $pool = $this->pool(
            ['servers' => ['a:4222', 'b:4222', 'c:4222']],
            self::reversingShuffle()
        );

        $this->assertSame(['nats://a:4222', 'nats://b:4222', 'nats://c:4222'], $pool->getServers());
    }

    /**
     * The scheme of the server that advertised an address is not the whole story: an
     * entry written as nats:// may still have been reached over TLS.
     */
    public function testServersAdvertisedOverAnEncryptedConnectionRequireTls()
    {
        $pool = $this->pool(['servers' => ['nats://a:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['b:4222']]), true);

        $this->assertSame(['tls://b:4222'], $pool->getDiscoveredServers());
    }

    public function testServersAdvertisedOverAPlainConnectionAreNotUpgraded()
    {
        $pool = $this->pool(['servers' => ['nats://a:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['b:4222']]), false);

        $this->assertSame(['nats://b:4222'], $pool->getDiscoveredServers());
    }

    public function testDiscoversAdvertisedServers()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);

        $update = $pool->processInfo($this->info(['connect_urls' => ['a:4222', 'b:4222', 'c:4222']]));

        $this->assertTrue($update->hasNew);
        $this->assertSame(['b:4222', 'c:4222'], $update->added);
        $this->assertSame([], $update->removed);
        $this->assertSame(['nats://b:4222', 'nats://c:4222'], $pool->getDiscoveredServers());
        $this->assertCount(3, $pool->getServers());
    }

    /**
     * The advertisement of an already configured server must not create a second
     * entry for the same node.
     */
    public function testConfiguredServerIsNotDuplicatedByItsAdvertisement()
    {
        $pool = $this->pool(['servers' => ['a:4222', 'b:4222']]);

        $update = $pool->processInfo($this->info(['connect_urls' => ['a:4222', 'b:4222']]));

        $this->assertFalse($update->hasNew);
        $this->assertSame([], $update->added);
        $this->assertCount(2, $pool->getServers());
        $this->assertSame([], $pool->getDiscoveredServers());
    }

    public function testRepeatedAdvertisementIsNotDuplicated()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['b:4222']]));
        $pool->processInfo($this->info(['connect_urls' => ['b:4222']]));

        $this->assertCount(2, $pool->getServers());
    }

    public function testDiscoveredServerThatStopsBeingAdvertisedIsRemoved()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);
        $pool->processInfo($this->info(['connect_urls' => ['b:4222', 'c:4222']]));

        $update = $pool->processInfo($this->info(['connect_urls' => ['b:4222']]));

        $this->assertSame(['c:4222'], $update->removed);
        $this->assertSame(['nats://b:4222'], $pool->getDiscoveredServers());
    }

    public function testConfiguredServerIsNeverRemoved()
    {
        $pool = $this->pool(['servers' => ['a:4222', 'b:4222']]);

        $update = $pool->processInfo($this->info(['connect_urls' => ['z:4222']]));

        $this->assertSame([], $update->removed);
        $this->assertContains('nats://a:4222', $pool->getServers());
        $this->assertContains('nats://b:4222', $pool->getServers());
    }

    /**
     * The server we are connected to is not in its own connect_urls, so it would
     * otherwise be removed the moment it told us about the cluster.
     */
    public function testCurrentServerIsNeverRemovedEvenWhenDiscovered()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);
        $pool->processInfo($this->info(['connect_urls' => ['b:4222']]));

        // Fail over onto the discovered server.
        $pool->next();
        $this->assertSame('b', $pool->current()->host);
        $this->assertTrue($pool->current()->isImplicit);

        $update = $pool->processInfo($this->info(['connect_urls' => ['a:4222']]));

        $this->assertSame([], $update->removed);
        $this->assertSame('b', $pool->current()->host);
    }

    /**
     * An absent or empty list means the server is not advertising, not that the
     * cluster disappeared.
     *
     * @dataProvider noAdvertisementProvider
     */
    public function testNoAdvertisementRemovesNothing(array $values)
    {
        $pool = $this->pool(['servers' => ['a:4222']]);
        $pool->processInfo($this->info(['connect_urls' => ['b:4222']]));

        $update = $pool->processInfo($this->info($values));

        $this->assertSame([], $update->removed);
        $this->assertSame([], $update->added);
        $this->assertFalse($update->hasNew);
        $this->assertCount(2, $pool->getServers());
    }

    public function noAdvertisementProvider(): array
    {
        return [
            'absent' => [['server_name' => 'no-advertise']],
            'empty' => [['connect_urls' => []]],
        ];
    }

    /**
     * A server that flaps out of connect_urls and back is not a new discovery, so it
     * must not notify a second time.
     */
    public function testReturningServerIsNotReportedAsNew()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);

        $this->assertTrue($pool->processInfo($this->info(['connect_urls' => ['b:4222']]))->hasNew);
        $pool->processInfo($this->info(['connect_urls' => []]));
        $pool->processInfo($this->info(['connect_urls' => ['z:4222']]));

        $returning = $pool->processInfo($this->info(['connect_urls' => ['b:4222']]));

        $this->assertSame(['b:4222'], $returning->added);
        $this->assertFalse($returning->hasNew, 'a previously seen address is not a new discovery');
    }

    public function testIgnoreDiscoveredServersSuppressesAdditionsAndRemovals()
    {
        $pool = $this->pool(['servers' => ['a:4222'], 'ignoreDiscoveredServers' => true]);

        $update = $pool->processInfo($this->info(['connect_urls' => ['b:4222']]));

        $this->assertSame([], $update->added);
        $this->assertFalse($update->hasNew);
        $this->assertSame(['nats://a:4222'], $pool->getServers());
    }

    /**
     * Only the entry we are connected to needs to keep its place, since advertised
     * servers arrive in no meaningful order.
     */
    public function testDiscoveryShufflesEverythingButTheCurrentServer()
    {
        $pool = $this->pool(
            ['noRandomize' => false, 'servers' => ['a:4222']],
            self::reversingShuffle()
        );

        $pool->processInfo($this->info(['connect_urls' => ['b:4222', 'c:4222']]));

        $this->assertSame(
            ['nats://a:4222', 'nats://c:4222', 'nats://b:4222'],
            $pool->getServers()
        );
        $this->assertSame('a', $pool->current()->host);
    }

    public function testMalformedAdvertisementIsSkipped()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);

        $update = $pool->processInfo($this->info(['connect_urls' => ['b:4222', 'nonsense:port']]));

        $this->assertSame(['b:4222'], $update->added);
        $this->assertCount(2, $pool->getServers());
    }

    public function testRotationVisitsEveryServerAndWrapsAround()
    {
        $pool = $this->pool(['servers' => ['a:4222', 'b:4222', 'c:4222']]);

        $visited = [$pool->current()->host];
        for ($i = 0; $i < 3; $i++) {
            $visited[] = $pool->next()->host;
        }

        $this->assertSame(['a', 'b', 'c', 'a'], $visited);
        $this->assertCount(3, $pool->getServers());
    }

    /**
     * The budget is per server and resets on a successful connect, matching
     * MaxReconnect in the go client.
     */
    public function testExhaustedServerIsEvicted()
    {
        $pool = $this->pool(['servers' => ['a:4222', 'b:4222'], 'maxReconnectAttempts' => 2]);

        $a = $pool->current();
        $pool->markFailed($a, new RuntimeException('down'));
        $pool->markFailed($a, new RuntimeException('down'));
        $this->assertSame(2, $a->reconnects);

        $pool->next();

        $this->assertSame(['nats://b:4222'], $pool->getServers());
    }

    public function testEvictingAnotherServerKeepsTheCurrentOne()
    {
        $pool = $this->pool(['servers' => ['a:4222', 'b:4222', 'c:4222']]);
        $a = $pool->current();
        $b = $pool->next();

        $pool->evict($a);

        $this->assertSame($b, $pool->current());
        $this->assertSame(['nats://b:4222', 'nats://c:4222'], $pool->getServers());
    }

    /**
     * What a failed connection leaves as the current server is gone, so the pool has
     * to hand out the next one rather than the one it just dropped.
     */
    public function testEvictingTheCurrentServerMovesOnToTheNextOne()
    {
        $pool = $this->pool(['servers' => ['a:4222', 'b:4222']]);

        $pool->evict($pool->current());

        $this->assertSame('b', $pool->current()->host);
        $this->assertSame(['nats://b:4222'], $pool->getServers());
    }

    public function testEvictingTheLastServerEmptiesThePool()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);

        $pool->evict($pool->current());

        $this->assertNull($pool->current());
        $this->assertSame(0, $pool->count());
        $this->assertSame([], $pool->getServers());
    }

    public function testEvictingAServerThatIsNotInThePoolChangesNothing()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);

        $pool->evict(new Server(host: 'elsewhere', port: 4222));

        $this->assertSame(['nats://a:4222'], $pool->getServers());
        $this->assertSame('a', $pool->current()->host);
    }

    /**
     * A server leaving the pool is the one event that explains every later
     * failure, so it has to be possible to hear about it.
     */
    public function testDroppingAServerForAnExhaustedBudgetIsReported()
    {
        $pool = $this->pool(['servers' => ['a:4222', 'b:4222'], 'maxReconnectAttempts' => 1]);
        $dropped = [];
        $pool->whenDropped(function (Server $server, string $reason) use (&$dropped) {
            $dropped[] = [$server->getAddress(), $reason];
        });

        $pool->markFailed($pool->current(), new RuntimeException('connection refused'));
        $pool->next();

        $this->assertSame([['a:4222', 'used up its 1 reconnect attempts, last error: connection refused']], $dropped);
    }

    public function testEvictingAServerIsReportedWithItsReason()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);
        $dropped = [];
        $pool->whenDropped(function (Server $server, string $reason) use (&$dropped) {
            $dropped[] = [$server->getAddress(), $reason];
        });

        $pool->evict($pool->current(), 'rejected the credentials twice');

        $this->assertSame([['a:4222', 'rejected the credentials twice']], $dropped);
    }

    public function testEvictingAServerThatIsNotInThePoolIsNotReported()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);
        $calls = 0;
        $pool->whenDropped(function () use (&$calls) {
            $calls++;
        });

        $pool->evict(Server::fromUrl('elsewhere:4222'));

        $this->assertSame(0, $calls);
    }

    /**
     * Running out of servers carries the error that retired the last of them, so the
     * cause is not lost behind the symptom.
     */
    public function testExhaustionCarriesTheLastError()
    {
        $pool = $this->pool(['servers' => ['a:4222'], 'maxReconnectAttempts' => 1]);
        $pool->markFailed($pool->current(), new RuntimeException('connection refused'));

        try {
            $pool->next();
            $this->fail('the pool is used up');
        } catch (RuntimeException $e) {
            $this->assertSame('connection refused', $e->getPrevious()?->getMessage());
        }
    }

    public function testUnlimitedBudgetNeverEvicts()
    {
        $pool = $this->pool(['servers' => ['a:4222'], 'maxReconnectAttempts' => -1]);

        $server = $pool->current();
        for ($i = 0; $i < 5; $i++) {
            $pool->markFailed($server, new RuntimeException('down'));
            $pool->next();
        }

        $this->assertCount(1, $pool->getServers());
    }

    public function testDrainedPoolThrows()
    {
        $pool = $this->pool(['servers' => ['a:4222'], 'maxReconnectAttempts' => 1]);

        $pool->markFailed($pool->current(), new RuntimeException('down'));

        // Whether a server can be reached is external state, so running out of them
        // is a runtime condition rather than a mistake in the calling code.
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('No servers available');
        $pool->next();
    }

    public function testMarkConnectedResetsTheBudget()
    {
        $pool = $this->pool(['servers' => ['a:4222']]);

        $server = $pool->current();
        $pool->markFailed($server, new RuntimeException('down'));
        $server->draining = true;

        $pool->markConnected($server);

        $this->assertSame(0, $server->reconnects);
        $this->assertTrue($server->didConnect);
        $this->assertNull($server->lastError);
        $this->assertFalse($server->draining);
    }

    /**
     * A cluster advertises addresses, so a certificate issued for the configured
     * host name cannot be verified against them. The name is carried over instead.
     */
    public function testDiscoveredIpInheritsTheConfiguredHostName()
    {
        $pool = $this->pool(['servers' => ['tls://nats.example.com:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['10.0.0.7:4222']]));
        $discovered = $this->serverAt($pool, '10.0.0.7:4222');

        $this->assertSame('nats.example.com', $discovered->tlsName);
        $this->assertSame('nats.example.com', $discovered->getTlsPeerName());
        $this->assertTrue($discovered->secure, 'the scheme is inherited too');
    }

    public function testDiscoveredHostNameKeepsItsOwnName()
    {
        $pool = $this->pool(['servers' => ['tls://nats.example.com:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['other.example.com:4222']]));

        $this->assertNull($this->serverAt($pool, 'other.example.com:4222')->tlsName);
    }

    public function testNoNameIsInheritedFromAnIpCurrentServer()
    {
        $pool = $this->pool(['servers' => ['tls://10.0.0.1:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['10.0.0.7:4222']]));

        $this->assertNull($this->serverAt($pool, '10.0.0.7:4222')->tlsName);
    }

    /**
     * connect_urls carries no credentials, so a discovered server can only be
     * reached with those of the server that advertised it.
     */
    public function testDiscoveredServerInheritsCredentials()
    {
        $pool = $this->pool(['servers' => ['nats://user:secret@a.example.com:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['b.example.com:4222']]));
        $discovered = $this->serverAt($pool, 'b.example.com:4222');

        $this->assertSame('user', $discovered->user);
        $this->assertSame('secret', $discovered->pass);
    }

    public function testDiscoveredServerInheritsToken()
    {
        $pool = $this->pool(['servers' => ['nats://t0k3n@a.example.com:4222']]);

        $pool->processInfo($this->info(['connect_urls' => ['b.example.com:4222']]));

        $this->assertSame('t0k3n', $this->serverAt($pool, 'b.example.com:4222')->token);
    }

    public function testIpv6AdvertisementMatchesAnIpv6Configuration()
    {
        $pool = $this->pool(['servers' => ['[::1]:4222']]);

        $update = $pool->processInfo($this->info(['connect_urls' => ['[::1]:4222', '[2001:db8::7]:4222']]));

        $this->assertSame(['[2001:db8::7]:4222'], $update->added);
        $this->assertCount(2, $pool->getServers());
    }

    private function serverAt(ServerPool $pool, string $address): Server
    {
        $property = new \ReflectionProperty(ServerPool::class, 'servers');

        foreach ($property->getValue($pool) as $server) {
            if ($server->getAddress() === $address) {
                return $server;
            }
        }

        $this->fail("No server at $address in the pool");
    }
}
