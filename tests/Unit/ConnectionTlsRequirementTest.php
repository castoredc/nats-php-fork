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
 * A tls:// entry is a promise that the connection is encrypted. A server whose INFO
 * does not ask for TLS, or an attacker who removed the flag from it, must not be able
 * to turn that into a cleartext CONNECT carrying the credentials.
 */
class ConnectionTlsRequirementTest extends TestCase
{
    private const PLAIN_INFO = 'INFO {"server_id":"X","server_name":"X","version":"2.10.0","host":"127.0.0.1","port":4222,"headers":true,"max_payload":1048576,"proto":1}' . "\r\n";

    public function testTlsEntryRefusesToSendCredentialsWithoutTls(): void
    {
        $server = ScriptedServer::start([['send', self::PLAIN_INFO], ['read']]);

        $client = $this->client('tls://' . $server->address);

        try {
            $client->connection->sendMessage(new Ping());
            $this->fail('a tls:// server that does not negotiate TLS has to be refused');
        } catch (Exception $e) {
            $this->assertStringContainsString('TLS', $e->getMessage());
        }

        $this->assertSame([null], $server->finish(), 'nothing may reach the server, CONNECT included');
    }

    /**
     * Control: the same INFO is fine for an entry that never asked for TLS, so it is
     * the scheme that is being enforced, not a blanket refusal.
     */
    public function testPlainEntryConnectsWithoutTls(): void
    {
        $server = ScriptedServer::start([
            ['send', self::PLAIN_INFO],
            ['read'],
            ['read'],
            ['send', "PONG\r\n"],
            ['read'],
        ]);

        $client = $this->client('nats://' . $server->address);
        $client->connection->sendMessage(new Ping());
        $client->connection->close();

        $lines = $server->finish();

        $this->assertStringStartsWith('CONNECT ', $lines[0]);
        $this->assertSame('PING', $lines[1]);
    }

    private function client(string $url): Client
    {
        return new Client(new Configuration([
            'servers' => [$url],
            'user' => 'alice',
            'pass' => 'secret',
            'timeout' => 1,
            'reconnect' => false,
        ]));
    }
}
