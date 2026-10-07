<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Connection;
use Basis\Nats\Connection\Server;
use ReflectionMethod;
use Tests\TestCase;

/**
 * Credentials carried by a server url take precedence over the configured ones, and
 * the user/pass and token forms never reach the CONNECT message together.
 */
class ConnectionCredentialsTest extends TestCase
{
    public function testServerUserAndPassReplaceConfiguredToken(): void
    {
        $options = $this->apply('nats://alice:secret@host:4222', ['token' => 'configured']);

        $this->assertSame('alice', $options['user']);
        $this->assertSame('secret', $options['pass']);
        $this->assertArrayNotHasKey('auth_token', $options);
    }

    public function testServerTokenReplacesConfiguredUserAndPass(): void
    {
        $options = $this->apply('nats://tok@host:4222', ['user' => 'bob', 'pass' => 'hunter2']);

        $this->assertSame('tok', $options['auth_token']);
        $this->assertArrayNotHasKey('user', $options);
        $this->assertArrayNotHasKey('pass', $options);
    }

    public function testServerUserWithoutPassDropsConfiguredPass(): void
    {
        // A server url carrying only a user cannot be told apart from a token, so build
        // the entry directly: the configured pass must not be sent with another user.
        $server = new Server(host: 'host', port: 4222, user: 'alice');
        $options = $this->applyServer($server, ['user' => 'bob', 'pass' => 'hunter2']);

        $this->assertSame('alice', $options['user']);
        $this->assertArrayNotHasKey('pass', $options);
    }

    public function testConfiguredCredentialsSurviveAServerWithoutAny(): void
    {
        $options = $this->apply('nats://host:4222', ['user' => 'bob', 'pass' => 'hunter2']);

        $this->assertSame('bob', $options['user']);
        $this->assertSame('hunter2', $options['pass']);
    }

    private function apply(string $url, array $configuration): array
    {
        return $this->applyServer(Server::fromUrl($url), $configuration);
    }

    private function applyServer(Server $server, array $configuration): array
    {
        $client = new Client(new Configuration($configuration));
        $method = new ReflectionMethod(Connection::class, 'applyCredentials');

        return $method->invoke($client->connection, $server, $client->configuration->getOptions());
    }
}
