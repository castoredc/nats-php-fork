<?php

declare(strict_types=1);

namespace Tests\Unit\Connection;

use Basis\Nats\Connection\Server;
use InvalidArgumentException;
use Tests\TestCase;

class ServerTest extends TestCase
{
    public function testBareHostAndPort()
    {
        $server = Server::fromUrl('localhost:4222');

        $this->assertSame('localhost', $server->host);
        $this->assertSame(4222, $server->port);
        $this->assertFalse($server->secure);
        $this->assertFalse($server->isImplicit);
        $this->assertSame('localhost:4222', $server->getAddress());
        $this->assertSame('tcp://localhost:4222', $server->getDsn());
        $this->assertSame('nats://localhost:4222', $server->getUrl());
    }

    public function testPortDefaultsToTheNatsPort()
    {
        $this->assertSame(4222, Server::fromUrl('localhost')->port);
    }

    public function testTlsScheme()
    {
        $server = Server::fromUrl('tls://secure.example.com:4222');

        $this->assertTrue($server->secure);
        $this->assertSame('tls://secure.example.com:4222', $server->getUrl());
    }

    public function testCredentialsFromUrl()
    {
        $server = Server::fromUrl('nats://user:secret@localhost:4222');

        $this->assertSame('user', $server->user);
        $this->assertSame('secret', $server->pass);
        $this->assertNull($server->token);
    }

    /**
     * The characters that delimit a url have to be percent-encoded to appear in its
     * credentials at all, so what reaches the server must be the decoded value.
     */
    public function testCredentialsArePercentDecoded()
    {
        $server = Server::fromUrl('nats://us%40er:p%40ss%3Aw%2Ford%23%25@localhost:4222');

        $this->assertSame('us@er', $server->user);
        $this->assertSame('p@ss:w/ord#%', $server->pass);
    }

    public function testTokenIsPercentDecoded()
    {
        $server = Server::fromUrl('nats://t%40k%2Fn@localhost:4222');

        $this->assertSame('t@k/n', $server->token);
    }

    /**
     * An entry that cannot be parsed ends up in the application's logs, and the
     * password in it must not.
     */
    public function testInvalidUrlDoesNotRevealItsCredentials()
    {
        try {
            Server::fromUrl('nats://user:p@ss@:4222');
            $this->fail('the url has no host');
        } catch (InvalidArgumentException $e) {
            $this->assertStringContainsString('Invalid server url', $e->getMessage());
            $this->assertStringNotContainsString('p@ss', $e->getMessage());
            $this->assertStringNotContainsString('user', $e->getMessage());
        }
    }

    /**
     * A username with no password is a token, the rule the go client uses.
     */
    public function testUsernameWithoutPasswordIsAToken()
    {
        $server = Server::fromUrl('nats://t0k3n@localhost:4222');

        $this->assertSame('t0k3n', $server->token);
        $this->assertNull($server->user);
        $this->assertNull($server->pass);
    }

    /**
     * Credentials must not be part of the identity, otherwise a seed url carrying
     * them never matches the bare address the cluster advertises for that node.
     */
    public function testCredentialsAreNotPartOfTheAddress()
    {
        $this->assertSame(
            Server::fromUrl('10.0.0.7:4222')->getAddress(),
            Server::fromUrl('nats://user:secret@10.0.0.7:4222')->getAddress()
        );
    }

    /**
     * The server advertises ipv6 through go's net.JoinHostPort, so bracketed. The
     * address has to come back out in the same shape for discovery to match it.
     *
     * @dataProvider ipv6Provider
     */
    public function testIpv6($url, $expectedHost)
    {
        $server = Server::fromUrl($url);

        $this->assertSame($expectedHost, $server->host);
        $this->assertSame("[$expectedHost]:4222", $server->getAddress());
        $this->assertSame("tcp://[$expectedHost]:4222", $server->getDsn());
        $this->assertTrue($server->hostIsIpAddress());
    }

    public function ipv6Provider(): array
    {
        return [
            'bracketed' => ['[::1]:4222', '::1'],
            'bare' => ['::1:4222', '::1'],
            'bracketed with scheme' => ['nats://[2001:db8::1]:4222', '2001:db8::1'],
            'bare global' => ['2001:db8::1:4222', '2001:db8::1'],
        ];
    }

    public function testHostIsIpAddress()
    {
        $this->assertTrue(Server::fromUrl('10.0.0.7:4222')->hostIsIpAddress());
        $this->assertFalse(Server::fromUrl('nats.example.com:4222')->hostIsIpAddress());
    }

    public function testTlsPeerNamePrefersTheInheritedName()
    {
        $inherited = Server::fromUrl('10.0.0.7:4222', true, 'nats.example.com');
        $this->assertSame('nats.example.com', $inherited->getTlsPeerName());

        $this->assertSame('10.0.0.7', Server::fromUrl('10.0.0.7:4222')->getTlsPeerName());
    }

    /**
     * @dataProvider invalidUrlProvider
     */
    public function testInvalidUrl($url)
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid server url');
        Server::fromUrl($url);
    }

    public function invalidUrlProvider(): array
    {
        return [
            'empty' => [''],
            'non numeric port' => ['host:abc'],
            'negative port' => ['host:-1'],
        ];
    }

    public function testCountersStartUnused()
    {
        $server = Server::fromUrl('localhost:4222');

        $this->assertSame(0, $server->reconnects);
        $this->assertFalse($server->didConnect);
        $this->assertFalse($server->draining);
        $this->assertNull($server->lastError);
    }
}
