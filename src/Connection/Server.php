<?php

declare(strict_types=1);

namespace Basis\Nats\Connection;

use InvalidArgumentException;
use Throwable;

/**
 * A single endpoint in the server pool.
 *
 * The identity of an entry is its address, which has to be formatted exactly the
 * way the server advertises it in connect_urls, otherwise discovery cannot tell an
 * already known server from a new one.
 */
class Server
{
    public const DEFAULT_PORT = 4222;

    /** Failed connection attempts since this server last accepted us. */
    public int $reconnects = 0;

    public bool $didConnect = false;

    public ?Throwable $lastError = null;

    /** Set when the server told us it entered lame duck mode. */
    public bool $draining = false;

    /** Set when this server rejected our credentials on the last attempt. */
    public bool $authenticationFailed = false;

    public function __construct(
        public readonly string $host,
        public readonly int $port,
        public readonly bool $secure = false,
        public readonly bool $isImplicit = false,
        public readonly ?string $tlsName = null,
        public readonly ?string $user = null,
        public readonly ?string $pass = null,
        public readonly ?string $token = null,
    ) {
    }

    /**
     * Parses an explicitly configured server, given either bare as host:port or as
     * a full nats:// or tls:// url, optionally carrying credentials.
     */
    public static function fromUrl(string $url, bool $isImplicit = false, ?string $tlsName = null): self
    {
        $normalized = str_contains($url, '://') ? $url : 'nats://' . $url;
        $parts = parse_url($normalized);

        if ($parts === false || !isset($parts['host']) || $parts['host'] === '') {
            throw new InvalidArgumentException('Invalid server url: ' . self::withoutCredentials($url));
        }

        // parse_url leaves the credentials percent-encoded, and the characters that
        // delimit a url can only appear in them that way, so a password containing
        // one would otherwise be sent to the server encoded.
        $user = isset($parts['user']) ? rawurldecode($parts['user']) : null;
        $pass = isset($parts['pass']) ? rawurldecode($parts['pass']) : null;
        $token = null;

        // A username with no password is a token, the same rule the go client uses.
        if ($user !== null && $pass === null) {
            $token = $user;
            $user = null;
        }

        return new self(
            host: self::normalizeHost($parts['host']),
            port: $parts['port'] ?? self::DEFAULT_PORT,
            secure: ($parts['scheme'] ?? 'nats') === 'tls',
            isImplicit: $isImplicit,
            tlsName: $tlsName,
            user: $user,
            pass: $pass,
            token: $token,
        );
    }

    /**
     * The dedup key, matching the host:port shape used in connect_urls. Credentials
     * are deliberately excluded: a seed url carrying them still has to match the
     * bare address the server advertises for the same node.
     */
    public function getAddress(): string
    {
        return self::formatHostPort($this->host, $this->port);
    }

    public function getDsn(): string
    {
        return 'tcp://' . self::formatHostPort($this->host, $this->port);
    }

    public function getUrl(): string
    {
        return ($this->secure ? 'tls://' : 'nats://') . $this->getAddress();
    }

    public function hostIsIpAddress(): bool
    {
        return filter_var($this->host, FILTER_VALIDATE_IP) !== false;
    }

    /**
     * Host name to verify the certificate against. Discovered servers are usually
     * advertised as bare addresses, so a name inherited from the server that told us
     * about this one takes precedence over its own address.
     */
    public function getTlsPeerName(): string
    {
        return $this->tlsName ?? $this->host;
    }

    /**
     * Hides the userinfo of a url that is about to be quoted in an error message.
     * Cut at the last @, since an unencoded one in the password is the typical reason
     * such a url fails to parse.
     */
    private static function withoutCredentials(string $url): string
    {
        $at = strrpos($url, '@');

        if ($at === false) {
            return $url;
        }

        $scheme = strpos($url, '://');
        $prefix = $scheme !== false && $scheme < $at ? substr($url, 0, $scheme + 3) : '';

        return $prefix . '***@' . substr($url, $at + 1);
    }

    /** Strips the brackets parse_url keeps around an ipv6 literal. */
    private static function normalizeHost(string $host): string
    {
        return trim($host, '[]');
    }

    /** Formats an address the way the server does, bracketing ipv6 literals. */
    private static function formatHostPort(string $host, int $port): string
    {
        if (str_contains($host, ':')) {
            return "[$host]:$port";
        }

        return "$host:$port";
    }
}
