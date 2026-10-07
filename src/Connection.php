<?php

declare(strict_types=1);

namespace Basis\Nats;

use Basis\Nats\Connection\Server;
use Basis\Nats\Connection\ServerPool;
use Basis\Nats\Message\Connect;
use Basis\Nats\Message\Factory;
use Basis\Nats\Message\Info;
use Basis\Nats\Message\Msg;
use Basis\Nats\Message\Ok;
use Basis\Nats\Message\Ping;
use Basis\Nats\Message\Publish;
use Basis\Nats\Message\Pong;
use Basis\Nats\Message\Prototype as Message;
use Basis\Nats\Message\Subscribe;
use Closure;
use LogicException;
use Psr\Log\LoggerInterface;
use Throwable;
use Exception;

class Connection
{
    /** Refuse to buffer a single protocol line larger than this. */
    private const CONTROL_LINE_LIMIT = 1_048_576;

    private $socket;
    private $context;

    /** Guards the handshake against re-entering the reconnect loop. */
    private bool $connecting = false;

    /** Set by forceReconnect(), acted on at the next read or write. */
    private bool $reconnectRequested = false;

    /** True once any connection has been established, as opposed to attempted. */
    private bool $hasConnected = false;

    /** True from the start of a handshake until its opening INFO has been seen. */
    private bool $awaitingOpeningInfo = false;

    /**
     * Notifications raised while a handshake is still running, delivered once it has
     * finished.
     *
     * @var Closure[]
     */
    private array $pendingNotifications = [];

    private float $activityAt = 0;
    private float $pingAt = 0;
    private float $pongAt = 0;
    private float $prolongateTill = 0;
    private int $packetSize = 1024;

    /** True once TLS has been negotiated on the current socket. */
    private bool $tlsEnabled = false;

    private ?Authenticator $authenticator;
    private Configuration $config;
    private Connect $connectMessage;
    private Info $infoMessage;

    /**
     * Built on first use rather than in the constructor, so that configuration
     * changed after the client was created is still picked up, and so that merely
     * creating a client never resolves an address.
     */
    private ?ServerPool $pool = null;

    public function __construct(
        private Client $client,
        public ?LoggerInterface $logger = null,
    ) {
        $this->authenticator = Authenticator::create($client->configuration);
        $this->config = $client->configuration;
    }

    public function getConnectMessage(): Connect
    {
        return $this->connectMessage;
    }

    public function getInfoMessage(): Info
    {
        return $this->infoMessage;
    }

    public function getPool(): ServerPool
    {
        return $this->pool ??= $this->createPool();
    }

    private function createPool(): ServerPool
    {
        $pool = new ServerPool($this->config);
        // Resolved when called rather than captured, since the logger can be replaced
        // after the pool exists.
        $pool->whenDropped(function (Server $server, string $reason): void {
            $this->logger?->warning('dropped ' . $server->getAddress() . ' from the pool: ' . $reason);
        });

        return $pool;
    }

    /** @return string[] every server the client may use, without credentials */
    public function getServers(): array
    {
        return $this->getPool()->getServers();
    }

    /** @return string[] only the servers learned from the cluster */
    public function getDiscoveredServers(): array
    {
        return $this->getPool()->getDiscoveredServers();
    }

    public function getMessage(null|int|float $timeout = 0): ?Message
    {
        // null means use config timeout, 0 means non-blocking check
        if ($timeout === null) {
            $timeout = $this->config->timeout;
        }

        $this->applyRequestedReconnect();

        $now = microtime(true);
        $max = $timeout > 0 ? $now + $timeout : PHP_FLOAT_MAX;
        $iteration = 0;

        while (true) {
            if (!is_resource($this->socket) || feof($this->socket)) {
                // Either reconnected onto a fresh socket or rethrown. Deliberately
                // falls through to the select below rather than looping back: a
                // socket that came back still unusable would spin here forever,
                // since a zero timeout only breaks out at the select.
                $this->processException(new LogicException('supplied resource is not a valid stream resource'));
            }

            $remainingTimeout = $max - microtime(true);
            if ($remainingTimeout <= 0) {
                break;
            }

            $read = [$this->socket];
            $write = null;
            $except = null;

            // Calculate timeout for stream_select
            if ($timeout === 0) {
                // Non-blocking check - use stream_select with 0 timeout to check if data is available
                $seconds = 0;
                $microseconds = 0;
            } else {
                $seconds = (int) floor($remainingTimeout);
                $microseconds = (int) (($remainingTimeout - $seconds) * 1_000_000);
            }

            $result = stream_select($read, $write, $except, $seconds, $microseconds);

            if ($result === false || $result === 0) {
                // For non-blocking check (timeout=0), exit immediately
                if ($timeout === 0) {
                    break;
                }
                // For blocking calls, continue waiting
                continue;
            }

            $message = null;
            $line = $this->readLine();
            $now = microtime(true);

            if ($line === false && $timeout === 0 && !feof($this->socket)) {
                // Only part of a line has arrived. It stays buffered, so selecting
                // again would report it readable and spin until the rest comes. A
                // closed socket is left to the check at the top of the loop.
                break;
            }
            if ($line) {
                $message = Factory::create($line);
                $this->activityAt = $now;
                if ($message instanceof Msg) {
                    $payload = $this->getPayload($message->length);
                    $message->parse($payload);
                    $message->setClient($this->client);
                    $this->logger?->debug('receive ' . $line . $payload);
                    return $message;
                }
                $this->logger?->debug('receive ' . $line);
                if ($message instanceof Ok) {
                    continue;
                } elseif ($message instanceof Ping) {
                    $this->sendMessage(new Pong([]));
                } elseif ($message instanceof Pong) {
                    $this->pongAt = $now;
                    return $message;
                } elseif ($message instanceof Info) {
                    // Asynchronous updates repeat the tls flags, so without this
                    // guard an already encrypted socket is handed to
                    // stream_socket_enable_crypto a second time.
                    if (!$this->tlsEnabled && !$this->config->tlsHandshakeFirst) {
                        if (isset($message->tls_verify) && $message->tls_verify) {
                            $this->enableTls(true);
                        } elseif (isset($message->tls_required) && $message->tls_required) {
                            $this->enableTls(false);
                        }
                    }
                    $this->processInfo($message);

                    // Only the handshake is waiting for an INFO. Every later one is
                    // an asynchronous topology update that the client acts on
                    // itself, so returning it would hand application code a message
                    // it never asked for, and would consume the read that the caller
                    // meant for its own reply.
                    if ($this->connecting) {
                        return $message;
                    }

                    continue;
                }
            } elseif ($this->activityAt && $this->activityAt + $this->config->timeout < $now) {
                if ($this->pingAt && $this->pingAt + $this->config->pingInterval < $now) {
                    if ($this->prolongateTill && $this->prolongateTill < $now) {
                        $this->sendMessage(new Ping());
                    }
                }
            }
            if ($message && $now < $max) {
                $this->logger?->debug('sleep', compact('max', 'now'));
                $this->config->delay($iteration++);
            }
        }

        if ($this->activityAt && $this->activityAt + $this->config->timeout < $now) {
            if ($this->pongAt && $this->pongAt + $this->config->pingInterval < $now) {
                if ($this->prolongateTill && $this->prolongateTill < $now) {
                    $this->processException(new LogicException('Socket read timeout'));
                }
            }
        }

        return null;
    }

    public function ping(): bool
    {
        $this->sendMessage(new Ping());
        $this->getMessage($this->config->timeout);

        return $this->pingAt <= $this->pongAt;
    }

    public function sendMessage(Message $message): void
    {
        // Before init(), so that a request made while there is no connection is
        // answered by the connection init() is about to make, not by a second one.
        $this->applyRequestedReconnect();
        $this->init();

        while (true) {
            try {
                $this->writeMessage($message);
                break;
            } catch (Throwable $e) {
                // Reconnects, or rethrows when reconnection is off or already
                // in progress. The retry restarts the message from its first
                // byte: resuming at the previous offset would write the tail of
                // this message onto a freshly connected socket.
                $this->processException($e);
            }
        }

        if ($message instanceof Publish) {
            if (strpos($message->subject, '$JS.API.CONSUMER.MSG.NEXT.') === 0) {
                $prolongate = $message->payload->expires / 1_000_000_000;
                $this->prolongateTill = microtime(true) + $prolongate;
            }
        }
        if ($message instanceof Ping) {
            $this->pingAt = microtime(true);
        }
    }

    public function setLogger(?LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    public function setTimeout(float $value): void
    {
        $this->init();
        $seconds = (int) floor($value);
        $microseconds = (int) (1000000 * ($value - $seconds));

        stream_set_timeout($this->socket, $seconds, $microseconds);
    }

    protected function init(): void
    {
        if (is_resource($this->socket)) {
            return;
        }

        // One pass over the pool, then give up: an initial connection reports that
        // it could not be made rather than blocking indefinitely.
        $this->connect(retry: false);
    }

    /**
     * Establishes a connection, trying the servers in the pool in turn.
     *
     * @param bool $retry whether to keep sweeping the pool, backing off between
     *                    sweeps, or to give up once every server has been tried
     */
    private function connect(bool $retry): void
    {
        $pool = $this->getPool();

        // A pool the previous attempt used up ended that attempt, not the client.
        // Without this every later call would fail at once, and a long running
        // worker that catches the error and carries on would never recover.
        if ($pool->current() === null) {
            $this->logger?->warning('no servers left in the pool, rebuilding it from the configuration');
            $pool = $this->pool = $this->createPool();
        }

        // Servers tried in the current pass, by identity. A pass ends when it comes
        // back round to one of them. Counting servers up front instead went stale as
        // soon as an INFO named new ones, or one was dropped, mid-pass.
        $tried = [];
        $failure = null;

        while (true) {
            $server = $pool->current();

            if ($server === null) {
                throw $failure ?? new Exception('No servers available');
            }

            if (isset($tried[spl_object_id($server)])) {
                if (!$retry) {
                    throw $failure ?? new Exception('No servers available');
                }

                // Backing off once per pass rather than between servers, as the go
                // client does, so a healthy peer is reached without an artificial
                // delay while a cluster that is entirely down is still not hammered.
                $this->wait();
                $tried = [];
            }

            $tried[spl_object_id($server)] = true;

            $this->closeSocket();

            try {
                // The handshake writes through sendMessage() and reads through
                // getMessage(), both of which recover from failure by reconnecting.
                // Left unguarded that recursion connects to another server,
                // completes its handshake, and then unwinds back into this one,
                // which carries on against a socket already connected elsewhere.
                $this->connecting = true;
                $this->pendingNotifications = [];

                try {
                    $this->handshake($server);
                } finally {
                    $this->connecting = false;
                }

                $this->hasConnected = true;
                $this->deliverPendingNotifications();

                return;
            } catch (Throwable $error) {
                $failure = $error;
                $this->pendingNotifications = [];
                $pool->markFailed($server, $error);

                // A socket that opened but never finished its handshake has no
                // subscriptions and has not sent CONNECT. Left assigned, init() would
                // take it for a live connection and the next write would use it.
                $this->closeSocket();

                // Which member failed and why, so a sweep of the pool reads as the
                // sequence of attempts it actually was.
                $this->logger?->debug(
                    'connection to ' . $server->getAddress() . ' failed: ' . $error->getMessage()
                );

                // Rejected credentials will not start working on their own, so a
                // server failing that way twice running is dropped rather than
                // retried around the pool forever.
                if ($this->isAuthenticationFailure($error)) {
                    if ($server->authenticationFailed) {
                        $pool->evict($server, 'rejected the credentials twice running');

                        // Dropping a server already leaves the next one in line as the
                        // current one, so rotating as well would hand the attempt to
                        // the server after it.
                        continue;
                    }
                    $server->authenticationFailed = true;
                } else {
                    // Only back to back rejections say the credentials are wrong. A
                    // failure of another kind in between says nothing either way.
                    $server->authenticationFailed = false;
                }
            }

            // Retires servers that used up their budget, and throws once the pool
            // has nothing left to offer.
            try {
                $pool->next();
            } catch (Throwable) {
                throw $failure;
            }
        }
    }

    private function handshake(Server $server): void
    {
        $config = $this->config;

        // Timestamps and the tls flag describe one socket. Carried into the next
        // connection they either skip the tls handshake, sending a cleartext CONNECT
        // to a tls port, or make the liveness check fire immediately and reconnect
        // in a loop.
        $this->resetConnectionState();
        $this->awaitingOpeningInfo = true;

        $flags = STREAM_CLIENT_CONNECT;
        $this->context = stream_context_create();
        $this->socket = @stream_socket_client(
            $server->getDsn(),
            $error,
            $errorMessage,
            $config->timeout,
            $flags,
            $this->context
        );

        if ($error || !$this->socket) {
            throw new Exception($errorMessage ?: "Connection error", $error);
        }

        $this->setTimeout($config->timeout);

        if ($config->tlsHandshakeFirst) {
            $this->enableTls(true);
        }

        $this->connectMessage = new Connect($this->applyCredentials($server, $config->getOptions()));

        if ($this->client->getName()) {
            $this->connectMessage->name = $this->client->getName();
        }

        $infoMessage = $this->getMessage($config->timeout);
        if (is_null($infoMessage)) {
            throw new Exception("Timeout waiting for message from server.");
        }
        if (!$infoMessage instanceof Info) {
            throw new Exception("Received unexpected message type: " . $infoMessage::class);
        }
        $this->infoMessage = $infoMessage;

        // Decided by what the server's INFO asked for, which is the one thing a
        // network attacker can edit, so a tls:// entry cannot rely on it alone. The
        // credentials are in the CONNECT that follows.
        if ($server->secure && !$this->tlsEnabled) {
            throw new Exception('TLS is required for ' . $server->getUrl() . ' but the connection is not encrypted');
        }

        if (isset($this->infoMessage->nonce) && $this->authenticator) {
            $this->connectMessage->sig = $this->authenticator->sign($this->infoMessage->nonce);
            $this->connectMessage->nkey = $this->authenticator->getPublicKey();
        }

        $this->sendMessage($this->connectMessage);

        $this->verifyConnection();

        $this->getPool()->markConnected($server);

        // Which member of the pool this connection actually landed on, which is the
        // first thing worth knowing when reading back a failover.
        $this->logger?->debug('connected to ' . $server->getAddress());

        $this->restoreSubscriptions();
    }

    /**
     * Confirms the server accepted the connection.
     *
     * Credentials are rejected asynchronously: the server takes the CONNECT bytes
     * and only afterwards answers -ERR and hangs up. Treating a successful write as
     * success would reset the server's reconnect budget on every attempt, so a node
     * that always rejects us is never retired from the pool, and the -ERR would
     * surface out of whichever unrelated read happened to come next. A PING round
     * trip brings the rejection here, where the caller can act on it.
     */
    private function verifyConnection(): void
    {
        $this->writeMessage(new Ping());
        $this->pingAt = microtime(true);

        $threshold = microtime(true) + $this->config->timeout;

        while (microtime(true) < $threshold) {
            $message = $this->getMessage($this->config->timeout);

            if ($message instanceof Pong) {
                return;
            }

            if ($message === null) {
                break;
            }

            // A server that has something to say about its cluster may do so before
            // answering. That INFO has already been applied, so keep waiting for the
            // reply rather than treating it as one.
        }

        throw new Exception('Handshake failed: no PONG received');
    }

    /**
     * Credentials carried by a server url take precedence over the configured ones,
     * the precedence the go client uses, and the three forms stay mutually
     * exclusive. nkey and jwt are untouched: they are signed per connection from the
     * nonce in that server's INFO, so they already apply to every server.
     *
     * Applied to the options rather than to the Connect message, so that a field
     * which must not be sent is never set instead of being unset afterwards.
     */
    private function applyCredentials(Server $server, array $options): array
    {
        if ($server->token !== null) {
            unset($options['user'], $options['pass']);
            $options['auth_token'] = $server->token;

            return $options;
        }

        if ($server->user !== null) {
            unset($options['auth_token'], $options['pass']);
            $options['user'] = $server->user;

            if ($server->pass !== null) {
                $options['pass'] = $server->pass;
            }
        }

        return $options;
    }

    private function resetConnectionState(): void
    {
        $this->tlsEnabled = false;
        $this->activityAt = 0;
        $this->pingAt = 0;
        $this->pongAt = 0;
        $this->prolongateTill = 0;
    }

    /**
     * Replays the client's subscriptions onto a freshly established connection.
     *
     * This belongs to establishing a connection rather than to handling an
     * exception: a socket that was closed underneath us is reconnected by init()
     * itself, which never went through processException().
     */
    private function restoreSubscriptions(): void
    {
        foreach ($this->client->getSubscriptions() as $subscription) {
            $this->sendMessage(new Subscribe([
                'sid' => $subscription['sid'],
                'subject' => $subscription['name'],
                // Without the queue group a replayed subscription becomes a plain
                // one, so every member of the group receives every message.
                'group' => $subscription['group'] ?? null,
            ]));
        }

        if ($this->client->requestsSubscribed()) {
            $this->client->subscribeRequests(true);
        }
    }

    protected function enableTls(bool $requireClientCert): void
    {
        if ($requireClientCert) {
            if (!empty($this->config->tlsKeyFile)) {
                if (!file_exists($this->config->tlsKeyFile)) {
                    throw new Exception("tlsKeyFile file does not exist: " . $this->config->tlsKeyFile);
                }
                stream_context_set_option($this->context, 'ssl', 'local_pk', $this->config->tlsKeyFile);
            }
            if (!empty($this->config->tlsCertFile)) {
                if (!file_exists($this->config->tlsCertFile)) {
                    throw new Exception("tlsCertFile file does not exist: " . $this->config->tlsCertFile);
                }
                stream_context_set_option($this->context, 'ssl', 'local_cert', $this->config->tlsCertFile);
            }
        }

        if (!empty($this->config->tlsCaFile)) {
            if (!file_exists($this->config->tlsCaFile)) {
                throw new Exception("tlsCaFile file does not exist: " . $this->config->tlsCaFile);
            }
            stream_context_set_option($this->context, 'ssl', 'cafile', $this->config->tlsCaFile);
            stream_context_set_option($this->context, 'ssl', 'verify_peer', true);
            stream_context_set_option($this->context, 'ssl', 'verify_peer_name', false);
        } else {
            stream_context_set_option($this->context, 'ssl', 'verify_peer', false);
        }

        if (!stream_socket_enable_crypto($this->socket, true, STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT)) {
            throw new Exception('Failed to connect: Error enabling TLS');
        }

        $this->tlsEnabled = true;
    }

    /**
     * Applies an INFO message: the cluster topology it advertises, and the fields it
     * refreshes on the connection.
     */
    private function processInfo(Info $info): void
    {
        // The opening INFO of a connection is assigned wholesale by handshake()
        // itself, against a connection that has no previous state to keep. Every
        // INFO after it, including one that arrives before the PONG, is an update.
        $opening = $this->awaitingOpeningInfo;
        $this->awaitingOpeningInfo = false;

        if (!$opening) {
            $this->mergeInfoMessage($info);
        }

        $update = $this->getPool()->processInfo($info, $this->tlsEnabled);

        // The first connection fills the pool silently, so a client that starts up
        // against an assembled cluster is not told about a discovery it never made,
        // and a server already draining is not announced as having just started to.
        // Both match the go and python clients. Anything met while reconnecting is
        // news, though.
        if ($this->connecting && !$this->hasConnected) {
            return;
        }

        $notifications = [];

        if ($update->hasNew) {
            $notifications[] = fn () => $this->invokeHandler($this->config->discoveredServersHandler);
        }

        if ($info->ldm ?? false) {
            // Checked when delivered rather than now: a connection that is still being
            // confirmed has not marked its server as connected yet, which clears the
            // draining flag, and the flag is what makes this edge triggered.
            $notifications[] = function () {
                $server = $this->getPool()->current();

                if ($server === null || $server->draining) {
                    return;
                }

                $server->draining = true;
                $this->logger?->info('server entered lame duck mode: ' . $server->getAddress());
                $this->invokeHandler($this->config->lameDuckModeHandler);
            };
        }

        if ($this->connecting) {
            // A handler that publishes, as one asking to migrate might, would write
            // to a connection that has not sent CONNECT yet.
            array_push($this->pendingNotifications, ...$notifications);

            return;
        }

        foreach ($notifications as $notification) {
            $notification();
        }
    }

    private function deliverPendingNotifications(): void
    {
        $pending = $this->pendingNotifications;
        $this->pendingNotifications = [];

        foreach ($pending as $notification) {
            $notification();
        }
    }

    /**
     * Runs a configured handler.
     *
     * A synchronous client has no callback loop to hand these to, so they run inline
     * on the read that delivered the update. A handler that throws must not take
     * down the connection that was reading.
     */
    private function invokeHandler(?Closure $handler): void
    {
        if ($handler === null) {
            return;
        }

        try {
            $handler($this->client);
        } catch (Throwable $e) {
            $message = 'handler failed: ' . $e->getMessage();

            // Without a logger there would be no trace at all, and a lame duck
            // handler that threw before asking to migrate would simply never migrate.
            if ($this->logger !== null) {
                $this->logger->error($message, ['exception' => $e]);
            } else {
                trigger_error($message, E_USER_WARNING);
            }
        }
    }

    /**
     * Asks for the connection to be moved to another server.
     *
     * Deferred to the next read or write rather than done here, because the way to
     * migrate off a draining server is to call this from the lame duck handler,
     * which runs inside the read that delivered the notification. Reconnecting from
     * there would tear down the call stack still processing that message.
     *
     * Applies whatever the reconnect option says, that one governing whether the
     * client recovers on its own rather than whether it does as it is told. If no
     * server can be reached, the failure raised is the connection error itself.
     */
    public function forceReconnect(): void
    {
        $this->reconnectRequested = true;
    }

    private function applyRequestedReconnect(): void
    {
        if (!$this->reconnectRequested || $this->connecting) {
            return;
        }

        $this->reconnectRequested = false;

        // Nothing to move off yet: the next connection picks a server regardless.
        if (!is_resource($this->socket)) {
            return;
        }

        $this->logger?->debug(
            'reconnecting on request, leaving ' . $this->getPool()->current()?->getAddress()
        );

        // Deliberately not routed through processException(): this is a requested
        // operation rather than a failure. Handing it a synthetic exception logged
        // an error for an ordinary event, refused to run at all when automatic
        // reconnection was disabled, and, once the pool was exhausted, reported
        // that synthetic exception as the cause in place of the connection errors
        // that actually stopped it.
        //
        // A single pass, however the budget is set. This is a call the application is
        // waiting on, and with an unlimited budget sweeping until something answers
        // would never return when nothing does. Recovering from a lost connection is
        // the job of the failure path; here the answer to "move me" can be "no".
        $this->getPool()->next();
        $this->connect(retry: false);
    }

    /**
     * Merged rather than replaced, because an asynchronous INFO carries only some of
     * the fields. Replacing would turn a property that callers read directly, such
     * as tls_required, back into an uninitialized one, and reading that is an Error.
     */
    private function mergeInfoMessage(Info $info): void
    {
        if (!isset($this->infoMessage)) {
            $this->infoMessage = $info;

            return;
        }

        foreach (get_object_vars($info) as $property => $value) {
            $this->infoMessage->$property = $value;
        }
    }

    /**
     * Writes a message to the socket, throwing on failure rather than reconnecting.
     */
    private function writeMessage(Message $message): void
    {
        $line = $message->render() . "\r\n";
        $length = strlen($line);
        $total = 0;

        $this->logger?->debug('send ' . $line);

        while ($total < $length) {
            $written = @fwrite($this->socket, substr($line, $total, $this->packetSize));
            if ($written === false) {
                throw new LogicException('Error sending data');
            }
            if ($written === 0) {
                throw new LogicException('Broken pipe or closed connection');
            }
            $total += $written;
        }
    }

    /**
     * Reads a single protocol control line.
     *
     * Asks for the whole line in one call. stream_get_line() only recognises a
     * delimiter that lies inside the window it was given, so a smaller window splits
     * a line whose carriage return is the last byte of it, and a clustered INFO
     * carrying many connect_urls reaches such lengths. When the line has not fully
     * arrived the call returns false without consuming it, so the rest can complete
     * it on a later read.
     *
     * @return string|false false when there is no complete line yet
     */
    private function readLine(): string|false
    {
        $line = stream_get_line($this->socket, self::CONTROL_LINE_LIMIT, "\r\n");

        if ($line !== false && strlen($line) >= self::CONTROL_LINE_LIMIT) {
            // What is left of the line cannot be told from the start of the next one,
            // so the connection is dropped and the next read reconnects.
            $this->closeSocket();

            throw new LogicException(sprintf(
                'Protocol line exceeds the %d byte limit',
                self::CONTROL_LINE_LIMIT
            ));
        }

        return $line;
    }

    protected function getPayload(int $length): string
    {
        $payload = '';
        $iteration = 0;
        while (strlen($payload) < $length) {
            $payloadLine = stream_get_line($this->socket, $length, '');
            if (!$payloadLine) {
                if ($iteration > 16) {
                    break;
                }
                $this->config->delay($iteration++);
                continue;
            }
            if (strlen($payloadLine) !== $length) {
                $this->logger?->debug(
                    'got ' . strlen($payloadLine) . '/' . $length . ': ' . $payloadLine
                );
            }
            $payload .= $payloadLine;
        }
        return $payload;
    }

    private function processException(Throwable $e): void
    {
        $this->logger?->error($e->getMessage(), ['exception' => $e]);

        if (!$this->config->reconnect || $this->connecting) {
            throw $e;
        }

        $pool = $this->getPool();

        // Move off the server that just dropped us before trying anything. On a
        // single server pool this comes back round to the same one.
        try {
            $pool->next();
        } catch (Throwable) {
            throw $e;
        }

        try {
            $this->connect(retry: true);
        } catch (Throwable) {
            // Report the disconnect that started this rather than the last failed
            // attempt to recover from it.
            throw $e;
        }
    }

    private function isAuthenticationFailure(Throwable $e): bool
    {
        $message = strtolower($e->getMessage());

        return str_contains($message, 'authorization violation')
            || str_contains($message, 'authentication expired')
            || str_contains($message, 'authentication timeout');
    }

    /** Backs off before sweeping the pool again, jittered to spread a fleet out. */
    private function wait(): void
    {
        $seconds = $this->config->reconnectWait;

        if ($this->config->reconnectJitter > 0) {
            $seconds += mt_rand(0, (int) ($this->config->reconnectJitter * 1_000_000)) / 1_000_000;
        }

        if ($seconds > 0) {
            usleep((int) ($seconds * 1_000_000));
        }
    }

    /**
     * Closed explicitly: otherwise every reconnection leaks the old descriptor and
     * the process eventually crosses PHP's FD_SETSIZE limit, see #139.
     */
    private function closeSocket(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }

        $this->socket = null;
    }

    public function setPacketSize(int $size): void
    {
        $this->packetSize = $size;
    }

    public function close(): void
    {
        $this->closeSocket();
        $this->resetConnectionState();
    }
}
