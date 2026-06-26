# EC-4495 Solution — `nats-php-fork` byte-offset reset on reconnect

## TL;DR

The `$total` byte-offset counter in `Connection::sendMessage()` was not reset
after a mid-message socket reconnect, so only the **tail** of a multi-write
command was resent on the fresh connection. NATS saw the tail bytes as a new
protocol command and replied `-ERR 'Unknown Protocol Operation'`, which
surfaced as a `LogicException` and failed the queue job.

Fixed by resetting `$total = 0` in the reconnect `catch` block. A deterministic
regression test reproduces the failure and now passes.

---

## Root cause

`Connection::sendMessage()` writes a rendered message to the socket in
`$packetSize` (1024-byte) chunks, tracking progress in `$total`:

```php
$line = $message->render() . "\r\n";
$length = strlen($line);
$total = 0;

while ($total < $length) {
    try {
        $written = @fwrite($this->socket, substr($line, $total, $this->packetSize));
        // ...
        $total += $written;
        // ...
    } catch (Throwable $e) {
        $this->processException($e);   // reconnects the socket
        $line = $message->render() . "\r\n";
        // BUG: $total kept its stale value here
    }
}
```

When a message is larger than `$packetSize` it takes two or more `fwrite`
calls. If the connection dies **after** the first chunk has been written
(typical for a stale TCP connection that a Messenger worker only discovers on
write), the second `fwrite` throws. `processException()` reconnects and
completes a fresh `CONNECT` handshake, but control returns to the loop with
`$total` still pointing past the start of the message. The resend therefore
begins at `substr($line, $total, ...)` — sending only the last bytes of the
`PUB` command onto a brand-new connection that expects a fresh protocol verb.

NATS answers `-ERR 'Unknown Protocol Operation'`, which is later read by
`process()` and thrown as `LogicException('Unknown Protocol Operation')`,
failing the `ICFSignedEventMessage` job.

See `EC-4495.md` for the full failure sequence and corroborating Datadog
evidence.

## The fix

`src/Connection.php`, inside the `sendMessage()` reconnect `catch` block:

```php
} catch (Throwable $e) {
    $this->processException($e);
    $line = $message->render() . "\r\n";
    $total = 0; // reset so the full message is resent on the fresh socket
}
```

Resetting `$total` makes the write loop resend the **entire** rendered message
from byte 0 on the reconnected socket, so NATS receives a well-formed command.

This is the **primary fix** from EC-4495. The **secondary hardening** (moving
`$this->client->process()` inside the retry scope in `NatsPublisher::publish()`)
lives in the separate `castoredc/php-event-publisher` repository and is **out of
scope for this repo**.

## Regression test

`tests/Functional/ConnectionReconnectMidMessageTest.php`

The failure is timing-sensitive over real TCP (a write must partially succeed,
then fail mid-message), so the test makes it **deterministic** without a real
broker:

- It binds a local listening socket, then `pcntl_fork`s a scripted fake NATS
  server in the child process.
- **Connection 1:** the fake server sends `INFO`, reads the `CONNECT` handshake
  plus the first bytes of the `PUB` command, then closes the socket — forcing
  the client's write to fail mid-message (`$total > 0`). A ~1 MB payload
  guarantees the OS send buffer cannot flush the whole message before the peer's
  RST is observed, so the failure reliably lands mid-message.
- **Connection 2 (the reconnect):** the fake server captures the bytes the
  client resends and the test asserts they start with `PUB test.subject ` — a
  complete command, not a mid-message tail.

Behaviour:

- **Before the fix:** the reconnected socket received raw payload bytes
  (`AAAA…`) instead of the `PUB …` prefix → test **fails** (bug reproduced).
- **After the fix:** the full `PUB` command is resent from byte 0 → test
  **passes**.

The test extends `Tests\TestCase` (not `FunctionalTestCase`) so it needs **no
running NATS server**, and it skips itself if `pcntl`/`sockets` are unavailable.

### Running it

```bash
vendor/bin/phpunit tests/Functional/ConnectionReconnectMidMessageTest.php
```

The rest of the suite still requires a broker:

```bash
cd docker && docker compose up -d   # NATS_IMAGE_TAG=2.10 (or 2.11/2.12/latest)
vendor/bin/phpunit --testsuite Tests
```

## Affected files

| File | Change |
|---|---|
| `src/Connection.php` | Reset `$total = 0` after reconnect in `sendMessage()` |
| `tests/Functional/ConnectionReconnectMidMessageTest.php` | New deterministic regression test |
