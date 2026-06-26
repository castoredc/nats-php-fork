# EC-4495: Investigation — eConsent Queue Jobs SLO Breach

## Summary

The `[eConsent] Rate of failed and successfully processed queue jobs` SLO has consumed ~73% of its 30-day error budget (~1,422 failed messages out of ~210,761 received). The dominant failure is `ICFSignedEventMessage` dying with `LogicException: Unknown Protocol Operation` from the NATS client.

The root cause is a bug in `castoredc/nats-php-fork` (`vendor/castoredc/nats/src/Connection.php`) where the byte-offset counter `$total` is not reset after a socket reconnect, causing the tail of a PUB command to be sent as the first bytes on a fresh NATS connection.

---

## Failure Sequence

### 1. Payload size exceeds packet size

The `ICFSignedEvent` is serialized as a CloudEvent with:
- `avroschema` extension: the full Avro schema JSON string (~350–400 bytes when escaped)
- `data_base64`: Avro binary-encoded event data (~280 bytes)
- Standard CloudEvent fields: ~500 bytes

Total CloudEvent JSON body: **~1,200–1,300 bytes**, which exceeds the NATS client's `$packetSize = 1024`. Sending this message requires **two `fwrite` calls**.

### 2. Stale TCP connection

Messenger workers sit idle between messages. The NATS server closes connections that go quiet (timeout = 1.0 s, ping interval = 2 s in `ConfigurationFactory` defaults). The worker does not detect this until it tries to write.

### 3. First write succeeds, second fails

```
fwrite($socket, substr($line, 0, 1024))   → 1024 bytes written, $total = 1024
fwrite($socket, substr($line, 1024, ...)) → returns 0 (TCP RST received)
                                             throws LogicException('Broken pipe or closed connection')
```

### 4. Reconnect — but `$total` is not reset

`processException()` reconnects the socket and completes the NATS `CONNECT` handshake. Control returns to the `catch` block in `sendMessage()`:

```php
// vendor/castoredc/nats/src/Connection.php, lines 150–153
} catch (Throwable $e) {
    $this->processException($e);
    $line = $message->render() . "\r\n";
    // BUG: $total is still 1024 — not reset to 0
}
```

The while loop continues with `$total = 1024`. The next call:

```php
fwrite($socket, substr($line, 1024, 1024))
```

sends the **last ~200 bytes of the PUB command** to the fresh socket.

### 5. NATS server rejects garbage

The fresh socket just completed `CONNECT`. It expects a new protocol command. Instead it receives the tail bytes of a `PUB` message. The NATS server responds:

```
-ERR 'Unknown Protocol Operation'
```

`sendMessage()` returns without exception (the garbled `fwrite` itself succeeded at the socket level).

### 6. Exception surfaces outside the retry wrapper

`NatsPublisher::publish()` calls `$this->client->process()` after `$this->client->publish()`:

```php
// vendor/castoredc/php-event-publisher/src/Publisher/NatsPublisher.php
$this->client->publish($subject->__toString(), new Payload($serializedPayload));
$this->client->process();   // <-- reads -ERR from socket
```

`process()` calls `getMessage(0)` → `Factory::create('-ERR ...')` → throws `LogicException('Unknown Protocol Operation')`.

This call is made **directly on the parent `Basis\Nats\Client`**, bypassing the retry wrapper in `castoredc/nats-php-wrapper`. The exception propagates to the Symfony Messenger handler and fails the `ICFSignedEventMessage`.

### 7. Why retries don't help

The `castoredc/nats-php-wrapper`'s `Client::publish()` wraps only `parent::publish()` in the retry strategy. Since `publish()` returned without throwing (the garbled write was silently "successful"), no retry is triggered. The error only manifests in the subsequent `process()` call, which is not retried.

---

## The Fix

### Primary fix — `castoredc/nats-php-fork`

Add `$total = 0;` after `$this->processException($e);` so the fresh socket receives the full message from byte 0:

```php
// vendor/castoredc/nats/src/Connection.php
} catch (Throwable $e) {
    $this->processException($e);
    $line = $message->render() . "\r\n";
    $total = 0; // reset so the full message is resent on the new socket
}
```

File: `src/Connection.php` in `github.com/castoredc/nats-php-fork`

### Secondary hardening — `castoredc/php-event-publisher`

Move the `process()` call inside the retry scope in `NatsPublisher::publish()`, or wrap it separately, so a `LogicException` from reading NATS server errors also triggers a retry:

```php
// vendor/castoredc/php-event-publisher/src/Publisher/NatsPublisher.php
$this->client->publish($subject->__toString(), new Payload($serializedPayload));
$this->client->process(); // should be inside or adjacent to the retry wrapper
```

---

## Affected Components

| Repository | File | Version |
|---|---|---|
| `castoredc/nats-php-fork` | `src/Connection.php` | v0.1.1 |
| `castoredc/php-event-publisher` | `src/Publisher/NatsPublisher.php` | — |

## Corroborating Evidence

- Datadog shows ~1,078 log occurrences of `Unknown Protocol Operation` for `ICFSignedEventMessage`
- Datadog shows `Broken pipe or closed connection` errors from the same hosts immediately before the `Unknown Protocol Operation` errors — this is `processException()` logging the socket error at step 3 above
- A secondary failure type (`AuditTrailEventMessage` with MySQL deadlocks) accounts for the remainder of the SLO budget consumption and has a separate root cause
