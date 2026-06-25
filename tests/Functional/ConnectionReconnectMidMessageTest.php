<?php

declare(strict_types=1);

namespace Tests\Functional;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Message\Payload;
use Basis\Nats\Message\Publish;
use Tests\TestCase;

/**
 * Reproduces EC-4495.
 *
* When a message is larger than the connection packet size it is written to the
* socket with several fwrite() calls. If the socket dies *after* the first
* chunk has been written, Connection::sendMessage() catches the error,
* reconnects, and resumes the write loop. The byte offset counter $total must
* be reset to 0 so the *whole* message is resent on the fresh socket.
*
* Before the fix $total kept its stale value, so only the tail of the PUB
* command was sent on the new connection. The NATS server then saw the tail
* bytes as a fresh protocol command and answered `-ERR 'Unknown Protocol
* Operation'`.
 *
 * This test stands up a scripted fake NATS server in a child process so the
 * mid-message failure is deterministic (no real broker, no TCP-timing luck),
 * and asserts that the bytes received on the reconnected socket form a complete
 * PUB command rather than a mid-message tail.
 */
class ConnectionReconnectMidMessageTest extends TestCase
{
    public function testTotalIsResetSoFullMessageIsResentAfterReconnect(): void
    {
        if (!function_exists('pcntl_fork') || !function_exists('stream_socket_server')) {
            $this->markTestSkipped('pcntl/sockets extensions are required for this test');
        }

        // Writing to a peer that has gone away must not kill the test process.
        if (function_exists('pcntl_signal') && defined('SIGPIPE')) {
            pcntl_signal(SIGPIPE, SIG_IGN);
        }

        // Bind the listening socket in the parent so we know the port up front,
        // then fork: the child runs the fake server, the parent runs the client.
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server, "failed to bind fake server: $errstr");

        $address = stream_socket_get_name($server, false);
        $port = (int) substr($address, strrpos($address, ':') + 1);

        $resultFile = tempnam(sys_get_temp_dir(), 'nats-reconnect-');

        $pid = pcntl_fork();
        $this->assertNotSame(-1, $pid, 'pcntl_fork failed');

        if ($pid === 0) {
            // Child: run the scripted fake server, then terminate hard so no
            // PHPUnit shutdown logic runs in the forked process.
            $this->runFakeServer($server, $resultFile, $port);
            exit(0);
        }

        // Parent: it does not accept connections.
        fclose($server);

        $subject = 'test.subject';
        // A payload larger than the OS send buffer guarantees the write loop
        // cannot flush the whole message before the peer's RST is observed, so
        // the failure happens reliably *mid-message* (with $total > 0).
        $body = str_repeat('A', 1024 * 1024);

        $config = new Configuration(
            [],
            host: '127.0.0.1',
            port: $port,
            reconnect: true,
            timeout: 0.5,
            delay: 0.01,
        );

        $client = new Client($config);
        $client->connection->sendMessage(new Publish([
            'subject' => $subject,
            'payload' => new Payload($body),
        ]));
        // Closing the client lets the fake server see EOF and stop capturing.
        $client->connection->close();

        pcntl_waitpid($pid, $status);

        $captured = file_get_contents($resultFile);
        @unlink($resultFile);

        $this->assertNotSame(
            'NO_CONN1',
            $captured,
            'fake server never received the first connection'
        );
        $this->assertNotSame(
            'NO_CONN2',
            $captured,
            'the reconnect was never triggered — the write did not fail mid-message'
        );

        // The reconnected socket must receive a complete PUB command. With the
        // bug it receives a mid-message tail (e.g. "subject 1048576\r\nAAA..."),
        // which is exactly what makes NATS answer "Unknown Protocol Operation".
        $this->assertStringStartsWith(
            "PUB $subject ",
            $captured,
            'after reconnect the socket received a mid-message tail instead of a '
            . 'complete PUB command — $total was not reset (EC-4495). '
            . 'Received prefix: ' . substr($captured, 0, 40)
        );
    }

    /**
     * Minimal scripted NATS server.
     *
     * Connection 1: send INFO, read the CONNECT handshake and the first few
     * bytes of the PUB command, then slam the socket shut to force the client's
     * write to fail mid-message.
     *
     * Connection 2 (the reconnect): send INFO, swallow the CONNECT handshake,
     * then record every remaining byte — those are the bytes the client resends.
     */
    private function runFakeServer($server, string $resultFile, int $port): void
    {
        $info = 'INFO ' . json_encode([
            'server_id' => 'FAKE',
            'server_name' => 'fake',
            'version' => '2.10.0',
            'proto' => 1,
            'go' => 'go1.21',
            'host' => '127.0.0.1',
            'port' => $port,
            'max_payload' => 8 * 1024 * 1024,
            'headers' => true,
        ]) . "\r\n";

        // --- connection 1: provoke the mid-message failure ---
        $c1 = @stream_socket_accept($server, 5);
        if (!$c1) {
            file_put_contents($resultFile, 'NO_CONN1');
            return;
        }
        @fwrite($c1, $info);

        // Read until the CONNECT line has ended (\r\n) and at least a few bytes
        // of the following PUB command have arrived, so the client is provably
        // mid-PUB (its $total > 0) before we cut the socket.
        $buf = '';
        $deadline = microtime(true) + 5;
        while (microtime(true) < $deadline) {
            $b = @fread($c1, 4096);
            if ($b === '' || $b === false) {
                if (feof($c1)) {
                    break;
                }
                usleep(1000);
                continue;
            }
            $buf .= $b;
            $crlf = strpos($buf, "\r\n");
            if ($crlf !== false && strlen($buf) >= $crlf + 2 + 4) {
                break;
            }
        }
        fclose($c1);

        // --- connection 2: capture what the client resends ---
        $c2 = @stream_socket_accept($server, 5);
        if (!$c2) {
            file_put_contents($resultFile, 'NO_CONN2');
            return;
        }
        @fwrite($c2, $info);

        // Swallow the CONNECT handshake line (everything up to its \r\n).
        $handshake = '';
        $deadline = microtime(true) + 5;
        while (strpos($handshake, "\r\n") === false && microtime(true) < $deadline) {
            $b = @fread($c2, 1);
            if ($b === '' || $b === false) {
                if (feof($c2)) {
                    break;
                }
                usleep(1000);
                continue;
            }
            $handshake .= $b;
        }
        $rest = substr($handshake, strpos($handshake, "\r\n") + 2);

        // Everything after the handshake is the resent message. We only need the
        // first bytes for the assertion, but we must keep draining the socket to
        // EOF: if we stop early the client's remaining writes fail and it
        // reconnects forever (the fake server only serves two connections).
        $captured = $rest;
        $deadline = microtime(true) + 5;
        while (microtime(true) < $deadline) {
            $b = @fread($c2, 65536);
            if ($b === '' || $b === false) {
                if (feof($c2)) {
                    break;
                }
                usleep(1000);
                continue;
            }
            if (strlen($captured) < 256) {
                $captured .= $b;
            }
        }
        fclose($c2);
        $rest = $captured;

        file_put_contents($resultFile, $rest);
    }
}
