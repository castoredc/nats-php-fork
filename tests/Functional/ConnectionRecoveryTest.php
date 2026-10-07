<?php

declare(strict_types=1);

namespace Tests\Functional;

use Basis\Nats\Connection;
use LogicException;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use ReflectionMethod;
use ReflectionProperty;
use Tests\FunctionalTestCase;

class ConnectionRecoveryTest extends FunctionalTestCase
{
    /**
     * The handshake writes through sendMessage() and reads through getMessage(),
     * both of which reconnect on failure. Recursing there connects to another
     * server and finishes its handshake, then unwinds into the outer handshake,
     * which keeps operating on a socket that is already connected elsewhere.
     */
    public function testHandshakeFailureDoesNotReconnect(): void
    {
        $client = $this->createClient(['reconnect' => true]);
        $connection = $client->connection;

        $connecting = new ReflectionProperty(Connection::class, 'connecting');
        $connecting->setValue($connection, true);

        $processException = new ReflectionMethod(Connection::class, 'processException');

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('handshake failed');

        $processException->invoke($connection, new LogicException('handshake failed'));
    }

    public function testReconnectionStillHappensOutsideTheHandshake(): void
    {
        $client = $this->createClient(['reconnect' => true]);
        $client->ping();

        $connection = $client->connection;
        $socket = new ReflectionProperty(Connection::class, 'socket');
        fclose($socket->getValue($connection));

        // Broken socket: publishing has to recover rather than throw.
        $client->publish('recovery.subject', 'payload');

        $this->assertTrue($client->ping());
    }

    /**
     * Guards protocol integrity across a reconnect that happens mid-publish: the
     * message has to arrive exactly once and intact, and the stream has to stay in
     * sync afterwards.
     *
     * Note this does not by itself reproduce the stale write offset that
     * sendMessage() used to carry into a retry, since closing the socket fails the
     * very first write and leaves the offset at zero. Forcing a partial write
     * deterministically would mean filling the kernel buffer mid-message. The
     * offset no longer exists in the retry path at all, so that bug is now
     * structurally impossible rather than merely covered.
     */
    public function testMessageSurvivesReconnectMidPublish(): void
    {
        $client = $this->createClient(['reconnect' => true]);
        $client->ping();

        $connection = $client->connection;
        // Force many small writes so a break lands mid-message.
        $connection->setPacketSize(8);

        $subscriber = $this->createClient();
        $received = [];
        $subscriber->subscribe('recovery.whole', function ($message) use (&$received) {
            $received[] = (string) $message;
        });
        $subscriber->process(1);

        $socket = new ReflectionProperty(Connection::class, 'socket');
        fclose($socket->getValue($connection));

        $payload = str_repeat('abcdefgh', 16);
        $client->publish('recovery.whole', $payload);
        $connection->setPacketSize(1024);

        $threshold = microtime(true) + 2;
        while ($received === [] && microtime(true) < $threshold) {
            $subscriber->process(0.1);
        }

        $this->assertSame([$payload], $received, 'the message must arrive exactly once and intact');
        $this->assertTrue($client->ping(), 'the protocol stream must still be in sync');

        $subscriber->disconnect();
    }

    /**
     * The stored subscription used to keep only the subject and the sid, so the
     * replayed SUB dropped its queue group. The subscriber then silently became a
     * plain one and every member of the group received every message.
     */
    public function testQueueGroupSurvivesReconnect(): void
    {
        $client = $this->createClient();
        $client->subscribeQueue('group.subject', 'workers', fn () => null);
        $client->ping();

        if (!$client->connection->logger) {
            $client->connection->logger = new Logger('client');
        }
        assert($client->connection->logger instanceof Logger);
        $client->connection->logger->pushHandler($spy = new class ('') extends StreamHandler {
            public array $records = [];

            protected function write(array $record): void
            {
                $this->records[] = $record['message'];
            }
        });

        $socket = new ReflectionProperty(Connection::class, 'socket');
        fclose($socket->getValue($client->connection));

        $this->assertTrue($client->ping());

        $replayed = array_values(array_filter(
            $spy->records,
            fn ($row) => str_contains($row, 'send SUB group.subject')
        ));

        $this->assertCount(1, $replayed, 'the subscription has to be replayed once');
        $this->assertStringContainsString(
            'SUB group.subject workers ',
            $replayed[0],
            'the replayed subscription has to keep its queue group'
        );
    }
}
