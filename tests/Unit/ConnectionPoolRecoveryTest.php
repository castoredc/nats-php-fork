<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Message\Ping;
use Exception;
use Tests\TestCase;

/**
 * Running out of servers ends the attempt to recover, not the client: a worker that
 * catches the failure and tries again has to find the servers it was configured with.
 */
class ConnectionPoolRecoveryTest extends TestCase
{
    public function testClientTriesItsServersAgainAfterTheBudgetIsSpent(): void
    {
        $client = new Client(new Configuration([
            'servers' => ['127.0.0.1:1'],
            'maxReconnectAttempts' => 1,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
            'timeout' => 0.1,
        ]));

        for ($call = 1; $call <= 3; $call++) {
            try {
                $client->connection->sendMessage(new Ping());
                $this->fail("call $call cannot succeed without a server");
            } catch (Exception $e) {
                $this->assertNotSame(
                    'No servers available',
                    $e->getMessage(),
                    "call $call gave up without trying the configured server"
                );
            }
        }
    }
}
