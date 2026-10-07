<?php

declare(strict_types=1);

namespace Tests\Unit\Message;

use Basis\Nats\Message\Factory;
use Basis\Nats\Message\Info;
use Basis\Nats\Message\Pong;
use InvalidArgumentException;
use Tests\TestCase;

class InfoTest extends TestCase
{
    /**
     * The server owns this message and grows it between releases. Advertising
     * protocol level 1 already brings back connect_info and remote_account, which
     * older clients never saw, so rejecting an unrecognised field would break
     * against every newer server.
     */
    public function testUnknownFieldsAreIgnored()
    {
        $message = Factory::create('INFO ' . json_encode([
            'server_id' => 'ID',
            'proto' => 1,
            'some_field_from_a_future_release' => ['nested' => true],
        ]));

        $this->assertInstanceOf(Info::class, $message);
        $this->assertSame('ID', $message->server_id);
        $this->assertSame(1, $message->proto);
        $this->assertFalse(property_exists($message, 'some_field_from_a_future_release'));
    }

    public function testAsynchronousUpdateFields()
    {
        $message = Factory::create('INFO ' . json_encode([
            'server_id' => 'ID',
            'connect_info' => true,
            'remote_account' => '$G',
            'ldm' => true,
            'connect_urls' => ['10.0.0.7:4222'],
        ]));

        $this->assertTrue($message->connect_info);
        $this->assertSame('$G', $message->remote_account);
        $this->assertTrue($message->ldm);
        $this->assertSame(['10.0.0.7:4222'], $message->connect_urls);
    }

    /**
     * Tolerance is limited to INFO: the client writes the other messages itself, so
     * an unexpected field there is a mistake worth reporting.
     */
    public function testOtherMessagesStayStrict()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid property nick for message ' . Pong::class);

        new Pong(new \Basis\Nats\Message\Payload(json_encode(['nick' => 'nekufa'])));
    }

    /**
     * Fields that were absent from the message stay absent, so a caller reading one
     * directly gets a clear error rather than a made up value.
     */
    public function testAbsentFieldsAreNotInitialized()
    {
        $message = Factory::create('INFO {"server_id":"ID"}');

        $this->assertFalse(isset($message->ldm));
        $this->assertFalse(isset($message->connect_urls));
        $this->assertNull($message->ldm ?? null);
    }
}
