<?php

declare(strict_types=1);

namespace Tests\Unit\Message;

use Basis\Nats\Message\Connect;
use Tests\TestCase;

class ConnectTest extends TestCase
{
    /**
     * The server only pushes asynchronous INFO updates (cluster topology and the
     * lame duck flag) to clients that advertise protocol level 1 or higher, and it
     * unmarshals the field into an int. Sending it as a string is rejected with
     * -ERR 'Invalid Connect Options'.
     */
    public function testProtocolIsAnInteger()
    {
        $connect = new Connect(['protocol' => 1]);

        $this->assertSame(1, $connect->protocol);
        $this->assertSame('CONNECT {"protocol":1}', $connect->render());
    }

    public function testTlsRequiredIsABoolean()
    {
        $connect = new Connect(['tls_required' => true]);

        $this->assertSame('CONNECT {"tls_required":true}', $connect->render());
    }

    public function testEchoIsABoolean()
    {
        $connect = new Connect(['echo' => false]);

        $this->assertSame('CONNECT {"echo":false}', $connect->render());
    }

    /**
     * Only the properties that were actually assigned are serialized, so an
     * unauthenticated connect does not leak empty credential fields.
     */
    public function testUnsetPropertiesAreOmitted()
    {
        $connect = new Connect(['lang' => 'php', 'version' => 'dev']);

        $this->assertSame('CONNECT {"lang":"php","version":"dev"}', $connect->render());
    }
}
