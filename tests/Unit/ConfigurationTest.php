<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Configuration;
use Basis\Nats\Message\Connect;
use Tests\TestCase;

class ConfigurationTest extends TestCase
{
    public function testExponentialDelay()
    {
        $configuration = new Configuration([
            'delayMode' => Configuration::DELAY_EXPONENTIAL,
        ]);

        $this->assertSame($configuration->getDelayMode(), Configuration::DELAY_EXPONENTIAL);

        $start = microtime(true);
        $configuration->delay(0);
        $this->assertLessThan(0.01, microtime(true) - $start);
    }

    public function testInvalidDelayConfiguration()
    {
        $configuration = new Configuration();
        $this->expectExceptionMessage("Invalid mode: dreaming");
        $configuration->setDelay(1, 'dreaming');
    }

    public function testInvalidConfiguration()
    {
        $this->expectExceptionMessage("Invalid config option hero");
        new Configuration(['hero' => true]);
    }

    public function testMaxReconnectAttemptsDefault()
    {
        $this->assertSame(-1, (new Configuration())->maxReconnectAttempts);
    }

    public function testMaxReconnectAttemptsNamedArgument()
    {
        $configuration = new Configuration(maxReconnectAttempts: 5);
        $this->assertSame(5, $configuration->maxReconnectAttempts);
    }

    public function testMaxReconnectAttemptsDeprecatedArray()
    {
        $configuration = new Configuration(['maxReconnectAttempts' => 3]);
        $this->assertSame(3, $configuration->maxReconnectAttempts);
    }

    /**
     * The server only pushes asynchronous INFO updates, which is how cluster
     * topology and lame duck mode are announced, to clients at protocol level 1.
     */
    public function testProtocolLevelIsAdvertised()
    {
        $this->assertSame(1, (new Configuration())->getOptions()['protocol']);
    }

    public function testClientConfigurationToken()
    {
        $connection = new Configuration(['token' => 'zzz']);
        $this->assertArrayHasKey('auth_token', $connection->getOptions());
    }

    public function testClientConfigurationJwt()
    {
        $connection = new Configuration(['jwt' => random_bytes(16)]);
        $this->assertArrayHasKey('jwt', $connection->getOptions());
    }

    public function testClientConfigurationBasicAuth()
    {
        $connection = new Configuration(['user' => 'nekufa', 'pass' => 't0p53cr3t']);
        $this->assertArrayHasKey('user', $connection->getOptions());
        $this->assertArrayHasKey('pass', $connection->getOptions());
    }

    /**
     * Connect::$pass is a non-nullable string, so emitting a null password
     * throws a TypeError when the options are hydrated into the message.
     */
    public function testClientConfigurationUserWithoutPassword()
    {
        $options = (new Configuration(['user' => 'nekufa']))->getOptions();

        $this->assertArrayHasKey('user', $options);
        $this->assertArrayNotHasKey('pass', $options);

        $this->assertSame('CONNECT {"user":"nekufa"}', (new Connect(['user' => 'nekufa']))->render());
    }
}
