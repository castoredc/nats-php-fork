<?php

declare(strict_types=1);

namespace Tests\Utils;

use RuntimeException;

/**
 * A server that plays a fixed script to the one client that connects to it, for
 * tests that need to control the exact order of protocol lines. It runs in a child
 * process because a single process cannot be both the client waiting on a read and
 * the server that has to answer it.
 */
final class ScriptedServer
{
    /** @var resource */
    private $process;

    /** @var resource */
    private $output;

    private function __construct(
        $process,
        $output,
        public readonly string $address,
    ) {
        $this->process = $process;
        $this->output = $output;
    }

    /**
     * @param array $steps ["send", bytes], ["flood", prefix, byte, count] (a long
     *                     line that would not fit on the command line), ["read"]
     *                     (one line from the client) or ["sleep", seconds]
     * @param int $connections how many clients get the script, one after another.
     *                         The listener is closed after the last one, so a further
     *                         attempt is refused.
     */
    public static function start(array $steps, int $connections = 1): self
    {
        $command = [PHP_BINARY, '-n', __DIR__ . '/scripted-server.php', json_encode($steps), (string) $connections];
        $process = proc_open($command, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);

        if (!is_resource($process)) {
            throw new RuntimeException('Could not start the scripted server');
        }

        stream_set_timeout($pipes[1], 5);
        $address = fgets($pipes[1]);

        if ($address === false) {
            throw new RuntimeException('The scripted server did not report its address');
        }

        return new self($process, $pipes[1], trim($address));
    }

    /**
     * Ends a server nobody connected to, instead of waiting out its accept timeout.
     */
    public function stop(): void
    {
        proc_terminate($this->process);
        fclose($this->output);
        proc_close($this->process);
    }

    /**
     * Waits for the script to run out and returns the lines the client sent, in
     * order across connections, with null for a read that got nothing.
     *
     * @return array<int, string|null>
     */
    public function finish(): array
    {
        $lines = [];
        while (($line = fgets($this->output)) !== false) {
            $lines[] = trim($line);
        }

        fclose($this->output);
        proc_close($this->process);

        return json_decode((string) end($lines), true) ?? [];
    }
}
