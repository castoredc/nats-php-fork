<?php

/**
 * Child process of ScriptedServer: listens on an ephemeral port, accepts the given
 * number of clients one after another, plays the script it was given to each, then
 * prints the lines it read as json.
 *
 * Steps: ["send", bytes], ["flood", prefix, byte, count] (a prefix followed by count
 * copies of one byte, for lines too long to pass on the command line), ["read"],
 * ["sleep", seconds].
 */

declare(strict_types=1);

$steps = json_decode($argv[1], true, 512, JSON_THROW_ON_ERROR);
$connections = (int) $argv[2];

$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
if ($server === false) {
    fwrite(STDERR, "cannot listen: $error\n");
    exit(1);
}

fwrite(STDOUT, stream_socket_get_name($server, false) . "\n");
fflush(STDOUT);

$read = [];

for ($i = 0; $i < $connections; $i++) {
    $connection = @stream_socket_accept($server, 5);

    if ($connection === false) {
        break;
    }

    stream_set_timeout($connection, 1);

    foreach ($steps as $step) {
        if ($step[0] === 'send') {
            fwrite($connection, $step[1]);
        } elseif ($step[0] === 'flood') {
            fwrite($connection, $step[1]);
            $chunk = str_repeat($step[2], 65536);
            for ($sent = 0; $sent < $step[3]; $sent += 65536) {
                fwrite($connection, substr($chunk, 0, min(65536, $step[3] - $sent)));
            }
        } elseif ($step[0] === 'read') {
            $line = stream_get_line($connection, 1048576, "\r\n");
            $read[] = $line === false ? null : $line;
        } elseif ($step[0] === 'sleep') {
            usleep((int) ($step[1] * 1_000_000));
        }
    }

    fclose($connection);
}

fclose($server);
fwrite(STDOUT, json_encode($read) . "\n");
