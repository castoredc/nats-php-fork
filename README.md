# Nats client for php
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Testing](https://github.com/basis-company/nats.php/actions/workflows/tests.yml/badge.svg)](https://github.com/basis-company/nats.php/actions/workflows/tests.yml)
[![Coverage Status](https://coveralls.io/repos/github/basis-company/nats.php/badge.svg)](https://coveralls.io/github/basis-company)
[![Latest Version](https://img.shields.io/github/release/basis-company/nats.php.svg)](https://github.com/basis-company/nats.php/releases)
[![Total Downloads](https://img.shields.io/packagist/dt/basis-company/nats.svg)](https://packagist.org/packages/basis-company/nats)


Feel free to contribute or give any feedback.

- [Nats client for php](#nats-client-for-php)
  - [Installation](#installation)
  - [Connecting](#connecting)
    - [Connecting with TLS](#connecting-with-tls)
    - [Connecting with JWT](#connecting-with-jwt)
  - [Publish Subscribe](#publish-subscribe)
  - [Request Response](#request-response)
  - [JetStream Api Usage](#jetstream-api-usage)
  - [Microservices](#microservices)
  - [Key Value Storage](#key-value-storage)
  - [Performance](#performance)
  - [Configuration Options](#configuration-options)

## Installation
The recommended way to install the library is through [Composer](http://getcomposer.org):
```bash
$ composer require basis-company/nats
```

The NKeys functionality requires Ed25519, which is provided in `libsodium` extension or `sodium_compat` package.

## Connecting
```php
use Basis\Nats\Client;
use Basis\Nats\Configuration;

// you can override any default configuraiton key using constructor
$configuration = new Configuration(
    host: 'nats-service',
    user: 'basis',
    pass: 'secret',
);

// delay configuration options are changed via setters. The delay paces the retries
// made while reading from the connection. It does not pace reconnection attempts:
// those wait for reconnectWait, plus up to reconnectJitter, once per pass over the servers.
// default delay mode is constant - first retry be in 1ms, second in 1ms, third in 1ms
$configuration->setDelay(0.001);

// linear delay mode - first retry be in 1ms, second in 2ms, third in 3ms, fourth in 4ms, etc...
$configuration->setDelay(0.001, Configuration::DELAY_LINEAR);

// exponential delay mode - first retry be in 10ms, second in 100ms, third in 1s, fourth if 10 seconds, etc...
$configuration->setDelay(0.01, Configuration::DELAY_EXPONENTIAL);


$client = new Client($configuration);
$client->ping(); // true

```

### Connecting to a cluster

Pass several endpoints in `servers` instead of a single `host` and `port`. The
client tries them in turn, both when connecting for the first time and whenever
it has to reconnect, so one unreachable member does not take the client down.

Entries may be bare `host:port`, or full urls carrying a scheme and credentials:
`nats://user:secret@host:4222`, `tls://host:4222`. A user name with no password
is treated as a token. Credentials are percent-decoded, so write a password containing
`@`, `:`, `/`, `#` or `%` encoded (`p%40ss` for `p@ss`). When `servers` is set, `host` and `port` are ignored.

```php
use Basis\Nats\Client;
use Basis\Nats\Configuration;

$configuration = new Configuration(
    servers: ['one.example.com:4222', 'two.example.com:4222', 'three.example.com:4222'],
);

$client = new Client($configuration);
$client->ping(); // true
```

The order is shuffled by default, which spreads a fleet of clients that share one
configuration across the cluster. Set `noRandomize: true` to connect in the order
given.

#### Discovered servers

A cluster advertises its members, so the client learns about servers that were
never configured and can fail over onto them. Discovery needs no setup, and
`ignoreDiscoveredServers: true` turns it off if only the configured endpoints
should ever be used.

```php
$client->getServers();           // every endpoint, configured and discovered
$client->getDiscoveredServers(); // only the ones the cluster advertised
```

`discoveredServersHandler` is called when a member the client has never seen
before joins. Note that a cluster which is already assembled advertises itself in
the message that opens the connection, so nothing is reported in that case: the
pool is complete before the client is even connected.

```php
$configuration = new Configuration(
    servers: ['one.example.com:4222'],
    discoveredServersHandler: function (Client $client) {
        // a member joined the cluster
    },
);
```

Credentials are not advertised, so a discovered server is reached with those of
the server that advertised it. A server advertised over an encrypted connection
is required to be encrypted as well: the client refuses to send its credentials
to it otherwise. A `tls://` entry in `servers` is held to the same rule, and
fails the connection instead of sending a cleartext CONNECT when the server does
not negotiate TLS.

The certificate is checked against `tlsCaFile`, not against the host name, for
configured and discovered servers alike. Without `tlsCaFile` it is not checked at
all.

#### Lame duck mode

A server being taken out of service announces it, then disconnects its clients
gradually. `lameDuckModeHandler` reports the announcement; like the go, python and
javascript clients, this one keeps the connection and leaves the decision to the
application. Call `forceReconnect()` to move to another member, applied at the
next read or write:

```php
$configuration = new Configuration(
    servers: ['one.example.com:4222', 'two.example.com:4222'],
    lameDuckModeHandler: fn (Client $client) => $client->forceReconnect(),
);
```

`forceReconnect()` makes one attempt at each server and throws the connection
error if none answers, rather than retrying until one does. That keeps the call
that triggered it from blocking for ever when every member is draining or down.

Doing nothing is also reasonable: the server closes the connection when it is
ready, and the client then reconnects to another member on its own, having moved
the draining server to the back of the queue.

### Connecting with TLS
Typically, when connecting to a cluster with TLS enabled the connection settings do not change. The client lib will automatically switch over to TLS 1.2. However, if you're using a self-signed certificate you may have to point to your local CA file using the tlsCaFile setting.

When connecting to a nats cluster that requires the client to provide TLS certificates use the tlsCertFile and tlsKeyFile to point at your local TLS certificate and private key file.

Nats Server documentation for:
- [Enabling TLS](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/tls)
- [Enabling TLS Authentication](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/tls_mutual_auth)
- [Creating self-signed TLS certs for Testing](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/tls#self-signed-certificates-for-testing)

Connection settings when connecting to a nats server that has TLS and TLS Client verify enabled.
```php
use Basis\Nats\Client;
use Basis\Nats\Configuration;

// you can override any default configuraiton key using constructor
$configuration = new Configuration(
    host: 'tls-service-endpoint',
    tlsCertFile: "./certs/client-cert.pem",
    tlsKeyFile': "./certs/client-key.pem",
    tlsCaFile': "./certs/client-key.pem",
);

$configuration->setDelay(0.001);

$client = new Client($configuration);
$client->ping(); // true
```

## Connecting with JWT

To use NKey with JWT, simply provide them in the `Configuration` options as `jwt` and `nkey`.
You can also provide a credentials file with `CredentialsParser`

```php
use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\NKeys\CredentialsParser;

$configuration = new Configuration(
    CredentialsParser::fromFile($credentialPath)
    host: 'localhost',
    port: 4222,
);

$client = new Client($configuration);
```

## Publish Subscribe

```php
// queue usage example
$queue = $client->subscribe('test_subject');

$client->publish('test_subject', 'hello');
$client->publish('test_subject', 'world');

// optional message fetch
// if there are no updates null will be returned
$message1 = $queue->fetch();
echo $message1->payload . PHP_EOL; // hello

// locks until message is fetched from subject
// to limit lock timeout, pass optional timeout value
$message2 = $queue->next();
echo $message2->payload . PHP_EOL; // world

$client->publish('test_subject', 'hello');
$client->publish('test_subject', 'batching');

// batch message fetching, limit argument is optional
$messages = $queue->fetchAll(10);
echo count($messages);

// fetch all messages that are published to the subject client connection
// queue will stop message fetching when another subscription receives a message
// in advance you can time limit batch fetching
$queue->setTimeout(1); // limit to 1 second
$messages = $queue->fetchAll();

// reset subscription
$client->unsubscribe($queue);

// callback hell example
$client->subscribe('hello', function ($message) {
    var_dump('got message', $message); // tester
});

$client->publish('hello', 'tester');
$client->process();

// if you want to append some headers, construct payload manually
use Basis\Nats\Message\Payload;

$payload = new Payload('tester', [
    'Nats-Msg-Id' => 'payload-example'
]);

$client->publish('hello', $payload);

```

## Request Response
There is a simple wrapper over publish and feedback processing, so payload can be constructed manually same way.
```php
$client->subscribe('hello.request', function ($name) {
    return "Hello, " . $name;
});

// async interaction
$client->request('hello.request', 'Nekufa1', function ($response) {
    var_dump($response); // Hello, Nekufa1
});

$client->process(); // process request

// sync interaction (block until response get back)
$client->dispatch('hello.request', 'Nekufa2'); // Hello, Nekufa2
```

## JetStream Api Usage
```php
use Basis\Nats\Stream\RetentionPolicy;
use Basis\Nats\Stream\StorageBackend;

$accountInfo = $client->getApi()->getInfo(); // account_info_response object

$stream = $client->getApi()->getStream('mailer');

$stream->getConfiguration()
    ->setRetentionPolicy(RetentionPolicy::WORK_QUEUE)
    ->setStorageBackend(StorageBackend::MEMORY)
    ->setSubjects(['mailer.greet', 'mailer.bye']);

// stream is created with given configuration
$stream->create();

// and put some tasks so workers would be doing something
$stream->put('mailer.greet', 'nekufa@gmail.com');
$stream->put('mailer.bye', 'nekufa@gmail.com');

var_dump($stream->info()); // can stream info

// this should be set in your worker
$greeter = $stream->getConsumer('greeter');
$greeter->getConfiguration()->setSubjectFilter('mailer.greet');
// consumer would be created would on first handle call
$greeter->handle(function ($address) {
    mail($address, "Hi there!");
});

var_dump($greeter->info()); // can consumer info

$goodbyer = $stream->getConsumer('goodbyer');
$goodbyer->getConfiguration()->setSubjectFilters(['mailer.bye']);
$goodbyer->create(); // create consumer if you don't want to handle anything right now
$goodbyer->handle(function ($address) {
    mail($address, "See you later");
});

// you can configure batching and iteration count using chain api
$goodbyer
    ->setBatching(2) // how many messages would be requested from nats stream
    ->setIterations(3) // how many times message request should be sent
    ->handle(function () {
        // if you need to break on next iteration simply call interrupt method
        // batch will be processed to the end and the handling would be stopped
        // $goodbyer->interrupt();
    });

// consumer can be used via queue interface
$queue = $goodbyer->getQueue();
while ($message = $queue->next()) {
    if (rand(1, 10) % 2 == 0) {
        mail($message->payload, "See you later");
        $message->ack();
    } else {
        // not ack with 1 second timeout
        $message->nack(1);
    }
    // stop processing
    if (rand(1, 10) % 2 == 10) {
        // don't forget to unsubscribe
        $client->unsubscribe($queue);
        break;
    }
}

// use fetchAll method to batch process messages
// let's set batch size to 50
$queue = $goodbyer->setBatching(50)->create()->getQueue();

// fetching 100 messages provides 2 stream requests
// limit message fetching to 1 second
// it means no more that 100 messages would be fetched
$messages = $queue->setTimeout(1)->fetchAll(100);

$recipients = [];
foreach ($messages as $message) {
    $recipients[] = (string) $message->payload;
}

mail_to_all($recipients, "See you later");

// ack all messages
foreach ($messages as $message) {
    $message->ack();
}


// you also can create ephemeral consumer
// the only thing that ephemeral consumer is created as soon as object is created
// you have to create full consumer configuration first
use Basis\Nats\Consumer\Configuration as ConsumerConfiguration;
use Basis\Nats\Consumer\DeliverPolicy;

$configuration = (new ConsumerConfiguration($stream->getName()))
    ->setDeliverPolicy(DeliverPolicy::NEW)
    ->setSubjectFilter('mailer.greet');

$ephemeralConsumer = $stream->createEphemeralConsumer($configuration);

// now you can use ephemeral consumer in the same way as durable consumer
$ephemeralConsumer->handle(function ($address) {
    mail($address, "Hi there!");
});

// the only difference - you don't have to remove it manually, it will be deleted by NATS when socket connection is closed
// be aware that NATS will not remove that consumer immediately, process can take few seconds
var_dump(
    $ephemeralConsumer->getName(),
    $ephemeralConsumer->info(),
);

// if you need to append some headers, construct payload manually
use Basis\Nats\Message\Payload;

$payload = new Payload('nekufa@gmail.com', [
    'Nats-Msg-Id' => 'single-send'
]);

$stream->put('mailer.bye', $payload);

```

## Microservices
The services feature provides a simple way to create microservices that leverage NATS.

In the example below, you will see an example of creating an index function for the posts microservice. The request can
be accessed under "v1.posts" and then individual post by "v1.posts.{post_id}".
```php
// Define a service
$service = $client->service(
    'PostsService',
    'This service is responsible for handling all things post related.',
    '1.0'
);

// Create the version group
$version = $service->addGroup('v1');

// Create the index posts endpoint handler
class IndexPosts implements \Basis\Nats\Service\EndpointHandler {

    public function handle(\Basis\Nats\Message\Payload $payload): array
    {
        // Your application logic
        return [
            'posts' => []
        ];
    }
}

// Create the index endpoint
$version->addEndpoint("posts", IndexPosts::class);

// Create the service group
$posts = $version->addGroup('posts');

// View post endpoint
$posts->addEndpoint(
    '*',
    function (\Basis\Nats\Message\Payload $payload) {
        $postId = explode('.', $payload->subject);
        $postId = $postId[count($postId)-1];

        return [
            'post' => []
        ];
    }
);

// Run the service
$service->run();
```


## Key Value Storage
```php
$bucket = $client->getApi()->getBucket('bucket_name');

// basics
$bucket->put('username', 'nekufa');
echo $bucket->get('username'); // nekufa

// safe update (given revision)
$entry = $bucket->getEntry('username');
echo $entry->value; // nekufa
$bucket->update('username', 'bazyaba', $entry->revision);

// delete value
$bucket->delete('username');

// purge value history
$bucket->purge('username');

// get bucket stats
var_dump($bucket->getStatus());

// in advance, you can fetch all bucket values
$bucket->update('email', 'nekufa@gmail.com');
var_dump($bucket->getAll()); // ['email' => 'nekufa@gmail.com', 'username' => 'nekufa']
```

## Performance
Testing on AMD Ryzen 5 3600X with nats running in docker gives about 370k rps for publish and 360k rps for receive in non-verbose mode.

You can run tests on your environment.
```bash
 % wget https://getcomposer.org/download/latest-stable/composer.phar
...
Saving to: ‘composer.phar’

 % ./composer.phar install
Installing dependencies from lock file (including require-dev)
...

 % export NATS_HOST=0.0.0.0
 % export NATS_PORT=4222
 % export NATS_CLIENT_LOG=1
 % composer run perf-test
PHPUnit 9.6.18 by Sebastian Bergmann and contributors.

Runtime:       PHP 8.3.11
Configuration: /home/nekufa/software/github/nats.php/phpunit.xml
Warning:       No code coverage driver available

[2024-12-24T12:07:04.063687+00:00] PerformanceTest.testPerformance.DEBUG: send CONNECT {"headers":true,"pedantic":false,"verbose":false,"lang":"php","version":"dev"}  [] []
[2024-12-24T12:07:04.063725+00:00] PerformanceTest.testPerformance.INFO: start performance test [] []
[2024-12-24T12:07:05.391707+00:00] PerformanceTest.testPerformance.INFO: publishing {"rps":376536.0,"length":500000,"time":1.3278908729553223} []
[2024-12-24T12:07:06.762321+00:00] PerformanceTest.testPerformance.INFO: processing {"rps":364814.0,"length":500000,"time":1.3705589771270752} []

 % export NATS_CLIENT_VERBOSE=1
 % composer run perf-test
PHPUnit 9.6.18 by Sebastian Bergmann and contributors.

Runtime:       PHP 8.3.11
Configuration: /home/nekufa/software/github/nats.php/phpunit.xml
Warning:       No code coverage driver available

[2024-12-24T12:07:43.721568+00:00] PerformanceTest.testPerformance.DEBUG: send CONNECT {"headers":true,"pedantic":false,"verbose":true,"lang":"php","version":"dev"}  [] []
[2024-12-24T12:07:43.721606+00:00] PerformanceTest.testPerformance.INFO: start performance test [] []
[2024-12-24T12:07:45.053410+00:00] PerformanceTest.testPerformance.INFO: publishing {"rps":375456.0,"length":500000,"time":1.3317129611968994} []
[2024-12-24T12:07:46.566548+00:00] PerformanceTest.testPerformance.INFO: processing {"rps":330450.0,"length":500000,"time":1.513084888458252} []

nekufa@fasiga ~ % cat /proc/cpuinfo | grep AMD
model name	: AMD Ryzen 5 3600X 6-Core Processor
```

## Configuration Options

The following is the list of configuration options and default values.

| Option                 | Default    | Description                                                                                                                                                                                 |
| ---------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `discoveredServersHandler` |        | Called when the cluster advertises a member the client has not seen before.                                                                                                                  |
| `host`                 | `localhost` | Host to connect to (only used if `servers` is not specified).                                                                                                                              |
| `ignoreDiscoveredServers` | `false` | Use only the configured servers, ignoring the members a cluster advertises.                                                                                                                 |
| `inboxPrefix`          | `"_INBOX"` | Sets de prefix for automatically created inboxes                                                                                                                                            |
| `jwt`                  |            | Token for [JWT Authentication](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/jwt). Alternatively you can use [CredentialsParser](#connecting-with-jwt) |
| `lameDuckModeHandler`  |            | Called when the connected server announces it is being taken out of service.                                                                                                                |
| `maxReconnectAttempts` | `-1`       | Maximum failed connection attempts **per server** before that server is dropped from the pool. Negative = unlimited. Once every server is dropped, that recovery attempt gives up and raises the connection error; the next call starts again from the configured servers.                    |
| `nkey`                 |            | Ed25519 based public key signature used for [NKEY Authentication](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/nkey_auth).                            |
| `noRandomize`          | `false`    | Connect to the servers in the order configured instead of shuffling them.                                                                                                                   |
| `pass`                 |            | Sets the password for a connection.                                                                                                                                                         |
| `pedantic`             | `false`    | Turns on strict subject format checks.                                                                                                                                                      |
| `pingInterval`         | `2`        | Number of seconds between client-sent pings.                                                                                                                                                |
| `port`                 | `4222`     | Port to connect to (only used if `servers` is not specified).                                                                                                                               |
| `reconnect`            | `true`     | If true, the client reconnects automatically when the connection is lost.                                                                                                                   |
| `reconnectJitter`      | `0.1`      | Upper bound, in seconds, of a random amount added to `reconnectWait`, so a fleet of clients does not reconnect in lockstep.                                                                 |
| `reconnectWait`        | `0.2`      | Seconds to wait after every server in the pool has been tried unsuccessfully. Attempts within one pass are not delayed.                                                                     |
| `servers`              | `[]`       | Endpoints to connect to, as `host:port` or as full urls. Replaces `host` and `port` when set. See [Connecting to a cluster](#connecting-to-a-cluster).                                       |
| `timeout`              | 1          | Number of seconds the client will wait for a connection to be established. Confirming a connection takes a further round trip, so this has to cover that as well: a value too small to fit both makes every attempt fail, and with unlimited reconnect attempts the client keeps trying. |
| `token`                |            | Sets a authorization token for a connection.                                                                                                                                                |
| `tlsHandshakeFirst`    | `false`    | If true, the client performs the TLS handshake immediately after connecting, without waiting for the server’s INFO message.                                                                 |
| `tlsKeyFile`           |            | TLS 1.2 Client key file path.                                                                                                                                                               |
| `tlsCertFile`          |            | TLS 1.2 Client certificate file path.                                                                                                                                                       |
| `tlsCaFile`            |            | TLS 1.2 CA certificate filepath.                                                                                                                                                            |
| `user`                 |            | Sets the username for a connection.                                                                                                                                                         |
| `verbose`              | `false`    | Turns on `+OK` protocol acknowledgements.                                                                                                                                                   |
