<?php

declare(strict_types=1);

namespace Basis\Nats\Message;

use InvalidArgumentException;

abstract class Prototype
{
    abstract public function render(): string;

    public static function create(string $data): self
    {
        // @phan-suppress-next-line PhanTypeInstantiateAbstractStatic
        return new static(Payload::parse($data));
    }

    public function __construct(array|Payload|null $payload = null)
    {
        if ($payload === null) {
            return;
        }

        $values = is_array($payload) ? $payload : $payload->getValues();
        if ($values === null) {
            // A non-empty body that does not decode means the line was damaged, most
            // often a protocol line truncated mid-json. Returning silently here leaves
            // every property uninitialized while the message still passes an
            // instanceof check, so the failure surfaces much later and far away.
            if ($payload instanceof Payload && !$payload->isEmpty()) {
                throw new InvalidArgumentException(
                    'Invalid payload for message ' . get_class($this) . ': ' . $payload->body
                );
            }
            return;
        }

        foreach ($values as $k => $v) {
            if (!property_exists($this, $k)) {
                if ($this->ignoresUnknownProperties()) {
                    continue;
                }

                throw new InvalidArgumentException("Invalid property $k for message " . get_class($this));
            }
            $this->$k = $v;
        }
    }

    /**
     * Whether a field this message does not declare is ignored rather than
     * rejected. Strict by default, since the client authors those messages itself
     * and a stray field means a mistake.
     */
    protected function ignoresUnknownProperties(): bool
    {
        return false;
    }
}
