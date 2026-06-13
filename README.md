# Ternary Bus

**Ternary Bus** is a pub/sub communication bus for inter-room messaging with ternary payloads — carrying {-1, 0, +1} trit vectors between fleet rooms with topic-based subscription, bounded queues, and delivery statistics.

## Why It Matters

Decoupled communication is the backbone of any modular system. In a ternary fleet, rooms need to exchange ternary state vectors (agent decisions, sensor readings, strategy updates) without tight coupling. Ternary Bus provides topic-based pub/sub where each message carries a payload of `Vec<Trit>` {-1, 0, +1}, enabling rooms to broadcast state changes, request coordination, or report anomalies. The bounded queue per subscriber prevents slow consumers from blocking fast producers.

## How It Works

### Pub/Sub Model

```
Publisher → Bus → [Subscriber 1, Subscriber 2, ...]

bus.publish(topic, payload):
    for each subscriber subscribed to topic:
        if subscriber.queue has space:
            enqueue(message)
        else:
            dropped_count += 1
```

Publish cost: **O(S)** where S = subscribers matching topic. Each subscriber has a bounded `VecDeque` (configurable capacity, default 256).

### Message Structure

```rust
Message {
    topic: String,         // e.g. "state.delta", "alert.thermal"
    payload: Vec<Trit>,    // {-1, 0, +1} ternary data
    timestamp: Instant,    // send time
    source: String,        // originating room
}
```

Message creation: **O(N)** where N = payload length (Vec clone).

### Subscriber Lifecycle

```
subscribe(name, topics) → SubscriberId
  - Creates bounded queue
  - Registers topic filters

unsubscribe(id)
  - Removes queue and topic registrations

poll(id) → Option<Message>
  - Non-blocking dequeue
```

Subscribe/unsubscribe: **O(1)** (HashMap insert/remove). Poll: **O(1)** (VecDeque pop_front).

### Statistics

```
total_published: usize     // lifetime publish count
dropped_count: usize       // messages dropped (queue full)
per_subscriber: { received, dropped, queue_depth }
```

All counters: **O(1)** to read.

## Quick Start

```rust
use ternary_bus::{Bus, Trit};

let mut bus = Bus::new();
let mut rx = bus.subscribe("alpha", &["state.delta", "alert"]);

bus.publish("alpha", "state.delta", vec![Trit::Pos, Trit::Zero, Trit::Neg]);

if let Ok(msg) = rx.poll() {
    println!("Topic: {}, payload: {:?}", msg.topic, msg.payload);
}
```

## API

| Type | Description |
|------|-------------|
| `Bus` | Pub/sub bus with topic routing |
| `Message` | topic, payload (Vec<Trit>), timestamp, source |
| `Trit` | Neg (-1), Zero (0), Pos (+1) |
| `Subscriber` | Bounded queue with topic filters |

Key methods: `subscribe()`, `publish()`, `poll()`, `unsubscribe()`.

## Architecture Notes

Ternary Bus provides the messaging backbone for inter-room communication in SuperInstance. In γ + η = C, published payloads carry both γ (+1 growth signals) and η (-1 avoidance signals), with the neutral 0 state representing "no change." The conservation law applies: the bus preserves the sum of all trits in transit. Integrates with `ternary-channel` for point-to-point connections and `ternary-command` for structured command dispatch.

See [ARCHITECTURE.md](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md) for fleet messaging architecture.


### Bus Router and Message Queue

For complex routing topologies, the `BusRouter` provides explicit route management:

```
add_route(topic, subscriber_id)   → route specific topic
add_global(subscriber_id)          → receive all topics
resolve(topic) → HashSet<SubscriberId>
```

Route resolution: **O(G + R)** where G = global subscribers, R = topic-specific. The `MessageQueue` provides offline consumer buffering with `enqueue(msg)` and `dequeue()` — both **O(1)** amortized, enabling catch-up for temporarily disconnected subscribers.

### Backpressure Detection

```
backpressure(bus, threshold) → bool:
    any subscriber.queue.len() >= threshold    — O(S) scan

bus_health(bus) → BusHealth {
    total_published, dropped_count,
    subscriber_count, max_queue_depth,
    drop_rate = dropped / total_published
}
```

Drop rate > 5% suggests a subscriber can't keep up — scale out or increase queue capacity.

## References

1. Hohpe, G. & Woolf, B. (2003). *Enterprise Integration Patterns*. Addison-Wesley.
2. Eugster, P. T. et al. (2003). "The Many Faces of Publish/Subscribe." *ACM Computing Surveys*, 35(2), 114–131.
3. Kreps, J. (2014). "Questioning the Lambda Architecture." *O'Reilly Radar*.

## License

MIT
