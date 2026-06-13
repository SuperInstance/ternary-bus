# ternary-bus

**Communication bus for inter-room messaging with ternary payloads.**

`ternary-bus` provides a publish/subscribe message bus where rooms in the fleet communicate via typed ternary messages. It supports topic-based routing, capacity-bounded queues with overflow handling, health metrics, and backpressure detection.

## Why It Matters

In a distributed fleet of cooperating agents, rooms need to exchange information asynchronously. A message bus decouples producers from consumers: publishers emit messages without knowing who (if anyone) is listening, and subscribers receive only the topics they care about.

This crate implements the bus with ternary payloads — each message carries `Vec<Trit>` where `Trit ∈ {-1, 0, +1}` — making it the native communication substrate for ternary systems. Features include:

- **Topic routing:** Subscribers filter by topic or subscribe to all.
- **Bounded queues:** Each subscriber has a capacity-bounded FIFO queue with oldest-first drop policy on overflow.
- **Health metrics:** Drop rate, queue depth, subscriber count for observability.
- **Backpressure signaling:** Detect when any subscriber's queue is saturated.

## How It Works

### Publish/Subscribe Model

The bus maintains a set of subscribers, each with:

- A **topic filter** (set of topic strings; empty set = wildcard = receive all).
- A **bounded queue** (FIFO with capacity $C$).
- A **name** for identification.

When a message is published, the bus iterates all subscribers and enqueues a clone for each match:

$$\text{deliver}(m, s) \iff s.\text{topics} = \emptyset \;\vee\; m.\text{topic} \in s.\text{topics}$$

**Complexity:** $O(S)$ per publish for $S$ subscribers, with $O(1)$ per matched subscriber for enqueue.

### Queue Overflow Handling

When a subscriber's queue is full ($|Q| = C$), a new message causes the **oldest** message to be dropped:

$$Q.\text{push}(m): \quad \text{if } |Q| = C \text{ then } Q.\text{pop\_front}();\; \text{dropped} \mathrel{+}= 1$$

This **drop-oldest** policy ensures that the queue always contains the most recent messages, sacrificing old data for freshness. It is preferred for real-time systems where stale data is less valuable than current data.

Alternative policies not implemented but analyzable:

| Policy | Behavior | Use Case |
|--------|----------|----------|
| Drop-oldest (implemented) | Discard front, push new | Real-time telemetry |
| Drop-newest | Discard new message | Historical completeness |
| Block | Publisher waits | Strict delivery guarantees |

### Topic Router

The `BusRouter` provides a decoupled routing layer:

- **Topic routes:** Explicit `topic → subscriber_ids` mapping via `add_route(topic, id)`.
- **Global subscribers:** Receive all messages via `add_global(id)`.

Resolution unions topic-specific and global subscribers:

$$\text{recipients}(\text{topic}) = \text{routes}[\text{topic}] \cup \text{global}$$

**Complexity:** $O(|\text{routes}[\text{topic}]| + |\text{global}|)$ per resolution.

### Health Metrics

The `bus_health` function computes aggregate statistics:

$$\text{drop\_rate} = \frac{\text{dropped}}{\text{total\_published}}$$

Additional metrics:
- `total_published`: Lifetime message count.
- `dropped_count`: Total overflow drops across all subscribers.
- `subscriber_count`: Active subscribers.
- `max_queue_depth`: Deepest queue across all subscribers.

### Backpressure Detection

Simple threshold-based backpressure:

$$\text{backpressure}(B, \tau) = \exists\; s \in B : |Q_s| \geq \tau$$

Returns `true` if any subscriber's queue depth exceeds threshold $\tau$, signaling that the publisher should slow down.

**Complexity:** $O(S)$ — scan all subscribers.

### Message Queue

A standalone bounded FIFO for offline consumers:

$$Q.\text{enqueue}(m): \quad \text{if full, drop front and increment dropped}$$

Supports `enqueue`, `dequeue`, `len`, `is_empty`, `dropped_count`.

## Quick Start

```toml
[dependencies]
ternary-bus = "0.1"
```

```rust
use ternary_bus::{Bus, Message, Trit, BusRouter, bus_health, backpressure, broadcast, multicast};
use std::collections::HashSet;

let mut bus = Bus::new();

// Subscribe with topic filter
let topics: HashSet<String> = ["alerts".into()].into_iter().collect();
let sub_alerts = bus.subscribe("watcher", topics, 100);

// Subscribe to all topics (empty set = wildcard)
let sub_all = bus.subscribe("logger", HashSet::new(), 1000);

// Publish messages
bus.publish(Message::new("sensor-1", "alerts", vec![Trit::Neg, Trit::Neg]));
bus.publish(Message::new("sensor-2", "telemetry", vec![Trit::Pos]));

// Receive
let msg = bus.receive(sub_alerts).unwrap();
assert_eq!(msg.topic, "alerts");
assert_eq!(bus.pending(sub_alerts), 0);

// Check health
let health = bus_health(&bus);
println!("Drop rate: {:.4}", health.drop_rate);

// Backpressure check
let saturated = backpressure(&bus, 50);
println!("Backpressure: {}", saturated);

// Convenience: broadcast and multicast
broadcast(&mut bus, "system", vec![Trit::Zero]);
multicast(&mut bus, "sensor-1", "alerts", vec![Trit::Pos]);
```

## API

| Type/Function | Purpose | Complexity |
|---------------|---------|------------|
| `Trit` | The $\{-1, 0, +1\}$ payload value | $O(1)$ |
| `Message` | Typed bus message with topic, payload, source, timestamp | $O(1)$ construction |
| `Bus` | Core pub/sub message bus | $O(S)$ publish, $O(1)$ receive |
| `Bus::subscribe()` | Register subscriber with topic filter and capacity | $O(1)$ |
| `Bus::publish()` | Deliver message to matching subscribers | $O(S)$ |
| `Bus::receive()` | Non-blocking dequeue for a subscriber | $O(1)$ |
| `BusRouter` | Decoupled topic routing | $O(|\text{recipients}|)$ resolve |
| `MessageQueue` | Standalone bounded FIFO | $O(1)$ enqueue/dequeue |
| `bus_health()` | Aggregate health metrics | $O(S)$ |
| `backpressure()` | Threshold-based saturation check | $O(S)$ |
| `broadcast()` / `multicast()` | Convenience publish helpers | $O(S)$ |

## Architecture Notes

The message bus is the **communication artery** of the SuperInstance conservation law **γ + η = C**. Messages carry $\gamma$ (structured information between rooms) through the bus channel. Queue depth represents $\eta$ — information buffered but not yet consumed.

When queue depth approaches capacity ($\eta \to C$), the drop-oldest policy converts old $\gamma$ into $\eta$ (entropy increases as data is lost). The conservation bound is maintained: total information in the system (delivered + queued + dropped) equals the total published.

The backpressure signal is the bus's manifestation of the conservation law: when $\eta$ exceeds threshold $\tau$, the system signals producers to reduce $\gamma$ input, restoring the equilibrium $\gamma + \eta \approx C$.

Health metrics provide observability into the $\gamma$/$\eta$ balance: a high drop rate signals chronic $\eta > C$ conditions (the bus cannot drain fast enough), requiring architectural intervention (more subscribers, larger queues, or rate limiting).

## References

- Hohpe, G. & Woolf, B. *Enterprise Integration Patterns.* Addison-Wesley, 2003. — Publish/Subscribe channel, message router patterns.
- Eugster, P.T. et al. *The Many Faces of Publish/Subscribe.* ACM Computing Surveys 35(2), 2003. — Survey of pub/sub variants.
- Banks, J. et al. *Discrete-Event System Simulation.* 5th ed., Pearson, 2010. — Queueing theory and overflow policies.
- Jain, R. *The Art of Computer Systems Performance Analysis.* Wiley, 1991. — Queue depth analysis and backpressure.

## License

MIT
