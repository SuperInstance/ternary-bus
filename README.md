# ternary-bus

**Communication bus for inter-room messaging with ternary payloads.**

`ternary-bus` provides a publish/subscribe message bus where rooms in the fleet communicate via typed ternary messages. It supports topic-based routing, capacity-bounded queues with overflow handling, health metrics, and backpressure detection.

## Why It Matters

In a fleet with dozens of rooms, direct point-to-point communication creates $O(n^2)$ connections. A message bus reduces this to $O(n)$ — each room publishes to the bus and subscribes to relevant topics.

This crate provides:

1. **Topic-based pub/sub** — messages routed by topic string, with wildcard (empty set = all topics) support.
2. **Bounded queues** — each subscriber has a capacity; overflow drops oldest with tracking.
3. **Health metrics** — drop rate, queue depth, subscriber count for monitoring.
4. **Backpressure detection** — O(1) check if any subscriber queue exceeds threshold.
5. **Message routing** — explicit router for topic→subscriber mapping resolution.

## How It Works

### Publish/Subscribe Model

Each subscriber registers with:
- A set of topic filters (empty set = subscribe to all)
- A queue capacity $C$

When a message is published on topic $\tau$:

$$\text{delivers}(s, \tau) = \text{topics}(s) = \emptyset \vee \tau \in \text{topics}(s)$$

If the subscriber's queue is full ($|Q| = C$), the oldest message is dropped (FIFO eviction) and `dropped_count` increments.

**Publish complexity:** $O(S)$ for $S$ subscribers — each subscriber checked once.
**Receive complexity:** $O(1) — deque from front.

### Queue Overflow Handling

The bounded queue implements **drop-oldest** semantics:

```
if |Q| ≥ C:
    Q.pop_front()    // drop oldest
    dropped_count += 1
Q.push_back(msg)
```

This ensures the subscriber always gets the *most recent* messages, trading historical completeness for freshness. Alternative strategies (drop-newest, reject) can be layered on top.

### Bus Router

The `BusRouter` provides topic→subscriber resolution independent of delivery:

$$R(\tau) = G \cup \{s : (s, \tau) \in \text{routes}\}$$

where $G$ is the set of global subscribers. **Resolution complexity:** $O(|G| + |R_\tau|)$.

### Health Metrics

The `BusHealth` struct computes:

| Metric | Formula |
|--------|---------|
| Total published | Cumulative count |
| Dropped | Cumulative drops |
| Subscribers | Current count |
| Max queue depth | $\max_s |Q_s|$ |
| Drop rate | $\frac{\text{dropped}}{\text{published}}$ |

**Complexity:** $O(S)$ to scan all subscriber queues for max depth.

### Backpressure Detection

A simple threshold check:

$$\text{backpressure} = \exists s : |Q_s| \geq \text{threshold}$$

**Complexity:** $O(S)$ — early exit on first match.

## Quick Start

```toml
[dependencies]
ternary-bus = "0.1"
```

```rust
use ternary_bus::*;
use std::collections::HashSet;

let mut bus = Bus::new();

// Subscribe to specific topics
let topics: HashSet<String> = vec!["alerts".into()].into_iter().collect();
let sub_a = bus.subscribe("room_a", topics, 10);

// Subscribe to all topics
let sub_b = bus.subscribe("room_b", HashSet::new(), 10);

// Publish
bus.publish(Message::new("sensor", "alerts", vec![Trit::Pos]));
bus.publish(Message::new("sensor", "chatter", vec![Trit::Zero]));

// sub_a gets only "alerts"; sub_b gets both
assert_eq!(bus.pending(sub_a), 1);
assert_eq!(bus.pending(sub_b), 2);

// Receive
let msg = bus.receive(sub_a).unwrap();
assert_eq!(msg.topic, "alerts");

// Check health
let health = bus_health(&bus);
println!("Drop rate: {:.3}", health.drop_rate);
```

## API

| Type | Purpose |
|------|---------|
| `Trit` | Ternary payload value: Neg, Zero, Pos |
| `Message` | Typed message with topic, payload, source, timestamp |
| `Bus` | Pub/sub bus with bounded subscriber queues |
| `BusRouter` | Topic→subscriber routing table |
| `MessageQueue` | Standalone FIFO queue for offline consumers |
| `BusHealth` | Health metrics snapshot |
| `broadcast` / `multicast` | Convenience publish functions |
| `backpressure` | Threshold-based congestion check |

## Architecture Notes

The bus models **γ + η = C** through queue dynamics. Messages in flight represent growth energy (γ) — active work being communicated. Queue backlog represents entropy (η) — accumulated unprocessed information. When queues fill, backpressure signals that $\gamma + \eta$ has reached capacity $C$, and the system must either increase processing rate (raise $C$) or reduce publish rate (lower γ).

Drop-oldest eviction is an entropy-management strategy: discarding old messages reduces η at the cost of information loss. The drop rate metric directly quantifies how much information entropy is being "paid" to maintain system stability.

## References

- Eugster, P.Th. et al. *The Many Faces of Publish/Subscribe.* ACM Comput. Surv. 35 (2003). — Survey of pub/sub patterns.
- Kafka Documentation, *Log Compaction and Retention.* — On bounded queue strategies.
- Hinze, A. & Buchmann, A. *Principles of Distributed Messaging.* Springer, 2015.

## License

MIT
