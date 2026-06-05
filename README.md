# ternary-bus

**Pub/sub message bus with ternary payloads. Inter-room communication for multi-agent systems.**

When agents live in rooms — each with their own state, rhythm, and attention — they need a way to talk to each other without coupling. The bus is that layer. Messages carry ternary payloads (`-1`, `0`, `+1`), topics provide filtering, and each subscriber gets their own queue.

The design is deliberately simple: no serialization, no networking, no persistence. Just in-process message routing with backpressure awareness. If your queue is full, old messages drop. If nobody's listening, the message vanishes. Like a real room — say it when people are there, or it's gone.

## What's Inside

- **`Bus`** — the central message router. Pub/sub with topic filtering
- **`Message`** — timestamped envelope with source, topic, and `Vec<Trit>` payload
- **`Trit`** — the ternary payload value: `Neg`, `Zero`, `Pos`
- **`SubscriberId`** — opaque handle for managing subscriptions
- **Topic filtering** — subscribe to specific topics or all topics (empty filter)
- **Queue capacity** — each subscriber gets a bounded queue with configurable overflow behavior
- **Metrics** — dropped count, total published, per-subscriber stats

## Quick Example

```rust
use ternary_bus::*;
use std::collections::HashSet;

let mut bus = Bus::new();

// Subscribe a listener to "energy" topic
let listener = bus.subscribe("agent-1", HashSet::from(["energy".into()]), 100);

// Publish a ternary message
bus.publish(Message::new("source", "energy", vec![Trit::Pos, Trit::Pos, Trit::Neg]));

// Receive it
while let Some(msg) = bus.receive(listener) {
    println!("{}: {:?}", msg.source, msg.payload);
    // "source: [Pos, Pos, Neg]"
}

// Check stats
let stats = bus.stats();
println!("Published: {}, Dropped: {}", stats.total_published, stats.dropped_count);
```

## Why a Ternary Bus?

**Not everything needs Protobuf.** When agents communicate gut-feel signals — *positive/negative/neutral*, *attack/defend/hold*, *interested/bored/confused* — a ternary payload is exactly the right resolution. No over-engineering, no schema negotiation, just three-valued messages routed by topic.

**Use cases:**
- **Multi-agent coordination** — lightweight signaling between autonomous agents
- **Game engines** — event routing with simple payloads (damage/heal/neutral)
- **Sensor networks** — propagate threshold-crossing events
- **Chat/messaging backends** — reaction signals (upvote/downvote/neutral)
- **Process supervision** — health signaling between supervised services

## See Also
- **ternary-channel** — related
- **ternary-event** — related
- **ternary-protocol** — related
- **ternary-room** — related
- **ternary-streaming** — related

## Install

```bash
cargo add ternary-bus
```

## License

MIT
