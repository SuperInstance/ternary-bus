//! # ternary-bus
//! Communication bus for inter-room messaging with ternary payloads.

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// A ternary payload value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Trit {
    Neg = -1,
    Zero = 0,
    Pos = 1,
}

/// A typed message on the bus.
#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub payload: Vec<Trit>,
    pub timestamp: Instant,
    pub source: String,
}

impl Message {
    pub fn new(source: &str, topic: &str, payload: Vec<Trit>) -> Self {
        Self {
            topic: topic.to_string(),
            payload,
            timestamp: Instant::now(),
            source: source.to_string(),
        }
    }
}

/// Subscriber handle.
pub type SubscriberId = usize;

/// A pub/sub message bus.
#[derive(Debug)]
pub struct Bus {
    subscribers: HashMap<SubscriberId, Subscriber>,
    next_id: SubscriberId,
    message_log: Vec<Message>,
    dropped_count: usize,
    total_published: usize,
}

#[derive(Debug)]
struct Subscriber {
    id: SubscriberId,
    topics: HashSet<String>,
    queue: VecDeque<Message>,
    queue_capacity: usize,
    name: String,
}

impl Bus {
    pub fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            next_id: 0,
            message_log: Vec::new(),
            dropped_count: 0,
            total_published: 0,
        }
    }

    /// Subscribe to specific topics. Empty set = all topics.
    pub fn subscribe(&mut self, name: &str, topics: HashSet<String>, queue_capacity: usize) -> SubscriberId {
        let id = self.next_id;
        self.next_id += 1;
        self.subscribers.insert(id, Subscriber {
            id,
            topics,
            queue: VecDeque::new(),
            queue_capacity,
            name: name.to_string(),
        });
        id
    }

    /// Publish a message. Delivers to matching subscribers.
    pub fn publish(&mut self, msg: Message) {
        self.total_published += 1;
        self.message_log.push(msg.clone());
        for sub in self.subscribers.values_mut() {
            let matches = sub.topics.is_empty() || sub.topics.contains(&msg.topic);
            if matches {
                if sub.queue.len() >= sub.queue_capacity {
                    sub.queue.pop_front();
                    self.dropped_count += 1;
                }
                sub.queue.push_back(msg.clone());
            }
        }
    }

    /// Receive next message for a subscriber (non-blocking).
    pub fn receive(&mut self, subscriber_id: SubscriberId) -> Option<Message> {
        self.subscribers.get_mut(&subscriber_id)?.queue.pop_front()
    }

    /// Get number of pending messages for a subscriber.
    pub fn pending(&self, subscriber_id: SubscriberId) -> usize {
        self.subscribers.get(&subscriber_id).map(|s| s.queue.len()).unwrap_or(0)
    }
}

/// A bus router that directs messages by topic.
#[derive(Debug)]
pub struct BusRouter {
    routes: HashMap<String, HashSet<SubscriberId>>,
    global_subscribers: HashSet<SubscriberId>,
}

impl BusRouter {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            global_subscribers: HashSet::new(),
        }
    }

    /// Add a route: messages on `topic` go to `subscriber`.
    pub fn add_route(&mut self, topic: &str, subscriber_id: SubscriberId) {
        self.routes.entry(topic.to_string()).or_default().insert(subscriber_id);
    }

    /// Subscribe to all topics.
    pub fn add_global(&mut self, subscriber_id: SubscriberId) {
        self.global_subscribers.insert(subscriber_id);
    }

    /// Resolve which subscribers should receive a message.
    pub fn resolve(&self, topic: &str) -> HashSet<SubscriberId> {
        let mut recipients = self.global_subscribers.clone();
        if let Some(ids) = self.routes.get(topic) {
            recipients.extend(ids);
        }
        recipients
    }
}

/// Broadcast a message to all subscribers (convenience).
pub fn broadcast(bus: &mut Bus, source: &str, payload: Vec<Trit>) {
    let msg = Message::new(source, "", payload);
    bus.publish(msg);
}

/// Multicast a message to a specific topic.
pub fn multicast(bus: &mut Bus, source: &str, topic: &str, payload: Vec<Trit>) {
    let msg = Message::new(source, topic, payload);
    bus.publish(msg);
}

/// A message queue for offline consumers.
#[derive(Debug)]
pub struct MessageQueue {
    queue: VecDeque<Message>,
    capacity: usize,
    dropped: usize,
}

impl MessageQueue {
    pub fn new(capacity: usize) -> Self {
        Self { queue: VecDeque::new(), capacity, dropped: 0 }
    }

    pub fn enqueue(&mut self, msg: Message) {
        if self.queue.len() >= self.capacity {
            self.queue.pop_front();
            self.dropped += 1;
        }
        self.queue.push_back(msg);
    }

    pub fn dequeue(&mut self) -> Option<Message> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn dropped_count(&self) -> usize {
        self.dropped
    }
}

/// Bus health metrics.
#[derive(Debug, Clone)]
pub struct BusHealth {
    pub total_published: usize,
    pub dropped_count: usize,
    pub subscriber_count: usize,
    pub max_queue_depth: usize,
    pub drop_rate: f64,
}

/// Get bus health metrics.
pub fn bus_health(bus: &Bus) -> BusHealth {
    let max_depth = bus.subscribers.values().map(|s| s.queue.len()).max().unwrap_or(0);
    let drop_rate = if bus.total_published > 0 {
        bus.dropped_count as f64 / bus.total_published as f64
    } else {
        0.0
    };
    BusHealth {
        total_published: bus.total_published,
        dropped_count: bus.dropped_count,
        subscriber_count: bus.subscribers.len(),
        max_queue_depth: max_depth,
        drop_rate,
    }
}

/// Simple backpressure: returns true if any subscriber's queue is above threshold.
pub fn backpressure(bus: &Bus, threshold: usize) -> bool {
    bus.subscribers.values().any(|s| s.queue.len() >= threshold)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bus_subscribe_and_publish() {
        let mut bus = Bus::new();
        let sub = bus.subscribe("room_a", HashSet::new(), 10);
        bus.publish(Message::new("src", "topic1", vec![Trit::Pos]));
        assert_eq!(bus.pending(sub), 1);
    }

    #[test]
    fn test_bus_receive() {
        let mut bus = Bus::new();
        let sub = bus.subscribe("room_a", HashSet::new(), 10);
        bus.publish(Message::new("src", "topic1", vec![Trit::Neg]));
        let msg = bus.receive(sub).unwrap();
        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload, vec![Trit::Neg]);
    }

    #[test]
    fn test_bus_topic_filter() {
        let mut bus = Bus::new();
        let topics: HashSet<String> = vec!["alerts".into()].into_iter().collect();
        let sub = bus.subscribe("room_a", topics, 10);
        bus.publish(Message::new("src", "alerts", vec![Trit::Pos]));
        bus.publish(Message::new("src", "chatter", vec![Trit::Zero]));
        assert_eq!(bus.pending(sub), 1);
    }

    #[test]
    fn test_bus_queue_overflow_drops() {
        let mut bus = Bus::new();
        let sub = bus.subscribe("room_a", HashSet::new(), 2);
        bus.publish(Message::new("src", "t", vec![Trit::Pos]));
        bus.publish(Message::new("src", "t", vec![Trit::Zero]));
        bus.publish(Message::new("src", "t", vec![Trit::Neg]));
        assert_eq!(bus.pending(sub), 2);
        assert_eq!(bus.dropped_count, 1);
    }

    #[test]
    fn test_bus_no_subscribers() {
        let mut bus = Bus::new();
        bus.publish(Message::new("src", "t", vec![Trit::Pos]));
        assert_eq!(bus.total_published, 1);
    }

    #[test]
    fn test_router_basic() {
        let mut router = BusRouter::new();
        router.add_route("alerts", 1);
        router.add_route("alerts", 2);
        let recips = router.resolve("alerts");
        assert_eq!(recips.len(), 2);
    }

    #[test]
    fn test_router_global() {
        let mut router = BusRouter::new();
        router.add_global(0);
        router.add_route("alerts", 1);
        let recips = router.resolve("alerts");
        assert_eq!(recips.len(), 2);
        let other = router.resolve("other");
        assert_eq!(other.len(), 1); // only global
    }

    #[test]
    fn test_broadcast() {
        let mut bus = Bus::new();
        let sub = bus.subscribe("room", HashSet::new(), 10);
        broadcast(&mut bus, "src", vec![Trit::Pos, Trit::Neg]);
        assert_eq!(bus.pending(sub), 1);
    }

    #[test]
    fn test_multicast() {
        let mut bus = Bus::new();
        let topics: HashSet<String> = vec!["music".into()].into_iter().collect();
        let sub = bus.subscribe("room", topics, 10);
        multicast(&mut bus, "src", "music", vec![Trit::Pos]);
        assert_eq!(bus.pending(sub), 1);
    }

    #[test]
    fn test_message_queue_basic() {
        let mut mq = MessageQueue::new(5);
        mq.enqueue(Message::new("src", "t", vec![Trit::Pos]));
        assert_eq!(mq.len(), 1);
        let msg = mq.dequeue().unwrap();
        assert_eq!(msg.payload, vec![Trit::Pos]);
        assert!(mq.is_empty());
    }

    #[test]
    fn test_message_queue_overflow() {
        let mut mq = MessageQueue::new(2);
        mq.enqueue(Message::new("s", "t", vec![Trit::Pos]));
        mq.enqueue(Message::new("s", "t", vec![Trit::Zero]));
        mq.enqueue(Message::new("s", "t", vec![Trit::Neg]));
        assert_eq!(mq.dropped_count(), 1);
        assert_eq!(mq.len(), 2);
    }

    #[test]
    fn test_bus_health() {
        let mut bus = Bus::new();
        bus.subscribe("room", HashSet::new(), 10);
        bus.publish(Message::new("s", "t", vec![Trit::Pos]));
        let health = bus_health(&bus);
        assert_eq!(health.total_published, 1);
        assert_eq!(health.subscriber_count, 1);
        assert_eq!(health.drop_rate, 0.0);
    }

    #[test]
    fn test_backpressure() {
        let mut bus = Bus::new();
        bus.subscribe("room", HashSet::new(), 100);
        // queue is empty
        assert!(!backpressure(&bus, 5));
        // fill it up
        for _ in 0..10 {
            bus.publish(Message::new("s", "t", vec![Trit::Pos]));
        }
        assert!(backpressure(&bus, 5));
    }

    #[test]
    fn test_message_source() {
        let msg = Message::new("room_a", "topic", vec![Trit::Zero]);
        assert_eq!(msg.source, "room_a");
    }

    #[test]
    fn test_bus_health_drop_rate() {
        let mut bus = Bus::new();
        bus.subscribe("room", HashSet::new(), 1);
        bus.publish(Message::new("s", "t", vec![Trit::Pos]));
        bus.publish(Message::new("s", "t", vec![Trit::Zero]));
        let health = bus_health(&bus);
        assert!(health.drop_rate > 0.0);
    }
}
