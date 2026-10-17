use super::*;

#[test]
fn a_zero_rate_is_the_disabled_spelling() {
    assert!(KeyedRateLimit::<u32>::per_second(0, DEFAULT_CAPACITY).is_none());
    assert!(KeyedRateLimit::<u32>::per_minute(0, DEFAULT_CAPACITY).is_none());
    assert!(KeyedRateLimit::<u32>::per_second(5, 0).is_none(), "a zero table tracks nothing");
}

#[test]
fn the_burst_equals_the_rate_and_then_refuses() {
    let limiter = KeyedRateLimit::per_second(3, DEFAULT_CAPACITY).expect("enabled");

    for attempt in 1..=3 {
        assert!(limiter.allow(7u32), "attempt {attempt} is inside the burst");
    }
    assert!(!limiter.allow(7u32), "the fourth immediate event exceeds the burst");
}

#[test]
fn keys_do_not_share_a_budget() {
    let limiter = KeyedRateLimit::per_second(1, DEFAULT_CAPACITY).expect("enabled");

    assert!(limiter.allow(1u32));
    assert!(!limiter.allow(1u32));
    assert!(limiter.allow(2u32), "a different key has its own bucket");
}

#[test]
fn a_forgotten_key_starts_over_and_leaves_the_table() {
    let limiter = KeyedRateLimit::per_second(1, DEFAULT_CAPACITY).expect("enabled");

    assert!(limiter.allow(1u32));
    assert_eq!(limiter.tracked(), 1);

    limiter.forget(&1u32);
    assert_eq!(limiter.tracked(), 0);
    assert!(limiter.allow(1u32), "a new session is not charged for the old one");
}

#[test]
fn the_table_is_bounded_and_denies_rather_than_growing() {
    // Capacity 2, and every tracked key is drained, so nothing is
    // sweepable: the third key is refused instead of admitted.
    let limiter = KeyedRateLimit::per_second(1, 2).expect("enabled");

    assert!(limiter.allow(1u32));
    assert!(limiter.allow(2u32));
    assert!(!limiter.allow(3u32), "a full table of live buckets fails closed");
    assert_eq!(limiter.tracked(), 2, "and it does not grow past the ceiling");
}

#[test]
fn idle_keys_are_swept_before_the_table_fails_closed() {
    let limiter = KeyedRateLimit::per_second(4, 2).expect("enabled");

    // Two keys with untouched (full) buckets: recorded, but idle.
    limiter.buckets.insert(1u32, TokenBucketState { tokens: 4.0, last_refill: 0.0 });
    limiter.buckets.insert(2u32, TokenBucketState { tokens: 4.0, last_refill: 0.0 });
    assert_eq!(limiter.tracked(), 2);

    assert!(limiter.allow(3u32), "idle buckets carry no state worth keeping");
    assert_eq!(limiter.tracked(), 1, "the swept entries are gone");
}

#[test]
fn tokens_refill_over_time() {
    // 600/min = 10/s: one token is back well inside a 200 ms wait.
    let limiter = KeyedRateLimit::per_minute(600, DEFAULT_CAPACITY).expect("enabled");
    for _ in 0..600 {
        limiter.allow(1u32);
    }
    assert!(!limiter.allow(1u32), "the burst is spent");

    std::thread::sleep(std::time::Duration::from_millis(250));
    assert!(limiter.allow(1u32), "the bucket refilled");
}
