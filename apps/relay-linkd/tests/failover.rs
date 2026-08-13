use std::time::{Duration, Instant};

use relay_core::model::{LinkHealth, PathId};
use relay_linkd::server::ServerState;

#[test]
fn server_keeps_active_path_then_fails_over_to_newest_fresh_path() {
    let now = Instant::now();
    let mut state = ServerState::new(Duration::from_secs(3), Duration::from_secs(3));
    state.register_relay("relay-a", now);
    state.register_relay("relay-b", now);
    state.register_link("relay-a", "link-1", "udp_unicast");
    state.register_link("relay-b", "link-1", "udp_unicast");

    state.set_relay_status("relay-a", true);
    state.set_relay_status("relay-b", true);
    state.set_link_health(
        "relay-a",
        "link-1",
        LinkHealth {
            adapter_ready: true,
            last_rx_at: Some(now - Duration::from_millis(200)),
            ..LinkHealth::default()
        },
    );
    state.set_link_health(
        "relay-b",
        "link-1",
        LinkHealth {
            adapter_ready: true,
            last_rx_at: Some(now - Duration::from_millis(50)),
            ..LinkHealth::default()
        },
    );

    assert_eq!(
        state.refresh_active_path(now, "initial"),
        Some(PathId::new("relay-b", "link-1"))
    );
    assert_eq!(
        state.refresh_active_path(now, "new data on active"),
        Some(PathId::new("relay-b", "link-1"))
    );

    let later = now + Duration::from_secs(4);
    state.set_link_health(
        "relay-a",
        "link-1",
        LinkHealth {
            adapter_ready: true,
            last_rx_at: Some(later - Duration::from_millis(100)),
            ..LinkHealth::default()
        },
    );
    state.heartbeat("relay-a", later);
    assert_eq!(
        state.refresh_active_path(later, "active stale"),
        Some(PathId::new("relay-a", "link-1"))
    );
}

#[test]
fn server_returns_no_control_target_when_every_path_is_stale() {
    let now = Instant::now();
    let mut state = ServerState::new(Duration::from_secs(3), Duration::from_secs(3));
    state.register_relay("relay-a", now - Duration::from_secs(10));
    state.register_link("relay-a", "link-1", "serial");
    state.set_relay_status("relay-a", true);
    state.set_link_health(
        "relay-a",
        "link-1",
        LinkHealth {
            adapter_ready: true,
            last_rx_at: Some(now - Duration::from_secs(10)),
            ..LinkHealth::default()
        },
    );

    assert_eq!(state.refresh_active_path(now, "all stale"), None);
    assert_eq!(state.control_target(), None);
}

#[test]
fn server_drops_aircraft_data_before_link_announcement() {
    let now = Instant::now();
    let mut state = ServerState::new(Duration::from_secs(3), Duration::from_secs(3));
    state.register_relay("relay-a", now);

    assert_eq!(
        state.record_aircraft_data("relay-a", "unannounced", now, 12),
        None
    );
    assert!(state.snapshot().relays[0].links.is_empty());
}

#[test]
fn reconnect_resets_link_freshness_until_new_aircraft_data_arrives() {
    let now = Instant::now();
    let mut state = ServerState::new(Duration::from_secs(3), Duration::from_secs(3));
    state.register_relay("relay-a", now);
    state.register_link("relay-a", "link-1", "udp_unicast");
    state.set_link_health(
        "relay-a",
        "link-1",
        LinkHealth {
            adapter_ready: true,
            last_rx_at: Some(now),
            ..LinkHealth::default()
        },
    );
    assert_eq!(
        state.refresh_active_path(now, "initial"),
        Some(PathId::new("relay-a", "link-1"))
    );

    state.register_relay("relay-a", now + Duration::from_millis(10));
    assert_eq!(state.refresh_active_path(now, "reconnect"), None);
    assert!(!state.snapshot().relays[0].links[0].adapter_ready);
}
