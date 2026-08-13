use std::time::{Duration, Instant};

use relay_core::model::{LinkHealth, PathId};
use relay_core::path_selector::{PathCandidate, PathSelector};

fn candidate(relay: &str, link: &str, rx_age_ms: u64) -> PathCandidate {
    PathCandidate {
        path: PathId::new(relay, link),
        connected: true,
        heartbeat_at: Some(Instant::now()),
        link: LinkHealth {
            adapter_ready: true,
            last_rx_at: Some(Instant::now() - Duration::from_millis(rx_age_ms)),
            ..LinkHealth::default()
        },
    }
}

#[test]
fn retains_current_path_while_it_is_eligible() {
    let now = Instant::now();
    let current = PathId::new("relay-a", "link-a");
    let selected = PathSelector::choose(
        now,
        Some(&current),
        &[
            candidate("relay-a", "link-a", 400),
            candidate("relay-b", "link-b", 10),
        ],
        Duration::from_secs(3),
    );

    assert_eq!(selected, Some(current));
}

#[test]
fn chooses_newest_fresh_path_after_current_fails() {
    let now = Instant::now();
    let current = PathId::new("relay-a", "link-a");
    let selected = PathSelector::choose(
        now,
        Some(&current),
        &[
            PathCandidate {
                link: LinkHealth {
                    last_rx_at: Some(now - Duration::from_secs(4)),
                    ..LinkHealth::default()
                },
                ..candidate("relay-a", "link-a", 400)
            },
            candidate("relay-b", "link-b", 100),
            candidate("relay-c", "link-c", 200),
        ],
        Duration::from_secs(3),
    );

    assert_eq!(selected, Some(PathId::new("relay-b", "link-b")));
}

#[test]
fn returns_none_when_adapter_is_ready_but_aircraft_data_is_stale() {
    let selected = PathSelector::choose(
        Instant::now(),
        None,
        &[candidate("relay-a", "link-a", 4_000)],
        Duration::from_secs(3),
    );

    assert_eq!(selected, None);
}

#[test]
fn tie_breaks_by_path_id_without_storing_control_data() {
    let now = Instant::now();
    let rx_at = now - Duration::from_millis(100);
    let mut first = candidate("relay-b", "link-b", 100);
    first.link.last_rx_at = Some(rx_at);
    let mut second = candidate("relay-a", "link-a", 100);
    second.link.last_rx_at = Some(rx_at);

    assert_eq!(
        PathSelector::choose(now, None, &[first, second], Duration::from_secs(3)),
        Some(PathId::new("relay-a", "link-a"))
    );
    let selector = PathSelector;
    assert!(!selector.has_pending_control());
}
