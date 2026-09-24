use std::time::{Duration, Instant};

use crate::model::{LinkHealth, PathId};

#[derive(Debug, Clone)]
pub struct PathCandidate {
    pub path: PathId,
    pub connected: bool,
    pub heartbeat_at: Option<Instant>,
    pub link: LinkHealth,
    pub priority: u32,
}

impl PathCandidate {
    pub fn is_eligible(
        &self,
        now: Instant,
        heartbeat_timeout: Duration,
        data_timeout: Duration,
    ) -> bool {
        self.connected
            && is_fresh(self.heartbeat_at, now, heartbeat_timeout)
            && self.link.adapter_ready
            && is_fresh(self.link.last_rx_at, now, data_timeout)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PathSelector;

impl PathSelector {
    pub fn choose(
        now: Instant,
        current: Option<&PathId>,
        candidates: &[PathCandidate],
        timeout: Duration,
    ) -> Option<PathId> {
        Self::choose_with_timeouts(now, current, candidates, timeout, timeout)
    }

    pub fn choose_with_timeouts(
        now: Instant,
        current: Option<&PathId>,
        candidates: &[PathCandidate],
        heartbeat_timeout: Duration,
        data_timeout: Duration,
    ) -> Option<PathId> {
        let eligible: Vec<&PathCandidate> = candidates
            .iter()
            .filter(|c| c.is_eligible(now, heartbeat_timeout, data_timeout))
            .collect();

        if eligible.is_empty() {
            return None;
        }

        let max_priority = eligible.iter().map(|c| c.priority).max().unwrap_or(0);

        if let Some(current) = current
            && let Some(current_candidate) = eligible.iter().find(|c| &c.path == current)
        {
            if current_candidate.priority >= max_priority {
                return Some(current.clone());
            }
        }

        eligible
            .into_iter()
            .filter(|c| c.priority == max_priority)
            .max_by(|left, right| {
                left.link
                    .last_rx_at
                    .cmp(&right.link.last_rx_at)
                    .then_with(|| right.path.cmp(&left.path))
            })
            .map(|candidate| candidate.path.clone())
    }

    pub fn has_pending_control(&self) -> bool {
        false
    }
}

fn is_fresh(at: Option<Instant>, now: Instant, timeout: Duration) -> bool {
    at.is_some_and(|at| now.checked_duration_since(at).unwrap_or_default() <= timeout)
}
