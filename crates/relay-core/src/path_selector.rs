use std::time::{Duration, Instant};

use crate::model::{LinkHealth, PathId};

#[derive(Debug, Clone)]
pub struct PathCandidate {
    pub path: PathId,
    pub connected: bool,
    pub heartbeat_at: Option<Instant>,
    pub link: LinkHealth,
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
        if let Some(current) = current
            && candidates
                .iter()
                .find(|candidate| &candidate.path == current)
                .is_some_and(|candidate| {
                    candidate.is_eligible(now, heartbeat_timeout, data_timeout)
                })
        {
            return Some(current.clone());
        }

        candidates
            .iter()
            .filter(|candidate| candidate.is_eligible(now, heartbeat_timeout, data_timeout))
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
