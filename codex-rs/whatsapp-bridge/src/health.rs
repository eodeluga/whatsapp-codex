//! Coherent readiness state shared by the coordinator and HTTP health route.

use std::sync::RwLock;

/// One coherent view of bridge and dependency health.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeReadinessSnapshot {
    pub ready: bool,
    pub state_healthy: bool,
    pub app_server_connected: bool,
    pub transport_healthy: bool,
}

/// Lock-protected readiness shared between the coordinator and health server.
pub struct BridgeReadiness {
    snapshot: RwLock<BridgeReadinessSnapshot>,
}

impl BridgeReadiness {
    /// Creates readiness state from the coordinator's startup result.
    pub fn new(snapshot: BridgeReadinessSnapshot) -> Self {
        Self {
            snapshot: RwLock::new(snapshot),
        }
    }

    /// Returns a coherent component snapshot for diagnostics.
    pub fn snapshot(&self) -> BridgeReadinessSnapshot {
        *self
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn update(&self, snapshot: BridgeReadinessSnapshot) {
        *self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot;
    }

    pub(crate) fn mark_stopped(&self) {
        let mut snapshot = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.ready = false;
        snapshot.app_server_connected = false;
    }
}
