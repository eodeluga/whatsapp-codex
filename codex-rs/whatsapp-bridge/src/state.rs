//! Durable, bounded bridge state.

use crate::attachment::InboundAttachment;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use thiserror::Error;

pub const STATE_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BridgeState {
    pub schema_version: u32,
    pub binding: Option<ThreadBinding>,
    #[serde(default)]
    pub orphaned_thread_id: Option<String>,
    pub active_turn: Option<ActiveTurn>,
    #[serde(default)]
    pub queued_prompts: Vec<QueuedPrompt>,
    #[serde(default)]
    pub pending_steers: Vec<PendingSteer>,
    #[serde(default)]
    pub outbox: Vec<OutboundMessage>,
    #[serde(default)]
    pub processed_events: BTreeMap<String, u64>,
    #[serde(default)]
    pub outbound_message_ids: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadBinding {
    pub self_chat_id: String,
    pub codex_thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActiveTurn {
    pub inbound_message_id: String,
    #[serde(default)]
    pub thread_id: String,
    pub codex_turn_id: String,
    pub working_output_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueuedPrompt {
    pub idempotency_key: String,
    pub message_id: String,
    pub body: String,
    #[serde(default)]
    pub attachment: Option<InboundAttachment>,
    pub accepted_at: u64,
    #[serde(default)]
    pub submission_uncertain: bool,
    #[serde(default)]
    pub failure_notified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingSteer {
    pub idempotency_key: String,
    pub message_id: String,
    pub body: String,
    #[serde(default)]
    pub attachment: Option<InboundAttachment>,
    pub thread_id: String,
    pub expected_turn_id: String,
    pub accepted_at: u64,
    #[serde(default)]
    pub submission_uncertain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutboundMessage {
    pub response_id: String,
    pub chat_id: String,
    pub body: String,
    pub attempts: u32,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("unsupported bridge state schema")]
    UnsupportedSchema,
    #[error("failed to read bridge state")]
    Read,
    #[error("failed to parse bridge state")]
    Parse,
    #[error("failed to persist bridge state")]
    Write,
}

impl BridgeState {
    pub fn empty() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            ..Self::default()
        }
    }

    pub fn load(path: &Path) -> Result<Self, StateError> {
        match std::fs::read(path) {
            Ok(bytes) => {
                set_path_private_permissions(path).map_err(|_| StateError::Read)?;
                let mut state: Self =
                    serde_json::from_slice(&bytes).map_err(|_| StateError::Parse)?;
                match state.schema_version {
                    STATE_SCHEMA_VERSION => Ok(state),
                    2 => {
                        state.schema_version = STATE_SCHEMA_VERSION;
                        Ok(state)
                    }
                    _ => Err(StateError::UnsupportedSchema),
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(_) => Err(StateError::Read),
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), StateError> {
        let parent = path.parent().ok_or(StateError::Write)?;
        std::fs::create_dir_all(parent).map_err(|_| StateError::Write)?;
        let bytes = serde_json::to_vec_pretty(self).map_err(|_| StateError::Write)?;
        let temporary = path.with_extension("json.tmp");
        write_synced(&temporary, &bytes).map_err(|_| StateError::Write)?;
        std::fs::rename(&temporary, path).map_err(|_| StateError::Write)?;
        sync_directory(parent).map_err(|_| StateError::Write)
    }

    pub fn mark_processed(&mut self, key: String, timestamp: u64) {
        self.processed_events.insert(key, timestamp);
    }

    pub fn was_processed(&self, key: &str) -> bool {
        self.processed_events.contains_key(key)
    }

    pub fn was_sent_by_bridge(&self, message_id: &str) -> bool {
        self.outbound_message_ids.contains_key(message_id)
    }

    pub fn mark_outbound(&mut self, message_id: String, timestamp: u64) {
        self.outbound_message_ids.insert(message_id, timestamp);
    }

    pub fn prune(&mut self, now: u64, ttl_hours: u64, capacity: usize) {
        let minimum = now.saturating_sub(ttl_hours.saturating_mul(60 * 60));
        prune_records(&mut self.processed_events, minimum, capacity);
        prune_records(&mut self.outbound_message_ids, minimum, capacity);
    }
}

fn prune_records(records: &mut BTreeMap<String, u64>, minimum: u64, capacity: usize) {
    records.retain(|_, timestamp| *timestamp >= minimum);
    while records.len() > capacity {
        let Some(key) = records
            .iter()
            .min_by_key(|(_, timestamp)| *timestamp)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        records.remove(&key);
    }
}

#[cfg(unix)]
fn set_path_private_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_path_private_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn write_synced(path: &Path, contents: &[u8]) -> io::Result<()> {
    use std::io::Write;

    let mut file = std::fs::File::create(path)?;
    set_private_permissions(&file)?;
    file.write_all(contents)?;
    file.sync_all()
}

#[cfg(unix)]
fn set_private_permissions(file: &std::fs::File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_permissions(_file: &std::fs::File) -> io::Result<()> {
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
