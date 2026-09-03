//! Provider-neutral messaging primitives for remote Codex surfaces.
//!
//! This crate deliberately knows nothing about WhatsApp, HTTP, credentials, or
//! provider SDKs. It owns the values and durable scheduling state shared by
//! provider adapters.

mod delivery;
mod model;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

pub use delivery::DeliveryJournal;
pub use delivery::DeliveryRecord;
pub use delivery::DeliveryState;
pub use delivery::DeliveryStore;
pub use delivery::DeliveryWorker;
pub use delivery::DeliveryWorkerCommand;
pub use delivery::DeliveryWorkerEvent;
pub use delivery::DeliveryWorkerHandle;
pub use delivery::FileDeliveryStore;
pub use model::DeliveryIntent;
pub use model::InboundEnvelope;
pub use model::ProviderAdapter;
pub use model::ProviderAttachment;
pub use model::ProviderCapabilities;
pub use model::ProviderConversationId;
pub use model::ProviderDeliveryId;
pub use model::ProviderError;
pub use model::ProviderMessageId;
pub use model::ProviderStatus;
pub use model::segment_text;
