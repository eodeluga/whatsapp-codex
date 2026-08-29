//! Building blocks for the self-hosted WhatsApp-to-Codex bridge.

pub mod codex;
pub mod commands;
pub mod coordinator;
pub mod health;
mod notifications;
pub mod output;
pub mod state;
pub mod transport;
pub mod transport_webhook;

mod command_catalog;

pub use command_catalog::CommandCatalog;
pub use command_catalog::CommandCatalogError;
