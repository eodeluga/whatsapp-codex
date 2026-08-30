//! Native WhatsApp attachment data passed through to Codex.

use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use std::path::PathBuf;

/// An attachment received from WhatsApp.
///
/// Images are represented as data URLs at the Codex boundary. Documents are
/// stored in a temporary directory shared with Codex so the normal local-input
/// path can consume them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum InboundAttachment {
    Image {
        mime_type: String,
        data_base64: String,
    },
    Document {
        path: PathBuf,
        file_name: Option<String>,
        mime_type: Option<String>,
    },
    Unsupported {
        kind: String,
    },
}

impl InboundAttachment {
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Document { path, .. } => Some(path),
            Self::Image { .. } | Self::Unsupported { .. } => None,
        }
    }
}
