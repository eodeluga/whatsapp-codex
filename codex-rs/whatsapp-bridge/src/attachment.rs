//! Native WhatsApp attachment data passed through to Codex.

use serde::Deserialize;
use serde::Serialize;

/// An attachment received from WhatsApp.
///
/// Images are represented as data URLs at the Codex boundary. Audio and other
/// media are represented only as rejected attachment kinds because the
/// currently available Codex models do not accept audio input and the normal
/// turn protocol has no generic file or video input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum InboundAttachment {
    Image {
        mime_type: String,
        data_base64: String,
    },
    Audio {
        #[serde(default)]
        mime_type: Option<String>,
    },
    Unsupported {
        kind: String,
    },
}
