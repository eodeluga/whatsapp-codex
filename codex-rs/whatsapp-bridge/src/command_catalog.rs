//! User-editable help and display configuration for the WhatsApp bridge.

use serde::Deserialize;
use serde::Serialize;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

const DEFAULT_COMMAND_CATALOG: &str = include_str!("../default-commands.json");
const COMMAND_CATALOG_FILE: &str = "commands.json";
const COMMAND_CATALOG_SCHEMA_VERSION: u32 = 1;
const MAX_CATALOG_BYTES: u64 = 64 * 1024;
const MAX_GROUPS: usize = 16;
const MAX_COMMANDS_PER_GROUP: usize = 128;
const MAX_FIELD_CHARS: usize = 512;
const MAX_RESPONSE_PREFIX_CHARS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandCatalog {
    schema_version: u32,
    response_prefix: String,
    help_heading: String,
    help_footer: String,
    groups: Vec<CommandGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommandGroup {
    heading: String,
    commands: Vec<CommandEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommandEntry {
    usage: String,
    description: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CommandCatalogError {
    #[error("failed to read WhatsApp command catalogue")]
    Read,
    #[error("WhatsApp command catalogue exceeds 64 KiB")]
    TooLarge,
    #[error("failed to parse WhatsApp command catalogue")]
    Parse,
    #[error("unsupported WhatsApp command catalogue schema")]
    UnsupportedSchema,
    #[error("invalid WhatsApp command catalogue")]
    Invalid,
    #[error("failed to create the default WhatsApp command catalogue")]
    Write,
}

impl CommandCatalog {
    pub fn load_or_create(codex_home: &Path) -> Result<(Self, PathBuf), CommandCatalogError> {
        let path = command_catalog_path(codex_home);
        match Self::load(&path) {
            Ok(catalog) => Ok((catalog, path)),
            Err(CommandCatalogError::Read) if !path.exists() => {
                write_default_catalog(&path)?;
                Self::load(&path).map(|catalog| (catalog, path))
            }
            Err(error) => Err(error),
        }
    }

    pub fn load(path: &Path) -> Result<Self, CommandCatalogError> {
        let metadata = fs::metadata(path).map_err(|_| CommandCatalogError::Read)?;
        if metadata.len() > MAX_CATALOG_BYTES {
            return Err(CommandCatalogError::TooLarge);
        }
        let bytes = fs::read(path).map_err(|_| CommandCatalogError::Read)?;
        let catalog: Self =
            serde_json::from_slice(&bytes).map_err(|_| CommandCatalogError::Parse)?;
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn render_help(&self) -> String {
        let mut output = self.help_heading.clone();
        for group in &self.groups {
            output.push_str("\n\n");
            output.push_str(&group.heading);
            for command in &group.commands {
                output.push_str("\n• `");
                output.push_str(&command.usage);
                output.push_str("` — ");
                output.push_str(&command.description);
            }
        }
        if !self.help_footer.is_empty() {
            output.push_str("\n\n");
            output.push_str(&self.help_footer);
        }
        output
    }

    pub fn rewrite_legacy_prefix(&self, text: &str) -> String {
        let Some(labelled) = text.strip_prefix("[codex") else {
            return text.to_string();
        };
        let Some((suffix, content)) = labelled.split_once("] ") else {
            return text.to_string();
        };
        if !suffix.is_empty() && !suffix.starts_with(' ') {
            return text.to_string();
        }
        if self.response_prefix.is_empty() {
            return content.to_string();
        }
        let suffix = suffix.trim();
        let prefix = if suffix.is_empty() {
            self.response_prefix.clone()
        } else if let Some(prefix) = self.response_prefix.strip_suffix(']') {
            format!("{prefix} {suffix}]")
        } else {
            format!("{} {suffix}", self.response_prefix)
        };
        format!("{prefix} {content}")
    }

    fn validate(&self) -> Result<(), CommandCatalogError> {
        if self.schema_version != COMMAND_CATALOG_SCHEMA_VERSION {
            return Err(CommandCatalogError::UnsupportedSchema);
        }
        if self.response_prefix.chars().count() > MAX_RESPONSE_PREFIX_CHARS
            || !valid_field(&self.help_heading)
            || self.groups.is_empty()
            || self.groups.len() > MAX_GROUPS
            || self.groups.iter().any(|group| {
                !valid_field(&group.heading)
                    || group.commands.is_empty()
                    || group.commands.len() > MAX_COMMANDS_PER_GROUP
                    || group.commands.iter().any(|command| {
                        !valid_field(&command.usage) || !valid_field(&command.description)
                    })
            })
            || self.help_footer.chars().count() > MAX_FIELD_CHARS
        {
            return Err(CommandCatalogError::Invalid);
        }
        Ok(())
    }
}

fn command_catalog_path(codex_home: &Path) -> PathBuf {
    codex_home.join("whatsapp").join(COMMAND_CATALOG_FILE)
}

fn valid_field(value: &str) -> bool {
    !value.trim().is_empty() && value.chars().count() <= MAX_FIELD_CHARS
}

fn write_default_catalog(path: &Path) -> Result<(), CommandCatalogError> {
    let parent = path.parent().ok_or(CommandCatalogError::Write)?;
    fs::create_dir_all(parent).map_err(|_| CommandCatalogError::Write)?;
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            use std::io::Write;

            file.write_all(DEFAULT_COMMAND_CATALOG.as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|_| CommandCatalogError::Write)?;
            set_private_permissions(path).map_err(|_| CommandCatalogError::Write)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(_) => Err(CommandCatalogError::Write),
    }
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
#[path = "command_catalog_tests.rs"]
mod tests;
