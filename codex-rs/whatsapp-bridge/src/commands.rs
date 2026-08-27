//! Parsing for WhatsApp bridge commands. Parsing deliberately performs no I/O.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeCommand {
    Prompt(String),
    New,
    Status,
    Stop,
    WhatsAppAttach(String),
    WhatsAppListThreads,
    Approve { token: String, session: bool },
    Deny { token: String },
    Answer { token: String, answer: String },
    Help,
}

/// Parses a first-class WhatsApp input message.
pub fn parse_command(message: &str) -> BridgeCommand {
    let command = message.trim();
    if command.is_empty() {
        return BridgeCommand::Help;
    }
    match command {
        "/new" => BridgeCommand::New,
        "/status" => BridgeCommand::Status,
        "/stop" => BridgeCommand::Stop,
        "/" | "/help" => BridgeCommand::Help,
        "/whatsapp list-threads" => BridgeCommand::WhatsAppListThreads,
        _ if command.starts_with("/whatsapp attach ") => command
            .strip_prefix("/whatsapp attach ")
            .filter(|token| !token.is_empty())
            .map_or(BridgeCommand::Help, |token| {
                BridgeCommand::WhatsAppAttach(token.to_string())
            }),
        _ if is_reserved_name(command.split_whitespace().next().unwrap_or_default()) => {
            parse_reserved(command).unwrap_or(BridgeCommand::Help)
        }
        _ => BridgeCommand::Prompt(message.to_string()),
    }
}

fn is_reserved_name(name: &str) -> bool {
    matches!(
        name,
        "/approve"
            | "/approve-session"
            | "/deny"
            | "/answer"
            | "/help"
            | "/new"
            | "/status"
            | "/stop"
            | "/whatsapp"
    )
}

fn parse_reserved(command: &str) -> Option<BridgeCommand> {
    let mut words = command.splitn(3, ' ');
    let name = words.next()?;
    let token = words.next()?.to_string();
    match name {
        "/approve" => Some(BridgeCommand::Approve {
            token,
            session: false,
        }),
        "/approve-session" => Some(BridgeCommand::Approve {
            token,
            session: true,
        }),
        "/deny" => Some(BridgeCommand::Deny { token }),
        "/answer" => Some(BridgeCommand::Answer {
            token,
            answer: words.next()?.to_string(),
        }),
        _ => None,
    }
}

#[cfg(test)]
#[path = "commands_tests.rs"]
mod tests;
