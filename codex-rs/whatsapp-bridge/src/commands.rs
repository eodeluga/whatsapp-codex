//! Parsing for WhatsApp bridge commands. Parsing deliberately performs no I/O.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeCommand {
    Prompt(String),
    New,
    Status,
    Stop,
    Approve { token: String, session: bool },
    Deny { token: String },
    Answer { token: String, answer: String },
    Help,
}

/// Parses a message only when it starts with the exact configured prefix.
pub fn parse_command(prefix: &str, message: &str) -> Option<BridgeCommand> {
    let suffix = message.strip_prefix(prefix)?;
    let command = suffix.trim();
    if command.is_empty() {
        return Some(BridgeCommand::Help);
    }
    match command {
        "new" => Some(BridgeCommand::New),
        "status" => Some(BridgeCommand::Status),
        "stop" => Some(BridgeCommand::Stop),
        _ => parse_reserved(command).or_else(|| Some(BridgeCommand::Prompt(suffix.to_string()))),
    }
}

fn parse_reserved(command: &str) -> Option<BridgeCommand> {
    let mut words = command.splitn(3, ' ');
    let name = words.next()?;
    let token = words.next()?.to_string();
    match name {
        "approve" => Some(BridgeCommand::Approve {
            token,
            session: false,
        }),
        "approve-session" => Some(BridgeCommand::Approve {
            token,
            session: true,
        }),
        "deny" => Some(BridgeCommand::Deny { token }),
        "answer" => Some(BridgeCommand::Answer {
            token,
            answer: words.next()?.to_string(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn prefix_is_exact_and_commands_take_precedence() {
        assert_eq!(parse_command("!codex ", "!Codex status"), None);
        assert_eq!(
            parse_command("!codex ", "!codex status"),
            Some(BridgeCommand::Status)
        );
        assert_eq!(
            parse_command("!codex ", "!codex approve-session abc"),
            Some(BridgeCommand::Approve {
                token: "abc".to_string(),
                session: true
            })
        );
    }
}
