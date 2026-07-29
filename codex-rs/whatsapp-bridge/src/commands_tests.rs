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
    assert_eq!(
        parse_command("!codex ", "!codex approve"),
        Some(BridgeCommand::Help)
    );
}
