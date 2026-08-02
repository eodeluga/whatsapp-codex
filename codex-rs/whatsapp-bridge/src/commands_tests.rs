use super::*;
use pretty_assertions::assert_eq;

#[test]
fn plain_messages_are_prompts_and_standard_controls_are_recognized() {
    assert_eq!(
        parse_command("inspect the current project"),
        BridgeCommand::Prompt("inspect the current project".to_string())
    );
    assert_eq!(parse_command("/status"), BridgeCommand::Status);
    assert_eq!(
        parse_command("/approve abc"),
        BridgeCommand::Approve {
            token: "abc".to_string(),
            session: false
        }
    );
}
