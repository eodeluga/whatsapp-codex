use super::*;
use pretty_assertions::assert_eq;

#[test]
fn plain_messages_are_prompts_and_standard_controls_are_recognized() {
    assert_eq!(
        parse_command("inspect the current project"),
        BridgeCommand::Prompt("inspect the current project".to_string())
    );
    assert_eq!(parse_command("/status"), BridgeCommand::Status);
    assert_eq!(parse_command("/"), BridgeCommand::Help);
    assert_eq!(
        parse_command("/approve abc"),
        BridgeCommand::Approve {
            token: "abc".to_string(),
            session: false
        }
    );
}

#[test]
fn parses_whatsapp_thread_controls() {
    assert_eq!(
        parse_command("/whatsapp list-threads"),
        BridgeCommand::WhatsAppListThreads
    );
    assert_eq!(
        parse_command("/whatsapp attach thread-123"),
        BridgeCommand::WhatsAppAttach("thread-123".to_string())
    );
}
