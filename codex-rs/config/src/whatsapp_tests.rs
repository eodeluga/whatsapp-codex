use super::*;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

#[test]
fn canonical_e164_derives_self_chat_jid() {
    let config = WhatsAppConfigToml {
        account_phone_number: Some("+447700900000".to_string()),
        ..Default::default()
    };

    assert_eq!(config.self_chat_jid().unwrap(), "447700900000@c.us");
    assert_eq!(canonical_e164("447700900000"), None);
    assert_eq!(canonical_e164("+0447700900000"), None);
}

#[test]
fn disabled_configuration_is_valid() {
    assert_eq!(WhatsAppConfigToml::default().validate(), Ok(()));
}

#[test]
fn redaction_hides_both_secrets() {
    let config = WhatsAppConfigToml {
        openwa: Some(OpenWaConfigToml {
            api_key: Some("operator-key".to_string()),
            webhook_signing_secret: Some("super-secret".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let debug = format!("{config:?}");
    assert!(!debug.contains("operator-key"));
    assert!(!debug.contains("super-secret"));
    assert_eq!(
        config.redacted().openwa.unwrap().api_key.as_deref(),
        Some(REDACTED_SECRET)
    );
}

#[test]
fn user_loader_ignores_unrelated_config() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("config.toml");
    std::fs::write(
        &path,
        "model = 'gpt-5'\n[whatsapp]\nenabled = false\nonboarding_complete = true\n",
    )
    .unwrap();

    let config = load_user_whatsapp_config(&path).unwrap().unwrap();
    assert!(config.onboarding_complete);
    assert!(!config.enabled);
}
