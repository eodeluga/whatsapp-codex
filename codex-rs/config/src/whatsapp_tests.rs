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
fn enabled_configuration_requires_an_owner_number() {
    assert_eq!(WhatsAppConfigToml::default().validate(), Ok(()));
    assert!(!WhatsAppConfigToml::default().is_complete());
    assert_eq!(
        WhatsAppConfigToml {
            onboarding_complete: true,
            enabled: true,
            ..Default::default()
        }
        .validate(),
        Err(WhatsAppConfigError::Incomplete)
    );
}

#[test]
fn redacted_user_configuration_preserves_the_owner_setting() {
    let config = WhatsAppConfigToml {
        onboarding_complete: true,
        enabled: true,
        account_phone_number: Some("+447700900000".to_string()),
    };

    assert_eq!(config.redacted(), config);
}

#[test]
fn rejects_unknown_user_configuration_fields() {
    assert!(toml::from_str::<WhatsAppConfigToml>("enabled = false\nworkspace = '/tmp'").is_err());
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
