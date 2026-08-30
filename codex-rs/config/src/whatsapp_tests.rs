use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;
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

#[test]
fn legacy_runtime_delivery_knobs_are_read_but_not_written() {
    let runtime: WhatsAppRuntimeConfig = serde_json::from_value(json!({
        "bridgeName": "WhatsApp",
        "transportBaseUrl": "http://gateway",
        "transportApiToken": "token",
        "webhookSigningSecret": "secret",
        "webhookUrl": "http://bridge/webhook",
        "bridgeListen": "127.0.0.1:8787",
        "statePath": "/tmp/state.json",
        "maxQueuedPrompts": 20,
        "outputChunkChars": 3500,
        "editIntervalMs": 1500,
        "dedupeCapacity": 100,
        "dedupeTtlHours": 24
    }))
    .unwrap();

    assert_eq!(runtime.output_chunk_chars, 3500);
    assert_eq!(runtime.edit_interval_ms, 1500);
    let serialized = serde_json::to_string(&runtime).unwrap();
    assert!(!serialized.contains("outputChunkChars"));
    assert!(!serialized.contains("editIntervalMs"));
}
