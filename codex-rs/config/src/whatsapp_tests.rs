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
    assert!(!WhatsAppConfigToml::default().is_complete());
    assert!(
        WhatsAppConfigToml {
            onboarding_complete: true,
            enabled: false,
            ..Default::default()
        }
        .is_complete()
    );
}

#[test]
fn rejects_unknown_fields_and_relative_state_paths() {
    assert!(toml::from_str::<WhatsAppConfigToml>("enabled = false\nunknown = true").is_err());

    let directory = tempdir().unwrap();
    let secret = URL_SAFE_NO_PAD.encode([7_u8; 32]);
    let config = WhatsAppConfigToml {
        onboarding_complete: true,
        enabled: true,
        account_phone_number: Some("+447700900000".to_string()),
        workspace: Some(directory.path().to_path_buf()),
        openwa: Some(OpenWaConfigToml {
            session_id: Some("personal".to_string()),
            api_key: Some("key".to_string()),
            webhook_signing_secret: Some(secret),
            ..Default::default()
        }),
        bridge: Some(WhatsAppBridgeConfigToml {
            state_path: Some(PathBuf::from("relative.json")),
            ..Default::default()
        }),
        ..Default::default()
    };

    assert_eq!(
        config.validate(),
        Err(WhatsAppConfigError::InvalidStatePath)
    );
}

#[test]
fn rejects_malformed_service_addresses() {
    let directory = tempdir().unwrap();
    let complete =
        |openwa: OpenWaConfigToml, bridge: WhatsAppBridgeConfigToml| WhatsAppConfigToml {
            onboarding_complete: true,
            enabled: true,
            account_phone_number: Some("+447700900000".to_string()),
            workspace: Some(directory.path().to_path_buf()),
            openwa: Some(openwa),
            bridge: Some(bridge),
            ..Default::default()
        };
    let openwa = OpenWaConfigToml {
        session_id: Some("personal".to_string()),
        api_key: Some("key".to_string()),
        webhook_signing_secret: Some(URL_SAFE_NO_PAD.encode([7_u8; 32])),
        ..Default::default()
    };

    assert_eq!(
        complete(
            OpenWaConfigToml {
                api_base_url: Some("file:///api".to_string()),
                ..openwa.clone()
            },
            WhatsAppBridgeConfigToml::default(),
        )
        .validate(),
        Err(WhatsAppConfigError::InvalidOpenWaApiBaseUrl)
    );
    assert_eq!(
        complete(
            openwa,
            WhatsAppBridgeConfigToml {
                app_server_endpoint: Some("unix://relative.sock".to_string()),
                ..Default::default()
            },
        )
        .validate(),
        Err(WhatsAppConfigError::InvalidAppServerEndpoint)
    );
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
