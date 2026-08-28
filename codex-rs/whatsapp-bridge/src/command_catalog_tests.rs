use super::*;
use pretty_assertions::assert_eq;

#[test]
fn default_catalog_is_valid_and_contains_both_command_surfaces() {
    let catalog: CommandCatalog = serde_json::from_str(DEFAULT_COMMAND_CATALOG).unwrap();
    catalog.validate().unwrap();

    let help = catalog.render_help();
    assert!(help.contains("`/whatsapp list-threads`"));
    assert!(help.contains("`/answer <token> <answer>`"));
    assert!(help.contains("displayed number"));
    assert!(!help.contains("/approve"));
    assert!(!help.contains("/deny"));
}

#[test]
fn creates_and_loads_a_user_editable_default_catalog() {
    let directory = tempfile::tempdir().unwrap();
    let (catalog, path) = CommandCatalog::load_or_create(directory.path()).unwrap();

    assert_eq!(path, directory.path().join("whatsapp/commands.json"));
    assert_eq!(CommandCatalog::load(&path).unwrap(), catalog);
}

#[test]
fn renders_custom_help_and_prefix_from_disk() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("commands.json");
    fs::write(
        &path,
        r#"{
          "schemaVersion": 1,
          "responsePrefix": "[remote]",
          "helpHeading": "Remote controls",
          "helpFooter": "Edit this file to change help.",
          "groups": [{
            "heading": "Session",
            "commands": [{"usage": "/status", "description": "show status"}]
          }]
        }"#,
    )
    .unwrap();

    let catalog = CommandCatalog::load(&path).unwrap();
    assert_eq!(
        catalog.render_help(),
        "Remote controls\n\nSession\n• `/status` — show status\n\nEdit this file to change help."
    );
    assert_eq!(
        catalog.rewrite_legacy_prefix("[codex] Ready."),
        "[remote] Ready."
    );
    assert_eq!(
        catalog.rewrite_legacy_prefix("[codex 2/3] More."),
        "[remote 2/3] More."
    );
}
