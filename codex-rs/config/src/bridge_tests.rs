use super::*;

#[test]
fn bridge_output_categories_default_to_off() {
    assert_eq!(
        BridgeConfigToml::default(),
        BridgeConfigToml {
            include_reasoning: false,
            include_tool_calls: false,
            include_approval_notices: false,
            include_automatic_approval_reviews: false,
        }
    );
}

#[test]
fn bridge_output_categories_are_provider_neutral() {
    let config: BridgeConfigToml = toml::from_str(
        "include_reasoning = true\ninclude_tool_calls = true\ninclude_approval_notices = true\ninclude_automatic_approval_reviews = true\n",
    )
    .expect("bridge options should parse");
    assert_eq!(
        config,
        BridgeConfigToml {
            include_reasoning: true,
            include_tool_calls: true,
            include_approval_notices: true,
            include_automatic_approval_reviews: true,
        }
    );
}
