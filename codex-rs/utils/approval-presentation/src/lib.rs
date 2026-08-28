//! Transport-neutral presentation of Codex approval requests.

use codex_app_server_protocol::AdditionalPermissionProfile;
use codex_app_server_protocol::CommandExecutionApprovalDecision;
use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
use codex_app_server_protocol::FileChangeApprovalDecision;
use codex_app_server_protocol::FileChangeRequestApprovalParams;
use codex_app_server_protocol::FileSystemAccessMode;
use codex_app_server_protocol::FileSystemPath;
use codex_app_server_protocol::FileSystemSpecialPath;
use codex_app_server_protocol::GrantedPermissionProfile;
use codex_app_server_protocol::NetworkApprovalContext;
use codex_app_server_protocol::NetworkPolicyAmendment;
use codex_app_server_protocol::NetworkPolicyRuleAction;
use codex_app_server_protocol::PermissionGrantScope;
use codex_app_server_protocol::PermissionsRequestApprovalParams;
use codex_app_server_protocol::RequestPermissionProfile;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalPresentation {
    pub title: String,
    pub details: Vec<String>,
    pub choices: Vec<ApprovalChoice>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalChoice {
    pub label: String,
    pub decision: ApprovalDecision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalDecision {
    Command(CommandExecutionApprovalDecision),
    FileChange(FileChangeApprovalDecision),
    Permissions {
        permissions: GrantedPermissionProfile,
        scope: PermissionGrantScope,
        strict_auto_review: Option<bool>,
    },
}

pub fn default_command_execution_decisions(
    network_approval_context: Option<&NetworkApprovalContext>,
    proposed_execpolicy_amendment: Option<&codex_app_server_protocol::ExecPolicyAmendment>,
    proposed_network_policy_amendments: Option<&[NetworkPolicyAmendment]>,
    additional_permissions: Option<&AdditionalPermissionProfile>,
) -> Vec<CommandExecutionApprovalDecision> {
    if network_approval_context.is_some() {
        let mut decisions = vec![
            CommandExecutionApprovalDecision::Accept,
            CommandExecutionApprovalDecision::AcceptForSession,
        ];
        if let Some(amendment) = proposed_network_policy_amendments.and_then(|amendments| {
            amendments
                .iter()
                .find(|amendment| amendment.action == NetworkPolicyRuleAction::Allow)
        }) {
            decisions.push(
                CommandExecutionApprovalDecision::ApplyNetworkPolicyAmendment {
                    network_policy_amendment: amendment.clone(),
                },
            );
        }
        decisions.push(CommandExecutionApprovalDecision::Cancel);
        return decisions;
    }

    if additional_permissions.is_some() {
        return vec![
            CommandExecutionApprovalDecision::Accept,
            CommandExecutionApprovalDecision::Cancel,
        ];
    }

    let mut decisions = vec![CommandExecutionApprovalDecision::Accept];
    if let Some(amendment) = proposed_execpolicy_amendment {
        decisions.push(
            CommandExecutionApprovalDecision::AcceptWithExecpolicyAmendment {
                execpolicy_amendment: amendment.clone(),
            },
        );
    }
    decisions.push(CommandExecutionApprovalDecision::Cancel);
    decisions
}

/// Returns the standard TUI wording and decision order for command approval.
pub fn command_execution_choices(
    available_decisions: &[CommandExecutionApprovalDecision],
    network_approval_context: Option<&NetworkApprovalContext>,
    additional_permissions: Option<&AdditionalPermissionProfile>,
) -> Vec<ApprovalChoice> {
    available_decisions
        .iter()
        .filter_map(|decision| {
            let label = match decision {
                CommandExecutionApprovalDecision::Accept => {
                    if network_approval_context.is_some() {
                        "Yes, just this once".to_string()
                    } else {
                        "Yes, proceed".to_string()
                    }
                }
                CommandExecutionApprovalDecision::AcceptWithExecpolicyAmendment {
                    execpolicy_amendment,
                } => {
                    let prefix = render_command(&execpolicy_amendment.command);
                    if prefix.contains(['\n', '\r']) {
                        return None;
                    }
                    format!("Yes, and don't ask again for commands that start with `{prefix}`")
                }
                CommandExecutionApprovalDecision::AcceptForSession => {
                    if network_approval_context.is_some() {
                        "Yes, and allow this host for this conversation".to_string()
                    } else if additional_permissions.is_some() {
                        "Yes, and allow these permissions for this session".to_string()
                    } else {
                        "Yes, and don't ask again for this command in this session".to_string()
                    }
                }
                CommandExecutionApprovalDecision::ApplyNetworkPolicyAmendment {
                    network_policy_amendment,
                } => match network_policy_amendment.action {
                    NetworkPolicyRuleAction::Allow => {
                        "Yes, and allow this host in the future".to_string()
                    }
                    NetworkPolicyRuleAction::Deny => {
                        "No, and block this host in the future".to_string()
                    }
                },
                CommandExecutionApprovalDecision::Decline => {
                    "No, continue without running it".to_string()
                }
                CommandExecutionApprovalDecision::Cancel => {
                    "No, and tell Codex what to do differently".to_string()
                }
            };
            Some(ApprovalChoice {
                label,
                decision: ApprovalDecision::Command(decision.clone()),
            })
        })
        .collect()
}

pub fn command_execution_presentation(
    params: &CommandExecutionRequestApprovalParams,
) -> ApprovalPresentation {
    let title = params.network_approval_context.as_ref().map_or_else(
        || "Would you like to run the following command?".to_string(),
        |context| {
            format!(
                "Do you want to approve network access to \"{}\"?",
                context.host
            )
        },
    );
    let decisions = params.available_decisions.clone().unwrap_or_else(|| {
        default_command_execution_decisions(
            params.network_approval_context.as_ref(),
            params.proposed_execpolicy_amendment.as_ref(),
            params.proposed_network_policy_amendments.as_deref(),
            params.additional_permissions.as_ref(),
        )
    });
    let mut details = Vec::new();
    if let Some(environment_id) = &params.environment_id {
        details.push(format!("Environment: {environment_id}"));
    }
    if let Some(reason) = &params.reason {
        details.push(format!("Reason: {reason}"));
    }
    if let Some(additional_permissions) = &params.additional_permissions
        && let Some(rule) = format_additional_permissions_rule(additional_permissions)
    {
        details.push(format!("Permission rule: {rule}"));
    }
    if params.network_approval_context.is_none()
        && let Some(command) = &params.command
    {
        details.push(format!("$ {command}"));
    }
    ApprovalPresentation {
        title,
        details,
        choices: command_execution_choices(
            &decisions,
            params.network_approval_context.as_ref(),
            params.additional_permissions.as_ref(),
        ),
    }
}

pub fn file_change_presentation(
    params: &FileChangeRequestApprovalParams,
    paths: &[String],
) -> ApprovalPresentation {
    let mut details = Vec::new();
    if let Some(reason) = &params.reason {
        details.push(format!("Reason: {reason}"));
    }
    if !paths.is_empty() {
        details.push(format!("Files: {}", paths.join(", ")));
    }
    if let Some(root) = &params.grant_root {
        details.push(format!("Permission rule: write under `{}`", root.display()));
    }
    ApprovalPresentation {
        title: "Would you like to make the following edits?".to_string(),
        details,
        choices: vec![
            ApprovalChoice {
                label: "Yes, proceed".to_string(),
                decision: ApprovalDecision::FileChange(FileChangeApprovalDecision::Accept),
            },
            ApprovalChoice {
                label: "Yes, and don't ask again for these files".to_string(),
                decision: ApprovalDecision::FileChange(
                    FileChangeApprovalDecision::AcceptForSession,
                ),
            },
            ApprovalChoice {
                label: "No, and tell Codex what to do differently".to_string(),
                decision: ApprovalDecision::FileChange(FileChangeApprovalDecision::Cancel),
            },
        ],
    }
}

pub fn permissions_presentation(params: &PermissionsRequestApprovalParams) -> ApprovalPresentation {
    let mut details = Vec::new();
    if let Some(environment_id) = &params.environment_id {
        details.push(format!("Environment: {environment_id}"));
    }
    if let Some(reason) = &params.reason {
        details.push(format!("Reason: {reason}"));
    }
    if let Some(rule) = format_requested_permissions_rule(&params.permissions) {
        details.push(format!("Permission rule: {rule}"));
    }
    let granted = GrantedPermissionProfile {
        network: params.permissions.network.clone(),
        file_system: params.permissions.file_system.clone(),
    };
    ApprovalPresentation {
        title: "Would you like to grant these permissions?".to_string(),
        details,
        choices: vec![
            ApprovalChoice {
                label: "Yes, grant these permissions for this turn".to_string(),
                decision: ApprovalDecision::Permissions {
                    permissions: granted.clone(),
                    scope: PermissionGrantScope::Turn,
                    strict_auto_review: None,
                },
            },
            ApprovalChoice {
                label: "Yes, grant for this turn with strict auto review".to_string(),
                decision: ApprovalDecision::Permissions {
                    permissions: granted.clone(),
                    scope: PermissionGrantScope::Turn,
                    strict_auto_review: Some(true),
                },
            },
            ApprovalChoice {
                label: "Yes, grant these permissions for this session".to_string(),
                decision: ApprovalDecision::Permissions {
                    permissions: granted,
                    scope: PermissionGrantScope::Session,
                    strict_auto_review: None,
                },
            },
            ApprovalChoice {
                label: "No, continue without permissions".to_string(),
                decision: ApprovalDecision::Permissions {
                    permissions: GrantedPermissionProfile::default(),
                    scope: PermissionGrantScope::Turn,
                    strict_auto_review: None,
                },
            },
        ],
    }
}

fn render_command(command: &[String]) -> String {
    let command = if command.len() >= 3
        && command[1] == "-lc"
        && (command[0].ends_with("bash") || command[0].ends_with("zsh"))
    {
        &command[2..]
    } else {
        command
    };
    command
        .iter()
        .map(|part| {
            if part
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "_./:-".contains(character))
            {
                part.clone()
            } else {
                format!("'{}'", part.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn format_additional_permissions_rule(permissions: &AdditionalPermissionProfile) -> Option<String> {
    let mut parts = Vec::new();
    if permissions
        .network
        .as_ref()
        .and_then(|network| network.enabled)
        .unwrap_or(false)
    {
        parts.push("network".to_string());
    }
    if let Some(file_system) = &permissions.file_system {
        for (access, label) in [
            (FileSystemAccessMode::Read, "read"),
            (FileSystemAccessMode::Write, "write"),
            (FileSystemAccessMode::Deny, "deny read"),
        ] {
            let paths = file_system
                .entries
                .iter()
                .flatten()
                .filter(|entry| entry.access == access)
                .map(|entry| format_file_system_path(&entry.path))
                .collect::<Vec<_>>()
                .join(", ");
            if !paths.is_empty() {
                parts.push(format!("{label} {paths}"));
            }
        }
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

fn format_requested_permissions_rule(permissions: &RequestPermissionProfile) -> Option<String> {
    let additional = AdditionalPermissionProfile {
        network: permissions.network.clone(),
        file_system: permissions.file_system.clone(),
    };
    format_additional_permissions_rule(&additional)
}

fn format_file_system_path(path: &FileSystemPath) -> String {
    match path {
        FileSystemPath::Path { path } => format!("`{path}`"),
        FileSystemPath::GlobPattern { pattern } => format!("glob `{pattern}`"),
        FileSystemPath::Special { value } => format!("`{}`", format_special_path(value)),
    }
}

fn format_special_path(path: &FileSystemSpecialPath) -> String {
    match path {
        FileSystemSpecialPath::Root => ":root".to_string(),
        FileSystemSpecialPath::Minimal => ":minimal".to_string(),
        FileSystemSpecialPath::ProjectRoots { subpath } => subpath
            .as_ref()
            .map_or(":workspace_roots".to_string(), |path| {
                format!(":workspace_roots/{path}")
            }),
        FileSystemSpecialPath::Tmpdir => ":tmpdir".to_string(),
        FileSystemSpecialPath::SlashTmp => "/tmp".to_string(),
        FileSystemSpecialPath::Unknown { path, subpath } => subpath
            .as_ref()
            .map_or_else(|| path.clone(), |subpath| format!("{path}/{subpath}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn command_choices_preserve_standard_order_and_labels() {
        let decisions = vec![
            CommandExecutionApprovalDecision::Accept,
            CommandExecutionApprovalDecision::AcceptWithExecpolicyAmendment {
                execpolicy_amendment: codex_app_server_protocol::ExecPolicyAmendment {
                    command: vec!["git".to_string(), "push".to_string()],
                },
            },
            CommandExecutionApprovalDecision::AcceptForSession,
            CommandExecutionApprovalDecision::Cancel,
        ];
        let choices = command_execution_choices(&decisions, None, None);
        assert_eq!(
            choices
                .iter()
                .map(|choice| choice.label.as_str())
                .collect::<Vec<_>>(),
            vec![
                "Yes, proceed",
                "Yes, and don't ask again for commands that start with `git push`",
                "Yes, and don't ask again for this command in this session",
                "No, and tell Codex what to do differently",
            ]
        );
        assert_eq!(
            choices.last().unwrap().decision,
            ApprovalDecision::Command(CommandExecutionApprovalDecision::Cancel)
        );
    }
}
