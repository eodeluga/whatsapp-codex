use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use codex_config::OpenWaConfigToml;
use codex_config::WhatsAppBridgeConfigToml;
use codex_config::WhatsAppConfigToml;
use codex_config::canonical_e164;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyEventKind;
use crossterm::event::KeyModifiers;
use rand::RngCore;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::widgets::Block;
use ratatui::widgets::BorderType;
use ratatui::widgets::Borders;
use ratatui::widgets::Paragraph;
use ratatui::widgets::WidgetRef;
use ratatui::widgets::Wrap;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use crate::key_hint::KeyBindingListExt;
use crate::onboarding::keys;
use crate::onboarding::onboarding_screen::KeyboardHandler;
use crate::onboarding::onboarding_screen::StepState;
use crate::onboarding::onboarding_screen::StepStateProvider;
use crate::render::Insets;
use crate::render::renderable::ColumnRenderable;
use crate::render::renderable::Renderable;
use crate::render::renderable::RenderableExt as _;
use crate::selection_list::selection_option_row;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupChoice {
    Configure,
    NotNow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupStage {
    Choice,
    Preflight,
    Field(usize),
    Review,
    Saving,
    Saved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayPreflight {
    checks: Vec<GatewayCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayCheck {
    label: &'static str,
    detail: String,
    passed: bool,
}

impl GatewayPreflight {
    fn check(codex_home: &std::path::Path, workspace: &std::path::Path) -> Self {
        let docker_cli = command_succeeds("docker", &["--version"]);
        let docker_daemon = docker_cli && command_succeeds("docker", &["info"]);
        let compose = docker_cli && command_succeeds("docker", &["compose", "version"]);
        let codex_home_writable = if codex_home.exists() {
            std::fs::metadata(codex_home)
                .map(|metadata| !metadata.permissions().readonly())
                .unwrap_or(false)
        } else {
            codex_home.parent().is_some_and(|parent| {
                std::fs::metadata(parent)
                    .map(|metadata| !metadata.permissions().readonly())
                    .unwrap_or(false)
            })
        };
        Self {
            checks: vec![
                GatewayCheck {
                    label: "Docker",
                    detail: if docker_cli {
                        "available".to_string()
                    } else {
                        "not installed or not on PATH".to_string()
                    },
                    passed: docker_cli,
                },
                GatewayCheck {
                    label: "Docker daemon",
                    detail: if docker_daemon {
                        "running and accessible".to_string()
                    } else {
                        "not running or current user cannot access it".to_string()
                    },
                    passed: docker_daemon,
                },
                GatewayCheck {
                    label: "Docker Compose",
                    detail: if compose {
                        "v2 available".to_string()
                    } else {
                        "Docker Compose v2 is required".to_string()
                    },
                    passed: compose,
                },
                GatewayCheck {
                    label: "Codex home",
                    detail: if codex_home_writable {
                        "writable".to_string()
                    } else {
                        "must be writable".to_string()
                    },
                    passed: codex_home_writable,
                },
                GatewayCheck {
                    label: "Workspace",
                    detail: if workspace.is_dir() {
                        "valid".to_string()
                    } else {
                        "must be an existing directory".to_string()
                    },
                    passed: workspace.is_dir(),
                },
            ],
        }
    }

    fn is_ready(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }
}

fn command_succeeds(program: &str, arguments: &[&str]) -> bool {
    let Ok(mut child) = Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return false;
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

pub(crate) struct WhatsAppWidget {
    stage: SetupStage,
    highlighted: SetupChoice,
    phone_number: String,
    workspace: String,
    session_id: String,
    api_key: String,
    webhook_secret: String,
    codex_home: PathBuf,
    preflight: Option<GatewayPreflight>,
    save_request: Option<WhatsAppConfigToml>,
    skipped: bool,
    error: Option<String>,
}

impl WhatsAppWidget {
    pub(crate) fn new(
        current_workspace: PathBuf,
        codex_home: PathBuf,
        existing: Option<WhatsAppConfigToml>,
    ) -> Self {
        let existing = existing.unwrap_or_default();
        let openwa = existing.openwa.unwrap_or_default();
        Self {
            stage: SetupStage::Choice,
            highlighted: SetupChoice::Configure,
            phone_number: existing.account_phone_number.unwrap_or_default(),
            workspace: existing
                .workspace
                .unwrap_or(current_workspace)
                .display()
                .to_string(),
            session_id: openwa.session_id.unwrap_or_default(),
            api_key: openwa.api_key.unwrap_or_default(),
            webhook_secret: generate_webhook_secret(),
            codex_home,
            preflight: None,
            save_request: None,
            skipped: false,
            error: None,
        }
    }

    #[cfg(test)]
    fn new_for_test(current_workspace: PathBuf) -> Self {
        let mut widget = Self::new(current_workspace.clone(), current_workspace, None);
        widget.preflight = Some(GatewayPreflight {
            checks: vec![GatewayCheck {
                label: "Test",
                detail: "ready".to_string(),
                passed: true,
            }],
        });
        widget
    }

    fn run_preflight(&mut self) {
        self.preflight = Some(GatewayPreflight::check(
            &self.codex_home,
            PathBuf::from(&self.workspace).as_path(),
        ));
    }

    pub(crate) fn take_save_request(&mut self) -> Option<WhatsAppConfigToml> {
        self.save_request.take()
    }

    pub(crate) fn mark_saved(&mut self) {
        self.error = None;
        self.stage = SetupStage::Saved;
    }

    pub(crate) fn mark_save_failed(&mut self, error: String) {
        self.error = Some(error);
        self.stage = if self.skipped {
            SetupStage::Choice
        } else {
            SetupStage::Review
        };
    }

    pub(crate) fn is_text_entry_active(&self) -> bool {
        matches!(self.stage, SetupStage::Field(_))
    }

    fn begin_save(&mut self, config: WhatsAppConfigToml, skipped: bool) {
        self.skipped = skipped;
        self.save_request = Some(config);
        self.error = None;
        self.stage = SetupStage::Saving;
    }

    fn configured_value(&self) -> WhatsAppConfigToml {
        WhatsAppConfigToml {
            onboarding_complete: true,
            enabled: true,
            account_phone_number: Some(self.phone_number.clone()),
            workspace: Some(PathBuf::from(&self.workspace)),
            trigger_prefix: None,
            openwa: Some(OpenWaConfigToml {
                session_id: Some(self.session_id.clone()),
                api_key: Some(self.api_key.clone()),
                webhook_signing_secret: Some(self.webhook_secret.clone()),
                ..Default::default()
            }),
            bridge: Some(WhatsAppBridgeConfigToml::default()),
        }
    }

    fn disabled_value() -> WhatsAppConfigToml {
        WhatsAppConfigToml {
            onboarding_complete: true,
            enabled: false,
            ..Default::default()
        }
    }

    fn current_field_mut(&mut self) -> Option<&mut String> {
        match self.stage {
            SetupStage::Field(0) => Some(&mut self.phone_number),
            SetupStage::Field(1) => Some(&mut self.workspace),
            SetupStage::Field(2) => Some(&mut self.session_id),
            SetupStage::Field(3) => Some(&mut self.api_key),
            _ => None,
        }
    }

    fn validate_field(&self, index: usize) -> Result<(), &'static str> {
        match index {
            0 if canonical_e164(&self.phone_number).is_none() => {
                Err("Enter a canonical E.164 number such as +447700900000.")
            }
            1 if !PathBuf::from(&self.workspace).is_absolute()
                || !PathBuf::from(&self.workspace).is_dir() =>
            {
                Err("Workspace must be an absolute existing directory.")
            }
            2 if self.session_id.trim().is_empty() => Err("Session ID cannot be empty."),
            3 if self.api_key.trim().is_empty() => Err("API key cannot be empty."),
            _ => Ok(()),
        }
    }

    fn field_label(index: usize) -> &'static str {
        match index {
            0 => "WhatsApp account number (E.164)",
            1 => "Host workspace",
            2 => "OpenWA session ID",
            3 => "OpenWA API key",
            _ => "",
        }
    }
}

impl KeyboardHandler for WhatsAppWidget {
    fn handle_key_event(&mut self, key_event: KeyEvent) {
        if key_event.kind == KeyEventKind::Release {
            return;
        }
        match self.stage {
            SetupStage::Choice => {
                if keys::MOVE_UP.is_pressed(key_event) {
                    self.highlighted = SetupChoice::Configure;
                } else if keys::MOVE_DOWN.is_pressed(key_event) {
                    self.highlighted = SetupChoice::NotNow;
                } else if keys::CONFIRM.is_pressed(key_event) {
                    match self.highlighted {
                        SetupChoice::Configure => {
                            self.run_preflight();
                            self.stage = SetupStage::Preflight;
                        }
                        SetupChoice::NotNow => {
                            self.begin_save(Self::disabled_value(), /*skipped*/ true);
                        }
                    }
                }
            }
            SetupStage::Preflight => {
                if keys::CANCEL.is_pressed(key_event) {
                    self.error = None;
                    self.stage = SetupStage::Choice;
                } else if keys::CONFIRM.is_pressed(key_event) {
                    self.run_preflight();
                    if self
                        .preflight
                        .as_ref()
                        .is_some_and(GatewayPreflight::is_ready)
                    {
                        self.error = None;
                        self.stage = SetupStage::Field(0);
                    } else {
                        self.error = Some(
                            "WhatsApp needs the failed checks above. Fix them and press Enter to retry, or press Esc to continue without WhatsApp."
                                .to_string(),
                        );
                    }
                }
            }
            SetupStage::Field(index) => {
                if keys::CANCEL.is_pressed(key_event) {
                    self.error = None;
                    self.stage = SetupStage::Choice;
                } else if keys::CONFIRM.is_pressed(key_event) {
                    match self.validate_field(index) {
                        Ok(()) if index == 3 => {
                            self.error = None;
                            self.stage = SetupStage::Review;
                        }
                        Ok(()) => {
                            self.error = None;
                            self.stage = SetupStage::Field(index + 1);
                        }
                        Err(error) => self.error = Some(error.to_string()),
                    }
                } else if matches!(key_event.code, KeyCode::Backspace) {
                    if let Some(field) = self.current_field_mut() {
                        field.pop();
                    }
                } else if let KeyCode::Char(character) = key_event.code
                    && !key_event
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                    && let Some(field) = self.current_field_mut()
                    && field.chars().count() < 4_096
                {
                    field.push(character);
                }
            }
            SetupStage::Review => {
                if keys::CANCEL.is_pressed(key_event) {
                    self.stage = SetupStage::Field(3);
                } else if keys::CONFIRM.is_pressed(key_event) {
                    self.begin_save(self.configured_value(), /*skipped*/ false);
                }
            }
            SetupStage::Saving | SetupStage::Saved => {}
        }
    }

    fn handle_paste(&mut self, pasted: String) {
        if let Some(field) = self.current_field_mut() {
            let remaining = 4_096_usize.saturating_sub(field.chars().count());
            field.extend(pasted.trim().chars().take(remaining));
        }
    }
}

impl StepStateProvider for WhatsAppWidget {
    fn get_step_state(&self) -> StepState {
        if matches!(self.stage, SetupStage::Saved) {
            StepState::Complete
        } else {
            StepState::InProgress
        }
    }
}

impl WidgetRef for &WhatsAppWidget {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        let mut column = ColumnRenderable::new();
        column.push(Line::from(vec![
            "> ".into(),
            "WhatsApp remote access".bold(),
        ]));
        column.push("");
        match self.stage {
            SetupStage::Choice => {
                column.push(
                    Paragraph::new(
                        "Connect one private WhatsApp self-chat to this workspace through your self-hosted OpenWA service.",
                    )
                    .wrap(Wrap { trim: true })
                    .inset(Insets::tlbr(
                        /*top*/ 0, /*left*/ 2, /*bottom*/ 0, /*right*/ 0,
                    )),
                );
                column.push("");
                for (index, (label, choice)) in [
                    ("Configure WhatsApp", SetupChoice::Configure),
                    ("Not now", SetupChoice::NotNow),
                ]
                .iter()
                .enumerate()
                {
                    column.push(selection_option_row(
                        index,
                        (*label).to_string(),
                        self.highlighted == *choice,
                    ));
                }
            }
            SetupStage::Preflight => {
                column.push("  Gateway preflight".bold());
                column.push("");
                if let Some(preflight) = &self.preflight {
                    for check in &preflight.checks {
                        let status = if check.passed {
                            "ok".green()
                        } else {
                            "needs attention".red()
                        };
                        column.push(Line::from(vec![
                            format!("  {}: ", check.label).into(),
                            status,
                            format!(" — {}", check.detail).dim(),
                        ]));
                    }
                    if preflight.is_ready() {
                        column.push("");
                        column.push("  All checks passed. Press Enter to continue.".green());
                    }
                }
            }
            SetupStage::Field(index) => {
                let value = match index {
                    0 => self.phone_number.as_str(),
                    1 => self.workspace.as_str(),
                    2 => self.session_id.as_str(),
                    3 => self.api_key.as_str(),
                    _ => "",
                };
                let shown = if index == 3 {
                    "•".repeat(value.chars().count())
                } else {
                    value.to_string()
                };
                column.push(
                    Paragraph::new(WhatsAppWidget::field_label(index))
                        .bold()
                        .inset(Insets::tlbr(
                            /*top*/ 0, /*left*/ 2, /*bottom*/ 0, /*right*/ 0,
                        )),
                );
                column.push(
                    Paragraph::new(shown)
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_type(BorderType::Rounded),
                        )
                        .inset(Insets::tlbr(
                            /*top*/ 0, /*left*/ 2, /*bottom*/ 0, /*right*/ 0,
                        )),
                );
            }
            SetupStage::Review => {
                column.push("  Review");
                column.push(format!("  Account: {}", self.phone_number));
                column.push(format!("  Workspace: {}", self.workspace));
                column.push(format!("  OpenWA session: {}", self.session_id));
                column.push("  OpenWA API key: [redacted]");
                column.push("  Webhook signing secret: [generated and redacted]");
                column.push("  Trigger prefix: !codex ");
                column.push(format!(
                    "  OpenWA API: {}",
                    codex_config::whatsapp::DEFAULT_OPENWA_API_BASE_URL
                ));
                column.push(format!(
                    "  Webhook URL: {}",
                    codex_config::whatsapp::DEFAULT_OPENWA_WEBHOOK_URL
                ));
                column.push(format!(
                    "  App-server: {}",
                    codex_config::whatsapp::DEFAULT_APP_SERVER_ENDPOINT
                ));
                column.push(format!(
                    "  State: {}",
                    codex_config::whatsapp::DEFAULT_STATE_PATH
                ));
                column.push(format!(
                    "  Queue/output: {} prompts, {} chars per part",
                    codex_config::whatsapp::DEFAULT_MAX_QUEUED_PROMPTS,
                    codex_config::whatsapp::DEFAULT_OUTPUT_CHUNK_CHARS,
                ));
            }
            SetupStage::Saving => column.push("  Saving WhatsApp configuration…".dim()),
            SetupStage::Saved if self.skipped => {
                column.push("  WhatsApp setup skipped. You can enable it later in config.toml.");
            }
            SetupStage::Saved => {
                column.push("  WhatsApp configuration saved.".green());
            }
        }
        if let Some(error) = &self.error {
            column.push("");
            column.push(
                Paragraph::new(error.clone())
                    .red()
                    .wrap(Wrap { trim: true })
                    .inset(Insets::tlbr(
                        /*top*/ 0, /*left*/ 2, /*bottom*/ 0, /*right*/ 0,
                    )),
            );
        }
        if matches!(
            self.stage,
            SetupStage::Choice | SetupStage::Preflight | SetupStage::Field(_) | SetupStage::Review
        ) {
            column.push("");
            column.push("  Press Enter to continue".dim());
        }
        column.render(area, buf);
    }
}

fn generate_webhook_secret() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
#[path = "whatsapp_tests.rs"]
mod tests;
