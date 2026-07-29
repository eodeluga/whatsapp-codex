use super::*;
use crate::test_backend::VT100Backend;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyModifiers;
use pretty_assertions::assert_eq;
use ratatui::Terminal;
use tempfile::tempdir;

fn render(widget: &WhatsAppWidget) -> String {
    let mut terminal =
        Terminal::new(VT100Backend::new(/*width*/ 76, /*height*/ 22)).expect("terminal");
    terminal
        .draw(|frame| frame.render_widget_ref(widget, frame.area()))
        .expect("draw");
    terminal.backend().to_string()
}

#[test]
fn renders_setup_choice() {
    let directory = tempdir().unwrap();
    let widget = WhatsAppWidget::new(directory.path().to_path_buf(), None);
    insta::assert_snapshot!(render(&widget));
}

#[test]
fn renders_validation_error() {
    let directory = tempdir().unwrap();
    let mut widget = WhatsAppWidget::new(directory.path().to_path_buf(), None);
    widget.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    widget.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    insta::assert_snapshot!(render(&widget));
}

#[test]
fn renders_redacted_review() {
    let directory = tempdir().unwrap();
    let mut widget = WhatsAppWidget::new(directory.path().to_path_buf(), None);
    widget.phone_number = "+447700900000".to_string();
    widget.session_id = "personal".to_string();
    widget.api_key = "operator-secret".to_string();
    widget.stage = SetupStage::Review;
    let rendered = render(&widget);
    assert!(!rendered.contains("operator-secret"));
    insta::assert_snapshot!(rendered);
}

#[test]
fn renders_saved_state() {
    let directory = tempdir().unwrap();
    let mut widget = WhatsAppWidget::new(directory.path().to_path_buf(), None);
    widget.mark_saved();
    insta::assert_snapshot!(render(&widget));
}

#[test]
fn renders_skipped_state_and_persists_opt_out() {
    let directory = tempdir().unwrap();
    let mut widget = WhatsAppWidget::new(directory.path().to_path_buf(), None);
    widget.highlighted = SetupChoice::NotNow;
    widget.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let config = widget.take_save_request().unwrap();
    assert_eq!(config.onboarding_complete, true);
    assert_eq!(config.enabled, false);
    widget.mark_saved();
    insta::assert_snapshot!(render(&widget));
}
