use super::*;
use codex_protocol::models::MessagePhase;
use pretty_assertions::assert_eq;

#[test]
fn chunks_unicode_without_exceeding_limit() {
    let chunks = chunk_text("one 😀 two\n\nthree four", 8);
    assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 8));
    assert_eq!(chunks.concat().replace(' ', ""), "one😀twothreefour");
}

#[test]
fn completed_items_preserve_arrival_order_and_replace_deltas() {
    let mut output = OutputAggregator::default();
    output.push_delta("thread".into(), "turn".into(), "first".into(), "stale");
    output.push_delta("thread".into(), "turn".into(), "second".into(), "two");
    output.complete_item(
        "thread".into(),
        "turn".into(),
        "first".into(),
        None,
        "one".into(),
    );

    assert_eq!(output.finish_turn("thread", "turn"), "onetwo");
}

#[test]
fn labels_fit_within_the_transport_limit() {
    let output = "x".repeat(8_500);
    let chunks = labelled_chunks(&output, 4_000);

    assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 4_096));
    assert_eq!(chunks.len(), 3);
}

#[test]
fn caps_the_number_of_assistant_items_with_a_visible_notice() {
    let mut output = OutputAggregator::default();
    for index in 0..=MAX_ASSISTANT_ITEMS_PER_TURN {
        output.complete_item(
            "thread".into(),
            "turn".into(),
            format!("item-{index}"),
            None,
            format!("value-{index};"),
        );
    }

    let completed = output.finish_turn("thread", "turn");
    assert!(completed.ends_with(TRUNCATION_NOTICE));
    assert!(!completed.contains(&format!("value-{MAX_ASSISTANT_ITEMS_PER_TURN};")));
}

#[test]
fn commentary_is_not_forwarded_with_the_final_answer() {
    let mut output = OutputAggregator::default();
    output.push_delta(
        "thread".into(),
        "turn".into(),
        "commentary".into(),
        "I’ll inspect the file type.",
    );
    output.complete_item(
        "thread".into(),
        "turn".into(),
        "commentary".into(),
        Some(MessagePhase::Commentary),
        "I’ll inspect the file type.".into(),
    );
    output.push_delta(
        "thread".into(),
        "turn".into(),
        "final".into(),
        "The file is ready.",
    );
    output.complete_item(
        "thread".into(),
        "turn".into(),
        "final".into(),
        Some(MessagePhase::FinalAnswer),
        "The file is ready.".into(),
    );

    assert_eq!(output.finish_turn("thread", "turn"), "The file is ready.");
}
