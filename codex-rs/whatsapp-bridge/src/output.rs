//! WhatsApp-safe assistant-output aggregation and chunking.

use codex_protocol::models::MessagePhase;

const MAX_ASSISTANT_OUTPUT_CHARS: usize = 100_000;
const MAX_ASSISTANT_ITEMS_PER_TURN: usize = 64;
const TRUNCATION_NOTICE: &str = "\n\n[codex] Output truncated at 100,000 characters.";

#[derive(Debug, Default)]
pub struct OutputAggregator {
    items: Vec<OutputItem>,
}

#[derive(Debug)]
struct OutputItem {
    thread_id: String,
    turn_id: String,
    item_id: String,
    text: String,
}

impl OutputAggregator {
    pub fn push_delta(&mut self, thread_id: String, turn_id: String, item_id: String, delta: &str) {
        if let Some(item) = self.item_mut(&thread_id, &turn_id, &item_id) {
            append_bounded(&mut item.text, delta);
        } else if self
            .items
            .iter()
            .filter(|item| item.thread_id == thread_id && item.turn_id == turn_id)
            .count()
            < MAX_ASSISTANT_ITEMS_PER_TURN
        {
            let mut text = String::new();
            append_bounded(&mut text, delta);
            self.items.push(OutputItem {
                thread_id,
                turn_id,
                item_id,
                text,
            });
        } else {
            if let Some(item) = self
                .items
                .iter_mut()
                .rev()
                .find(|item| item.thread_id == thread_id && item.turn_id == turn_id)
            {
                force_truncation_notice(&mut item.text);
            }
        }
    }

    pub fn complete_item(
        &mut self,
        thread_id: String,
        turn_id: String,
        item_id: String,
        phase: Option<MessagePhase>,
        text: String,
    ) {
        if matches!(phase, Some(MessagePhase::Commentary)) {
            self.items.retain(|item| {
                !(item.thread_id == thread_id && item.turn_id == turn_id && item.item_id == item_id)
            });
            return;
        }
        if let Some(item) = self.item_mut(&thread_id, &turn_id, &item_id) {
            item.text = truncate_output(text);
        } else if self
            .items
            .iter()
            .filter(|item| item.thread_id == thread_id && item.turn_id == turn_id)
            .count()
            < MAX_ASSISTANT_ITEMS_PER_TURN
        {
            self.items.push(OutputItem {
                thread_id,
                turn_id,
                item_id,
                text: truncate_output(text),
            });
        } else if let Some(item) = self
            .items
            .iter_mut()
            .rev()
            .find(|item| item.thread_id == thread_id && item.turn_id == turn_id)
        {
            force_truncation_notice(&mut item.text);
        }
    }

    pub fn finish_turn(&mut self, thread_id: &str, turn_id: &str) -> String {
        let mut completed = Vec::new();
        self.items.retain(|item| {
            if item.thread_id == thread_id && item.turn_id == turn_id {
                completed.push(item.text.clone());
                false
            } else {
                true
            }
        });
        truncate_output(completed.concat())
    }

    pub fn turn_text(&self, thread_id: &str, turn_id: &str) -> String {
        truncate_output(
            self.items
                .iter()
                .filter(|item| item.thread_id == thread_id && item.turn_id == turn_id)
                .map(|item| item.text.as_str())
                .collect(),
        )
    }

    fn item_mut(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Option<&mut OutputItem> {
        self.items.iter_mut().find(|item| {
            item.thread_id == thread_id && item.turn_id == turn_id && item.item_id == item_id
        })
    }
}

fn append_bounded(output: &mut String, delta: &str) {
    if output.ends_with(TRUNCATION_NOTICE) {
        return;
    }
    let current = output.chars().count();
    let incoming = delta.chars().count();
    if current.saturating_add(incoming) <= MAX_ASSISTANT_OUTPUT_CHARS {
        output.push_str(delta);
        return;
    }
    let keep = truncation_content_limit();
    if current <= keep {
        output.extend(delta.chars().take(keep - current));
    }
    force_truncation_notice(output);
}

fn force_truncation_notice(output: &mut String) {
    if output.ends_with(TRUNCATION_NOTICE) {
        return;
    }
    let keep = truncation_content_limit();
    if output.chars().count() > keep {
        *output = output.chars().take(keep).collect();
    }
    output.push_str(TRUNCATION_NOTICE);
}

fn truncation_content_limit() -> usize {
    MAX_ASSISTANT_OUTPUT_CHARS.saturating_sub(TRUNCATION_NOTICE.chars().count())
}

fn truncate_output(output: String) -> String {
    if output.chars().count() <= MAX_ASSISTANT_OUTPUT_CHARS {
        return output;
    }
    let keep = truncation_content_limit();
    let mut truncated = output.chars().take(keep).collect::<String>();
    truncated.push_str(TRUNCATION_NOTICE);
    truncated
}

/// Splits text by Unicode scalar values, preferring paragraph, line, then word boundaries.
pub fn chunk_text(text: &str, target_chars: usize) -> Vec<String> {
    if text.is_empty() || target_chars == 0 {
        return Vec::new();
    }
    let mut remaining = text.trim();
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        if remaining.chars().count() <= target_chars {
            chunks.push(remaining.to_string());
            break;
        }
        let boundary = char_boundary_at(remaining, target_chars);
        let candidate = &remaining[..boundary];
        let split = candidate
            .rfind("\n\n")
            .map(|index| index + 2)
            .or_else(|| candidate.rfind('\n').map(|index| index + 1))
            .or_else(|| candidate.rfind(char::is_whitespace).map(|index| index + 1))
            .filter(|index| *index > 0)
            .unwrap_or(boundary);
        chunks.push(remaining[..split].trim_end().to_string());
        remaining = remaining[split..].trim_start();
    }
    chunks
}

fn char_boundary_at(value: &str, max_chars: usize) -> usize {
    value
        .char_indices()
        .nth(max_chars)
        .map_or(value.len(), |(index, _)| index)
}

pub fn labelled_chunks(text: &str, target_chars: usize) -> Vec<String> {
    const LABEL_RESERVE: usize = 32;
    let chunks = chunk_text(text, target_chars.min(4096 - LABEL_RESERVE));
    if chunks.len() <= 1 {
        return chunks
            .into_iter()
            .map(|chunk| format!("[codex] {chunk}"))
            .collect();
    }
    let total = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| format!("[codex {}/{}] {chunk}", index + 1, total))
        .collect()
}

#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
