//! WhatsApp-safe assistant-output aggregation and chunking.

use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct OutputAggregator {
    items: HashMap<(String, String, String), String>,
}

impl OutputAggregator {
    pub fn push_delta(&mut self, thread_id: String, turn_id: String, item_id: String, delta: &str) {
        self.items
            .entry((thread_id, turn_id, item_id))
            .or_default()
            .push_str(delta);
    }

    pub fn finish_turn(&mut self, thread_id: &str, turn_id: &str) -> String {
        let mut completed = self
            .items
            .extract_if(|(thread, turn, _), _| thread == thread_id && turn == turn_id)
            .map(|(_, text)| text)
            .collect::<Vec<_>>();
        completed.sort();
        completed.concat()
    }
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
    let chunks = chunk_text(text, target_chars);
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
mod tests {
    use super::*;

    #[test]
    fn chunks_unicode_without_exceeding_limit() {
        let chunks = chunk_text("one 😀 two\n\nthree four", 8);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 8));
        assert_eq!(chunks.concat().replace(' ', ""), "one😀twothreefour");
    }
}
