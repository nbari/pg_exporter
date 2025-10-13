use once_cell::sync::OnceCell;
use std::sync::Arc;

/// Global holder for excluded databases, set once at startup via CLI/env.
static EXCLUDED: OnceCell<Arc<[String]>> = OnceCell::new();

/// Set the excluded databases from CLI/env. Call this once during startup.
pub fn set_excluded_databases(list: Vec<String>) {
    // Normalize: trim and drop empties, de-dup while preserving order (optional).
    let mut cleaned: Vec<String> = list
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Optional: stable de-dup
    cleaned.dedup();

    // Ignore if already set to avoid panics during multi-init (e.g., tests)
    let _ = EXCLUDED.set(Arc::from(cleaned));
}

/// Get the excluded databases as a static slice.
pub fn get_excluded_databases() -> &'static [String] {
    match EXCLUDED.get() {
        Some(arc) => &arc[..],
        None => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        set_excluded_databases(vec![
            "postgres".into(),
            "template0".into(),
            "template0".into(),
            " ".into(),
        ]);

        let got = get_excluded_databases();

        assert_eq!(got, &["postgres".to_string(), "template0".to_string()]);
    }
}
