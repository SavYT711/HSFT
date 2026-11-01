//! Per-unit compression, skipped for data that won't shrink such as video or archives.

use anyhow::{bail, Context, Result};

pub const FLAG_ZSTD: u8 = 1;

const LEVEL: i32 = 1;
const SAMPLE: usize = 64 * 1024;

/// Compresses a unit if a sample of it suggests that's worthwhile. Returns the flags and the body.
pub fn encode(raw: Vec<u8>) -> (u8, Vec<u8>) {
    if !looks_compressible(&raw) {
        return (0, raw);
    }
    match zstd::bulk::compress(&raw, LEVEL) {
        Ok(packed) if packed.len() < raw.len() => (FLAG_ZSTD, packed),
        _ => (0, raw),
    }
}

fn looks_compressible(raw: &[u8]) -> bool {
    if raw.len() <= SAMPLE {
        return true;
    }
    let start = (raw.len() - SAMPLE) / 2;
    let sample = &raw[start..start + SAMPLE];
    zstd::bulk::compress(sample, LEVEL).is_ok_and(|packed| packed.len() * 10 < sample.len() * 9)
}

/// Restores a unit. `raw_len` comes from the plan, so hostile input can't make this allocate more.
pub fn decode(flags: u8, body: Vec<u8>, raw_len: usize) -> Result<Vec<u8>> {
    match flags {
        0 => Ok(body),
        FLAG_ZSTD => {
            let raw = zstd::bulk::decompress(&body, raw_len).context("bad compressed data")?;
            if raw.len() != raw_len {
                bail!("compressed data has the wrong size");
            }
            Ok(raw)
        }
        _ => bail!("unknown unit flags"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_compresses_and_round_trips() {
        let raw = b"hello hello hello ".repeat(100_000);
        let (flags, body) = encode(raw.clone());
        assert_eq!(flags, FLAG_ZSTD);
        assert!(body.len() < raw.len() / 10);
        assert_eq!(decode(flags, body, raw.len()).unwrap(), raw);
    }

    #[test]
    fn random_data_is_left_alone() {
        let mut raw = vec![0u8; 1024 * 1024];
        blake3::Hasher::new()
            .update(b"seed")
            .finalize_xof()
            .fill(&mut raw);
        let (flags, body) = encode(raw.clone());
        assert_eq!(flags, 0);
        assert_eq!(body, raw);
    }

    #[test]
    fn oversized_output_is_refused() {
        let raw = vec![0u8; 1024 * 1024];
        let (flags, body) = encode(raw);
        assert!(decode(flags, body, 1024).is_err());
    }

    #[test]
    fn unknown_flags_are_refused() {
        assert!(decode(9, vec![1, 2, 3], 3).is_err());
    }
}
