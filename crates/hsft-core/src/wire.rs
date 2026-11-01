//! Messages exchanged between two peers.

use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::disk::Hash;
use crate::plan::Manifest;

const MAX_MESSAGE: usize = 512 * 1024 * 1024;

pub const HEADER_LEN: usize = 45;

/// Sent on the control stream of a transfer.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Control {
    Offer { manifest: Manifest },
    Accept { have: Vec<(u32, u32)> },
    Decline { reason: String },
    Resend { unit: u32 },
    Done { root: String },
    Verdict { ok: bool },
    Saved,
}

pub async fn write_msg<W, T>(writer: &mut W, msg: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = serde_json::to_vec(msg)?;
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

pub async fn read_msg<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = reader.read_u32().await? as usize;
    if len > MAX_MESSAGE {
        bail!("message is too large");
    }
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Comes before each unit on a data stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub unit: u32,
    pub flags: u8,
    pub raw_len: u32,
    pub wire_len: u32,
    pub hash: Hash,
}

impl FrameHeader {
    pub fn to_bytes(self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0..4].copy_from_slice(&self.unit.to_le_bytes());
        out[4] = self.flags;
        out[5..9].copy_from_slice(&self.raw_len.to_le_bytes());
        out[9..13].copy_from_slice(&self.wire_len.to_le_bytes());
        out[13..45].copy_from_slice(&self.hash);
        out
    }

    pub fn from_bytes(bytes: &[u8; HEADER_LEN]) -> Self {
        Self {
            unit: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            flags: bytes[4],
            raw_len: u32::from_le_bytes(bytes[5..9].try_into().unwrap()),
            wire_len: u32::from_le_bytes(bytes[9..13].try_into().unwrap()),
            hash: bytes[13..45].try_into().unwrap(),
        }
    }
}

/// Packs a sorted list of unit numbers into `(start, count)` runs.
pub fn to_runs(units: &[u32]) -> Vec<(u32, u32)> {
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for &unit in units {
        match runs.last_mut() {
            Some((start, count)) if *start + *count == unit => *count += 1,
            _ => runs.push((unit, 1)),
        }
    }
    runs
}

/// Expands runs into per-unit flags, refusing anything outside the plan.
pub fn from_runs(runs: &[(u32, u32)], unit_count: usize) -> Result<Vec<bool>> {
    let mut have = vec![false; unit_count];
    for &(start, count) in runs {
        let end = start as usize + count as usize;
        if end > unit_count {
            bail!("peer listed a unit that doesn't exist");
        }
        have[start as usize..end].fill(true);
    }
    Ok(have)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_header_round_trips() {
        let header = FrameHeader {
            unit: 7,
            flags: 1,
            raw_len: 4096,
            wire_len: 100,
            hash: [9; 32],
        };
        assert_eq!(FrameHeader::from_bytes(&header.to_bytes()), header);
    }

    #[test]
    fn runs_round_trip() {
        let runs = to_runs(&[0, 1, 2, 5, 9, 10]);
        assert_eq!(runs, vec![(0, 3), (5, 1), (9, 2)]);
        let have = from_runs(&runs, 11).unwrap();
        assert_eq!(have.iter().filter(|h| **h).count(), 6);
        assert!(have[10] && !have[3]);
    }

    #[test]
    fn runs_outside_the_plan_are_refused() {
        assert!(from_runs(&[(u32::MAX, u32::MAX)], 10).is_err());
        assert!(from_runs(&[(9, 2)], 10).is_err());
    }
}
