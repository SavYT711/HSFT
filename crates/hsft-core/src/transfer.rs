//! The transfer protocol.
//!
//! The sender offers a manifest on a control stream, and the receiver answers with the units it
//! already holds. Units then flow over several parallel streams, each hashed by the sender and
//! checked by the receiver. Damaged units are requested again. At the end both sides compare a hash
//! over all units before anything is moved to its real name.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Notify};
use tokio::task::{spawn_blocking, JoinSet};

use crate::codec;
use crate::disk::{root_hash, Hash, Partial, Source};
use crate::plan::{Manifest, Scanned};
use crate::wire::{from_runs, read_msg, to_runs, write_msg, Control, FrameHeader, HEADER_LEN};

const STREAMS: usize = 8;
const MAX_DAMAGED_UNITS: u32 = 32;

#[derive(Debug, Default)]
pub struct Progress {
    pub done: AtomicU64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendResult {
    Delivered,
    Declined(String),
}

type Hashes = Arc<Mutex<Vec<Option<Hash>>>>;

/// Sends the scanned files and returns once the friend has saved them.
pub async fn send(
    conn: &Connection,
    scanned: Scanned,
    progress: Arc<Progress>,
) -> Result<SendResult> {
    let Scanned { manifest, sources } = scanned;
    let (mut control_out, mut control_in) = conn.open_bi().await?;
    write_msg(&mut control_out, &Control::Offer { manifest: manifest.clone() }).await?;

    let have = match read_msg(&mut control_in).await? {
        Control::Accept { have } => have,
        Control::Decline { reason } => return Ok(SendResult::Declined(reason)),
        other => bail!("unexpected reply: {other:?}"),
    };

    let source = Arc::new(Source::new(&manifest, sources));
    let have = from_runs(&have, source.units.len())?;
    let hashes: Hashes = Arc::new(Mutex::new(vec![None; source.units.len()]));
    let (queue_tx, queue_rx) = mpsc::unbounded_channel();
    for (unit, held) in have.iter().enumerate() {
        if *held {
            progress.done.fetch_add(source.units[unit].len as u64, Ordering::Relaxed);
        } else {
            queue_tx.send(unit as u32)?;
        }
    }

    let queue_rx = Arc::new(tokio::sync::Mutex::new(queue_rx));
    let mut workers = JoinSet::new();
    for _ in 0..STREAMS {
        workers.spawn(send_stream(
            conn.clone(),
            source.clone(),
            hashes.clone(),
            queue_rx.clone(),
            progress.clone(),
        ));
    }

    let mut damaged = 0;
    let their_root = loop {
        tokio::select! {
            msg = read_msg(&mut control_in) => match msg? {
                Control::Resend { unit } => {
                    let Some(info) = source.units.get(unit as usize) else {
                        bail!("peer asked for a unit that doesn't exist");
                    };
                    damaged += 1;
                    if damaged > MAX_DAMAGED_UNITS {
                        bail!("too many units arrived damaged");
                    }
                    progress.done.fetch_sub(info.len as u64, Ordering::Relaxed);
                    queue_tx.send(unit)?;
                }
                Control::Done { root } => break root,
                other => bail!("unexpected message: {other:?}"),
            },
            Some(result) = workers.join_next() => result??,
        }
    };
    drop(queue_tx);
    while let Some(result) = workers.join_next().await {
        result??;
    }

    let root = {
        let source = source.clone();
        spawn_blocking(move || our_root(&source, &hashes)).await??
    };
    let ok = hex::encode(root) == their_root;
    write_msg(&mut control_out, &Control::Verdict { ok }).await?;
    if !ok {
        bail!("the received files didn't match; the partial copy was discarded");
    }
    match read_msg(&mut control_in).await? {
        Control::Saved => Ok(SendResult::Delivered),
        other => bail!("unexpected message: {other:?}"),
    }
}

async fn send_stream(
    conn: Connection,
    source: Arc<Source>,
    hashes: Hashes,
    queue: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<u32>>>,
    progress: Arc<Progress>,
) -> Result<()> {
    let mut stream: Option<SendStream> = None;
    loop {
        let Some(unit) = queue.lock().await.recv().await else {
            break;
        };
        let reader = source.clone();
        let (raw_len, hash, flags, body) = spawn_blocking(move || -> Result<_> {
            let raw = reader.read_unit(unit)?;
            let hash = *blake3::hash(&raw).as_bytes();
            let raw_len = raw.len() as u32;
            let (flags, body) = codec::encode(raw);
            Ok((raw_len, hash, flags, body))
        })
        .await??;
        hashes.lock().unwrap()[unit as usize] = Some(hash);

        let header = FrameHeader {
            unit,
            flags,
            raw_len,
            wire_len: body.len() as u32,
            hash,
        };
        let out = match &mut stream {
            Some(out) => out,
            None => stream.insert(conn.open_uni().await?),
        };
        out.write_all(&header.to_bytes()).await?;
        out.write_all(&body).await?;
        progress.done.fetch_add(raw_len as u64, Ordering::Relaxed);
    }
    if let Some(mut out) = stream {
        out.finish()?;
    }
    Ok(())
}

/// Finishes the hash over all units, reading the ones the friend already had from an earlier try.
fn our_root(source: &Source, hashes: &Mutex<Vec<Option<Hash>>>) -> Result<Hash> {
    let missing: Vec<u32> = {
        let hashes = hashes.lock().unwrap();
        (0..hashes.len() as u32)
            .filter(|&unit| hashes[unit as usize].is_none())
            .collect()
    };
    let next = AtomicUsize::new(0);
    let workers = std::thread::available_parallelism().map_or(4, |n| n.get());
    std::thread::scope(|scope| {
        let tasks: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| -> Result<()> {
                    while let Some(&unit) = missing.get(next.fetch_add(1, Ordering::Relaxed)) {
                        let raw = source.read_unit(unit)?;
                        hashes.lock().unwrap()[unit as usize] = Some(*blake3::hash(&raw).as_bytes());
                    }
                    Ok(())
                })
            })
            .collect();
        tasks
            .into_iter()
            .try_for_each(|task| task.join().expect("hashing thread panicked"))
    })?;
    let hashes = hashes.lock().unwrap();
    Ok(root_hash(hashes.iter().flatten()))
}

/// An offer that has arrived and is waiting for a yes or no.
pub struct Incoming {
    pub manifest: Arc<Manifest>,
    control_out: SendStream,
    control_in: RecvStream,
}

/// Reads and validates the sender's offer.
pub async fn read_offer(conn: &Connection) -> Result<Incoming> {
    let (control_out, mut control_in) = conn.accept_bi().await?;
    let Control::Offer { manifest } = read_msg(&mut control_in).await? else {
        bail!("expected an offer");
    };
    manifest.validate()?;
    Ok(Incoming {
        manifest: Arc::new(manifest),
        control_out,
        control_in,
    })
}

impl Incoming {
    pub async fn decline(mut self, reason: &str) -> Result<()> {
        write_msg(
            &mut self.control_out,
            &Control::Decline { reason: reason.into() },
        )
        .await
    }

    /// Receives the files into `dest`, and returns where they were saved. `key` names the partial
    /// data, so the same offer from the same friend resumes where it stopped.
    pub async fn accept(
        mut self,
        conn: &Connection,
        dest: std::path::PathBuf,
        key: String,
        progress: Arc<Progress>,
    ) -> Result<std::path::PathBuf> {
        let total = self.manifest.total_bytes();
        std::fs::create_dir_all(&dest)
            .with_context(|| format!("can't create {}", dest.display()))?;
        let manifest = self.manifest.clone();
        let opened_dest = dest.clone();
        let partial = spawn_blocking(move || Partial::open(&opened_dest, &key, manifest)).await??;
        let partial = Arc::new(partial);

        let have = partial.have();
        if have.is_empty() && fs4::available_space(&dest).is_ok_and(|free| free < total) {
            partial.discard();
            self.decline("not enough disk space").await?;
            bail!("not enough disk space in {}", dest.display());
        }
        progress.done.store(partial.bytes_held(), Ordering::Relaxed);
        write_msg(&mut self.control_out, &Control::Accept { have: to_runs(&have) }).await?;

        let complete = Arc::new(Notify::new());
        let (resend_tx, mut resend_rx) = mpsc::unbounded_channel();
        let mut streams = JoinSet::new();
        while !partial.is_complete() {
            tokio::select! {
                _ = complete.notified() => {}
                stream = conn.accept_uni() => {
                    streams.spawn(receive_stream(
                        stream?,
                        partial.clone(),
                        resend_tx.clone(),
                        complete.clone(),
                        progress.clone(),
                    ));
                }
                Some(result) = streams.join_next() => result??,
                Some(unit) = resend_rx.recv() => {
                    write_msg(&mut self.control_out, &Control::Resend { unit }).await?;
                }
            }
        }

        let root = partial.root()?;
        write_msg(&mut self.control_out, &Control::Done { root: hex::encode(root) }).await?;
        match read_msg(&mut self.control_in).await? {
            Control::Verdict { ok: true } => {}
            Control::Verdict { ok: false } => {
                partial.discard();
                bail!("the received files didn't match and were discarded");
            }
            other => bail!("unexpected message: {other:?}"),
        }
        let saved = spawn_blocking(move || partial.finalize()).await??;
        write_msg(&mut self.control_out, &Control::Saved).await?;
        // Keep the connection up until the sender has read the last message.
        let _ = self.control_out.finish();
        conn.closed().await;
        Ok(saved)
    }
}

async fn receive_stream(
    mut stream: RecvStream,
    partial: Arc<Partial>,
    resend: mpsc::UnboundedSender<u32>,
    complete: Arc<Notify>,
    progress: Arc<Progress>,
) -> Result<()> {
    let mut header = [0u8; HEADER_LEN];
    loop {
        // A clean end of stream shows up as a read error on the first header byte.
        if AsyncReadExt::read_exact(&mut stream, &mut header).await.is_err() {
            return Ok(());
        }
        let header = FrameHeader::from_bytes(&header);
        let unit = partial
            .units()
            .get(header.unit as usize)
            .context("peer sent a unit that doesn't exist")?;
        if header.raw_len != unit.len || header.wire_len > unit.len {
            bail!("peer sent a unit with the wrong size");
        }
        let mut body = vec![0u8; header.wire_len as usize];
        AsyncReadExt::read_exact(&mut stream, &mut body).await?;

        let store = partial.clone();
        let done = spawn_blocking(move || -> Result<Option<bool>> {
            let raw = match codec::decode(header.flags, body, header.raw_len as usize) {
                Ok(raw) => raw,
                Err(_) => return Ok(None),
            };
            if *blake3::hash(&raw).as_bytes() != header.hash {
                return Ok(None);
            }
            store.store(header.unit, &raw, header.hash).map(Some)
        })
        .await??;

        match done {
            None => {
                tracing::warn!(unit = header.unit, "unit arrived damaged");
                resend.send(header.unit)?;
            }
            Some(finished) => {
                progress.done.fetch_add(header.raw_len as u64, Ordering::Relaxed);
                if finished {
                    complete.notify_one();
                }
            }
        }
    }
}
