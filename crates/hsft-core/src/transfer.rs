//! The transfer protocol.
//!
//! The sender offers a manifest on a control stream, and the receiver answers with the units it
//! already holds. Units then flow over several parallel streams, each hashed by the sender and
//! checked by the receiver. Damaged units are requested again. At the end both sides compare a hash
//! over all units before anything is moved to its real name.
//!
//! Two things can add speed on top:
//!
//! - **Extra connection.** The sender opens a second connection through another network adapter,
//!   such as Wi-Fi joined to a phone hotspot, and its streams take units from the same queue.
//! - **Swarm.** When several friends receive the same files, each is sent its own share of units
//!   first and the rest last, in reverse. Friends connect to each other and pass on what the other
//!   is missing, so each friend's upload carries pieces too, and they report what they got so the
//!   sender can skip it.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Notify};
use tokio::task::{spawn_blocking, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::codec;
use crate::disk::{root_hash, Hash, Partial, Source};
use crate::plan::{Manifest, Scanned};
use crate::wire::{
    from_runs, read_msg, to_runs, write_msg, Control, FrameHeader, JoinRequest, SwarmInfo,
    HEADER_LEN,
};

pub const EXTRA_ALPN: &[u8] = b"hsft/extra/1";
pub const SWARM_ALPN: &[u8] = b"hsft/swarm/1";

const STREAMS: usize = 16;
/// Fewer, because each stream holds a whole unit and the extra connection is often the slower one.
const EXTRA_STREAMS: usize = 4;
const PEER_STREAMS: usize = 4;
const MAX_DAMAGED_UNITS: u32 = 32;
/// Progress is counted in steps much smaller than a unit, so a slow link never looks frozen.
const PROGRESS_STEP: usize = 64 * 1024;
const EXTRA_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// An extra connection that can't go direct would only borrow the main line through a relay.
const EXTRA_DIRECT_WAIT: Duration = Duration::from_secs(10);
const GOT_INTERVAL: Duration = Duration::from_millis(500);
const HAVE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
pub struct Progress {
    pub done: AtomicU64,
    /// Bytes carried by the extra connection.
    pub extra: AtomicU64,
    pub extra_active: AtomicBool,
    /// Bytes that came from other friends in a swarm.
    pub from_peers: AtomicU64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendResult {
    Delivered,
    Declined(String),
}

/// Caps upload speed, to measure how transfers behave on a slower connection.
#[derive(Debug)]
pub struct Limiter {
    bytes_per_sec: f64,
    next: Mutex<std::time::Instant>,
}

impl Limiter {
    pub fn new(megabits_per_sec: f64) -> Arc<Self> {
        Arc::new(Self {
            bytes_per_sec: megabits_per_sec * 1_000_000.0 / 8.0,
            next: Mutex::new(std::time::Instant::now()),
        })
    }

    async fn take(&self, bytes: usize) {
        let wait = {
            let mut next = self.next.lock().unwrap();
            let now = std::time::Instant::now();
            let start = (*next).max(now);
            *next = start + Duration::from_secs_f64(bytes as f64 / self.bytes_per_sec);
            start.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Where a connection's units come from, which decides how they are counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Sender,
    Extra,
    Peer,
}

/// Returns how a connection currently reaches the other side.
pub fn route(conn: &Connection) -> &'static str {
    match conn.paths().iter().find(|path| path.is_selected()) {
        Some(path) if path.is_relay() => "relay",
        Some(_) => "direct",
        None => "connecting",
    }
}

/// Files being sent, shared by everyone they are sent to.
pub struct Outgoing {
    pub manifest: Manifest,
    source: Source,
    hashes: Mutex<Vec<Option<Hash>>>,
}

impl Outgoing {
    pub fn new(scanned: Scanned) -> Arc<Self> {
        let Scanned { manifest, sources } = scanned;
        let source = Source::new(&manifest, sources);
        let hashes = Mutex::new(vec![None; source.units.len()]);
        Arc::new(Self {
            manifest,
            source,
            hashes,
        })
    }

    fn encode(&self, unit: u32) -> Result<(FrameHeader, Vec<u8>)> {
        let raw = self.source.read_unit(unit)?;
        let hash = *blake3::hash(&raw).as_bytes();
        self.hashes.lock().unwrap()[unit as usize] = Some(hash);
        Ok(frame(unit, raw, hash))
    }

    /// Finishes the hash over all units, reading any this side never had to send.
    fn root(&self) -> Result<Hash> {
        let missing: Vec<u32> = {
            let hashes = self.hashes.lock().unwrap();
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
                            let raw = self.source.read_unit(unit)?;
                            self.hashes.lock().unwrap()[unit as usize] =
                                Some(*blake3::hash(&raw).as_bytes());
                        }
                        Ok(())
                    })
                })
                .collect();
            tasks
                .into_iter()
                .try_for_each(|task| task.join().expect("hashing thread panicked"))
        })?;
        let hashes = self.hashes.lock().unwrap();
        Ok(root_hash(hashes.iter().flatten()))
    }
}

fn frame(unit: u32, raw: Vec<u8>, hash: Hash) -> (FrameHeader, Vec<u8>) {
    let raw_len = raw.len() as u32;
    let (flags, body) = codec::encode(raw);
    let header = FrameHeader {
        unit,
        flags,
        raw_len,
        wire_len: body.len() as u32,
        hash,
    };
    (header, body)
}

/// A second connection through another network adapter.
pub struct ExtraLink {
    pub endpoint: Endpoint,
    pub addr: EndpointAddr,
}

#[derive(Default)]
pub struct SendOptions {
    pub swarm: Option<SwarmInfo>,
    pub extra: Option<ExtraLink>,
    pub limit: Option<Arc<Limiter>>,
    pub extra_limit: Option<Arc<Limiter>>,
}

/// Units still to send to one receiver, shared by every stream that sends to them.
struct WorkQueue {
    state: Mutex<QueueState>,
    ready: Notify,
    lens: Vec<u32>,
}

struct QueueState {
    items: VecDeque<u32>,
    /// The receiver has these, so they are skipped.
    held: Vec<bool>,
    /// Already included in the progress count.
    counted: Vec<bool>,
    /// Units being sent right now, oldest first, with the connection sending them and whether a
    /// second copy is already under way.
    in_flight: Vec<InFlight>,
    closed: bool,
}

struct InFlight {
    unit: u32,
    extra: bool,
    copied: bool,
}

impl WorkQueue {
    fn new(order: Vec<u32>, held: Vec<bool>, lens: Vec<u32>) -> Self {
        let counted = held.clone();
        Self {
            state: Mutex::new(QueueState {
                items: order.into(),
                held,
                counted,
                in_flight: Vec::new(),
                closed: false,
            }),
            ready: Notify::new(),
            lens,
        }
    }

    /// Hands out the next unit to send. Once none are left, a stream that would sit idle sends a
    /// second copy of the oldest unit still under way on the *other* connection, so a slow extra
    /// connection never holds up the end of a transfer. Whichever copy arrives first is kept.
    async fn pop(&self, extra: bool) -> Option<u32> {
        loop {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                while let Some(unit) = state.items.pop_front() {
                    if !state.held[unit as usize] {
                        state.in_flight.push(InFlight {
                            unit,
                            extra,
                            copied: false,
                        });
                        return Some(unit);
                    }
                }
                if state.closed {
                    return None;
                }
                let QueueState { held, in_flight, .. } = &mut *state;
                if let Some(entry) = in_flight
                    .iter_mut()
                    .find(|e| e.extra != extra && !e.copied && !held[e.unit as usize])
                {
                    entry.copied = true;
                    return Some(entry.unit);
                }
            }
            ready.await;
        }
    }

    /// Puts a unit back at the front, after it was lost or arrived damaged.
    fn retry(&self, unit: u32) {
        let mut state = self.state.lock().unwrap();
        state.held[unit as usize] = false;
        state.in_flight.retain(|e| e.unit != unit);
        state.items.push_front(unit);
        drop(state);
        self.ready.notify_waiters();
    }

    fn sent(&self, unit: u32) {
        let mut state = self.state.lock().unwrap();
        state.counted[unit as usize] = true;
        state.held[unit as usize] = true;
        state.in_flight.retain(|e| e.unit != unit);
        drop(state);
        self.ready.notify_waiters();
    }

    /// Notes units the receiver got elsewhere, and returns their bytes not yet counted.
    fn got(&self, units: &[bool]) -> u64 {
        let mut state = self.state.lock().unwrap();
        let mut added = 0;
        for (unit, &got) in units.iter().enumerate() {
            if got {
                state.held[unit] = true;
                if !state.counted[unit] {
                    state.counted[unit] = true;
                    added += self.lens[unit] as u64;
                }
            }
        }
        added
    }

    fn uncount(&self, unit: u32) {
        self.state.lock().unwrap().counted[unit as usize] = false;
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.ready.notify_waiters();
    }
}

/// The order units go out in. Swarm members get their own share first and the rest last, in
/// reverse, so the sender's units and the ones passed between friends rarely overlap.
fn send_order(units: usize, swarm: Option<&SwarmInfo>) -> Vec<u32> {
    let units = units as u32;
    let Some(swarm) = swarm else {
        return (0..units).collect();
    };
    let (index, members) = (swarm.index, swarm.members.len().max(1) as u32);
    let mut order: Vec<u32> = (0..units).filter(|u| u % members == index).collect();
    order.extend((0..units).rev().filter(|u| u % members != index));
    order
}

/// Sends files to one friend and returns once they have saved them.
pub async fn send(
    conn: &Connection,
    outgoing: Arc<Outgoing>,
    options: SendOptions,
    progress: Arc<Progress>,
) -> Result<SendResult> {
    let (mut control_out, control_in) = conn.open_bi().await?;
    let offer = Control::Offer {
        manifest: outgoing.manifest.clone(),
        swarm: options.swarm.clone(),
    };
    write_msg(&mut control_out, &offer).await?;
    let mut messages = read_messages(control_in);
    let mut next_message = async || messages.recv().await.context("connection lost")?;

    let (have, token) = match next_message().await? {
        Control::Accept { have, token } => (have, token),
        Control::Decline { reason } => return Ok(SendResult::Declined(reason)),
        other => bail!("unexpected reply: {other:?}"),
    };

    let units = &outgoing.source.units;
    let held = from_runs(&have, units.len())?;
    let lens: Vec<u32> = units.iter().map(|u| u.len).collect();
    let already: u64 = (0..units.len()).filter(|&u| held[u]).map(|u| lens[u] as u64).sum();
    progress.done.fetch_add(already, Ordering::Relaxed);
    let order = send_order(units.len(), options.swarm.as_ref());
    let queue = Arc::new(WorkQueue::new(order, held, lens));

    let mut workers = JoinSet::new();
    for _ in 0..STREAMS {
        let (conn, outgoing, queue, progress) =
            (conn.clone(), outgoing.clone(), queue.clone(), progress.clone());
        let limit = options.limit.clone();
        workers.spawn(send_stream(conn, outgoing, queue, progress, false, limit));
    }
    let mut extras = JoinSet::new();
    if let Some(link) = options.extra.filter(|_| !token.is_empty()) {
        let (outgoing, queue, progress) = (outgoing.clone(), queue.clone(), progress.clone());
        let limit = options.extra_limit.clone();
        extras.spawn(run_extra(link, token, outgoing, queue, progress, limit));
    }

    let mut damaged = 0;
    let their_root = loop {
        tokio::select! {
            msg = next_message() => match msg? {
                Control::Resend { unit } => {
                    let Some(info) = units.get(unit as usize) else {
                        bail!("peer asked for a unit that doesn't exist");
                    };
                    damaged += 1;
                    if damaged > MAX_DAMAGED_UNITS {
                        bail!("too many units arrived damaged");
                    }
                    progress.done.fetch_sub(info.len as u64, Ordering::Relaxed);
                    queue.uncount(unit);
                    queue.retry(unit);
                }
                Control::Got { units: got } => {
                    let got = from_runs(&got, units.len())?;
                    progress.done.fetch_add(queue.got(&got), Ordering::Relaxed);
                }
                Control::Done { root } => break root,
                other => bail!("unexpected message: {other:?}"),
            },
            Some(result) = workers.join_next() => result??,
            Some(result) = extras.join_next() => {
                if let Ok(Err(error)) = result {
                    tracing::info!("extra connection stopped: {error:#}");
                }
            }
        }
    };
    // The receiver has everything, so anything still being sent is a spare copy. Waiting for it
    // could hang, because the receiver no longer takes new streams.
    queue.close();
    workers.abort_all();
    extras.abort_all();

    let root = {
        let outgoing = outgoing.clone();
        spawn_blocking(move || outgoing.root()).await??
    };
    let ok = hex::encode(root) == their_root;
    write_msg(&mut control_out, &Control::Verdict { ok }).await?;
    if !ok {
        bail!("the received files didn't match; the partial copy was discarded");
    }
    match next_message().await? {
        Control::Saved => Ok(SendResult::Delivered),
        other => bail!("unexpected message: {other:?}"),
    }
}

/// Reads control messages in a task of their own. Reading inside `select!` could stop halfway
/// through a message when another branch wins, and lose the rest of it.
fn read_messages(mut input: RecvStream) -> mpsc::UnboundedReceiver<Result<Control>> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let msg = read_msg(&mut input).await;
            let failed = msg.is_err();
            if tx.send(msg).is_err() || failed {
                break;
            }
        }
    });
    rx
}

async fn send_stream(
    conn: Connection,
    outgoing: Arc<Outgoing>,
    queue: Arc<WorkQueue>,
    progress: Arc<Progress>,
    extra: bool,
    limit: Option<Arc<Limiter>>,
) -> Result<()> {
    let mut stream: Option<SendStream> = None;
    while let Some(unit) = queue.pop(extra).await {
        let sent = async {
            let reader = outgoing.clone();
            let (header, body) = spawn_blocking(move || reader.encode(unit)).await??;
            let out = match &mut stream {
                Some(out) => out,
                None => stream.insert(conn.open_uni().await?),
            };
            let counters: &[&AtomicU64] = if extra {
                &[&progress.done, &progress.extra]
            } else {
                &[&progress.done]
            };
            write_frame(out, header, &body, counters, limit.as_deref()).await
        }
        .await;
        if let Err(error) = sent {
            // Another stream, or the main connection, will deliver it instead.
            queue.retry(unit);
            return Err(error);
        }
        queue.sent(unit);
    }
    if let Some(mut out) = stream {
        out.finish()?;
    }
    Ok(())
}

async fn write_frame(
    out: &mut SendStream,
    header: FrameHeader,
    body: &[u8],
    counters: &[&AtomicU64],
    limit: Option<&Limiter>,
) -> Result<()> {
    out.write_all(&header.to_bytes()).await?;
    let mut counted = 0;
    let mut written = 0;
    for slice in body.chunks(PROGRESS_STEP) {
        if let Some(limit) = limit {
            limit.take(slice.len()).await;
        }
        out.write_all(slice).await?;
        written += slice.len() as u64;
        let raw_so_far = header.raw_len as u64 * written / body.len() as u64;
        for counter in counters {
            counter.fetch_add(raw_so_far - counted, Ordering::Relaxed);
        }
        counted = raw_so_far;
    }
    Ok(())
}

/// Opens the extra connection and sends from the shared queue until it runs dry.
async fn run_extra(
    link: ExtraLink,
    token: String,
    outgoing: Arc<Outgoing>,
    queue: Arc<WorkQueue>,
    progress: Arc<Progress>,
    limit: Option<Arc<Limiter>>,
) -> Result<()> {
    let conn = tokio::time::timeout(
        EXTRA_CONNECT_TIMEOUT,
        link.endpoint.connect(link.addr, EXTRA_ALPN),
    )
    .await
    .context("timed out")??;
    let direct = async {
        while route(&conn) != "direct" {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };
    if tokio::time::timeout(EXTRA_DIRECT_WAIT, direct).await.is_err() {
        conn.close(0u32.into(), b"no direct route");
        bail!("no direct route over the extra adapter");
    }
    let (mut out, _) = conn.open_bi().await?;
    write_msg(&mut out, &JoinRequest { token }).await?;

    progress.extra_active.store(true, Ordering::Relaxed);
    let mut workers = JoinSet::new();
    for _ in 0..EXTRA_STREAMS {
        let (conn, outgoing, queue, progress) =
            (conn.clone(), outgoing.clone(), queue.clone(), progress.clone());
        workers.spawn(send_stream(conn, outgoing, queue, progress, true, limit.clone()));
    }
    let mut result = Ok(());
    while let Some(joined) = workers.join_next().await {
        if let Ok(Err(error)) = joined {
            result = Err(error);
        }
    }
    progress.extra_active.store(false, Ordering::Relaxed);
    conn.close(0u32.into(), b"done");
    result
}

/// An offer that has arrived and is waiting for a yes or no.
pub struct Incoming {
    pub manifest: Arc<Manifest>,
    pub swarm: Option<SwarmInfo>,
    control_out: SendStream,
    control_in: RecvStream,
}

/// Reads and validates the sender's offer.
pub async fn read_offer(conn: &Connection) -> Result<Incoming> {
    let (control_out, mut control_in) = conn.accept_bi().await?;
    let Control::Offer { manifest, swarm } = read_msg(&mut control_in).await? else {
        bail!("expected an offer");
    };
    manifest.validate()?;
    if let Some(swarm) = &swarm {
        if swarm.index as usize >= swarm.members.len() || swarm.members.len() > 64 {
            bail!("bad swarm");
        }
    }
    Ok(Incoming {
        manifest: Arc::new(manifest),
        swarm,
        control_out,
        control_in,
    })
}

/// What every connection carrying units for one incoming transfer shares.
pub struct Receiving {
    pub partial: Arc<Partial>,
    progress: Arc<Progress>,
    complete: Notify,
    /// Wakes anyone passing units on, whenever a new one is stored.
    stored: Notify,
    resend: mpsc::UnboundedSender<u32>,
    got: mpsc::UnboundedSender<u32>,
    /// Caps the pieces passed on to other swarm members, when measuring.
    limit: Option<Arc<Limiter>>,
    /// Cancelled when the transfer ends, which stops extra and swarm connections.
    pub closed: CancellationToken,
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
    /// data, so the same offer from the same friend resumes where it stopped. `register` is told
    /// the token for extra connections before the sender can use it.
    pub async fn accept(
        mut self,
        conn: &Connection,
        dest: PathBuf,
        key: String,
        progress: Arc<Progress>,
        limit: Option<Arc<Limiter>>,
        register: impl FnOnce(&str, Arc<Receiving>),
    ) -> Result<PathBuf> {
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

        let (resend_tx, mut resend_rx) = mpsc::unbounded_channel();
        let (got_tx, mut got_rx) = mpsc::unbounded_channel();
        let state = Arc::new(Receiving {
            partial: partial.clone(),
            progress,
            complete: Notify::new(),
            stored: Notify::new(),
            resend: resend_tx,
            got: got_tx,
            limit,
            closed: CancellationToken::new(),
        });
        let _closed = state.closed.clone().drop_guard();
        let token = hex::encode(rand::random::<[u8; 16]>());
        register(&token, state.clone());
        write_msg(
            &mut self.control_out,
            &Control::Accept { have: to_runs(&have), token },
        )
        .await?;

        let mut streams = JoinSet::new();
        let mut got = Vec::new();
        let mut tick = tokio::time::interval(GOT_INTERVAL);
        while !partial.is_complete() {
            tokio::select! {
                _ = state.complete.notified() => {}
                stream = conn.accept_uni() => {
                    streams.spawn(receive_stream(stream?, state.clone(), Origin::Sender));
                }
                Some(result) = streams.join_next() => result??,
                Some(unit) = resend_rx.recv() => {
                    write_msg(&mut self.control_out, &Control::Resend { unit }).await?;
                }
                Some(unit) = got_rx.recv() => got.push(unit),
                _ = tick.tick(), if !got.is_empty() => {
                    got.sort_unstable();
                    got.dedup();
                    let units = to_runs(&got);
                    got.clear();
                    write_msg(&mut self.control_out, &Control::Got { units }).await?;
                }
            }
        }

        tracing::info!(
            received = state.progress.done.load(Ordering::Relaxed),
            from_peers = state.progress.from_peers.load(Ordering::Relaxed),
            total,
            "all units stored"
        );
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
        state.closed.cancel();
        let saved = spawn_blocking(move || partial.finalize()).await??;
        tracing::info!("files saved");
        write_msg(&mut self.control_out, &Control::Saved).await?;
        // Keep the connection up until the sender has read the last message.
        let _ = self.control_out.finish();
        conn.closed().await;
        Ok(saved)
    }
}

/// Takes units from an extra or swarm connection until it closes. Failures here never fail the
/// transfer, since the sender still delivers anything missing.
pub async fn serve_streams(conn: Connection, state: Arc<Receiving>, origin: Origin) {
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            _ = state.closed.cancelled() => break,
            stream = conn.accept_uni() => match stream {
                Ok(stream) => {
                    streams.spawn(receive_stream(stream, state.clone(), origin));
                }
                Err(_) => break,
            },
            Some(result) = streams.join_next() => {
                if let Ok(Err(error)) = result {
                    tracing::debug!(?origin, "stream ended: {error:#}");
                }
            }
        }
    }
}

async fn receive_stream(mut stream: RecvStream, state: Arc<Receiving>, origin: Origin) -> Result<()> {
    let mut header = [0u8; HEADER_LEN];
    let progress = &state.progress;
    loop {
        // A clean end of stream shows up as a read error on the first header byte.
        if AsyncReadExt::read_exact(&mut stream, &mut header).await.is_err() {
            return Ok(());
        }
        let header = FrameHeader::from_bytes(&header);
        let unit = state
            .partial
            .units()
            .get(header.unit as usize)
            .context("peer sent a unit that doesn't exist")?;
        if header.raw_len != unit.len || header.wire_len > unit.len {
            bail!("peer sent a unit with the wrong size");
        }
        let mut body = vec![0u8; header.wire_len as usize];
        let mut counted = 0;
        let mut received = 0;
        for slice in body.chunks_mut(PROGRESS_STEP) {
            AsyncReadExt::read_exact(&mut stream, slice).await?;
            received += slice.len() as u64;
            let raw_so_far = header.raw_len as u64 * received / header.wire_len as u64;
            progress.done.fetch_add(raw_so_far - counted, Ordering::Relaxed);
            if origin == Origin::Peer {
                progress.from_peers.fetch_add(raw_so_far - counted, Ordering::Relaxed);
            }
            counted = raw_so_far;
        }

        let store = state.partial.clone();
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
                tracing::warn!(unit = header.unit, ?origin, "unit arrived damaged");
                progress.done.fetch_sub(header.raw_len as u64, Ordering::Relaxed);
                state.resend.send(header.unit)?;
            }
            Some(finished) => {
                if origin == Origin::Peer {
                    let _ = state.got.send(header.unit);
                }
                state.stored.notify_waiters();
                if finished {
                    state.complete.notify_one();
                }
            }
        }
    }
}

/// Swaps units with one other member of a swarm until both have everything, or the transfer ends.
pub async fn peer_session(
    conn: Connection,
    state: Arc<Receiving>,
    swarm: SwarmInfo,
    mut control_out: SendStream,
    control_in: RecvStream,
) -> Result<()> {
    let mut messages = read_messages(control_in);
    let units = state.partial.units().len();
    let peer_has = Arc::new(Mutex::new(vec![false; units]));
    let pushed = Arc::new(Mutex::new(vec![false; units]));
    let changed = Arc::new(Notify::new());

    let mut tasks = JoinSet::new();
    let (serve_conn, serve_state) = (conn.clone(), state.clone());
    tasks.spawn(async move {
        serve_streams(serve_conn, serve_state, Origin::Peer).await;
        Ok(())
    });
    for _ in 0..PEER_STREAMS {
        tasks.spawn(push_stream(
            conn.clone(),
            state.clone(),
            swarm.clone(),
            peer_has.clone(),
            pushed.clone(),
            changed.clone(),
        ));
    }

    let mut tick = tokio::time::interval(HAVE_INTERVAL);
    loop {
        tokio::select! {
            _ = state.closed.cancelled() => break,
            _ = tick.tick() => {
                let have = to_runs(&state.partial.have());
                write_msg(&mut control_out, &Control::Got { units: have }).await?;
                let theirs_complete = peer_has.lock().unwrap().iter().all(|h| *h);
                if theirs_complete && state.partial.is_complete() {
                    break;
                }
            }
            msg = messages.recv() => match msg.context("swarm member left")?? {
                Control::Got { units: got } => {
                    let got = from_runs(&got, units)?;
                    let mut has = peer_has.lock().unwrap();
                    for (unit, got) in got.into_iter().enumerate() {
                        has[unit] |= got;
                    }
                    drop(has);
                    changed.notify_waiters();
                }
                other => bail!("unexpected message from a swarm member: {other:?}"),
            },
            Some(result) = tasks.join_next() => {
                if let Ok(Err(error)) = result {
                    tracing::debug!("swarm stream ended: {error:#}");
                }
            }
        }
    }
    tasks.abort_all();
    conn.close(0u32.into(), b"done");
    Ok(())
}

/// Passes on units the other member lacks, this member's own share first.
async fn push_stream(
    conn: Connection,
    state: Arc<Receiving>,
    swarm: SwarmInfo,
    peer_has: Arc<Mutex<Vec<bool>>>,
    pushed: Arc<Mutex<Vec<bool>>>,
    changed: Arc<Notify>,
) -> Result<()> {
    let members = swarm.members.len().max(1) as u32;
    let mut stream: Option<SendStream> = None;
    loop {
        let stored = state.stored.notified();
        let peer_changed = changed.notified();
        tokio::pin!(stored, peer_changed);
        stored.as_mut().enable();
        peer_changed.as_mut().enable();

        let next = {
            let have = state.partial.have();
            let theirs = peer_has.lock().unwrap();
            let mut pushed = pushed.lock().unwrap();
            let wanted = |unit: &&u32| !theirs[**unit as usize] && !pushed[**unit as usize];
            let pick = have
                .iter()
                .filter(wanted)
                .find(|unit| **unit % members == swarm.index)
                .or_else(|| have.iter().find(wanted))
                .copied();
            if let Some(unit) = pick {
                pushed[unit as usize] = true;
            }
            pick
        };

        let Some(unit) = next else {
            tokio::select! {
                _ = state.closed.cancelled() => return Ok(()),
                _ = &mut stored => {}
                _ = &mut peer_changed => {}
                _ = tokio::time::sleep(HAVE_INTERVAL) => {}
            }
            continue;
        };

        let partial = state.partial.clone();
        let (header, body) = spawn_blocking(move || -> Result<_> {
            let (raw, hash) = partial.read_unit(unit)?;
            Ok(frame(unit, raw, hash))
        })
        .await??;
        let out = match &mut stream {
            Some(out) => out,
            None => stream.insert(conn.open_uni().await?),
        };
        if let Err(error) = write_frame(out, header, &body, &[], state.limit.as_deref()).await {
            pushed.lock().unwrap()[unit as usize] = false;
            return Err(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::SwarmMember;

    fn swarm(index: u32, members: usize) -> SwarmInfo {
        SwarmInfo {
            id: "s".into(),
            index,
            members: (0..members)
                .map(|i| SwarmMember {
                    id: i.to_string(),
                    name: i.to_string(),
                    addr: None,
                })
                .collect(),
        }
    }

    #[test]
    fn swarm_members_get_their_share_first_and_the_rest_reversed() {
        assert_eq!(send_order(6, Some(&swarm(0, 2))), vec![0, 2, 4, 5, 3, 1]);
        assert_eq!(send_order(6, Some(&swarm(1, 2))), vec![1, 3, 5, 4, 2, 0]);
        assert_eq!(send_order(4, None), vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn the_queue_skips_held_units_and_retries_first() {
        let queue = WorkQueue::new(vec![0, 1, 2], vec![false, true, false], vec![10, 10, 10]);
        assert_eq!(queue.pop(false).await, Some(0));
        assert_eq!(queue.got(&[false, false, true]), 10);
        queue.retry(0);
        assert_eq!(queue.pop(false).await, Some(0));
        queue.sent(0);
        queue.close();
        assert_eq!(queue.pop(false).await, None);
    }

    #[tokio::test]
    async fn the_main_connection_copies_what_the_extra_one_is_stuck_on() {
        let queue = WorkQueue::new(vec![0], vec![false], vec![10]);
        assert_eq!(queue.pop(true).await, Some(0));
        let idle = |extra| tokio::time::timeout(Duration::from_millis(50), queue.pop(extra));
        // Streams never copy units from their own connection.
        assert!(idle(true).await.is_err());
        // The main connection takes a second copy of the extra one's unit, but only one.
        assert_eq!(queue.pop(false).await, Some(0));
        assert!(idle(false).await.is_err());
        queue.sent(0);
        assert!(idle(false).await.is_err());
    }
}
