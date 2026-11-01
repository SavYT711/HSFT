//! A running HSFT peer: owns the network endpoint and everything happening on it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use data_encoding::BASE64URL_NOPAD;
use iroh::endpoint::{presets, Connection, QuicTransportConfig, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::plan::{scan, Manifest};
use crate::store::{Friend, Settings, Store};
use crate::transfer::{self, Incoming, Progress, SendResult};
use crate::wire::{read_msg, write_msg};

const TRANSFER_ALPN: &[u8] = b"hsft/transfer/1";
const PAIR_ALPN: &[u8] = b"hsft/pair/1";

const INVITE_PREFIX: &str = "hsft1-";
const INVITE_LIFETIME: Duration = Duration::from_secs(15 * 60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const ONLINE_TIMEOUT: Duration = Duration::from_secs(10);
const OFFER_TIMEOUT: Duration = Duration::from_secs(60);

const CLOSE_DONE: u32 = 0;
const CLOSE_CANCELLED: u32 = 1;
const CLOSE_NOT_FRIEND: u32 = 2;

/// Must stay small. The QUIC library drops the connection if a stream holds more than 1024
/// out-of-order fragments, and packet loss can leave one fragment per two packets in the window.
/// At 2 MiB that can't happen, so speed comes from running many streams side by side.
const STREAM_WINDOW: u32 = 2 * 1024 * 1024;
const SEND_WINDOW: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Network {
    /// Relays and address lookup, for reaching friends over the internet.
    Internet,
    /// Never connects directly, for testing what strict home routers lead to.
    RelayOnly,
    /// Direct addresses only, for tests and benchmarks on one machine.
    LocalOnly,
}

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Send,
    Receive,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Event {
    FriendAdded {
        friend: FriendInfo,
    },
    Offer {
        transfer: u64,
        from_id: String,
        from_name: String,
        title: String,
        files: usize,
        bytes: u64,
    },
    Started {
        transfer: u64,
        direction: Direction,
        peer_id: String,
        title: String,
        bytes: u64,
    },
    Progress {
        transfer: u64,
        done: u64,
        total: u64,
        bytes_per_sec: u64,
        route: &'static str,
    },
    Finished {
        transfer: u64,
        saved_to: Option<String>,
    },
    Failed {
        transfer: u64,
        error: String,
    },
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct FriendInfo {
    pub id: String,
    pub name: String,
}

impl From<&Friend> for FriendInfo {
    fn from(friend: &Friend) -> Self {
        Self {
            id: friend.id.clone(),
            name: friend.name.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Invite {
    addr: EndpointAddr,
    secret: String,
}

#[derive(Serialize, Deserialize)]
struct PairRequest {
    secret: String,
    name: String,
    addr: EndpointAddr,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum PairReply {
    Welcome { name: String, addr: EndpointAddr },
    Rejected,
}

struct Inner {
    endpoint: Endpoint,
    network: Network,
    store: Mutex<Store>,
    events: mpsc::UnboundedSender<Event>,
    invites: Mutex<HashMap<String, Instant>>,
    offers: Mutex<HashMap<u64, oneshot::Sender<Option<PathBuf>>>>,
    active: Mutex<HashMap<u64, Connection>>,
    next_transfer: AtomicU64,
}

#[derive(Clone)]
pub struct Node {
    inner: Arc<Inner>,
    router: Router,
}

impl Node {
    /// Starts a peer using `data_dir` for its identity and friends. Events are delivered on the
    /// returned channel.
    pub async fn start(
        data_dir: &Path,
        network: Network,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>)> {
        let store = Store::open(data_dir)?;
        // Stays on the default Cubic congestion control: in testing, BBR hung a 3 GB transfer.
        let transport = QuicTransportConfig::builder()
            .stream_receive_window(VarInt::from_u32(STREAM_WINDOW))
            .send_window(SEND_WINDOW)
            // Finish units one after another rather than all at once at the very end, so that a
            // transfer stopped on a slow link leaves as many finished units as possible.
            .send_fairness(false)
            .keep_alive_interval(Duration::from_secs(5))
            .build();
        let builder = match network {
            Network::Internet => Endpoint::builder(presets::N0),
            Network::RelayOnly => Endpoint::builder(presets::N0).clear_ip_transports(),
            Network::LocalOnly => {
                Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
            }
        };
        let endpoint = builder
            .secret_key(store.secret_key()?)
            .transport_config(transport)
            .bind()
            .await
            .context("can't open a network port")?;

        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            endpoint: endpoint.clone(),
            network,
            store: Mutex::new(store),
            events: events_tx,
            invites: Mutex::default(),
            offers: Mutex::default(),
            active: Mutex::default(),
            next_transfer: AtomicU64::new(1),
        });
        let router = Router::builder(endpoint)
            .accept(TRANSFER_ALPN, TransferProtocol(inner.clone()))
            .accept(PAIR_ALPN, PairProtocol(inner.clone()))
            .spawn();
        Ok((Self { inner, router }, events_rx))
    }

    pub fn id(&self) -> String {
        self.inner.endpoint.id().to_string()
    }

    pub async fn shutdown(&self) {
        let _ = self.router.shutdown().await;
    }

    pub fn friends(&self) -> Vec<FriendInfo> {
        let store = self.inner.store.lock().unwrap();
        store.friends.iter().map(FriendInfo::from).collect()
    }

    pub fn remove_friend(&self, id: &str) -> Result<()> {
        self.inner.store.lock().unwrap().remove_friend(id)
    }

    pub fn settings(&self) -> Settings {
        self.inner.store.lock().unwrap().settings.clone()
    }

    pub fn update_settings(&self, settings: Settings) -> Result<()> {
        let mut store = self.inner.store.lock().unwrap();
        store.settings = settings;
        store.save_settings()
    }

    /// Creates a single-use code. Anyone holding it can become a friend until it expires, so it
    /// should only travel over a private channel.
    pub async fn create_invite(&self) -> Result<String> {
        self.inner.wait_online().await;
        let secret = hex::encode(rand::random::<[u8; 16]>());
        let expires = Instant::now() + INVITE_LIFETIME;
        let mut invites = self.inner.invites.lock().unwrap();
        invites.retain(|_, expires| *expires > Instant::now());
        invites.insert(secret.clone(), expires);
        let invite = Invite {
            addr: self.inner.endpoint.addr(),
            secret,
        };
        let json = serde_json::to_vec(&invite)?;
        Ok(format!("{INVITE_PREFIX}{}", BASE64URL_NOPAD.encode(&json)))
    }

    /// Uses a friend's invite code. The code contains their public key, so the connection can only
    /// ever reach the person who made it.
    pub async fn join(&self, code: &str) -> Result<FriendInfo> {
        let invite = decode_invite(code)?;
        self.inner.wait_online().await;
        let conn = tokio::time::timeout(
            CONNECT_TIMEOUT,
            self.inner.endpoint.connect(invite.addr, PAIR_ALPN),
        )
        .await
        .context("couldn't reach your friend; is their app open?")??;
        let (mut out, mut input) = conn.open_bi().await?;
        let request = PairRequest {
            secret: invite.secret,
            name: self.settings().name,
            addr: self.inner.endpoint.addr(),
        };
        write_msg(&mut out, &request).await?;
        let reply = read_msg(&mut input)
            .await
            .context("the invite code didn't work")?;
        conn.close(CLOSE_DONE.into(), b"paired");
        match reply {
            PairReply::Welcome { name, addr } => {
                let friend = Friend {
                    id: conn.remote_id().to_string(),
                    name,
                    addr: Some(addr),
                };
                self.inner.store.lock().unwrap().add_friend(friend.clone())?;
                Ok(FriendInfo::from(&friend))
            }
            PairReply::Rejected => bail!("this invite code was already used or has expired"),
        }
    }

    /// Starts sending to a friend, and returns the transfer number used in events.
    pub fn send(&self, friend_id: &str, paths: Vec<PathBuf>) -> Result<u64> {
        let friend = self
            .inner
            .store
            .lock()
            .unwrap()
            .friend(friend_id)
            .cloned()
            .context("unknown friend")?;
        let transfer = self.inner.new_transfer();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let result = run_send(&inner, transfer, friend, paths).await;
            inner.finish(transfer, result);
        });
        Ok(transfer)
    }

    /// Answers an offer. `Some` accepts it into that folder, and `None` declines.
    pub fn respond(&self, transfer: u64, dest: Option<PathBuf>) {
        let offer = self.inner.offers.lock().unwrap().remove(&transfer);
        if let Some(offer) = offer {
            let _ = offer.send(dest);
        }
    }

    /// Stops a running transfer. Whatever already arrived is kept for resuming.
    pub fn cancel(&self, transfer: u64) {
        if let Some(conn) = self.inner.active.lock().unwrap().get(&transfer) {
            conn.close(CLOSE_CANCELLED.into(), b"cancelled");
        }
    }
}

fn decode_invite(code: &str) -> Result<Invite> {
    let body = code
        .trim()
        .strip_prefix(INVITE_PREFIX)
        .context("that isn't an HSFT invite code")?;
    let json = BASE64URL_NOPAD
        .decode(body.as_bytes())
        .context("that invite code is incomplete")?;
    serde_json::from_slice(&json).context("that invite code is damaged")
}

impl Inner {
    /// Gives the relay connection a moment, so that invites contain a reachable address.
    async fn wait_online(&self) {
        if self.network != Network::LocalOnly {
            let _ = tokio::time::timeout(ONLINE_TIMEOUT, self.endpoint.online()).await;
        }
    }

    fn new_transfer(&self) -> u64 {
        self.next_transfer.fetch_add(1, Ordering::Relaxed)
    }

    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }

    /// Reports the end of a transfer, however it went.
    fn finish(&self, transfer: u64, result: Result<Option<String>>) {
        let conn = self.active.lock().unwrap().remove(&transfer);
        match result {
            Ok(saved_to) => self.emit(Event::Finished { transfer, saved_to }),
            Err(error) => {
                tracing::warn!(transfer, "transfer failed: {error:#}");
                let lost = conn.is_some_and(|c| c.close_reason().is_some());
                let error = if lost {
                    "The transfer stopped before finishing. Sending it again continues from there."
                        .to_string()
                } else {
                    format!("{error:#}")
                };
                self.emit(Event::Failed { transfer, error });
            }
        }
    }

    /// Reports progress until dropped.
    fn watch(
        self: &Arc<Self>,
        transfer: u64,
        conn: Connection,
        progress: Arc<Progress>,
        total: u64,
    ) -> tokio::task::AbortHandle {
        let inner = self.clone();
        tokio::spawn(async move {
            let mut last_done = progress.done.load(Ordering::Relaxed);
            let mut last_time = Instant::now();
            let mut speed = 0.0f64;
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tick.tick().await;
                let done = progress.done.load(Ordering::Relaxed).min(total);
                let now = Instant::now();
                let seconds = now.duration_since(last_time).as_secs_f64().max(0.001);
                let instant = done.saturating_sub(last_done) as f64 / seconds;
                speed = if speed == 0.0 { instant } else { speed * 0.8 + instant * 0.2 };
                (last_done, last_time) = (done, now);
                inner.emit(Event::Progress {
                    transfer,
                    done,
                    total,
                    bytes_per_sec: speed as u64,
                    route: route(&conn),
                });
            }
        })
        .abort_handle()
    }
}

fn route(conn: &Connection) -> &'static str {
    match conn.paths().iter().find(|path| path.is_selected()) {
        Some(path) if path.is_relay() => "relay",
        Some(_) => "direct",
        None => "connecting",
    }
}

async fn run_send(
    inner: &Arc<Inner>,
    transfer: u64,
    friend: Friend,
    paths: Vec<PathBuf>,
) -> Result<Option<String>> {
    let scanned = tokio::task::spawn_blocking(move || scan(&paths)).await??;
    let total = scanned.manifest.total_bytes();
    inner.emit(Event::Started {
        transfer,
        direction: Direction::Send,
        peer_id: friend.id.clone(),
        title: scanned.manifest.title(),
        bytes: total,
    });

    let id: iroh::EndpointId = friend.id.parse().context("bad friend id")?;
    let addr = friend.addr.clone().unwrap_or_else(|| EndpointAddr::new(id));
    let conn = tokio::time::timeout(CONNECT_TIMEOUT, inner.endpoint.connect(addr, TRANSFER_ALPN))
        .await
        .map_err(|_| anyhow::anyhow!("{} seems to be offline", friend.name))?
        .with_context(|| format!("couldn't reach {}", friend.name))?;
    inner.active.lock().unwrap().insert(transfer, conn.clone());

    let progress = Arc::new(Progress::default());
    let watcher = inner.watch(transfer, conn.clone(), progress.clone(), total);
    let result = transfer::send(&conn, scanned, progress).await;
    watcher.abort();
    match result? {
        SendResult::Delivered => {
            conn.close(CLOSE_DONE.into(), b"done");
            Ok(None)
        }
        SendResult::Declined(reason) if reason.is_empty() => bail!("{} declined.", friend.name),
        SendResult::Declined(reason) => bail!("{} declined: {reason}", friend.name),
    }
}

fn transfer_key(friend_id: &str, manifest: &Manifest) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(friend_id.as_bytes());
    hasher.update(&serde_json::to_vec(manifest)?);
    Ok(hex::encode(&hasher.finalize().as_bytes()[..16]))
}

async fn run_receive(
    inner: &Arc<Inner>,
    transfer: u64,
    conn: Connection,
    friend: Friend,
    incoming: Incoming,
) -> Result<Option<String>> {
    let manifest = incoming.manifest.clone();
    let (answer_tx, answer_rx) = oneshot::channel();
    inner.offers.lock().unwrap().insert(transfer, answer_tx);
    inner.emit(Event::Offer {
        transfer,
        from_id: friend.id.clone(),
        from_name: friend.name.clone(),
        title: manifest.title(),
        files: manifest.files.len(),
        bytes: manifest.total_bytes(),
    });
    let answer = tokio::select! {
        answer = answer_rx => answer.ok().flatten(),
        _ = conn.closed() => {
            inner.offers.lock().unwrap().remove(&transfer);
            bail!("{} cancelled the transfer", friend.name);
        }
    };
    let Some(dest) = answer else {
        incoming.decline("").await?;
        // Give the reply time to arrive before the connection is dropped.
        let _ = tokio::time::timeout(Duration::from_secs(5), conn.closed()).await;
        return Ok(None);
    };

    let total = manifest.total_bytes();
    inner.emit(Event::Started {
        transfer,
        direction: Direction::Receive,
        peer_id: friend.id.clone(),
        title: manifest.title(),
        bytes: total,
    });
    let key = transfer_key(&friend.id, &manifest)?;
    let progress = Arc::new(Progress::default());
    let watcher = inner.watch(transfer, conn.clone(), progress.clone(), total);
    let saved = incoming.accept(&conn, dest, key, progress).await;
    watcher.abort();
    Ok(Some(saved?.display().to_string()))
}

#[derive(Clone)]
struct TransferProtocol(Arc<Inner>);

impl std::fmt::Debug for TransferProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TransferProtocol")
    }
}

impl ProtocolHandler for TransferProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let inner = &self.0;
        let remote = conn.remote_id().to_string();
        let friend = inner.store.lock().unwrap().friend(&remote).cloned();
        let Some(friend) = friend else {
            tracing::info!(peer = %remote, "refused a stranger");
            conn.close(CLOSE_NOT_FRIEND.into(), b"not a friend");
            return Ok(());
        };
        let incoming = match tokio::time::timeout(OFFER_TIMEOUT, transfer::read_offer(&conn)).await
        {
            Ok(Ok(incoming)) => incoming,
            Ok(Err(error)) => {
                tracing::warn!(peer = %remote, "bad offer: {error:#}");
                conn.close(CLOSE_CANCELLED.into(), b"bad offer");
                return Ok(());
            }
            Err(_) => {
                conn.close(CLOSE_CANCELLED.into(), b"no offer");
                return Ok(());
            }
        };
        let transfer = inner.new_transfer();
        inner.active.lock().unwrap().insert(transfer, conn.clone());
        let result = run_receive(inner, transfer, conn, friend, incoming).await;
        inner.finish(transfer, result);
        Ok(())
    }
}

#[derive(Clone)]
struct PairProtocol(Arc<Inner>);

impl std::fmt::Debug for PairProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PairProtocol")
    }
}

impl ProtocolHandler for PairProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let inner = &self.0;
        let result: Result<()> = async {
            let (mut out, mut input) = conn.accept_bi().await?;
            let request: PairRequest =
                tokio::time::timeout(OFFER_TIMEOUT, read_msg(&mut input)).await??;
            let valid = inner
                .invites
                .lock()
                .unwrap()
                .remove(&request.secret)
                .is_some_and(|expires| expires > Instant::now());
            if !valid {
                write_msg(&mut out, &PairReply::Rejected).await?;
                bail!("a bad or expired invite was used");
            }
            let friend = Friend {
                id: conn.remote_id().to_string(),
                name: request.name.chars().take(64).collect(),
                addr: Some(request.addr),
            };
            inner.store.lock().unwrap().add_friend(friend.clone())?;
            let (name, addr) = {
                let store = inner.store.lock().unwrap();
                (store.settings.name.clone(), inner.endpoint.addr())
            };
            write_msg(&mut out, &PairReply::Welcome { name, addr }).await?;
            inner.emit(Event::FriendAdded { friend: FriendInfo::from(&friend) });
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!("pairing failed: {error:#}");
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), conn.closed()).await;
        Ok(())
    }
}
