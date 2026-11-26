//! A running HSFT peer: owns the network endpoint and everything happening on it.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use data_encoding::BASE64URL_NOPAD;
use iroh::endpoint::{presets, Connection, QuicTransportConfig, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::net;
use crate::plan::{scan, Manifest};
use crate::store::{Friend, Settings, Store};
use crate::transfer::{
    self, route, ExtraLink, Incoming, Limiter, Origin, Outgoing, Progress, Receiving, SendOptions,
    SendResult, EXTRA_ALPN, SWARM_ALPN,
};
use crate::wire::{read_msg, write_msg, JoinRequest, SwarmHello, SwarmInfo, SwarmMember};

const TRANSFER_ALPN: &[u8] = b"hsft/transfer/1";
const PAIR_ALPN: &[u8] = b"hsft/pair/1";

const INVITE_PREFIX: &str = "hsft1-";
const INVITE_LIFETIME: Duration = Duration::from_secs(15 * 60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const ONLINE_TIMEOUT: Duration = Duration::from_secs(10);
const OFFER_TIMEOUT: Duration = Duration::from_secs(60);
/// Short, because the first try often lands before the other friend has said yes.
const SWARM_RETRY: Duration = Duration::from_secs(1);

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
        /// Other friends receiving the same files, who will swap pieces with this one.
        shared_with: Vec<String>,
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
        /// Bytes over the extra connection, when one is in use.
        extra_bytes: Option<u64>,
        /// Bytes that came from other friends in a swarm.
        peer_bytes: u64,
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

type SwarmEntry = (Arc<Receiving>, SwarmInfo);

struct Inner {
    endpoint: Endpoint,
    network: Network,
    store: Mutex<Store>,
    events: mpsc::UnboundedSender<Event>,
    invites: Mutex<HashMap<String, Instant>>,
    offers: Mutex<HashMap<u64, oneshot::Sender<Option<PathBuf>>>>,
    active: Mutex<HashMap<u64, Connection>>,
    next_transfer: AtomicU64,
    /// The endpoint bound to the extra adapter, kept while its address stays the same.
    extra: tokio::sync::Mutex<Option<(Ipv4Addr, Endpoint)>>,
    /// Incoming transfers that extra connections may join, by token.
    joins: Mutex<HashMap<String, Arc<Receiving>>>,
    /// Incoming transfers shared with other friends, by swarm id.
    swarms: Mutex<HashMap<String, SwarmEntry>>,
    /// Upload caps for the main and extra connection, only ever set when measuring.
    limits: Mutex<Limits>,
}

type Limits = (Option<Arc<Limiter>>, Option<Arc<Limiter>>);

#[derive(Clone)]
pub struct Node {
    inner: Arc<Inner>,
    router: Router,
}

fn transport() -> QuicTransportConfig {
    // Stays on the default Cubic congestion control: in testing, BBR hung a 3 GB transfer.
    QuicTransportConfig::builder()
        .stream_receive_window(VarInt::from_u32(STREAM_WINDOW))
        .send_window(SEND_WINDOW)
        // Finish units one after another rather than all at once at the very end, so that a
        // transfer stopped on a slow link leaves as many finished units as possible.
        .send_fairness(false)
        .keep_alive_interval(Duration::from_secs(5))
        .build()
}

impl Node {
    /// Starts a peer using `data_dir` for its identity and friends. Events are delivered on the
    /// returned channel.
    pub async fn start(
        data_dir: &Path,
        network: Network,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Event>)> {
        let store = Store::open(data_dir)?;
        let builder = match network {
            Network::Internet => Endpoint::builder(presets::N0),
            Network::RelayOnly => Endpoint::builder(presets::N0).clear_ip_transports(),
            Network::LocalOnly => {
                Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
            }
        };
        let endpoint = builder
            .secret_key(store.secret_key()?)
            .transport_config(transport())
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
            extra: tokio::sync::Mutex::default(),
            joins: Mutex::default(),
            swarms: Mutex::default(),
            limits: Mutex::default(),
        });
        let router = Router::builder(endpoint)
            .accept(TRANSFER_ALPN, TransferProtocol(inner.clone()))
            .accept(PAIR_ALPN, PairProtocol(inner.clone()))
            .accept(EXTRA_ALPN, ExtraProtocol(inner.clone()))
            .accept(SWARM_ALPN, SwarmProtocol(inner.clone()))
            .spawn();
        Ok((Self { inner, router }, events_rx))
    }

    pub fn id(&self) -> String {
        self.inner.endpoint.id().to_string()
    }

    pub async fn shutdown(&self) {
        if let Some((_, extra)) = self.inner.extra.lock().await.take() {
            extra.close().await;
        }
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
        Ok(self.send_group(&[friend_id.to_string()], paths)?[0])
    }

    /// Sends the same files to several friends at once. They pass pieces to each other, so all of
    /// them get the files in about the time one copy takes. Returns a transfer number per friend.
    pub fn send_group(&self, friend_ids: &[String], paths: Vec<PathBuf>) -> Result<Vec<u64>> {
        let mut friends: Vec<Friend> = Vec::new();
        {
            let store = self.inner.store.lock().unwrap();
            for id in friend_ids {
                let friend = store.friend(id).cloned().context("unknown friend")?;
                if !friends.iter().any(|f| f.id == friend.id) {
                    friends.push(friend);
                }
            }
        }
        if friends.is_empty() {
            bail!("choose at least one friend");
        }
        let transfers: Vec<(u64, Friend)> = friends
            .into_iter()
            .map(|friend| (self.inner.new_transfer(), friend))
            .collect();
        let ids = transfers.iter().map(|(id, _)| *id).collect();
        let inner = self.inner.clone();
        tokio::spawn(async move { run_group(inner, transfers, paths).await });
        Ok(ids)
    }

    /// Answers an offer. `Some` accepts it into that folder, and `None` declines.
    pub fn respond(&self, transfer: u64, dest: Option<PathBuf>) {
        let offer = self.inner.offers.lock().unwrap().remove(&transfer);
        if let Some(offer) = offer {
            let _ = offer.send(dest);
        }
    }

    /// Caps upload speed in megabits per second, as a slower connection would. Only for
    /// measuring: it lets several peers on one PC behave like friends on separate home lines.
    pub fn set_upload_limit(&self, main: Option<f64>, extra: Option<f64>) {
        *self.inner.limits.lock().unwrap() = (main.map(Limiter::new), extra.map(Limiter::new));
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
                let extra = progress.extra.load(Ordering::Relaxed);
                let extra_active = progress.extra_active.load(Ordering::Relaxed);
                inner.emit(Event::Progress {
                    transfer,
                    done,
                    total,
                    bytes_per_sec: speed as u64,
                    route: route(&conn),
                    extra_bytes: (extra_active || extra > 0).then_some(extra),
                    peer_bytes: progress.from_peers.load(Ordering::Relaxed),
                });
            }
        })
        .abort_handle()
    }

    /// Connects through the extra adapter, if one is set and it works.
    async fn extra_link(&self, conn: &Connection, friend: &Friend) -> Option<ExtraLink> {
        let adapter = self.store.lock().unwrap().settings.extra_adapter.clone()?;
        let Some(ip) = net::resolve(&adapter) else {
            tracing::info!(adapter, "extra adapter isn't connected");
            return None;
        };
        let endpoint = match self.extra_endpoint(ip).await {
            Ok(endpoint) => endpoint,
            Err(error) => {
                tracing::info!(adapter, "extra adapter can't be used: {error:#}");
                return None;
            }
        };
        let mut addr = friend
            .addr
            .clone()
            .unwrap_or_else(|| EndpointAddr::new(conn.remote_id()));
        let working = conn
            .paths()
            .iter()
            .find(|path| path.is_selected() && path.is_ip())
            .map(|path| path.remote_addr().clone());
        if let Some(working) = working {
            addr = addr.with_addrs([working]);
        }
        Some(ExtraLink { endpoint, addr })
    }

    async fn extra_endpoint(&self, ip: Ipv4Addr) -> Result<Endpoint> {
        let mut cached = self.extra.lock().await;
        if let Some((cached_ip, endpoint)) = cached.as_ref() {
            if *cached_ip == ip {
                return Ok(endpoint.clone());
            }
            endpoint.close().await;
        }
        let builder = match self.network {
            Network::LocalOnly => {
                Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
            }
            Network::Internet | Network::RelayOnly => Endpoint::builder(presets::N0),
        };
        let endpoint = builder
            .clear_ip_transports()
            .bind_addr(SocketAddr::from((ip, 0)))?
            .transport_config(transport())
            .bind()
            .await?;
        if self.network != Network::LocalOnly {
            let _ = tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await;
        }
        *cached = Some((ip, endpoint.clone()));
        Ok(endpoint)
    }
}

async fn run_group(inner: Arc<Inner>, transfers: Vec<(u64, Friend)>, paths: Vec<PathBuf>) {
    let scanned = match tokio::task::spawn_blocking(move || scan(&paths)).await {
        Ok(Ok(scanned)) => scanned,
        Ok(Err(error)) => {
            for (transfer, _) in transfers {
                inner.finish(transfer, Err(anyhow::anyhow!("{error:#}")));
            }
            return;
        }
        Err(error) => {
            for (transfer, _) in transfers {
                inner.finish(transfer, Err(anyhow::anyhow!("{error}")));
            }
            return;
        }
    };
    let outgoing = Outgoing::new(scanned);
    let swarm_id = (transfers.len() > 1).then(|| hex::encode(rand::random::<[u8; 16]>()));
    let members: Vec<SwarmMember> = transfers
        .iter()
        .map(|(_, friend)| SwarmMember {
            id: friend.id.clone(),
            name: friend.name.clone(),
            addr: friend.addr.clone(),
        })
        .collect();
    for (index, (transfer, friend)) in transfers.into_iter().enumerate() {
        let swarm = swarm_id.as_ref().map(|id| SwarmInfo {
            id: id.clone(),
            index: index as u32,
            members: members.clone(),
        });
        let (inner, outgoing) = (inner.clone(), outgoing.clone());
        tokio::spawn(async move {
            let result = send_one(&inner, transfer, friend, outgoing, swarm).await;
            inner.finish(transfer, result);
        });
    }
}

async fn send_one(
    inner: &Arc<Inner>,
    transfer: u64,
    friend: Friend,
    outgoing: Arc<Outgoing>,
    swarm: Option<SwarmInfo>,
) -> Result<Option<String>> {
    let total = outgoing.manifest.total_bytes();
    inner.emit(Event::Started {
        transfer,
        direction: Direction::Send,
        peer_id: friend.id.clone(),
        title: outgoing.manifest.title(),
        bytes: total,
    });

    let id: EndpointId = friend.id.parse().context("bad friend id")?;
    let addr = friend.addr.clone().unwrap_or_else(|| EndpointAddr::new(id));
    let conn = tokio::time::timeout(CONNECT_TIMEOUT, inner.endpoint.connect(addr, TRANSFER_ALPN))
        .await
        .map_err(|_| anyhow::anyhow!("{} seems to be offline", friend.name))?
        .with_context(|| format!("couldn't reach {}", friend.name))?;
    inner.active.lock().unwrap().insert(transfer, conn.clone());

    let extra = inner.extra_link(&conn, &friend).await;
    let progress = Arc::new(Progress::default());
    let watcher = inner.watch(transfer, conn.clone(), progress.clone(), total);
    let (limit, extra_limit) = inner.limits.lock().unwrap().clone();
    let options = SendOptions {
        swarm,
        extra,
        limit,
        extra_limit,
    };
    let result = transfer::send(&conn, outgoing, options, progress).await;
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
    mut incoming: Incoming,
) -> Result<Option<String>> {
    let me = inner.endpoint.id().to_string();
    // A swarm only makes sense if the sender listed this PC where it says it did.
    if let Some(swarm) = &incoming.swarm {
        if swarm.members[swarm.index as usize].id != me {
            incoming.swarm = None;
        }
    }
    let shared_with = incoming.swarm.as_ref().map_or_else(Vec::new, |swarm| {
        swarm
            .members
            .iter()
            .filter(|member| member.id != me)
            .map(|member| member.name.clone())
            .collect()
    });

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
        shared_with,
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
    let swarm = incoming.swarm.clone();
    let registrar = inner.clone();
    let register = move |token: &str, state: Arc<Receiving>| {
        registrar.joins.lock().unwrap().insert(token.to_string(), state.clone());
        let Some(swarm) = swarm else { return };
        registrar
            .swarms
            .lock()
            .unwrap()
            .insert(swarm.id.clone(), (state.clone(), swarm.clone()));
        // Of each pair of members, the one with the lower id makes the connection.
        for (peer, member) in swarm.members.iter().enumerate() {
            if peer as u32 != swarm.index && member.id > me {
                let (inner, state, swarm) = (registrar.clone(), state.clone(), swarm.clone());
                tokio::spawn(dial_member(inner, state, swarm, peer));
            }
        }
    };
    let limit = inner.limits.lock().unwrap().0.clone();
    let saved = incoming.accept(&conn, dest, key, progress, limit, register).await;
    watcher.abort();
    inner.joins.lock().unwrap().retain(|_, state| !state.closed.is_cancelled());
    inner
        .swarms
        .lock()
        .unwrap()
        .retain(|_, (state, _)| !state.closed.is_cancelled());
    Ok(Some(saved?.display().to_string()))
}

/// Keeps trying to reach another swarm member until the two have swapped everything.
async fn dial_member(inner: Arc<Inner>, state: Arc<Receiving>, swarm: SwarmInfo, peer: usize) {
    let member = swarm.members[peer].clone();
    let Ok(id) = member.id.parse::<EndpointId>() else {
        return;
    };
    loop {
        let addr = member.addr.clone().unwrap_or_else(|| EndpointAddr::new(id));
        let attempt = async {
            let conn =
                tokio::time::timeout(CONNECT_TIMEOUT, inner.endpoint.connect(addr, SWARM_ALPN))
                    .await??;
            let (mut out, input) = conn.open_bi().await?;
            let hello = SwarmHello {
                swarm: swarm.id.clone(),
                index: swarm.index,
            };
            write_msg(&mut out, &hello).await?;
            transfer::peer_session(conn, state.clone(), out, input).await
        };
        tokio::select! {
            _ = state.closed.cancelled() => return,
            result = attempt => match result {
                Ok(()) => return,
                Err(error) => tracing::debug!(peer = member.name, "swarm connection: {error:#}"),
            },
        }
        tokio::select! {
            _ = state.closed.cancelled() => return,
            _ = tokio::time::sleep(SWARM_RETRY) => {}
        }
    }
}

macro_rules! named_handler {
    ($name:ident) => {
        #[derive(Clone)]
        struct $name(Arc<Inner>);

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(stringify!($name))
            }
        }
    };
}

named_handler!(TransferProtocol);
named_handler!(PairProtocol);
named_handler!(ExtraProtocol);
named_handler!(SwarmProtocol);

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

/// A second connection for a transfer already under way. The token proves it belongs to one.
impl ProtocolHandler for ExtraProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let inner = &self.0;
        let joined: Result<Arc<Receiving>> = async {
            let (_, mut input) = conn.accept_bi().await?;
            let join: JoinRequest =
                tokio::time::timeout(OFFER_TIMEOUT, read_msg(&mut input)).await??;
            let state = inner.joins.lock().unwrap().get(&join.token).cloned();
            state.context("no transfer matches this connection")
        }
        .await;
        match joined {
            Ok(state) => transfer::serve_streams(conn, state, Origin::Extra).await,
            Err(error) => {
                tracing::info!("refused an extra connection: {error:#}");
                conn.close(CLOSE_NOT_FRIEND.into(), b"unknown transfer");
            }
        }
        Ok(())
    }
}

/// Another friend receiving the same files. The sender vouched for them by listing them.
impl ProtocolHandler for SwarmProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let inner = &self.0;
        let result: Result<()> = async {
            let (out, mut input) = conn.accept_bi().await?;
            let hello: SwarmHello =
                tokio::time::timeout(OFFER_TIMEOUT, read_msg(&mut input)).await??;
            let entry = inner.swarms.lock().unwrap().get(&hello.swarm).cloned();
            let Some((state, swarm)) = entry else {
                bail!("no swarm matches this connection");
            };
            let remote = conn.remote_id().to_string();
            let listed = hello.index != swarm.index
                && swarm
                    .members
                    .get(hello.index as usize)
                    .is_some_and(|member| member.id == remote);
            if !listed {
                bail!("{remote} isn't a member of this swarm");
            }
            transfer::peer_session(conn.clone(), state, out, input).await
        }
        .await;
        if let Err(error) = result {
            tracing::info!("swarm connection ended: {error:#}");
            conn.close(CLOSE_NOT_FRIEND.into(), b"not in this swarm");
        }
        Ok(())
    }
}
