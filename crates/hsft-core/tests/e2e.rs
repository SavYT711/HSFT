//! Two real peers on this machine, talking over real QUIC connections.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use hsft_core::{Event, Network, Node};
use tempfile::TempDir;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(60);

struct Peer {
    node: Node,
    events: UnboundedReceiver<Event>,
    _data: TempDir,
}

async fn peer(name: &str) -> Peer {
    let data = tempfile::tempdir().unwrap();
    let (node, events) = Node::start(data.path(), Network::LocalOnly).await.unwrap();
    let mut settings = node.settings();
    settings.name = name.into();
    node.update_settings(settings).unwrap();
    Peer {
        node,
        events,
        _data: data,
    }
}

async fn friends() -> (Peer, Peer) {
    let mut alice = peer("Alice").await;
    let bob = peer("Bob").await;
    let invite = alice.node.create_invite().await.unwrap();
    let friend = bob.node.join(&invite).await.unwrap();
    assert_eq!(friend.name, "Alice");
    assert_eq!(friend.id, alice.node.id());
    next_matching(&mut alice, |e| matches!(e, Event::FriendAdded { .. })).await;
    assert_eq!(alice.node.friends()[0].name, "Bob");
    (alice, bob)
}

async fn next_matching(peer: &mut Peer, want: impl Fn(&Event) -> bool) -> Event {
    timeout(WAIT, async {
        loop {
            let event = peer.events.recv().await.expect("events ended");
            if want(&event) {
                return event;
            }
        }
    })
    .await
    .expect("timed out waiting for an event")
}

fn is_end(event: &Event) -> bool {
    matches!(event, Event::Finished { .. } | Event::Failed { .. })
}

async fn accept_offer(peer: &mut Peer, dest: &Path) -> u64 {
    let event = next_matching(peer, |e| matches!(e, Event::Offer { .. })).await;
    let Event::Offer { transfer, .. } = event else { unreachable!() };
    peer.node.respond(transfer, Some(dest.to_path_buf()));
    transfer
}

fn pseudo_random(len: usize, seed: &str) -> Vec<u8> {
    let mut data = vec![0u8; len];
    blake3::Hasher::new()
        .update(seed.as_bytes())
        .finalize_xof()
        .fill(&mut data);
    data
}

fn assert_same_tree(a: &Path, b: &Path) {
    let names = |dir: &Path| {
        let mut names: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|e| PathBuf::from(e.unwrap().file_name()))
            .collect();
        names.sort();
        names
    };
    assert_eq!(names(a), names(b), "contents of {}", a.display());
    for name in names(a) {
        let (x, y) = (a.join(&name), b.join(&name));
        if x.is_dir() {
            assert_same_tree(&x, &y);
        } else {
            assert!(fs::read(&x).unwrap() == fs::read(&y).unwrap(), "{}", x.display());
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_folder_arrives_identical() {
    let (mut alice, mut bob) = friends().await;
    let source = tempfile::tempdir().unwrap();
    let root = source.path().join("stuff");
    fs::create_dir_all(root.join("sub/deep")).unwrap();
    fs::create_dir_all(root.join("empty")).unwrap();
    fs::write(root.join("big-random.bin"), pseudo_random(17 * 1024 * 1024, "big")).unwrap();
    fs::write(root.join("big-text.txt"), "compress me ".repeat(1_000_000)).unwrap();
    fs::write(root.join("sub/empty.txt"), "").unwrap();
    for i in 0..500 {
        fs::write(root.join(format!("sub/deep/{i}.txt")), format!("file {i}")).unwrap();
    }

    let dest = tempfile::tempdir().unwrap();
    let id = alice.node.id();
    let transfer = bob.node.send(&id, vec![root.clone()]).unwrap();
    accept_offer(&mut alice, dest.path()).await;

    let got = next_matching(&mut alice, is_end).await;
    assert!(matches!(got, Event::Finished { saved_to: Some(_), .. }), "{got:?}");
    let sent = next_matching(&mut bob, is_end).await;
    assert_eq!(sent, Event::Finished { transfer, saved_to: None });

    assert_same_tree(&root, &dest.path().join("stuff"));
    assert!(!dest.path().join(".hsft-partial").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn friends_can_send_in_both_directions() {
    let (mut alice, mut bob) = friends().await;
    let source = tempfile::tempdir().unwrap();
    let file = source.path().join("hello.txt");
    fs::write(&file, "hi").unwrap();

    let dest = tempfile::tempdir().unwrap();
    let id = bob.node.id();
    alice.node.send(&id, vec![file]).unwrap();
    accept_offer(&mut bob, dest.path()).await;
    assert!(matches!(next_matching(&mut alice, is_end).await, Event::Finished { .. }));
    assert_eq!(fs::read(dest.path().join("hello.txt")).unwrap(), b"hi");
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_folders_and_files_work_with_nothing_to_stream() {
    let (mut alice, bob) = friends().await;
    let source = tempfile::tempdir().unwrap();
    let root = source.path().join("hollow");
    fs::create_dir_all(root.join("inner")).unwrap();
    fs::write(root.join("zero"), "").unwrap();

    let dest = tempfile::tempdir().unwrap();
    let id = alice.node.id();
    bob.node.send(&id, vec![root.clone()]).unwrap();
    accept_offer(&mut alice, dest.path()).await;
    let got = next_matching(&mut alice, is_end).await;
    assert!(matches!(got, Event::Finished { .. }), "{got:?}");
    assert_same_tree(&root, &dest.path().join("hollow"));
}

#[tokio::test(flavor = "multi_thread")]
async fn declining_tells_the_sender_and_writes_nothing() {
    let (mut alice, mut bob) = friends().await;
    let source = tempfile::tempdir().unwrap();
    let file = source.path().join("no.txt");
    fs::write(&file, "nope").unwrap();

    let id = alice.node.id();
    bob.node.send(&id, vec![file]).unwrap();
    let offer = next_matching(&mut alice, |e| matches!(e, Event::Offer { .. })).await;
    let Event::Offer { transfer, from_name, files, bytes, .. } = offer else { unreachable!() };
    assert_eq!((from_name.as_str(), files, bytes), ("Bob", 1, 4));
    alice.node.respond(transfer, None);

    match next_matching(&mut bob, is_end).await {
        Event::Failed { error, .. } => assert!(error.contains("declined"), "{error}"),
        other => panic!("expected failure, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn strangers_are_turned_away() {
    let (alice, _bob) = friends().await;
    let mut stranger = peer("Mallory").await;
    let mut victim = alice;

    // A stranger who somehow knows Alice is treated as if the friend entry was forged locally.
    let invite = victim.node.create_invite().await.unwrap();
    let helper = peer("Helper").await;
    helper.node.join(&invite).await.unwrap();
    next_matching(&mut victim, |e| matches!(e, Event::FriendAdded { .. })).await;
    let copied = helper._data.path().join("friends.json");
    fs::copy(&copied, stranger._data.path().join("friends.json")).unwrap();
    drop(stranger.node);
    let (node, events) = Node::start(stranger._data.path(), Network::LocalOnly).await.unwrap();
    (stranger.node, stranger.events) = (node, events);

    let source = tempfile::tempdir().unwrap();
    let file = source.path().join("spam.txt");
    fs::write(&file, "spam").unwrap();
    let id = victim.node.id();
    stranger.node.send(&id, vec![file]).unwrap();

    let ended = next_matching(&mut stranger, is_end).await;
    assert!(matches!(ended, Event::Failed { .. }), "{ended:?}");
    assert!(timeout(Duration::from_millis(300), async {
        next_matching(&mut victim, |e| matches!(e, Event::Offer { .. })).await
    })
    .await
    .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invite_only_works_once() {
    let (alice, _bob) = friends().await;
    let invite = alice.node.create_invite().await.unwrap();
    let carol = peer("Carol").await;
    let dave = peer("Dave").await;
    carol.node.join(&invite).await.unwrap();
    assert!(dave.node.join(&invite).await.is_err());
    assert!(dave.node.friends().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_made_up_invite_is_rejected_cleanly() {
    let carol = peer("Carol").await;
    assert!(carol.node.join("hello").await.is_err());
    assert!(carol.node.join("hsft1-AAAA").await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_transfer_resumes_without_resending_everything() {
    let (mut alice, mut bob) = friends().await;
    let source = tempfile::tempdir().unwrap();
    let file = source.path().join("large.bin");
    let size = 300 * 1024 * 1024;
    fs::write(&file, pseudo_random(size, "large")).unwrap();

    let dest = tempfile::tempdir().unwrap();
    let id = alice.node.id();
    let first = bob.node.send(&id, vec![file.clone()]).unwrap();
    accept_offer(&mut alice, dest.path()).await;
    // Progress includes units still in flight, so wait for well past what must be stored.
    next_matching(&mut alice, |e| {
        matches!(e, Event::Progress { done, .. } if *done > 120 * 1024 * 1024)
    })
    .await;
    bob.node.cancel(first);
    assert!(matches!(next_matching(&mut alice, is_end).await, Event::Failed { .. }));
    assert!(matches!(next_matching(&mut bob, is_end).await, Event::Failed { .. }));
    assert!(dest.path().join(".hsft-partial").exists());

    bob.node.send(&id, vec![file.clone()]).unwrap();
    accept_offer(&mut alice, dest.path()).await;
    // Progress reads zero while the partial data is re-checked, then jumps to all that was kept.
    let resumed = next_matching(&mut alice, |e| {
        matches!(e, Event::Progress { done, .. } if *done > 0)
    })
    .await;
    let Event::Progress { done, .. } = resumed else { unreachable!() };
    assert!(done >= 40 * 1024 * 1024, "only {done} bytes were kept");

    let got = next_matching(&mut alice, is_end).await;
    assert!(matches!(got, Event::Finished { .. }), "{got:?}");
    assert!(matches!(next_matching(&mut bob, is_end).await, Event::Finished { .. }));
    assert!(fs::read(dest.path().join("large.bin")).unwrap() == fs::read(&file).unwrap());
    assert!(!dest.path().join(".hsft-partial").exists());
}
