//! Command-line version of HSFT, also handy for testing and measuring speed.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use hsft_core::{Event, FriendInfo, Network, Node};

const USAGE: &str = "\
Usage:
  hsft-cli id                     show your id
  hsft-cli invite                 create an invite code and wait for a friend to use it
  hsft-cli join <code>            become friends using a friend's invite code
  hsft-cli friends                list friends
  hsft-cli send <friend> <path>...  send files or folders to a friend
  hsft-cli receive <folder>       accept everything friends send, into a folder
  hsft-cli bench <path> [options] measure speed by sending to other peers on this PC

Bench options:
  --friends <n>          send to n friends at once, who pass pieces to each other
  --limit <mbps>         cap every peer's upload, as on a slower home connection
  --extra <adapter>      also send through this adapter's name or address
  --extra-limit <mbps>   cap the extra connection's upload
";

#[derive(Default)]
struct BenchOptions {
    friends: usize,
    limit: Option<f64>,
    extra: Option<String>,
    extra_limit: Option<f64>,
}

fn parse_bench(args: &[&str]) -> Result<BenchOptions> {
    let mut options = BenchOptions {
        friends: 1,
        ..Default::default()
    };
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let value = args.next().with_context(|| format!("{flag} needs a value"))?;
        match *flag {
            "--friends" => options.friends = value.parse().context("--friends needs a number")?,
            "--limit" => options.limit = Some(value.parse().context("--limit needs a number")?),
            "--extra" => options.extra = Some(value.to_string()),
            "--extra-limit" => {
                options.extra_limit = Some(value.parse().context("--extra-limit needs a number")?)
            }
            other => bail!("unknown option {other}"),
        }
    }
    if options.friends == 0 {
        bail!("--friends must be at least 1");
    }
    Ok(options)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("HSFT_LOG"))
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["bench", path, options @ ..] => bench(Path::new(path), parse_bench(options)?).await,
        ["id"] => {
            let (node, _) = start().await?;
            println!("{}", node.id());
            node.shutdown().await;
            Ok(())
        }
        ["friends"] => {
            let (node, _) = start().await?;
            for friend in node.friends() {
                println!("{}  {}", friend.name, friend.id);
            }
            node.shutdown().await;
            Ok(())
        }
        ["invite"] => invite().await,
        ["join", code] => {
            let (node, _) = start().await?;
            let joined = node.join(code).await;
            node.shutdown().await;
            println!("You and {} are now friends.", joined?.name);
            Ok(())
        }
        ["receive", dest] => receive(PathBuf::from(dest)).await,
        ["send", friend, paths @ ..] if !paths.is_empty() => {
            send(friend, paths.iter().map(PathBuf::from).collect()).await
        }
        _ => {
            print!("{USAGE}");
            Ok(())
        }
    }
}

fn data_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("HSFT_DATA") {
        return Ok(dir.into());
    }
    Ok(dirs::data_dir().context("no data folder")?.join("HSFT"))
}

async fn start() -> Result<(Node, tokio::sync::mpsc::UnboundedReceiver<Event>)> {
    let network = match std::env::var_os("HSFT_RELAY_ONLY") {
        Some(_) => Network::RelayOnly,
        None => Network::Internet,
    };
    Node::start(&data_dir()?, network).await
}

async fn invite() -> Result<()> {
    let (node, mut events) = start().await?;
    println!("{}", node.create_invite().await?);
    eprintln!("Waiting for your friend to use this code...");
    while let Some(event) = events.recv().await {
        if let Event::FriendAdded { friend } = event {
            println!("You and {} are now friends.", friend.name);
            break;
        }
    }
    node.shutdown().await;
    Ok(())
}

fn find_friend(node: &Node, query: &str) -> Result<FriendInfo> {
    let query = query.to_lowercase();
    let matches: Vec<_> = node
        .friends()
        .into_iter()
        .filter(|f| f.name.to_lowercase() == query || f.id.starts_with(&query))
        .collect();
    match <[FriendInfo; 1]>::try_from(matches) {
        Ok([friend]) => Ok(friend),
        Err(matches) if matches.is_empty() => bail!("no friend called {query}"),
        Err(_) => bail!("more than one friend matches {query}"),
    }
}

async fn send(friend: &str, paths: Vec<PathBuf>) -> Result<()> {
    let (node, mut events) = start().await?;
    let friend = find_friend(&node, friend)?;
    node.send(&friend.id, paths)?;
    let mut failure = None;
    while let Some(event) = events.recv().await {
        match event {
            Event::Progress { .. } => print_progress(&event),
            Event::Finished { .. } => {
                println!("\nDelivered to {}.", friend.name);
                break;
            }
            Event::Failed { error, .. } => {
                failure = Some(error);
                break;
            }
            _ => {}
        }
    }
    node.shutdown().await;
    failure.map_or(Ok(()), |error| bail!("{error}"))
}

async fn receive(dest: PathBuf) -> Result<()> {
    let (node, mut events) = start().await?;
    println!("Waiting for friends to send something...");
    while let Some(event) = events.recv().await {
        match event {
            Event::Offer { transfer, from_name, title, bytes, .. } => {
                println!("{from_name} is sending {title} ({})", size(bytes));
                node.respond(transfer, Some(dest.clone()));
            }
            Event::Progress { .. } => print_progress(&event),
            Event::Finished { saved_to, .. } => {
                println!("\nSaved to {}", saved_to.unwrap_or_default());
            }
            Event::Failed { error, .. } => println!("\nFailed: {error}"),
            _ => {}
        }
    }
    Ok(())
}

fn print_progress(event: &Event) {
    if let Event::Progress { done, total, bytes_per_sec, route, .. } = event {
        print!("\r{} of {}  {}/s  ({route})     ", size(*done), size(*total), size(*bytes_per_sec));
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Sends through real connections between peers in this process. Without `--limit`, disk and CPU
/// are the limit, so this measures what the software can do, not a real internet route.
async fn bench(path: &Path, options: BenchOptions) -> Result<()> {
    let temp = std::env::temp_dir().join(format!("hsft-bench-{}", std::process::id()));
    let result = run_bench(path, &options, &temp).await;
    let _ = std::fs::remove_dir_all(&temp);
    result
}

async fn run_bench(path: &Path, options: &BenchOptions, temp: &Path) -> Result<()> {
    let (sender, mut sender_events) = Node::start(&temp.join("sender"), Network::LocalOnly).await?;
    sender.set_upload_limit(options.limit, options.extra_limit);
    if let Some(extra) = &options.extra {
        let mut settings = sender.settings();
        settings.extra_adapter = Some(extra.clone());
        sender.update_settings(settings)?;
    }

    let mut receivers = Vec::new();
    for i in 0..options.friends {
        let (node, events) = Node::start(&temp.join(format!("friend{i}")), Network::LocalOnly).await?;
        node.set_upload_limit(options.limit, None);
        node.join(&sender.create_invite().await?).await?;
        receivers.push((node, events, temp.join(format!("received{i}"))));
    }
    let ids: Vec<String> = receivers.iter().map(|(node, _, _)| node.id()).collect();

    let started = Instant::now();
    sender.send_group(&ids, vec![path.to_path_buf()])?;
    let mut tasks = tokio::task::JoinSet::new();
    for (index, (node, mut events, dest)) in receivers.into_iter().enumerate() {
        tasks.spawn(async move {
            let (mut bytes, mut from_peers) = (0, 0);
            while let Some(event) = events.recv().await {
                match event {
                    Event::Offer { transfer, bytes: total, .. } => {
                        bytes = total;
                        node.respond(transfer, Some(dest.clone()));
                    }
                    Event::Progress { peer_bytes, .. } => from_peers = peer_bytes,
                    Event::Finished { .. } => break,
                    Event::Failed { error, .. } => bail!("friend {}: {error}", index + 1),
                    _ => {}
                }
            }
            let seconds = started.elapsed().as_secs_f64();
            node.shutdown().await;
            Ok((index, bytes, seconds, from_peers))
        });
    }

    let mut extra = 0;
    let mut results = Vec::new();
    while results.len() < options.friends {
        tokio::select! {
            Some(joined) = tasks.join_next() => results.push(joined??),
            Some(event) = sender_events.recv() => match event {
                Event::Progress { extra_bytes: Some(bytes), .. } => extra = extra.max(bytes),
                Event::Failed { error, .. } => bail!("{error}"),
                _ => {}
            },
        }
    }
    sender.shutdown().await;
    results.sort_by_key(|r| r.0);
    // When the last friend had everything, not counting the test peers shutting down.
    let seconds = results.iter().map(|r| r.2).fold(0.0, f64::max);

    let bytes = results.first().map_or(0, |r| r.1);
    if options.friends == 1 {
        println!("{} in {seconds:.2}s = {}/s", size(bytes), size((bytes as f64 / seconds) as u64));
    } else {
        println!("{} to {} friends, all done in {seconds:.2}s", size(bytes), options.friends);
        for (index, _, done_at, from_peers) in &results {
            println!(
                "  friend {}: done after {done_at:.2}s, {} from other friends",
                index + 1,
                size(*from_peers)
            );
        }
    }
    if options.extra.is_some() {
        println!("  extra connection carried {}", size(extra));
    }
    if let Some(limit) = options.limit {
        let one_copy = bytes as f64 * 8.0 / (limit * 1_000_000.0);
        println!(
            "  for comparison, one copy at {limit} Mbit/s takes {one_copy:.2}s, so sending to each friend in turn would take {:.2}s",
            one_copy * options.friends as f64
        );
    }
    Ok(())
}
