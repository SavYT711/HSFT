# HSFT

High-speed, end-to-end encrypted file transfer between friends, for Windows. Files go directly
from one PC to another, with no size limit and no cloud storage in between.

## Install

Download `HSFT_x.y.z_x64-setup.exe` from the [latest release](https://github.com/SavYT711/HSFT/releases/latest)
and run it. No admin rights are needed.

The installer isn't code-signed yet, so Windows SmartScreen may warn about it. Choose
**More info**, then **Run anyway**. On first use, Windows Firewall may ask whether HSFT can use
the network. Allow it, or transfers may be slower or fail.

## Use it

1. **Add a friend:** choose **Add friend**, create an invite code, and send it to them privately.
   They paste it into their copy of HSFT. A code works once, for 15 minutes.
2. **Send:** drop files or folders on a friend, or use **Send files** or **Folder**.
3. **Receive:** accept the offer that appears. Files are saved in your Downloads folder unless you
   choose another one in Settings.

HSFT must be running on both PCs. Closing the window keeps it in the system tray so friends can
still reach you. Use **Quit** in the tray menu to stop it.

If a transfer stops, send the same thing again and it continues where it left off.

### Send to several friends at once

Choose the group button next to **Add friend**, tick the friends, and pick files. Each friend gets
part of the files from you and passes it on to the others, torrent-style, so everyone has the
files in about the time one copy takes instead of one copy per friend. The friends you choose
connect to each other directly for that transfer, and each of them sees who else is receiving it
before accepting.

### Extra connection

In **Settings**, choose a second network adapter under **Extra connection**, such as Wi-Fi joined
to your phone's hotspot while Ethernet stays on your router. HSFT then sends through both, adding
the second connection's upload speed. Mobile data runs out fast, so keep an eye on it. The extra
connection is only used when it can reach your friend directly.

## How it works

- **Direct connections:** peers connect over QUIC using [iroh](https://github.com/n0-computer/iroh),
  which finds the fastest route and punches through home routers.
- **Fallback relay:** when no direct route exists, encrypted data passes through a relay.
  This is much slower, and the app shows when it happens.
- **Fast by design:** data flows over 16 parallel streams, is compressed only when that helps, and
  small files are packed together.
- **Verified:** every 4 MiB unit is hashed with BLAKE3 and checked on arrival. Damaged units are
  sent again, and files only appear under their real names once the whole transfer checks out.

## Security and privacy

- All traffic is end-to-end encrypted, and relays can't read it.
- Only paired friends can offer you files, and nothing arrives until you accept.
- Received paths are checked so a sender can't write outside your chosen folder, and existing
  files are never overwritten.
- In a group send, the other receivers can connect to you only for that transfer, because the
  sender listed them. They can see your IP address, as your friends can.
- HSFT currently uses the public relay and address-lookup servers run by
  [number 0](https://n0.computer), the makers of iroh. They can see which PCs connect and when,
  but not your files.
- An invite code contains your network addresses, including your public IP, so share it privately.
  Anyone who connects to you directly can see your IP, as with most peer-to-peer apps.

## Speed

Measured between two peers on one PC (Intel i5-12400F, NVMe SSD). This shows what the software can
do. Over the internet, the slower connection's upload or download speed sets the limit.

| Test | Result |
|---|---|
| 3 GB of incompressible data | 106 to 278 MB/s across runs |
| 20,000 files of 8 KB | 8 to 12 s, about 2,000 files per second |
| Through the public relay | About 0.35 MB/s |

Group sends and the extra connection only pay off when uploads are the limit, so they were
measured with every peer's upload capped at 100 Mbit/s (`--limit 100`), sending 400 MB:

| Test | Everyone has the files after | One copy at a time |
|---|---|---|
| 1 friend | 33.6 s | 33.6 s |
| 2 friends, group send | 37.9 s | 67.1 s |
| 3 friends, group send | 39.8 s | 100.7 s |
| 4 friends, group send | 42.4 s | 134.2 s |
| 1 friend, plus an extra connection capped at 20 Mbit/s | 29.2 s | 33.6 s |
| 1 friend, plus an extra connection capped at 50 Mbit/s | 23.5 s | 33.6 s |

These ran on one PC, so they show how the protocol behaves, not a real internet route.

The relay figure only applies when no direct route can be found, such as when both friends are
behind strict routers. The app shows "Through a relay, slow" when this happens. The public relay is
too slow for large files, so a self-hosted relay is planned.

### Getting the best speed

- Use Ethernet on both PCs if you can. Wi-Fi is often the real bottleneck.
- Keep both PCs on the same plan's fastest connection, and pause other uploads such as cloud backups.
- If a transfer shows "Through a relay, slow", connect both PCs with
  [Tailscale](https://tailscale.com) (free for personal use) before creating the invite code. HSFT
  picks up Tailscale addresses like any others, which can give you a direct route.

Run your own measurement with `hsft-cli bench <path>`. Add `--friends 3 --limit 100` to try a
group send, or `--extra Wi-Fi --extra-limit 20` to try an extra connection. `hsft-cli adapters`
lists adapter names.

## Build from source

Requires Rust and the Visual Studio C++ tools with the Windows SDK.

```bash
cargo test -p hsft-core
```

```bash
cargo run --release -p hsft-cli -- bench <path>
```

To build the installer:

```bash
cargo install tauri-cli --version 2.12.4 --locked
```

```bash
cd crates/hsft-app && cargo tauri build
```

## License

[MIT](LICENSE)
