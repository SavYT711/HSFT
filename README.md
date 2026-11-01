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

The relay figure only applies when no direct route can be found, such as when both friends are
behind strict routers. The app shows "through a relay" when this happens. The public relay is too
slow for large files, so a self-hosted relay is planned.

Run your own measurement with `hsft-cli bench <path>`.

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
cargo install tauri-cli --version "^2" --locked
```

```bash
cd crates/hsft-app && cargo tauri build
```

## License

[MIT](LICENSE)
