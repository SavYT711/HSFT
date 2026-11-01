//! Reading units on the sending side and storing them on the receiving side.
//!
//! Incoming data goes to numbered `.part` files in a hidden-away folder, with a journal of finished
//! units beside them. Nothing appears under its real name until the whole transfer is verified, and
//! an interrupted transfer picks up from the journal.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use crate::plan::{plan_units, Manifest, Unit};

pub type Hash = [u8; 32];

const JOURNAL_RECORD: usize = 4 + 32;
const MAX_OPEN_FILES: usize = 128;
const PARTIAL_DIR: &str = ".hsft-partial";

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

#[cfg(windows)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}

fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match read_at(file, buf, offset)? {
            0 => return Err(io::ErrorKind::UnexpectedEof.into()),
            n => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
        }
    }
    Ok(())
}

fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match write_at(file, buf, offset)? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            n => {
                buf = &buf[n..];
                offset += n as u64;
            }
        }
    }
    Ok(())
}

/// Keeps recently used files open, since big files are touched once per unit.
#[derive(Default)]
struct Handles(Mutex<HashMap<u32, Arc<File>>>);

impl Handles {
    fn get(&self, index: u32, open: impl FnOnce() -> Result<File>) -> Result<Arc<File>> {
        let mut map = self.0.lock().unwrap();
        if let Some(file) = map.get(&index) {
            return Ok(file.clone());
        }
        if map.len() >= MAX_OPEN_FILES {
            map.clear();
        }
        let file = Arc::new(open()?);
        map.insert(index, file.clone());
        Ok(file)
    }

    fn close_all(&self) {
        self.0.lock().unwrap().clear();
    }
}

fn read_unit_with(
    unit: &Unit,
    handle: impl Fn(u32) -> Result<Arc<File>>,
) -> Result<Vec<u8>> {
    let mut data = vec![0u8; unit.len as usize];
    let mut at = 0;
    for part in &unit.parts {
        let file = handle(part.file)?;
        let end = at + part.len as usize;
        read_exact_at(&file, &mut data[at..end], part.offset)?;
        at = end;
    }
    Ok(data)
}

/// The sending side: reads units from the original files.
pub struct Source {
    pub units: Vec<Unit>,
    paths: Vec<PathBuf>,
    handles: Handles,
}

impl Source {
    pub fn new(manifest: &Manifest, paths: Vec<PathBuf>) -> Self {
        Self {
            units: plan_units(&manifest.files),
            paths,
            handles: Handles::default(),
        }
    }

    pub fn read_unit(&self, index: u32) -> Result<Vec<u8>> {
        read_unit_with(&self.units[index as usize], |file| {
            let path = &self.paths[file as usize];
            self.handles.get(file, || {
                File::open(path).with_context(|| format!("can't open {}", path.display()))
            })
        })
        .context("a file changed or disappeared while sending")
    }
}

/// Combines the hashes of all units into one value both sides can compare.
pub fn root_hash<'a>(hashes: impl Iterator<Item = &'a Hash>) -> Hash {
    let mut hasher = blake3::Hasher::new();
    for hash in hashes {
        hasher.update(hash);
    }
    *hasher.finalize().as_bytes()
}

/// The unfinished state of one incoming transfer.
pub struct Partial {
    dest: PathBuf,
    dir: PathBuf,
    manifest: Arc<Manifest>,
    units: Vec<Unit>,
    handles: Handles,
    journal: Mutex<File>,
    state: Mutex<PartialState>,
}

struct PartialState {
    hashes: Vec<Option<Hash>>,
    missing: usize,
}

impl Partial {
    /// Opens or creates the partial state. Units from an earlier attempt are re-hashed from disk,
    /// because the journal only proves they were written, not that they survived.
    pub fn open(dest: &Path, key: &str, manifest: Arc<Manifest>) -> Result<Self> {
        let dir = dest.join(PARTIAL_DIR).join(key);
        fs::create_dir_all(&dir).with_context(|| format!("can't write to {}", dest.display()))?;
        let units = plan_units(&manifest.files);
        let mut hashes = vec![None; units.len()];

        let journal_path = dir.join("journal");
        let mut journaled = Vec::new();
        if let Ok(mut file) = File::open(&journal_path) {
            file.read_to_end(&mut journaled)?;
        }
        let handles = Handles::default();
        for (unit, hash) in check_journal(&dir, &units, &journaled, &manifest) {
            hashes[unit as usize] = Some(hash);
        }

        let mut journal = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&journal_path)?;
        let mut kept = Vec::new();
        for (unit, hash) in hashes.iter().enumerate() {
            if let Some(hash) = hash {
                kept.extend_from_slice(&(unit as u32).to_le_bytes());
                kept.extend_from_slice(hash);
            }
        }
        journal.write_all(&kept)?;

        let missing = hashes.iter().filter(|h| h.is_none()).count();
        Ok(Self {
            dest: dest.to_path_buf(),
            dir,
            manifest,
            units,
            handles,
            journal: Mutex::new(journal),
            state: Mutex::new(PartialState { hashes, missing }),
        })
    }

    pub fn have(&self) -> Vec<u32> {
        let state = self.state.lock().unwrap();
        (0..state.hashes.len() as u32)
            .filter(|&unit| state.hashes[unit as usize].is_some())
            .collect()
    }

    pub fn units(&self) -> &[Unit] {
        &self.units
    }

    pub fn bytes_held(&self) -> u64 {
        let state = self.state.lock().unwrap();
        self.units
            .iter()
            .zip(&state.hashes)
            .filter(|(_, hash)| hash.is_some())
            .map(|(unit, _)| unit.len as u64)
            .sum()
    }

    pub fn is_complete(&self) -> bool {
        self.state.lock().unwrap().missing == 0
    }

    /// Writes a verified unit. Returns true once every unit is held.
    pub fn store(&self, index: u32, data: &[u8], hash: Hash) -> Result<bool> {
        let unit = &self.units[index as usize];
        if self.state.lock().unwrap().hashes[index as usize].is_some() {
            return Ok(self.is_complete());
        }
        let mut at = 0;
        for part in &unit.parts {
            let size = self.manifest.files[part.file as usize].size;
            let file = self.handles.get(part.file, || {
                let path = part_path(&self.dir, part.file);
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .with_context(|| format!("can't write {}", path.display()))?;
                file.set_len(size).context("not enough disk space")?;
                Ok(file)
            })?;
            let end = at + part.len as usize;
            write_all_at(&file, &data[at..end], part.offset)?;
            at = end;
        }

        let mut record = [0u8; JOURNAL_RECORD];
        record[..4].copy_from_slice(&index.to_le_bytes());
        record[4..].copy_from_slice(&hash);
        self.journal.lock().unwrap().write_all(&record)?;

        let mut state = self.state.lock().unwrap();
        if state.hashes[index as usize].is_none() {
            state.hashes[index as usize] = Some(hash);
            state.missing -= 1;
        }
        Ok(state.missing == 0)
    }

    pub fn root(&self) -> Result<Hash> {
        let state = self.state.lock().unwrap();
        if state.missing != 0 {
            bail!("transfer is not complete");
        }
        Ok(root_hash(state.hashes.iter().flatten()))
    }

    /// Moves everything to its real name, and returns where it landed. Existing files are never
    /// replaced: a clash gets a numbered name instead.
    pub fn finalize(&self) -> Result<PathBuf> {
        self.handles.close_all();
        let mut renamed: HashMap<&str, String> = HashMap::new();
        let top_names: Vec<(&str, bool)> = self
            .manifest
            .files
            .iter()
            .map(|f| (f.path.as_str(), true))
            .chain(self.manifest.dirs.iter().map(|d| (d.as_str(), false)))
            .collect();
        for (path, is_file) in top_names {
            let (top, rest) = path.split_once('/').map_or((path, None), |(t, r)| (t, Some(r)));
            renamed
                .entry(top)
                .or_insert_with(|| free_name(&self.dest, top, is_file && rest.is_none()));
        }
        let target = |path: &str| {
            let (top, rest) = path.split_once('/').map_or((path, ""), |(t, r)| (t, r));
            let mut target = self.dest.join(&renamed[top]);
            for part in rest.split('/').filter(|p| !p.is_empty()) {
                target.push(part);
            }
            target
        };

        for dir in &self.manifest.dirs {
            fs::create_dir_all(target(dir))?;
        }
        for (index, entry) in self.manifest.files.iter().enumerate() {
            let target = target(&entry.path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            let part = part_path(&self.dir, index as u32);
            if entry.size == 0 {
                File::create_new(&part)?;
            }
            if entry.modified_ms != 0 {
                let modified = UNIX_EPOCH + Duration::from_millis(entry.modified_ms);
                OpenOptions::new().write(true).open(&part)?.set_modified(modified)?;
            }
            fs::rename(&part, &target)
                .with_context(|| format!("can't save {}", target.display()))?;
        }

        let saved_to = match renamed.len() {
            1 => self.dest.join(renamed.values().next().unwrap()),
            _ => self.dest.clone(),
        };
        self.discard();
        Ok(saved_to)
    }

    /// Removes the partial data.
    pub fn discard(&self) {
        self.handles.close_all();
        let _ = fs::remove_dir_all(&self.dir);
        if let Some(parent) = self.dir.parent() {
            let _ = fs::remove_dir(parent);
        }
    }
}

fn part_path(dir: &Path, file: u32) -> PathBuf {
    dir.join(format!("{file}.part"))
}

/// Returns the journal entries whose data on disk still matches.
fn check_journal(
    dir: &Path,
    units: &[Unit],
    journal: &[u8],
    manifest: &Manifest,
) -> Vec<(u32, Hash)> {
    let records: Vec<(u32, Hash)> = journal
        .chunks_exact(JOURNAL_RECORD)
        .map(|r| {
            let unit = u32::from_le_bytes(r[..4].try_into().unwrap());
            (unit, r[4..].try_into().unwrap())
        })
        .filter(|(unit, _)| (*unit as usize) < units.len())
        .collect();
    if records.is_empty() {
        return Vec::new();
    }
    let workers = std::thread::available_parallelism().map_or(4, |n| n.get());
    let next = std::sync::atomic::AtomicUsize::new(0);
    let good = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let handles = Handles::default();
                loop {
                    let at = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(&(unit, hash)) = records.get(at) else {
                        break;
                    };
                    let data = read_unit_with(&units[unit as usize], |file| {
                        handles.get(file, || {
                            let part = File::open(part_path(dir, file))?;
                            if part.metadata()?.len() != manifest.files[file as usize].size {
                                bail!("wrong size");
                            }
                            Ok(part)
                        })
                    });
                    if data.is_ok_and(|d| *blake3::hash(&d).as_bytes() == hash) {
                        good.lock().unwrap().push((unit, hash));
                    }
                }
            });
        }
    });
    good.into_inner().unwrap()
}

/// Picks a name that doesn't exist yet in `dest`, such as `movie (1).mp4`.
fn free_name(dest: &Path, name: &str, is_file: bool) -> String {
    if !dest.join(name).exists() {
        return name.to_string();
    }
    let path = Path::new(name);
    let (stem, ext) = match (is_file, path.file_stem(), path.extension()) {
        (true, Some(stem), Some(ext)) => (stem.to_string_lossy(), format!(".{}", ext.to_string_lossy())),
        _ => (name.into(), String::new()),
    };
    (1u32..)
        .map(|n| format!("{stem} ({n}){ext}"))
        .find(|candidate| !dest.join(candidate).exists())
        .expect("a free name exists")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{scan, FileEntry, UNIT_SIZE};

    fn manifest(files: &[(&str, u64)]) -> Arc<Manifest> {
        Arc::new(Manifest {
            files: files
                .iter()
                .map(|(path, size)| FileEntry {
                    path: path.to_string(),
                    size: *size,
                    modified_ms: 1_600_000_000_000,
                })
                .collect(),
            dirs: vec![],
        })
    }

    fn deliver(partial: &Partial, unit: u32, bytes: &[u8]) -> bool {
        partial.store(unit, bytes, *blake3::hash(bytes).as_bytes()).unwrap()
    }

    #[test]
    fn source_reads_back_what_is_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.bin");
        fs::write(&path, vec![7u8; (UNIT_SIZE + 10) as usize]).unwrap();
        let scanned = scan(&[path]).unwrap();
        let source = Source::new(&scanned.manifest, scanned.sources);
        assert_eq!(source.read_unit(0).unwrap().len(), UNIT_SIZE as usize);
        assert_eq!(source.read_unit(1).unwrap(), vec![7u8; 10]);
    }

    #[test]
    fn units_can_arrive_out_of_order() {
        let dest = tempfile::tempdir().unwrap();
        let m = manifest(&[("big.bin", UNIT_SIZE + 3)]);
        let partial = Partial::open(dest.path(), "k", m).unwrap();
        assert!(!deliver(&partial, 1, &[2, 2, 2]));
        assert!(deliver(&partial, 0, &vec![1u8; UNIT_SIZE as usize]));
        let saved = partial.finalize().unwrap();
        let data = fs::read(&saved).unwrap();
        assert_eq!(data.len() as u64, UNIT_SIZE + 3);
        assert_eq!(&data[UNIT_SIZE as usize..], &[2, 2, 2]);
        assert!(!dest.path().join(PARTIAL_DIR).exists());
    }

    #[test]
    fn resume_keeps_good_units_and_drops_damaged_ones() {
        let dest = tempfile::tempdir().unwrap();
        let m = manifest(&[("a.bin", UNIT_SIZE), ("b.bin", UNIT_SIZE)]);
        {
            let partial = Partial::open(dest.path(), "k", m.clone()).unwrap();
            deliver(&partial, 0, &vec![1u8; UNIT_SIZE as usize]);
            deliver(&partial, 1, &vec![2u8; UNIT_SIZE as usize]);
        }
        let reopened = Partial::open(dest.path(), "k", m.clone()).unwrap();
        assert_eq!(reopened.have(), vec![0, 1]);
        drop(reopened);

        // Damage the second file, as a crash or bad disk would.
        let part = dest.path().join(PARTIAL_DIR).join("k/1.part");
        let file = OpenOptions::new().write(true).open(part).unwrap();
        write_all_at(&file, &[9], 100).unwrap();
        drop(file);

        let reopened = Partial::open(dest.path(), "k", m).unwrap();
        assert_eq!(reopened.have(), vec![0]);
    }

    #[test]
    fn existing_files_are_never_replaced() {
        let dest = tempfile::tempdir().unwrap();
        fs::write(dest.path().join("note.txt"), "mine").unwrap();
        let partial = Partial::open(dest.path(), "k", manifest(&[("note.txt", 5)])).unwrap();
        deliver(&partial, 0, b"yours");
        let saved = partial.finalize().unwrap();
        assert_eq!(saved.file_name().unwrap(), "note (1).txt");
        assert_eq!(fs::read(dest.path().join("note.txt")).unwrap(), b"mine");
        assert_eq!(fs::read(saved).unwrap(), b"yours");
    }

    #[test]
    fn existing_folders_stay_untouched() {
        let dest = tempfile::tempdir().unwrap();
        fs::create_dir(dest.path().join("docs")).unwrap();
        fs::write(dest.path().join("docs/a.txt"), "mine").unwrap();
        let partial = Partial::open(dest.path(), "k", manifest(&[("docs/a.txt", 5)])).unwrap();
        deliver(&partial, 0, b"yours");
        let saved = partial.finalize().unwrap();
        assert_eq!(saved.file_name().unwrap(), "docs (1)");
        assert_eq!(fs::read(dest.path().join("docs/a.txt")).unwrap(), b"mine");
        assert_eq!(fs::read(saved.join("a.txt")).unwrap(), b"yours");
    }

    #[test]
    fn empty_files_and_folders_arrive_and_dates_are_kept() {
        let dest = tempfile::tempdir().unwrap();
        let mut m = (*manifest(&[("top/empty.txt", 0), ("top/a.txt", 1)])).clone();
        m.dirs = vec!["top".into(), "top/nothing".into()];
        let partial = Partial::open(dest.path(), "k", Arc::new(m)).unwrap();
        deliver(&partial, 0, b"x");
        let saved = partial.finalize().unwrap();
        assert_eq!(fs::metadata(saved.join("empty.txt")).unwrap().len(), 0);
        assert!(saved.join("nothing").is_dir());
        let modified = fs::metadata(saved.join("a.txt")).unwrap().modified().unwrap();
        assert_eq!(modified, UNIX_EPOCH + Duration::from_millis(1_600_000_000_000));
    }
}
