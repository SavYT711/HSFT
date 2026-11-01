//! What gets sent: the manifest of files and how it is split into units.
//!
//! A unit is the piece of work that is read, hashed, compressed and written as one. Large files are
//! split into several units, and small files are packed together into one, because thousands of
//! tiny transfers are far slower than one big one. Both sides derive the same units from the
//! manifest, so only unit numbers ever cross the wire.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub const UNIT_SIZE: u64 = 4 * 1024 * 1024;
pub const MAX_FILES: usize = 2_000_000;
const MAX_PATH_LEN: usize = 1024;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    /// Relative path with forward slashes.
    pub path: String,
    pub size: u64,
    pub modified_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    pub files: Vec<FileEntry>,
    /// Folders, kept so that empty ones arrive too.
    pub dirs: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Part {
    pub file: u32,
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Unit {
    pub parts: Vec<Part>,
    pub len: u32,
}

impl Manifest {
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    /// Short description for showing a transfer to a person.
    pub fn title(&self) -> String {
        let first = self
            .files
            .first()
            .map(|f| f.path.as_str())
            .or(self.dirs.first().map(String::as_str))
            .unwrap_or("Nothing");
        let top = first.split('/').next().unwrap_or(first);
        let tops: HashSet<&str> = self
            .files
            .iter()
            .map(|f| f.path.as_str())
            .chain(self.dirs.iter().map(String::as_str))
            .map(|p| p.split('/').next().unwrap_or(p))
            .collect();
        match tops.len() {
            0 | 1 => top.to_string(),
            n => format!("{top} and {} more", n - 1),
        }
    }

    /// Checks everything a remote peer could get wrong on purpose. Must pass before any path in the
    /// manifest is used on disk.
    pub fn validate(&self) -> Result<()> {
        if self.files.len() + self.dirs.len() > MAX_FILES {
            bail!("too many files");
        }
        let mut total = 0u64;
        let mut seen = HashSet::new();
        for file in &self.files {
            total = total.checked_add(file.size).context("sizes overflow")?;
            check_path(&file.path)?;
            if !seen.insert(file.path.to_lowercase()) {
                bail!("duplicate path {:?}", file.path);
            }
        }
        for dir in &self.dirs {
            check_path(dir)?;
        }
        // Keeps the unit table a sensible size whatever the manifest claims.
        if total / UNIT_SIZE > u32::MAX as u64 / 2 {
            bail!("transfer is too large");
        }
        Ok(())
    }
}

/// Rejects anything that could escape the destination folder or misbehave on Windows.
pub fn check_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > MAX_PATH_LEN {
        bail!("invalid path length");
    }
    for part in path.split('/') {
        check_component(part).with_context(|| format!("unsafe path {path:?}"))?;
    }
    Ok(())
}

fn check_component(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 255 || name == "." || name == ".." {
        bail!("invalid name");
    }
    if name.ends_with('.') || name.ends_with(' ') {
        bail!("name ends with a dot or space");
    }
    if name
        .chars()
        .any(|c| c.is_control() || r#"\:*?"<>|"#.contains(c))
    {
        bail!("name has a character Windows does not allow");
    }
    let stem = name.split('.').next().unwrap_or(name).to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit());
    if reserved {
        bail!("reserved Windows name");
    }
    Ok(())
}

/// Splits the files of a manifest into units.
pub fn plan_units(files: &[FileEntry]) -> Vec<Unit> {
    let mut units = Vec::new();
    let mut batch = Unit::default();
    for (index, file) in files.iter().enumerate() {
        let index = index as u32;
        if file.size == 0 {
            continue;
        }
        if file.size < UNIT_SIZE / 2 {
            if batch.len as u64 + file.size > UNIT_SIZE {
                units.push(std::mem::take(&mut batch));
            }
            batch.parts.push(Part {
                file: index,
                offset: 0,
                len: file.size as u32,
            });
            batch.len += file.size as u32;
            continue;
        }
        let mut offset = 0;
        while offset < file.size {
            let len = (file.size - offset).min(UNIT_SIZE) as u32;
            units.push(Unit {
                parts: vec![Part {
                    file: index,
                    offset,
                    len,
                }],
                len,
            });
            offset += len as u64;
        }
    }
    if !batch.parts.is_empty() {
        units.push(batch);
    }
    units
}

/// A manifest and where its files are on this PC.
#[derive(Debug)]
pub struct Scanned {
    pub manifest: Manifest,
    pub sources: Vec<PathBuf>,
}

/// Walks the chosen files and folders. Links are skipped so that a send can never pull in something
/// outside what was picked.
pub fn scan(paths: &[PathBuf]) -> Result<Scanned> {
    let mut found = Vec::new();
    let mut dirs = Vec::new();
    for path in paths {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .with_context(|| format!("can't send {}", path.display()))?
            .to_string();
        walk(path, name, &mut found, &mut dirs)?;
    }
    found.sort_by(|a: &(FileEntry, PathBuf), b| a.0.path.cmp(&b.0.path));
    dirs.sort();
    let (files, sources) = found.into_iter().unzip();
    let manifest = Manifest { files, dirs };
    manifest.validate()?;
    Ok(Scanned { manifest, sources })
}

fn walk(
    path: &Path,
    rel: String,
    found: &mut Vec<(FileEntry, PathBuf)>,
    dirs: &mut Vec<String>,
) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)
        .with_context(|| format!("can't read {}", path.display()))?;
    if meta.is_symlink() {
        return Ok(());
    }
    if meta.is_dir() {
        let children = std::fs::read_dir(path)
            .with_context(|| format!("can't read {}", path.display()))?;
        dirs.push(rel.clone());
        for child in children {
            let child = child?;
            let name = child.file_name();
            let name = name
                .to_str()
                .with_context(|| format!("can't send {}", child.path().display()))?;
            walk(&child.path(), format!("{rel}/{name}"), found, dirs)?;
        }
    } else if meta.is_file() {
        let modified_ms = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_millis() as u64);
        found.push((
            FileEntry {
                path: rel,
                size: meta.len(),
                modified_ms,
            },
            path.to_path_buf(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, size: u64) -> FileEntry {
        FileEntry {
            path: path.into(),
            size,
            modified_ms: 0,
        }
    }

    #[test]
    fn large_files_are_split_and_every_byte_is_covered() {
        let files = [entry("big", UNIT_SIZE * 2 + 5)];
        let units = plan_units(&files);
        assert_eq!(units.len(), 3);
        assert_eq!(units[2].parts[0].offset, UNIT_SIZE * 2);
        assert_eq!(units[2].len, 5);
    }

    #[test]
    fn small_files_share_a_unit() {
        let files: Vec<_> = (0..1000).map(|i| entry(&format!("{i}"), 1024)).collect();
        let units = plan_units(&files);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].parts.len(), 1000);
        assert_eq!(units[0].len, 1024 * 1000);
    }

    #[test]
    fn batches_never_exceed_the_unit_size() {
        let files: Vec<_> = (0..20)
            .map(|i| entry(&format!("{i}"), UNIT_SIZE / 2 - 1))
            .collect();
        let units = plan_units(&files);
        assert!(units.iter().all(|u| u.len as u64 <= UNIT_SIZE));
        let total: u64 = units.iter().map(|u| u.len as u64).sum();
        assert_eq!(total, 20 * (UNIT_SIZE / 2 - 1));
    }

    #[test]
    fn empty_files_need_no_units() {
        assert!(plan_units(&[entry("empty", 0)]).is_empty());
    }

    #[test]
    fn hostile_paths_are_rejected() {
        for path in [
            "../evil",
            "a/../../evil",
            "/abs",
            r"..\evil",
            r"C:\Windows\evil",
            "C:evil",
            "a//b",
            "file.txt:stream",
            "CON",
            "nul.txt",
            "com1.log",
            "trailing.",
            "trailing ",
            "tab\tname",
            "",
        ] {
            assert!(check_path(path).is_err(), "{path:?} should be rejected");
        }
    }

    #[test]
    fn normal_paths_are_allowed() {
        for path in ["a.txt", "folder/sub folder/ü.png", ".hidden", "console.log", "com.txt"] {
            check_path(path).unwrap();
        }
    }

    #[test]
    fn duplicates_that_differ_by_case_are_rejected() {
        let manifest = Manifest {
            files: vec![entry("A.txt", 1), entry("a.TXT", 1)],
            dirs: vec![],
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn absurd_sizes_are_rejected() {
        let manifest = Manifest {
            files: vec![entry("a", u64::MAX), entry("b", 2)],
            dirs: vec![],
        };
        assert!(manifest.validate().is_err());
        let manifest = Manifest {
            files: vec![entry("a", u64::MAX / 2)],
            dirs: vec![],
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn scan_keeps_folder_structure_and_empty_folders() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("photos");
        std::fs::create_dir_all(root.join("empty")).unwrap();
        std::fs::create_dir_all(root.join("trip")).unwrap();
        std::fs::write(root.join("trip/a.txt"), "hello").unwrap();

        let scanned = scan(&[root]).unwrap();
        assert_eq!(scanned.manifest.files[0].path, "photos/trip/a.txt");
        assert_eq!(scanned.manifest.files[0].size, 5);
        assert!(scanned.manifest.dirs.contains(&"photos/empty".to_string()));
        assert_eq!(scanned.manifest.title(), "photos");
    }
}
