//! Identity, friends and settings, kept as small files in the app's data folder.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use iroh::{EndpointAddr, SecretKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Friend {
    pub id: String,
    pub name: String,
    /// Where they were last seen. Only a hint: the connection is authenticated by `id`.
    pub addr: Option<EndpointAddr>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Settings {
    pub name: String,
    pub download_dir: Option<PathBuf>,
}

pub struct Store {
    dir: PathBuf,
    pub friends: Vec<Friend>,
    pub settings: Settings,
}

impl Store {
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("can't create {}", dir.display()))?;
        let mut settings: Settings = read_json(&dir.join("settings.json"))?.unwrap_or_default();
        if settings.name.is_empty() {
            settings.name = std::env::var("USERNAME")
                .or_else(|_| std::env::var("USER"))
                .unwrap_or_else(|_| "Friend".into());
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            friends: read_json(&dir.join("friends.json"))?.unwrap_or_default(),
            settings,
        })
    }

    pub fn secret_key(&self) -> Result<SecretKey> {
        let path = self.dir.join("identity.key");
        if let Ok(text) = fs::read_to_string(&path) {
            let bytes: [u8; 32] = hex::decode(text.trim())
                .ok()
                .and_then(|b| b.try_into().ok())
                .context("identity.key is damaged")?;
            return Ok(SecretKey::from_bytes(&bytes));
        }
        let key = SecretKey::generate();
        write_atomic(&path, hex::encode(key.to_bytes()).as_bytes())?;
        Ok(key)
    }

    /// Adds a friend, or refreshes them if already known.
    pub fn add_friend(&mut self, friend: Friend) -> Result<()> {
        self.friends.retain(|f| f.id != friend.id);
        self.friends.push(friend);
        self.friends.sort_by_key(|f| f.name.to_lowercase());
        self.save_friends()
    }

    pub fn remove_friend(&mut self, id: &str) -> Result<()> {
        self.friends.retain(|f| f.id != id);
        self.save_friends()
    }

    pub fn friend(&self, id: &str) -> Option<&Friend> {
        self.friends.iter().find(|f| f.id == id)
    }

    fn save_friends(&self) -> Result<()> {
        write_atomic(
            &self.dir.join("friends.json"),
            &serde_json::to_vec_pretty(&self.friends)?,
        )
    }

    pub fn save_settings(&self) -> Result<()> {
        write_atomic(
            &self.dir.join("settings.json"),
            &serde_json::to_vec_pretty(&self.settings)?,
        )
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("{} is damaged", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_extension("tmp");
    fs::write(&temp, bytes)?;
    fs::rename(&temp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_and_friends_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let key = store.secret_key().unwrap();
        store
            .add_friend(Friend {
                id: "abc".into(),
                name: "Sam".into(),
                addr: None,
            })
            .unwrap();
        store.settings.name = "Me".into();
        store.save_settings().unwrap();

        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.secret_key().unwrap().public(), key.public());
        assert_eq!(store.friend("abc").unwrap().name, "Sam");
        assert_eq!(store.settings.name, "Me");
    }

    #[test]
    fn adding_a_friend_twice_keeps_one_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        for name in ["Old", "New"] {
            store
                .add_friend(Friend {
                    id: "abc".into(),
                    name: name.into(),
                    addr: None,
                })
                .unwrap();
        }
        assert_eq!(store.friends.len(), 1);
        assert_eq!(store.friends[0].name, "New");
    }
}
