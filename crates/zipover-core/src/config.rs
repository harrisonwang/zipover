use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
};

pub type DeviceId = [u8; 16];
pub(crate) type PairingKey = [u8; 32];

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct TrustedPeer {
    pub id: DeviceId,
    pub name: String,
    pub key: PairingKey,
    #[serde(default)]
    pub addresses: Vec<SocketAddr>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DeviceConfig {
    pub id: DeviceId,
    pub name: String,
    pub peers: Vec<TrustedPeer>,
}

impl DeviceConfig {
    fn read(path: &Path) -> Result<Option<Self>> {
        match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| {
                    crate::i18n::localized!(
                        format,
                        "config.read_path_failed",
                        path = path.display()
                    )
                })
                .map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context(crate::i18n::tr("config.read_failed")),
        }
    }

    pub fn paired_devices() -> Result<Vec<TrustedPeer>> {
        Ok(Self::read(&config_dir()?.join("devices.json"))?
            .map(|config| config.peers)
            .unwrap_or_default())
    }
}

pub(crate) struct ConfigStore {
    pub config: DeviceConfig,
    path: PathBuf,
    _lock: File,
}

impl ConfigStore {
    pub fn load() -> Result<Self> {
        Self::open(config_dir()?.join("devices.json"))
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(
            path.parent()
                .context(crate::i18n::tr("config.invalid_directory"))?,
        )?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("lock"))?;
        lock.try_lock()
            .context(crate::i18n::tr("config.already_in_use"))?;
        let config = match DeviceConfig::read(&path)? {
            Some(config) => config,
            None => DeviceConfig {
                id: random_bytes()?,
                name: hostname::get()?.to_string_lossy().into_owned(),
                peers: Vec::new(),
            },
        };
        let store = Self {
            config,
            path,
            _lock: lock,
        };
        store.save(&store.config)?;
        Ok(store)
    }

    pub fn trust(&mut self, mut peer: TrustedPeer) -> Result<()> {
        if peer.id == self.config.id {
            crate::i18n::localized!(bail, "config.self_pairing")
        }
        let mut next = self.config.clone();
        if let Some(saved) = next.peers.iter().find(|saved| saved.id == peer.id) {
            peer.addresses.extend_from_slice(&saved.addresses);
        }
        peer.addresses.sort_unstable();
        peer.addresses.dedup();
        next.peers.retain(|saved| saved.id != peer.id);
        next.peers.push(peer);
        self.save(&next)?;
        self.config = next;
        Ok(())
    }

    pub fn forget(&mut self, id: DeviceId) -> Result<()> {
        let mut next = self.config.clone();
        next.peers.retain(|peer| peer.id != id);
        self.save(&next)?;
        self.config = next;
        Ok(())
    }

    fn save(&self, config: &DeviceConfig) -> Result<()> {
        let parent = self
            .path
            .parent()
            .context(crate::i18n::tr("config.invalid_directory"))?;
        fs::create_dir_all(parent)?;
        let temporary = self.path.with_extension("tmp");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(config)?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(temporary, &self.path).context(crate::i18n::tr("config.save_failed"))
    }
}

pub(crate) fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|error| {
        crate::i18n::localized!(anyhow, "config.random_key_failed", error = error)
    })?;
    Ok(bytes)
}

fn config_dir() -> Result<PathBuf> {
    // Also permits isolated CLI acceptance tests without touching the user's pairing.
    if let Some(path) = env::var_os("ZIPOVER_CONFIG_DIR") {
        return Ok(PathBuf::from(path));
    }
    #[cfg(windows)]
    let base = env::var_os("APPDATA").map(PathBuf::from);
    #[cfg(target_os = "macos")]
    let base =
        env::var_os("HOME").map(|path| PathBuf::from(path).join("Library/Application Support"));
    #[cfg(all(unix, not(target_os = "macos")))]
    let base = env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|path| PathBuf::from(path).join(".config")));
    Ok(base
        .context(crate::i18n::tr("config.user_directory_missing"))?
        .join("zipover"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_trust_is_private_readable_and_exclusively_writable() {
        let root = env::temp_dir().join(format!(
            "zipover-config-{}",
            hex::encode(random_bytes::<16>().unwrap())
        ));
        let path = root.join("devices.json");
        let mut store = ConfigStore::open(path.clone()).unwrap();
        let id = store.config.id;
        let peer = TrustedPeer {
            id: [1; 16],
            name: "test".into(),
            key: [2; 32],
            addresses: vec!["127.0.0.1:57321".parse().unwrap()],
        };
        store.trust(peer).unwrap();
        assert!(ConfigStore::open(path.clone()).is_err());
        assert_eq!(DeviceConfig::read(&path).unwrap().unwrap().peers.len(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        drop(store);
        let mut store = ConfigStore::open(path.clone()).unwrap();
        assert_eq!(store.config.id, id);
        assert_eq!(store.config.peers[0].key, [2; 32]);
        assert_eq!(
            store.config.peers[0].addresses,
            vec!["127.0.0.1:57321".parse::<SocketAddr>().unwrap()]
        );
        store.forget([1; 16]).unwrap();
        drop(store);
        assert!(DeviceConfig::read(&path).unwrap().unwrap().peers.is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn old_device_settings_do_not_require_saved_peer_addresses() {
        let id = [0; 16];
        let peer_id = [1; 16];
        let key = [2; 32];
        let value = serde_json::json!({
            "id": id,
            "name": "legacy",
            "peers": [{"id":peer_id,"name":"old peer","key":key}]
        });
        let config: DeviceConfig = serde_json::from_value(value).unwrap();
        assert!(config.peers[0].addresses.is_empty());
    }
}
