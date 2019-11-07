// Copyright 2019 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rand::{thread_rng, Rng};
use std::fs::File;
use std::io::Error as IoError;
use std::io::{BufWriter, Cursor, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use crypto::aessafe::AesSafe256Encryptor;
use crypto::blockmodes::CtrMode;
use crypto::buffer::{BufferResult, ReadBuffer, RefReadBuffer, RefWriteBuffer, WriteBuffer};
use crypto::hkdf::hkdf_expand;
use crypto::hmac::Hmac;
use crypto::mac::{Mac, MacResult};
use crypto::pbkdf2::pbkdf2;
use crypto::sha2::{Sha256, Sha512};
use crypto::symmetriccipher::{Decryptor, Encryptor};

use tantivy::directory::error::IOError as TvIoError;
use tantivy::directory::error::{
    DeleteError, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use tantivy::directory::Directory;
use tantivy::directory::WatchHandle;
use tantivy::directory::{
    AntiCallToken, DirectoryLock, Lock, ReadOnlySource, TerminatingWrite, WatchCallback, WritePtr,
};

use zeroize::Zeroizing;

use crate::encrypted_stream::{AesReader, AesWriter};

/// KeyBuffer type that makes sure that the buffer is zeroed out before being
/// dropped.
type KeyBuffer = Zeroizing<Vec<u8>>;

/// Key derivation result type for our initial key derivation. Consists of a
/// tuple containing a encryption key, a MAC key, and a random salt.
type InitialKeyDerivationResult = (KeyBuffer, KeyBuffer, Vec<u8>);

/// Key derivation result for our subsequent key derivations. The salt will be
/// read from our key file and we will re-derive our encryption and MAC keys.
type KeyDerivationResult = (KeyBuffer, KeyBuffer);

// The constants here are chosen to be similar to the constants for the Matrix
// key export format[1].
// [1] https://matrix.org/docs/spec/client_server/r0.5.0#key-exports
const KEYFILE: &str = "seshat-index.key";
// 16 byte random salt.
const SALT_SIZE: usize = 16;
// 16 byte random IV for the AES-CTR mode.
const IV_SIZE: usize = 16;
// 32 byte or 256 bit encryption keys.
const KEY_SIZE: usize = 32;
// 32 byte message authentication code since HMAC-SHA256 is used.
const MAC_LENGTH: usize = 32;
// 1 byte for the store version.
const VERSION: u8 = 1;

#[cfg(test)]
// Tests don't need to protect against brute force attacks.
const PBKDF_COUNT: u32 = 10;

#[cfg(not(test))]
// This is quite a bit lower than the spec since the encrypted key and index
// will not be public. An attacker would need to get hold of the encrypted index,
// its key and only then move on to bruteforcing the key. Even if the attacker
// decrypts the index he wouldn't get to the events itself, only to the index
// of them.
const PBKDF_COUNT: u32 = 10_000;

#[derive(Clone, Debug)]
/// A Directory implementation that wraps a MmapDirectory and adds AES based
/// encryption to the file read/write operations.
pub struct EncryptedMmapDirectory {
    path: PathBuf,
    mmap_dir: tantivy::directory::MmapDirectory,
    encryption_key: KeyBuffer,
    mac_key: KeyBuffer,
}

impl EncryptedMmapDirectory {
    /// Open a encrypted mmap directory. If the directory is empty a new
    /// directory key will be generated and encrypted with the given passphrase.
    ///
    /// # Arguments
    ///
    /// * `path` - The path where the directory should reside in.
    /// * `passphrase` - The passphrase.
    ///
    /// Returns an error if the path does not exist, if it is not a directory or
    /// if there was an error when trying to decrypt the directory key e.g. the
    /// given passphrase was incorrect.
    pub fn open<P: AsRef<Path>>(path: P, passphrase: &str) -> Result<Self, OpenDirectoryError> {
        if passphrase.is_empty() {
            return Err(IoError::new(ErrorKind::Other, "empty passphrase").into());
        }

        let path = PathBuf::from(path.as_ref());
        let key_path = path.as_path().join(KEYFILE);

        let key_file = File::open(&key_path);

        // Either load a store key or create a new store key if the key file
        // doesn't exist.
        let store_key = match key_file {
            Ok(k) => EncryptedMmapDirectory::load_store_key(k, passphrase)?,
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    return Err(e.into());
                }
                EncryptedMmapDirectory::create_new_store(&key_path, passphrase)?
            }
        };

        // Expand the store key into a encryption and MAC key.
        let (encryption_key, mac_key) = EncryptedMmapDirectory::expand_store_key(&store_key);

        // Open our underlying bare tantivy mmap based directory.
        let mmap_dir = tantivy::directory::MmapDirectory::open(&path)?;

        Ok(EncryptedMmapDirectory {
            path,
            mmap_dir,
            encryption_key,
            mac_key,
        })
    }

    /// Change the passphrase that is used to encrypt the store key.
    /// This will decrypt and re-encrypt the store key using the new passphrase.
    ///
    /// # Arguments
    ///
    /// * `path` - The path where the directory resides in.
    /// * `old_passphrase` - The currently used passphrase.
    /// * `new_passphrase` - The passphrase that should be used from now on.
    pub fn change_passphrase<P: AsRef<Path>>(
        path: P,
        old_passphrase: &str,
        new_passphrase: &str,
    ) -> Result<(), OpenDirectoryError> {
        if old_passphrase.is_empty() || new_passphrase.is_empty() {
            return Err(IoError::new(ErrorKind::Other, "empty passphrase").into());
        }

        let key_path = path.as_ref().join(KEYFILE);
        let key_file = File::open(&key_path)?;

        // Load our store key using the old passphrase.
        let store_key = EncryptedMmapDirectory::load_store_key(key_file, old_passphrase)?;
        // Derive new encryption keys using the new passphrase.
        let (key, hmac_key, salt) = EncryptedMmapDirectory::derive_key(new_passphrase)?;
        // Re-encrypt our store key using the newly derived keys.
        EncryptedMmapDirectory::encrypt_store_key(&key, &salt, &hmac_key, &store_key, &key_path)?;

        Ok(())
    }

    /// Expand the given store key into an encryption key and HMAC key.
    fn expand_store_key(store_key: &[u8]) -> (KeyBuffer, KeyBuffer) {
        let mut hkdf_result = Zeroizing::new([0u8; KEY_SIZE * 2]);

        hkdf_expand(Sha512::new(), &store_key, &[], &mut *hkdf_result);
        let (key, hmac_key) = hkdf_result.split_at(KEY_SIZE);
        (
            Zeroizing::new(Vec::from(key)),
            Zeroizing::new(Vec::from(hmac_key)),
        )
    }

    /// Load a store key from the given file and decrypt it using the given
    /// passphrase.
    fn load_store_key(
        mut key_file: File,
        passphrase: &str,
    ) -> Result<KeyBuffer, OpenDirectoryError> {
        let mut iv = [0u8; IV_SIZE];
        let mut salt = [0u8; SALT_SIZE];
        let mut expected_mac = [0u8; MAC_LENGTH];
        let mut version = [0u8; 1];
        let mut encrypted_key = vec![];

        // Read our iv, salt, mac, and encrypted key from our key file.
        key_file.read_exact(&mut version)?;
        key_file.read_exact(&mut iv)?;
        key_file.read_exact(&mut salt)?;
        key_file.read_exact(&mut expected_mac)?;

        // Our key will be AES encrypted in CTR mode meaning the ciphertext
        // will have the same size as the plaintext. Read at most KEY_SIZE
        // bytes here so we don't end up filling up memory unnecessarily if
        // someone modifies the file.
        key_file
            .take(KEY_SIZE as u64)
            .read_to_end(&mut encrypted_key)?;

        if version[0] != VERSION {
            return Err(IoError::new(ErrorKind::Other, "invalid index store version").into());
        }

        // Rederive our key using the passphrase and salt.
        let (key, hmac_key) = EncryptedMmapDirectory::rederive_key(passphrase, &salt);

        // First check our MAC of the encrypted key.
        let expected_mac = MacResult::new(&expected_mac);
        let mac = EncryptedMmapDirectory::calculate_hmac(
            version[0],
            &iv,
            &salt,
            &encrypted_key,
            &hmac_key,
        );

        if mac != expected_mac {
            return Err(IoError::new(ErrorKind::Other, "invalid MAC of the store key").into());
        }

        let algorithm = AesSafe256Encryptor::new(&key);
        let mut decryptor = CtrMode::new(algorithm, iv.to_vec());

        let mut out = Zeroizing::new(vec![0u8; KEY_SIZE]);
        let mut write_buf = RefWriteBuffer::new(&mut out);

        let remaining;
        // Decrypt the encrypted key and return it.
        let res;
        {
            let mut read_buf = RefReadBuffer::new(&encrypted_key);
            res = decryptor
                .decrypt(&mut read_buf, &mut write_buf, true)
                .map_err(|e| {
                    IoError::new(
                        ErrorKind::Other,
                        format!("error decrypting store key: {:?}", e),
                    )
                })?;
            remaining = read_buf.remaining();
        }

        if remaining != 0 {
            return Err(IoError::new(
                ErrorKind::Other,
                "unable to decrypt complete store key ciphertext",
            )
            .into());
        }

        match res {
            BufferResult::BufferUnderflow => (),
            BufferResult::BufferOverflow => {
                return Err(IoError::new(ErrorKind::Other, "error decrypting store key").into())
            }
        }

        Ok(out)
    }

    /// Calculate a HMAC for the given inputs.
    fn calculate_hmac(
        version: u8,
        iv: &[u8],
        salt: &[u8],
        encrypted_data: &[u8],
        hmac_key: &[u8],
    ) -> MacResult {
        let mut hmac = Hmac::new(Sha256::new(), hmac_key);
        hmac.input(&[version]);
        hmac.input(&iv);
        hmac.input(&salt);
        hmac.input(&encrypted_data);
        hmac.result()
    }

    /// Create a new store key, encrypt it with the given passphrase and store
    /// it in the given path.
    fn create_new_store(
        key_path: &Path,
        passphrase: &str,
    ) -> Result<KeyBuffer, OpenDirectoryError> {
        // Derive a AES key from our passphrase using a randomly generated salt
        // to prevent bruteforce attempts using rainbow tables.
        let (key, hmac_key, salt) = EncryptedMmapDirectory::derive_key(passphrase)?;
        // Generate a new random store key. This key will encrypt our tantivy
        // indexing files. The key itself is stored encrypted using the derived
        // key.
        let store_key = EncryptedMmapDirectory::generate_key()?;

        // Encrypt and save the encrypted store key to a file.
        EncryptedMmapDirectory::encrypt_store_key(&key, &salt, &hmac_key, &store_key, key_path)?;

        Ok(store_key)
    }

    /// Encrypt the given store key and save it in the given path.
    fn encrypt_store_key(
        key: &[u8],
        salt: &[u8],
        hmac_key: &[u8],
        store_key: &[u8],
        key_path: &Path,
    ) -> Result<(), OpenDirectoryError> {
        // Generate a random initialization vector for our AES encryptor.
        let iv = EncryptedMmapDirectory::generate_iv()?;
        let algorithm = AesSafe256Encryptor::new(&key);
        let mut encryptor = CtrMode::new(algorithm, iv.clone());

        let mut read_buf = RefReadBuffer::new(&store_key);
        let mut out = [0u8; KEY_SIZE];
        let mut write_buf = RefWriteBuffer::new(&mut out);
        let mut encrypted_key = Vec::new();

        let mut key_file = File::create(key_path)?;

        // Write down our public salt and iv first, those will be needed to
        // decrypt the key again.
        key_file.write_all(&[VERSION])?;
        key_file.write_all(&iv)?;
        key_file.write_all(&salt)?;

        // Encrypt our key.
        let res = encryptor
            .encrypt(&mut read_buf, &mut write_buf, true)
            .map_err(|e| {
                IoError::new(
                    ErrorKind::Other,
                    format!("unable to encrypt store key: {:?}", e),
                )
            })?;
        let mut enc = write_buf.take_read_buffer();
        let mut enc = Vec::from(enc.take_remaining());

        encrypted_key.append(&mut enc);

        match res {
            BufferResult::BufferUnderflow => (),
            _ => {
                return Err(IoError::new(
                    ErrorKind::Other,
                    "unable to encrypt store key: unable to encrypt whole plaintext".to_string(),
                )
                .into());
            }
        }

        // Calculate a MAC for our encrypted key and store it in the file before
        // the key.
        let mac =
            EncryptedMmapDirectory::calculate_hmac(VERSION, &iv, &salt, &encrypted_key, &hmac_key);
        key_file.write_all(mac.code())?;

        // Write down the encrypted key.
        key_file.write_all(&encrypted_key)?;

        Ok(())
    }

    /// Generate a random IV.
    fn generate_iv() -> Result<Vec<u8>, OpenDirectoryError> {
        let mut iv = vec![0u8; IV_SIZE];
        let mut rng = thread_rng();
        rng.try_fill(&mut iv[..])
            .map_err(|e| IoError::new(ErrorKind::Other, format!("error generating iv: {:?}", e)))?;
        Ok(iv)
    }

    /// Generate a random key.
    fn generate_key() -> Result<KeyBuffer, OpenDirectoryError> {
        let mut key = Zeroizing::new(vec![0u8; KEY_SIZE]);
        let mut rng = thread_rng();
        rng.try_fill(&mut key[..]).map_err(|e| {
            IoError::new(ErrorKind::Other, format!("error generating key: {:?}", e))
        })?;
        Ok(key)
    }

    /// Derive two keys from the given passphrase and the given salt using PBKDF2.
    fn rederive_key(passphrase: &str, salt: &[u8]) -> KeyDerivationResult {
        let mut mac = Hmac::new(Sha512::new(), passphrase.as_bytes());
        let mut pbkdf_result = Zeroizing::new([0u8; KEY_SIZE * 2]);

        pbkdf2(&mut mac, &salt, PBKDF_COUNT, &mut *pbkdf_result);
        let (key, hmac_key) = pbkdf_result.split_at(KEY_SIZE);
        (
            Zeroizing::new(Vec::from(key)),
            Zeroizing::new(Vec::from(hmac_key)),
        )
    }

    /// Generate a random salt and derive two keys from the salt and the given
    /// passphrase.
    fn derive_key(passphrase: &str) -> Result<InitialKeyDerivationResult, OpenDirectoryError> {
        let mut rng = thread_rng();
        let mut salt = vec![0u8; SALT_SIZE];
        rng.try_fill(&mut salt[..]).map_err(|e| {
            IoError::new(ErrorKind::Other, format!("error generating salt: {:?}", e))
        })?;

        let (key, hmac_key) = EncryptedMmapDirectory::rederive_key(passphrase, &salt);
        Ok((key, hmac_key, salt))
    }
}

// The Directory trait[dr] implementation for our EncryptedMmapDirectory.
// [dr] https://docs.rs/tantivy/0.10.2/tantivy/directory/trait.Directory.html
impl Directory for EncryptedMmapDirectory {
    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        let source = self.mmap_dir.open_read(path)?;

        let decryptor = AesSafe256Encryptor::new(&self.encryption_key);
        let mac = Hmac::new(Sha256::new(), &self.mac_key);
        let mut reader = AesReader::new(Cursor::new(source.as_slice()), decryptor, mac)
            .map_err(TvIoError::from)?;
        let mut decrypted = Vec::new();

        reader
            .read_to_end(&mut decrypted)
            .map_err(TvIoError::from)?;

        Ok(ReadOnlySource::from(decrypted))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.mmap_dir.delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.mmap_dir.exists(path)
    }

    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let file = match self.mmap_dir.open_write(path)?.into_inner() {
            Ok(f) => f,
            Err(e) => {
                let error = IoError::from(e);
                return Err(TvIoError::from(error).into());
            }
        };

        let encryptor = AesSafe256Encryptor::new(&self.encryption_key);
        let mac = Hmac::new(Sha256::new(), &self.mac_key);
        let writer = AesWriter::new(file, encryptor, mac).map_err(TvIoError::from)?;
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let data = self.mmap_dir.atomic_read(path)?;

        let decryptor = AesSafe256Encryptor::new(&self.encryption_key);
        let mac = Hmac::new(Sha256::new(), &self.mac_key);
        let mut reader =
            AesReader::new(Cursor::new(data), decryptor, mac).map_err(TvIoError::from)?;
        let mut decrypted = Vec::new();

        reader
            .read_to_end(&mut decrypted)
            .map_err(TvIoError::from)?;
        Ok(decrypted)
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        let encryptor = AesSafe256Encryptor::new(&self.encryption_key);
        let mac = Hmac::new(Sha256::new(), &self.mac_key);
        let mut encrypted = Vec::new();
        {
            let mut writer = AesWriter::new(&mut encrypted, encryptor, mac)?;
            writer.write_all(data)?;
        }

        self.mmap_dir.atomic_write(path, &encrypted)
    }

    fn watch(&self, watch_callback: WatchCallback) -> Result<WatchHandle, tantivy::Error> {
        self.mmap_dir.watch(watch_callback)
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        // The lock files aren't encrypted, this is fine since they won't
        // contain any data. They will be an empty file and a lock will be
        // placed on them using e.g. flock(2) on macOS and Linux.
        self.mmap_dir.acquire_lock(lock)
    }
}

// This tantivy trait is used to indicate when no more writes are expected to be
// done on a writer.
impl<E: crypto::symmetriccipher::BlockEncryptor, M: Mac, W: Write> TerminatingWrite
    for AesWriter<E, M, W>
{
    fn terminate_ref(&mut self, _: AntiCallToken) -> std::io::Result<()> {
        self.finalize()
    }
}

#[cfg(test)]
use tempfile::tempdir;

#[test]
fn create_new_store_and_reopen() {
    let tmpdir = tempdir().unwrap();
    let dir =
        EncryptedMmapDirectory::open(tmpdir.path(), "wordpass").expect("Can't create a new store");
    drop(dir);
    let dir = EncryptedMmapDirectory::open(tmpdir.path(), "wordpass")
        .expect("Can't open the existing store");
    drop(dir);
    let dir = EncryptedMmapDirectory::open(tmpdir.path(), "password");
    assert!(
        dir.is_err(),
        "Opened an existing store with the wrong passphrase"
    );
}

#[test]
fn create_store_with_empty_passphrase() {
    let tmpdir = tempdir().unwrap();
    let dir = EncryptedMmapDirectory::open(tmpdir.path(), "");
    assert!(
        dir.is_err(),
        "Opened an existing store with the wrong passphrase"
    );
}

#[test]
fn change_passphrase() {
    let tmpdir = tempdir().unwrap();
    let dir =
        EncryptedMmapDirectory::open(tmpdir.path(), "wordpass").expect("Can't create a new store");

    drop(dir);
    EncryptedMmapDirectory::change_passphrase(tmpdir.path(), "wordpass", "password")
        .expect("Can't change passphrase");
    let dir = EncryptedMmapDirectory::open(tmpdir.path(), "wordpass");
    assert!(
        dir.is_err(),
        "Opened an existing store with the old passphrase"
    );
    let _ = EncryptedMmapDirectory::open(tmpdir.path(), "password")
        .expect("Can't open the store with the new passphrase");
}
