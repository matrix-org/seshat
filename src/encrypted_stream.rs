// Copyright 2015 The adb-remote-control Developers
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

// This file originates from the rust-aes-stream repo[1], it has been moddified
// to use AES-CTR mode and to authenticate the encrypted files.
// [1] https://github.com/oberien/rust-aes-stream/

//! Read/Write Wrapper for AES Encryption and Decryption during I/O Operations
//!
use std::cmp;
use std::convert::TryFrom;
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::ops::Neg;

use crypto::blockmodes::CtrMode;
use crypto::buffer::{BufferResult, ReadBuffer, RefReadBuffer, RefWriteBuffer, WriteBuffer};
use crypto::mac::{Mac, MacResult};
use crypto::symmetriccipher::{BlockEncryptor, Decryptor, Encryptor};
use rand::{thread_rng, Rng};

const BUFFER_SIZE: usize = 8192;

// TODO We may want to generalize our writer/reader to take an Encryptor instead
// of a BlockEncryptor. This would allow us to swap easily between different
// encryption algorithms like we can swap between different MAC algorithms. This
// would require the new() method to take the IV/Nonce as an argument.
// Chacha20/Poly1305 would be available to us this way.

/// Wraps a [`Write`](https://doc.rust-lang.org/std/io/trait.Write.html) implementation with CTR
/// based on the given [`BlockEncryptor`][be], additionally authenticates the
/// writer with the given [`Mac`][mac]
///
/// [be]: https://docs.rs/rust-crypto/0.2.36/crypto/symmetriccipher/trait.BlockEncryptor.html
/// [mac]: https://docs.rs/rust-crypto/0.2.36/crypto/mac/trait.Mac.html
///
pub struct AesWriter<E: BlockEncryptor, M: Mac, W: Write> {
    /// Writer to write encrypted data to
    writer: W,
    /// Encryptor to encrypt data with
    enc: CtrMode<E>,
    mac: M,
    finalized: bool,
}

impl<E: BlockEncryptor, M: Mac, W: Write> AesWriter<E, M, W> {
    /// Creates a new AesWriter with a random IV.
    ///
    /// The IV will be written as first block of the file.
    ///
    /// The MAC will be written at the end of the file.
    ///
    /// # Arguments
    ///
    /// * `writer`: Writer to write encrypted data into
    /// * `enc`: [`BlockEncryptor`][be] to use for encryption
    /// * `mac`: [`Mac`][mac] to use for encryption
    ///
    /// [be]: https://docs.rs/rust-crypto/0.2.36/crypto/symmetriccipher/trait.BlockEncryptor.html
    /// [mac]: https://docs.rs/rust-crypto/0.2.36/crypto/mac/trait.Mac.html
    pub fn new(mut writer: W, enc: E, mac: M) -> Result<AesWriter<E, M, W>> {
        let mut iv = vec![0u8; enc.block_size()];
        let mut rng = thread_rng();

        rng.try_fill(&mut iv[..])
            .map_err(|e| Error::new(ErrorKind::Other, format!("error generating iv: {:?}", e)))?;
        writer.write_all(&iv)?;
        Ok(AesWriter {
            writer,
            enc: CtrMode::new(enc, iv),
            mac,
            finalized: false,
        })
    }

    /// Encrypts the passed buffer and writes all resulting encrypted blocks to the underlying writer
    ///
    /// # Arguments
    ///
    /// * `buf`: Plaintext to encrypt and write.
    fn encrypt_write(&mut self, buf: &[u8], eof: bool) -> Result<usize> {
        if self.finalized {
            return Err(Error::new(
                ErrorKind::Other,
                "File has been already finalized",
            ));
        }

        let mut read_buf = RefReadBuffer::new(buf);
        let mut out = [0u8; BUFFER_SIZE];
        let mut write_buf = RefWriteBuffer::new(&mut out);
        loop {
            let res = self
                .enc
                .encrypt(&mut read_buf, &mut write_buf, eof)
                .map_err(|e| Error::new(ErrorKind::Other, format!("encryption error: {:?}", e)))?;
            let mut enc = write_buf.take_read_buffer();
            let enc = enc.take_remaining();
            self.writer.write_all(enc)?;
            self.mac.input(enc);
            match res {
                BufferResult::BufferUnderflow => break,
                BufferResult::BufferOverflow => {}
            }
        }
        assert_eq!(read_buf.remaining(), 0);
        Ok(buf.len())
    }

    /// Finalize the file and mark it so no more writes can happen.
    pub fn finalize(&mut self) -> Result<()> {
        // If our encryptor is using padding this write will insert it now.
        // Otherwise it will do nothing.
        self.encrypt_write(&[], true)?;

        // Write our mac after our encrypted data.
        let mac_result = self.mac.result();
        self.writer.write_all(mac_result.code())?;

        // Mark the file as finalized and flush our underlying writer.
        self.finalized = true;
        self.flush()?;

        Ok(())
    }
}

impl<E: BlockEncryptor, M: Mac, W: Write> Write for AesWriter<E, M, W> {
    /// Encrypts the passed buffer and writes the result to the underlying writer.
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let written = self.encrypt_write(buf, false)?;
        Ok(written)
    }

    /// Flush this output stream, ensuring that all intermediately buffered contents reach their destination.
    /// [Read more](https://doc.rust-lang.org/nightly/std/io/trait.Write.html#tymethod.flush)
    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

impl<E: BlockEncryptor, M: Mac, W: Write> Drop for AesWriter<E, M, W> {
    /// Drop our AesWriter adding the MAC at the end of the file and flushing
    /// our buffers.
    fn drop(&mut self) {
        if self.finalized {
            return;
        }

        if std::thread::panicking() {
            let _ = self.finalize();
        } else {
            self.finalize().unwrap();
        }
    }
}

/// Wraps a [`Read`](https://doc.rust-lang.org/std/io/trait.Read.html) implementation with CTR
/// based on given [`CtrMode`][ct]
///
/// [ct]: https://docs.rs/rust-crypto/0.2.36/crypto/blockmodes/struct.CtrMode.html
pub struct AesReader<D: BlockEncryptor + Clone, R: Read + Seek + Clone> {
    /// Reader to read encrypted data from
    reader: R,
    /// Block encryptor that is used to reset the decryptor.
    enc: D,
    /// Decryptor to decrypt data with
    dec: CtrMode<D>,
    /// Buffer used to store blob needed to find out if we reached eof
    buffer: Vec<u8>,
    /// Indicates wheather eof of the underlying buffer was reached
    eof: bool,
    /// Total length of the reader
    pub(crate) length: u64,
    /// Length of the MAC.
    pub(crate) mac_length: u64,
    /// The IV for our decryptor, used when resetting.
    pub(crate) iv: Vec<u8>,

    pos: u64,
}

impl<D: BlockEncryptor + Clone, R: Read + Seek + Clone> AesReader<D, R> {
    /// Creates a new AesReader.
    ///
    /// Assumes that the first block of given reader is the IV.
    ///
    /// # Arguments
    ///
    /// * `reader`: Reader to read encrypted data from
    /// * `dec`: [`BlockDecryptor`][bd] to use for decyrption
    /// * `mac`: [`Mac`][mac] to use for authentication
    ///
    /// [bd]: https://docs.rs/rust-crypto/0.2.36/crypto/symmetriccipher/trait.BlockDecryptor.html
    /// [mac]: https://docs.rs/rust-crypto/0.2.36/crypto/mac/trait.Mac.html
    pub fn new<M: Mac>(mut reader: R, dec: D, mut mac: M) -> Result<AesReader<D, R>> {
        let iv_length = dec.block_size();
        let mac_length = mac.output_bytes();

        let u_iv_length = u64::try_from(iv_length)
            .map_err(|_| Error::new(ErrorKind::Other, "IV length is too big"))?;
        let u_mac_length = u64::try_from(mac_length)
            .map_err(|_| Error::new(ErrorKind::Other, "MAC length is too big"))?;
        let i_mac_length = i64::try_from(mac_length)
            .map_err(|_| Error::new(ErrorKind::Other, "MAC length is too big"))?;

        let mut iv = vec![0u8; iv_length];
        let mut expected_mac = vec![0u8; mac_length];

        reader.read_exact(&mut iv)?;
        let end = reader.seek(SeekFrom::End(0))?;

        if end < (u_iv_length + u_mac_length) {
            return Err(Error::new(
                ErrorKind::Other,
                "File doesn't contain a valid IV or MAC",
            ));
        }

        let seek_back = i_mac_length.neg();
        reader.seek(SeekFrom::End(seek_back))?;
        reader.read_exact(&mut expected_mac)?;
        let expected_mac = MacResult::new_from_owned(expected_mac);

        reader.seek(SeekFrom::Start(u_iv_length))?;

        let mut eof = false;

        while !eof {
            let (buffer, end_of_file) =
                AesReader::<D, R>::read_until_mac(&mut reader, end, u_mac_length)?;
            eof = end_of_file;
            mac.input(&buffer);
        }

        if mac.result() != expected_mac {
            return Err(Error::new(ErrorKind::Other, "Invalid MAC"));
        }

        reader.seek(SeekFrom::Start(u_iv_length))?;

        Ok(AesReader {
            reader,
            dec: CtrMode::new(dec.clone(), iv.clone()),
            enc: dec,
            buffer: Vec::new(),
            eof: false,
            length: end,
            mac_length: u_mac_length,
            iv,
            pos: 0,
        })
    }

    /// Read bytes from a reader until the start of the MAC instead of until the
    /// end of file.
    ///
    /// # Arguments
    ///
    /// * `reader`: Reader to read encrypted data from
    /// * `total_length`: The total number of bytes that the reader contains.
    /// * `mac_length`: The length of the MAC that is stored the file we are
    /// reading from.
    fn read_until_mac(
        reader: &mut R,
        total_length: u64,
        mac_length: u64,
    ) -> Result<(Vec<u8>, bool)> {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let read = reader.read(&mut buffer)?;

        let current_pos = reader.seek(SeekFrom::Current(0))?;
        let mac_start = total_length - mac_length;
        let read_mac_bytes = cmp::max(current_pos - mac_start, 0);
        let eof = current_pos >= mac_start;

        let read_mac_bytes = usize::try_from(read_mac_bytes).map_err(|_| {
            Error::new(
                ErrorKind::Other,
                "Cannot convert the number of read MAC bytes.",
            )
        })?;

        buffer.truncate(read - read_mac_bytes);

        Ok((buffer, eof))
    }

    /// Reads at max BUFFER_SIZE bytes, handles potential eof and returns the
    /// buffer as Vec<u8>
    fn fill_buf(&mut self) -> Result<Vec<u8>> {
        let (buffer, eof) =
            AesReader::<D, R>::read_until_mac(&mut self.reader, self.length, self.mac_length)?;
        self.eof = eof;
        Ok(buffer)
    }

    /// Reads and decrypts data from the underlying stream and writes it into the passed buffer.
    ///
    /// # Arguments
    ///
    /// * `buf`: Buffer to write decrypted data into.
    fn read_decrypt(&mut self, buf: &mut [u8]) -> Result<usize> {
        // If this is the first iteration, fill our internal buffer
        if self.buffer.is_empty() && !self.eof {
            self.buffer = self.fill_buf()?;
        }

        let buf_len = buf.len();
        let mut write_buf = RefWriteBuffer::new(buf);
        let res;
        let remaining;
        {
            let mut read_buf = RefReadBuffer::new(&self.buffer);

            // Test if our decryptor still has enough decrypted data or we have
            // enough buffered.
            res = self
                .dec
                .decrypt(&mut read_buf, &mut write_buf, self.eof)
                .map_err(|e| Error::new(ErrorKind::Other, format!("decryption error: {:?}", e)))?;
            remaining = read_buf.remaining();
        }
        // Keep the remaining bytes
        let len = self.buffer.len();
        self.buffer.drain(..(len - remaining));

        // If we were able to decrypt the whole file, return early.
        match res {
            BufferResult::BufferOverflow => return Ok(buf_len),
            BufferResult::BufferUnderflow if self.eof => return Ok(write_buf.position()),
            _ => {}
        }

        // Otherwise read/decrypt more.
        let mut dec_len = 0;
        // If the reader doesn't return enough so that we can decrypt a block,
        // we need to continue reading until we have enough data to return one
        // decrypted block, or until we reach eof.
        while dec_len == 0 && !self.eof {
            let eof_buffer = self.fill_buf()?;
            let remaining;
            {
                let mut read_buf = RefReadBuffer::new(&self.buffer);
                self.dec
                    .decrypt(&mut read_buf, &mut write_buf, self.eof)
                    .map_err(|e| {
                        Error::new(ErrorKind::Other, format!("decryption error: {:?}", e))
                    })?;
                let mut dec = write_buf.take_read_buffer();
                let dec = dec.take_remaining();
                dec_len = dec.len();
                remaining = read_buf.remaining();
            }
            // Keep the remaining bytes
            let len = self.buffer.len();
            self.buffer.drain(..(len - remaining));
            // Append the newly read bytes
            self.buffer.extend(eof_buffer);
        }
        Ok(dec_len)
    }

    fn seek_from_start(&mut self, offset: u64) -> Result<u64> {
        // The reset() method doesn't seem to do what it's supposed to do, we
        // create a completely new decryptor instead.
        // self.dec.reset(&self.iv);
        self.dec = CtrMode::new(self.enc.clone(), self.iv.clone());

        self.reader.seek(SeekFrom::Start(self.iv.len() as u64))?;
        self.buffer.clear();
        self.eof = false;
        self.pos = 0;

        let mut buffer = [0u8; BUFFER_SIZE];
        let mut read: usize = 0;

        if offset != 0 {
            loop {
                let left_to_read = offset as usize - read;

                if left_to_read < buffer.len() {
                    self.read_exact(&mut buffer[0..left_to_read as usize])
                        .expect("Can't read while seeking");
                    break;
                } else {
                    read += self.read(&mut buffer).expect("Can't read while seeking");
                }
            }
        }

        Ok(self.pos)
    }
}

impl<D: BlockEncryptor + Clone, R: Read + Seek + Clone> Clone for AesReader<D, R> {
    fn clone(&self) -> Self {
        let mut reader = self.reader.clone();
        reader
            .seek(SeekFrom::Start(self.iv.len() as u64))
            .expect("Can't seek reader clone to start");

        AesReader {
            reader,
            dec: CtrMode::new(self.enc.clone(), self.iv.clone()),
            enc: self.enc.clone(),
            buffer: Vec::new(),
            eof: false,
            length: self.length,
            mac_length: self.mac_length,
            iv: self.iv.clone(),
            pos: 0,
        }
    }
}

impl<D: BlockEncryptor + Clone, R: Read + Seek + Clone> Read for AesReader<D, R> {
    /// Reads encrypted data from the underlying reader, decrypts it and writes the result into the
    /// passed buffer.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let read = self.read_decrypt(buf)?;
        self.pos += read as u64;
        Ok(read)
    }
}

impl<D: BlockEncryptor + Clone, R: Read + Seek + Clone> Seek for AesReader<D, R> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(offset) => self.seek_from_start(offset),
            SeekFrom::Current(offset) => {
                if offset == 0 {
                    Ok(self.pos)
                } else {
                    let current = self.seek(SeekFrom::Current(0))?;
                    let offset = current.checked_add(offset as u64).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "invalid seek to a negative or overflowing position",
                        )
                    })?;
                    self.seek_from_start(offset)
                }
            }
            SeekFrom::End(offset) => {
                let end = self.length - self.mac_length;
                let offset = end
                    .checked_sub(offset.wrapping_neg() as u64 + self.iv.len() as u64)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "invalid seek to a negative or overflowing position",
                        )
                    })?;
                self.seek_from_start(offset)
            }
        }
    }
}

#[cfg(test)]
use crypto::aessafe::AesSafe128Encryptor;

#[cfg(test)]
use crypto::hmac::Hmac;

#[cfg(test)]
use crypto::poly1305::Poly1305;

#[cfg(test)]
use crypto::sha2::Sha256;

#[cfg(test)]
use std::io::Cursor;

#[cfg(test)]
fn encrypt(data: &[u8]) -> Vec<u8> {
    let key = [0u8; 16];
    let hmac_key = [0u8; 16];

    let mac = Hmac::new(Sha256::new(), &hmac_key);
    let block_enc = AesSafe128Encryptor::new(&key);
    let mut enc = Vec::new();
    {
        let mut aes = AesWriter::new(&mut enc, block_enc, mac).unwrap();
        aes.write_all(&data).unwrap();
    }
    enc
}

#[cfg(test)]
fn encrypt_poly1305(data: &[u8]) -> Vec<u8> {
    let key = [0u8; 16];
    let hmac_key = [0u8; 32];

    let mac = Poly1305::new(&hmac_key);
    let block_enc = AesSafe128Encryptor::new(&key);
    let mut enc = Vec::new();
    {
        let mut aes = AesWriter::new(&mut enc, block_enc, mac).unwrap();
        aes.write_all(&data).unwrap();
    }
    enc
}

#[cfg(test)]
fn decrypt<R: Read + Seek + Clone>(data: R) -> Vec<u8> {
    let key = [0u8; 16];
    let block_dec = AesSafe128Encryptor::new(&key);
    let mut dec = Vec::new();
    let hmac = Hmac::new(Sha256::new(), &key);
    let mut aes = AesReader::new(data, block_dec, hmac).unwrap();
    aes.read_to_end(&mut dec).unwrap();
    dec
}

#[cfg(test)]
fn decrypt_poly1305<R: Read + Seek + Clone>(data: R) -> Vec<u8> {
    let key = [0u8; 16];
    let hmac_key = [0u8; 32];
    let block_dec = AesSafe128Encryptor::new(&key);
    let mut dec = Vec::new();
    let hmac = Poly1305::new(&hmac_key);
    let mut aes = AesReader::new(data, block_dec, hmac).unwrap();
    aes.read_to_end(&mut dec).unwrap();
    dec
}

#[test]
fn enc_unaligned() {
    let orig = [0u8; 16];
    let key = [0u8; 16];
    let hmac_key = [0u8; 16];

    let mac = Hmac::new(Sha256::new(), &hmac_key);
    let block_enc = AesSafe128Encryptor::new(&key);
    let mut enc = Vec::new();
    {
        let mut aes = AesWriter::new(&mut enc, block_enc, mac).unwrap();
        for chunk in orig.chunks(3) {
            aes.write_all(&chunk).unwrap();
        }
    }
    let dec = decrypt(Cursor::new(&enc));
    assert_eq!(dec, &orig);
}

#[test]
fn enc_dec_single() {
    let orig = [0u8; 15];
    let enc = encrypt(&orig);
    let dec = decrypt(Cursor::new(&enc));
    assert_eq!(dec, &orig);
}

#[test]
fn enc_dec_single_poly() {
    let orig = [0u8; 15];
    let enc = encrypt_poly1305(&orig);
    let dec = decrypt_poly1305(Cursor::new(&enc));
    assert_eq!(dec, &orig);
}

#[test]
fn enc_dec_single_full() {
    let orig = [0u8; 16];
    let enc = encrypt(&orig);
    let dec = decrypt(Cursor::new(&enc));
    assert_eq!(dec, &orig);
}

#[test]
fn enc_dec_single_full_poly() {
    let orig = [0u8; 16];
    let enc = encrypt_poly1305(&orig);
    let dec = decrypt_poly1305(Cursor::new(&enc));
    assert_eq!(dec, &orig);
}

#[test]
fn dec_read_unaligned() {
    let orig = [0u8; 16];
    let enc = encrypt(&orig);

    let key = [0u8; 16];
    let block_dec = AesSafe128Encryptor::new(&key);
    let mut dec: Vec<u8> = Vec::new();
    let hmac = Hmac::new(Sha256::new(), &key);
    let mut aes = AesReader::new(Cursor::new(&enc), block_dec, hmac).unwrap();
    loop {
        let mut buf = [0u8; 3];
        let read = aes.read(&mut buf).unwrap();
        dec.extend(&buf[..read]);
        if read == 0 {
            break;
        }
    }
    assert_eq!(dec, &orig);
}

#[test]
fn dec_after_seek() {
    let key = [0u8; 16];
    let orig: Vec<u8> = (0..32).collect();

    let enc = encrypt(&orig);

    let block_dec = AesSafe128Encryptor::new(&key);
    let hmac = Hmac::new(Sha256::new(), &key);
    let mut aes = AesReader::new(Cursor::new(&enc), block_dec, hmac).unwrap();

    let mut dec = vec![0u8; 16];
    aes.seek(SeekFrom::Start(16)).unwrap();
    aes.read_exact(&mut dec).expect("Decryptor can't read");

    assert_eq!(dec, (16..32).collect::<Vec<u8>>());

    let mut dec = vec![0u8; 16];
    aes.seek(SeekFrom::End(-32)).unwrap();
    aes.read_exact(&mut dec).expect("Decryptor can't read");

    assert_eq!(dec, (0..16).collect::<Vec<u8>>());
}
