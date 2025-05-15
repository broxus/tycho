use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::mem::ManuallyDrop;

use zstd_safe::{get_error_name, CCtx, CParameter, DCtx, InBuffer, OutBuffer, ResetDirective};

type Result<T> = std::result::Result<T, ZstdError>;

/// tries to decompress data with known size from header, if it fails, fallbacks to streaming decompression
pub fn zstd_decompress(input: &[u8], output: &mut Vec<u8>) -> Result<()> {
    output.clear(); // clear even if input is empty

    if input.is_empty() {
        return Ok(());
    }

    // try to decompress with known size from header
    if try_decompress_with_size(input, output)? {
        return Ok(());
    }

    // otherwise fallback to streaming decompress
    let mut streaming_decoder = ZstdDecompressStream::new(input.len())?;
    streaming_decoder.write(input, output)?;

    Ok(())
}

fn try_decompress_with_size(input: &[u8], output: &mut Vec<u8>) -> Result<bool> {
    let decompressed_size =
        unsafe { zstd_sys::ZSTD_getFrameContentSize(input.as_ptr().cast(), input.len() as _) };
    // fixme: or ZSTD_findDecompressedSize should be used?

    // cast to i32 to match zstd_sys::ZSTD_CONTENTSIZE_*
    let decompressed_size_err = decompressed_size as i32;

    match decompressed_size_err {
        // fixme: should we try streaming decompression if zstd_sys::ZSTD_CONTENTSIZE_ERROR?
        zstd_sys::ZSTD_CONTENTSIZE_UNKNOWN | zstd_sys::ZSTD_CONTENTSIZE_ERROR => Ok(false),
        // fixme: i'm not sure, maybe this should kick in if input is too large (e.g. > 4GB)
        _ if decompressed_size > input.len().saturating_mul(10) as u64 => {
            Err(ZstdError::SuspiciousCompressionRatio {
                compressed_size: input.len(),
                decompressed_size,
            })
        }
        _ => {
            output.reserve(decompressed_size as _);
            zstd_safe::decompress(output, input).map_err(ZstdError::from_raw)?;
            Ok(true)
        }
    }
}

/// Compresses the input data using zstd with the specified compression level.
/// Writes decompressed size into the output buffer.
pub fn zstd_compress(input: &[u8], output: &mut Vec<u8>, compression_level: i32) {
    output.clear();

    // Calculate the maximum compressed size
    let max_compressed_size = zstd_safe::compress_bound(input.len());

    // Resize the output vector to accommodate the maximum possible compressed size
    output.reserve_exact(max_compressed_size);

    // Perform the compression
    zstd_safe::compress(output, input, compression_level).expect("buffer size is set correctly");
}

pub struct ZstdCompressedFile<W: Write> {
    writer: W,
    compressor: ZstdCompressStream<'static>,
    buffer: Vec<u8>,
}

impl<W: Write> ZstdCompressedFile<W> {
    pub fn new(writer: W, compression_level: i32, buffer_capacity: usize) -> Result<Self> {
        Ok(Self {
            writer,
            buffer: Vec::with_capacity(buffer_capacity),
            compressor: ZstdCompressStream::new(compression_level, buffer_capacity)?,
        })
    }

    /// Terminates the compression stream. All subsequent writes will fail.
    pub fn finish(mut self) -> std::io::Result<W> {
        self.finish_impl()?;

        let mut this = ManuallyDrop::new(self);
        let _buffer = std::mem::take(&mut this.buffer);

        // SAFETY: double-drops are prevented by putting `this` in a ManuallyDrop that is never dropped
        let writer = unsafe { std::ptr::read(&this.writer) };

        // SAFETY: double-drops are prevented by putting `this` in a ManuallyDrop that is never dropped
        let _compressor = unsafe { std::ptr::read(&this.compressor) };

        Ok(writer)
    }

    fn finish_impl(&mut self) -> std::io::Result<()> {
        self.compressor.finish(&mut self.buffer)?;
        if !self.buffer.is_empty() {
            self.writer.write_all(&self.buffer)?;
            self.buffer.clear();
        }
        Ok(())
    }

    fn flush_buf(&mut self) -> std::io::Result<()> {
        if !self.buffer.is_empty() {
            if self.compressor.finished {
                return Err(std::io::Error::other(
                    "compressor already terminated",
                ));
            }

            self.writer.write_all(&self.buffer)?;
            self.buffer.clear();
        }
        Ok(())
    }
}

impl<W: Write> Write for ZstdCompressedFile<W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.write_all(data).map(|_| data.len())
    }

    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.compressor.write(data, &mut self.buffer)?;
        self.flush_buf()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_buf()?;
        self.writer.flush()
    }
}

impl<W: Write> Drop for ZstdCompressedFile<W> {
    fn drop(&mut self) {
        if !self.compressor.finished {
            let _ = self.finish_impl();
        }
    }
}

pub struct ZstdCompressStream<'s> {
    cctx: CCtx<'s>,
    finished: bool,
    resize_by: usize,
}

impl ZstdCompressStream<'_> {
    /// # Arguments
    /// * `compression_level` - The compression level to use.
    /// * `resize_by` - The amount to resize the buffer by when it runs out of space.
    pub fn new(compression_level: i32, resize_by: usize) -> Result<Self> {
        let mut cctx = CCtx::create();
        cctx.set_parameter(CParameter::CompressionLevel(compression_level))
            .map_err(ZstdError::from_raw)?;

        Ok(Self {
            cctx,
            finished: false,
            resize_by,
        })
    }

    /// Sets the number of worker threads to use for compression.
    /// Can be called at any time.
    /// Setting `workers` to `>= 1` will make compression asynchronous.
    /// All compression will be done in background threads.
    /// So it's important to call `finish` before dropping the stream.
    pub fn multithreaded(&mut self, workers: u8) -> Result<()> {
        self.cctx
            .set_parameter(CParameter::NbWorkers(workers as _))
            .map_err(ZstdError::from_raw)?;

        Ok(())
    }

    pub fn write(&mut self, uncompressed: &[u8], compress_buffer: &mut Vec<u8>) -> Result<()> {
        const MODE: zstd_sys::ZSTD_EndDirective = zstd_sys::ZSTD_EndDirective::ZSTD_e_continue;
        if self.finished {
            return Err(ZstdError::StreamAlreadyFinished);
        }

        if uncompressed.is_empty() {
            return Ok(());
        }

        let mut input = InBuffer::around(uncompressed);

        // we check that there is spare space in the buffer, if it's true we fill spare space with zeroes
        // and then we compress the data
        // in the end of loop we resize the buffer to the actual size

        loop {
            let mut output = self.out_buffer(compress_buffer);

            self.cctx
                .compress_stream2(&mut output, &mut input, MODE)
                .map_err(ZstdError::from_raw)?;

            // from the https://facebook.github.io/zstd/zstd_manual.html
            //
            //   Select how many threads will be spawned to compress in parallel.
            //   When nbWorkers >= 1, triggers asynchronous mode when invoking ZSTD_compressStream*() :
            //   ZSTD_compressStream*() consumes input and flush output if possible, but immediately gives back control to caller,
            //   while compression is performed in parallel, within worker thread(s).
            //   (note : a strong exception to this rule is when first invocation of ZSTD_compressStream2() sets ZSTD_e_end :
            //    in which case, ZSTD_compressStream2() delegates to ZSTD_compress2(), which is always a blocking call).
            //   More workers improve speed, but also increase memory usage.
            //   Default value is `0`, aka "single-threaded mode" : no worker is spawned,
            //   compression is performed inside Caller's thread, and all invocations are blocking

            // For multithreaded compression, we should continue if there's more input to process

            if input.pos() >= input.src.len() {
                break Ok(());
            }
        }
    }

    fn out_buffer<'b>(&self, compress_buffer: &'b mut Vec<u8>) -> OutBuffer<'b, Vec<u8>> {
        // Ensure there's enough space in the output buffer
        let start = compress_buffer.len();
        // check if there is enough unused space in the buffer
        if compress_buffer.spare_capacity_mut().len() < self.resize_by {
            compress_buffer.reserve(self.resize_by);
        }

        OutBuffer::around_pos(compress_buffer, start)
    }

    pub fn finish(&mut self, compress_buffer: &mut Vec<u8>) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        loop {
            let mut output = self.out_buffer(compress_buffer);

            let remaining = self
                .cctx
                .end_stream(&mut output)
                .map_err(ZstdError::from_raw)?;

            if remaining == 0 {
                self.finished = true;
                return Ok(());
            }
        }
    }

    /// Resets the compression context.
    /// You can again write data to the stream after calling this method.
    pub fn reset(&mut self) -> Result<()> {
        self.cctx
            .reset(ResetDirective::SessionOnly)
            .map_err(ZstdError::from_raw)?;
        self.finished = false;

        Ok(())
    }
}

pub struct ZstdDecompressStream<'s> {
    dctx: DCtx<'s>,
    resize_by: usize,
    finished: bool,
}

impl ZstdDecompressStream<'_> {
    pub fn new(resize_by: usize) -> Result<Self> {
        let mut dctx = DCtx::create();
        dctx.init().map_err(ZstdError::from_raw)?;

        Ok(Self {
            dctx,
            resize_by,
            finished: false,
        })
    }

    pub fn write(&mut self, compressed: &[u8], decompress_buffer: &mut Vec<u8>) -> Result<()> {
        if self.finished {
            return Err(ZstdError::StreamAlreadyFinished);
        }
        if compressed.is_empty() {
            return Ok(());
        }

        let mut input = InBuffer::around(compressed);

        loop {
            let start = decompress_buffer.len();
            if decompress_buffer.spare_capacity_mut().len() < self.resize_by {
                decompress_buffer.reserve(self.resize_by);
            }

            // all input was read, chunky boy wants more
            if input.pos() == input.src.len() {
                break Ok(());
            }

            let mut output = OutBuffer::around_pos(decompress_buffer, start);
            let read = self
                .dctx
                .decompress_stream(&mut output, &mut input)
                .map_err(ZstdError::from_raw)?;

            // when a frame is completely decoded and fully flushed,
            if read == 0 {
                self.finished = true;
                break Ok(());
            }
        }
    }

    /// Resets the decompression context.
    /// You can again write data to the stream after calling this method.
    pub fn reset(&mut self) -> Result<()> {
        self.dctx
            .reset(ResetDirective::SessionOnly)
            .map_err(ZstdError::from_raw)?;
        self.finished = false;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ZstdError {
    #[error("Zstd error: {0}")]
    Raw(#[from] RawCompressorError),

    #[error("Suspicious compression ratio detected: compressed size: {compressed_size}, decompressed size: {decompressed_size}")]
    SuspiciousCompressionRatio {
        compressed_size: usize,
        decompressed_size: u64,
    },

    #[error("Invalid decompressed size: {decompressed_size}, input size: {input_size}")]
    InvalidDecompressedSize {
        decompressed_size: u64,
        input_size: usize,
    },

    #[error("Stream already finished")]
    StreamAlreadyFinished,
}

impl From<ZstdError> for std::io::Error {
    fn from(value: ZstdError) -> Self {
        std::io::Error::other(value)
    }
}

impl ZstdError {
    fn from_raw(code: usize) -> Self {
        ZstdError::Raw(RawCompressorError { code })
    }
}

pub struct RawCompressorError {
    code: usize,
}

impl Debug for RawCompressorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(get_error_name(self.code))
    }
}

impl Display for RawCompressorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(get_error_name(self.code))
    }
}

impl std::error::Error for RawCompressorError {}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek};

    use rand::prelude::StdRng;
    use rand::{RngCore, SeedableRng};

    use super::*;

    #[test]
    fn test_zstd_compress_decompress() {
        let seed = 42; // I've asked the universe
        let mut rng = StdRng::seed_from_u64(seed);

        for size in [10, 1024, 1024 * 1024, 10 * 1024 * 1024] {
            let mut input = vec![0; size];
            // without rng it will trigger check for too high compression ratio
            rng.fill_bytes(input.as_mut_slice());

            let mut compressed = Vec::new();
            zstd_compress(&input, &mut compressed, 3);

            let mut decompressed = Vec::new();
            zstd_decompress(&compressed, &mut decompressed).unwrap();
            assert_eq!(input, decompressed.as_slice());
        }

        let input = b"Hello, world!";
        let mut compressed = Vec::new();
        zstd_compress(input, &mut compressed, 3);
        let mut decompressed = Vec::new();
        zstd_decompress(&compressed, &mut decompressed).unwrap();
        assert_eq!(input, decompressed.as_slice());

        let mut input = b"bad".to_vec();
        input.extend_from_slice(&compressed);
        let mut decompressed = Vec::new();
        zstd_decompress(&input, &mut decompressed).unwrap_err();
    }

    #[test]
    fn test_streaming() {
        for size in [10usize, 1021, 1024, 1024 * 1024, 10 * 1024 * 1024] {
            let input = vec![0; size];
            check_compression(input, false);

            // NOTE: streaming compression will give slightly different results with one shot compression,
            // so we can't compare the compressed data directly, only that decompression works
        }

        let pseudo_random = (0..1024)
            .map(|i: u32| i.overflowing_mul(13).0 as u8)
            .collect::<Vec<_>>();
        check_compression(pseudo_random, false);

        let hello_world = Vec::from_iter(b"Hello, world!".repeat(1023));
        check_compression(hello_world, false);
    }

    // split on 2 tests because it's too long for a single test
    #[test]
    fn test_steaming_mt() {
        for size in [10usize, 1021, 1024, 1024 * 1024, 10 * 1024 * 1024] {
            let input = vec![0; size];
            check_compression(input, true);

            // NOTE: streaming compression will give slightly different results with one shot compression,
            // so we can't compare the compressed data directly, only that decompression works
        }

        let pseudo_random = (0..1024)
            .map(|i: u32| i.overflowing_mul(13).0 as u8)
            .collect::<Vec<_>>();
        check_compression(pseudo_random, true);

        let hello_world = Vec::from_iter(b"Hello, world!".repeat(1023));
        check_compression(hello_world, true);
    }

    fn check_compression(input: Vec<u8>, multithreaded: bool) {
        let mut compressor = ZstdCompressStream::new(3, 128).unwrap();
        if multithreaded {
            compressor.multithreaded(4).unwrap();
        }

        let mut compress_buffer = Vec::new();
        let mut result_buf = Vec::new();

        for chunk in input.chunks(1024) {
            compressor.write(chunk, &mut compress_buffer).unwrap();
            if compress_buffer.len() > 1024 {
                result_buf.extend_from_slice(&compress_buffer);
                compress_buffer.clear();
            }
        }
        compressor.finish(&mut compress_buffer).unwrap();
        result_buf.extend_from_slice(&compress_buffer);

        let decompressed = {
            let mut buff = Vec::new();
            zstd_decompress(&result_buf, &mut buff).unwrap();
            buff
        };
        assert_eq!(input, decompressed);

        let decompressed = {
            let mut streaming_decoder = ZstdDecompressStream::new(128).unwrap();
            let mut decompressed = Vec::new();
            streaming_decoder
                .write(&result_buf, &mut decompressed)
                .unwrap();
            decompressed
        };
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_dos() {
        for malicious in malicious_files() {
            if let Ok(true) = try_decompress_with_size(&malicious, &mut Vec::new()) {
                panic!("Malicious file was decompressed successfully");
            }
        }
    }

    fn malicious_files() -> Vec<Vec<u8>> {
        let mut files = Vec::new();

        // 1. Lie about content size (much larger)
        files.push(create_malicious_zstd(1_000_000_000, b"Small content"));

        // 2. Lie about content size (much smaller)
        files.push(create_malicious_zstd(
            10,
            b"This content is actually longer than claimed",
        ));

        // 3. Extremely high compression ratio
        let large_content = vec![b'A'; 1_000_000];
        files.push(create_malicious_zstd(
            large_content.len() as u64,
            &large_content,
        ));

        // 4. Invalid content size
        files.push(vec![
            0x28, 0xB5, 0x2F, 0xFD, 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ]);

        // 5. Truncated file
        let truncated_content = b"This file will be truncated";
        let mut truncated_compressed = encode_all(truncated_content.as_slice(), 3).unwrap();
        truncated_compressed.truncate(truncated_compressed.len() / 2);
        files.push(truncated_compressed);

        files
    }

    fn encode_all(input: &[u8], level: i32) -> Result<Vec<u8>> {
        let mut compressed = Vec::new();
        zstd_compress(input, &mut compressed, level);
        Ok(compressed)
    }

    fn create_malicious_zstd(content_size: u64, actual_content: &[u8]) -> Vec<u8> {
        let mut compressed = encode_all(actual_content, 3).unwrap();

        // Modify the Frame_Header_Descriptor to use an 8-byte content size
        compressed[4] = (compressed[4] & 0b11000000) | 0b00000011;

        // Insert the fake content size (8 bytes, little-endian)
        compressed.splice(5..9, content_size.to_le_bytes());

        compressed
    }

    #[test]
    fn test_decode_chunked() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(10 * 1024 * 1024);
        let mut pseudo_rand_patern = vec![0; 1024 * 1024];
        rng.fill_bytes(&mut pseudo_rand_patern);

        for _ in 0..10 {
            data.extend_from_slice(&pseudo_rand_patern);
        }

        let compressed = encode_all(&data, 3).unwrap();
        let mut decompressed = Vec::new();

        let mut decompressor = ZstdDecompressStream::new(128).unwrap();
        for chunk in compressed.chunks(1024) {
            decompressor.write(chunk, &mut decompressed).unwrap();
        }

        assert_eq!(data, decompressed);
    }

    #[test]
    fn buffered_compress_decompress() {
        const BUFFER_LEN: usize = 64 << 20; // 64 MB

        // Prepare
        let mut rng = StdRng::seed_from_u64(42);
        let mut original = vec![0; 4 << 20];
        rng.fill_bytes(&mut original);

        // Try each kind of prealloc: small, exact, huge
        for prealloc in [1024, 4194409, BUFFER_LEN] {
            // Compress
            let mut compressed = Vec::new();
            {
                let file = tempfile::tempfile().unwrap();
                file.set_len(prealloc as _).unwrap();
                let file = ZstdCompressedFile::new(file, 9, BUFFER_LEN).unwrap();

                let mut buffer = std::io::BufWriter::with_capacity(BUFFER_LEN, file);
                for chunk in original.chunks(2048) {
                    buffer.write_all(chunk).unwrap();
                }

                let file = buffer.into_inner().map_err(|e| e.into_error()).unwrap();
                let mut file = file.finish().unwrap();
                file.flush().unwrap();

                let file_size = file.stream_position().unwrap();
                file.set_len(file_size).unwrap(); // <- Truncate after prealloc

                file.seek(std::io::SeekFrom::Start(0)).unwrap();

                #[allow(clippy::verbose_file_reads)]
                file.read_to_end(&mut compressed).unwrap();
            }

            // Decompress
            {
                let mut stream = ZstdDecompressStream::new(1 << 20).unwrap();

                let mut decompressed = Vec::new();
                let mut decompressed_chunk = Vec::new();
                for chunk in compressed.chunks(1 << 20) {
                    decompressed_chunk.clear();
                    stream.write(chunk, &mut decompressed_chunk).unwrap();

                    decompressed.extend_from_slice(&decompressed_chunk);
                }

                assert_eq!(decompressed, original);
            }
        }
    }
}
