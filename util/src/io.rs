// TODO: Extend with required methods.
pub trait ByteOrderRead {
    fn read_be_uint(&mut self, bytes: usize) -> std::io::Result<u64>;
    fn read_byte(&mut self) -> std::io::Result<u8>;
    fn read_le_u32(&mut self) -> std::io::Result<u32>;
}

impl<T: std::io::Read> ByteOrderRead for T {
    #[inline]
    fn read_be_uint(&mut self, bytes: usize) -> std::io::Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf[8 - bytes..])?;
        Ok(u64::from_be_bytes(buf))
    }

    #[inline]
    fn read_byte(&mut self) -> std::io::Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    #[inline]
    fn read_le_u32(&mut self) -> std::io::Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}
