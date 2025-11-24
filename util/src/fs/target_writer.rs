use std::io::Seek;

use bytes::{Bytes, BytesMut};

use crate::fs::MappedFile;

pub enum TargetWriter {
    File(std::io::BufWriter<std::fs::File>),
    Bytes(bytes::buf::Writer<BytesMut>),
}

impl TargetWriter {
    pub fn try_freeze(self) -> anyhow::Result<Bytes, std::io::Error> {
        match self {
            Self::File(file) => match file.into_inner() {
                Ok(mut file) => {
                    file.seek(std::io::SeekFrom::Start(0))?;
                    MappedFile::from_existing_file(file).map(Bytes::from_owner)
                }
                Err(e) => Err(e.into_error()),
            },
            Self::Bytes(data) => Ok(data.into_inner().freeze()),
        }
    }
}

impl std::io::Write for TargetWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(writer) => writer.write(buf),
            Self::Bytes(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.flush(),
            Self::Bytes(writer) => writer.flush(),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.write_all(buf),
            Self::Bytes(writer) => writer.write_all(buf),
        }
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.write_fmt(fmt),
            Self::Bytes(writer) => writer.write_fmt(fmt),
        }
    }
}
