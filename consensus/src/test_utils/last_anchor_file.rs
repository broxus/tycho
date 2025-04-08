use std::io::{Read, Seek, SeekFrom, Write};
use std::str::FromStr;

use anyhow::Context;
use tycho_storage::FileDb;

pub struct LastAnchorFile {
    file: std::fs::File,
}
impl LastAnchorFile {
    pub fn new(mut file_builder: tycho_storage::FileBuilder) -> anyhow::Result<Self> {
        Ok(Self {
            file: file_builder
                .truncate(false)
                .append(false)
                .write(true)
                .read(true)
                .open()?,
        })
    }

    pub fn reopen_in(file_db: &FileDb) -> anyhow::Result<Self> {
        let mut builder = file_db.file("last_anchor").with_extension("txt");
        builder.create(true);
        Self::new(builder)
    }

    pub fn read(&mut self) -> anyhow::Result<u32> {
        self.read_opt()?.context("last anchor file is empty")
    }

    pub fn read_opt(&mut self) -> anyhow::Result<Option<u32>> {
        let mut string = String::new();
        self.file
            .read_to_string(&mut string)
            .context("read last anchor file")?;
        self.file
            .seek(SeekFrom::Start(0))
            .context("reset last anchor file cursor after read")?;
        if string.is_empty() {
            Ok(None)
        } else {
            let result = u32::from_str(&string).context("read last anchor file content")?;
            Ok(Some(result))
        }
    }

    pub fn update(&mut self, anchor: u32) -> anyhow::Result<()> {
        self.file
            .write_all(anchor.to_string().as_bytes())
            .context("write last anchor file")?;
        std::io::Write::flush(&mut self.file).context("flush last anchor file")?;
        self.file
            .seek(SeekFrom::Start(0))
            .context("reset last anchor file cursor after write")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn re_read() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir().context("new temp dir")?;
        let file_db = FileDb::new(tmp_dir.path()).context("new file db")?;

        let mut file = LastAnchorFile::reopen_in(&file_db).context("open")?;

        anyhow::ensure!(file.read().is_err(), "file is empty at start");

        file.update(1).context("write 1")?;
        anyhow::ensure!(file.read().context("1 read")? == 1, "read 1");
        anyhow::ensure!(file.read().context("1 read ++")? == 1, "read 1 again");

        file.update(2).context("write 2")?;
        anyhow::ensure!(file.read().context("2 read")? == 2, "read 2");
        anyhow::ensure!(file.read().context("2 read ++")? == 2, "read 2 again");

        drop(file);

        let mut file = LastAnchorFile::reopen_in(&file_db).context("reopen")?;

        anyhow::ensure!(file.read().context("2 read #")? == 2, "read 2 after open");

        file.update(3).context("write 3")?;
        file.update(4).context("write 4")?;
        anyhow::ensure!(file.read().context("4 read")? == 4, "read 4");
        anyhow::ensure!(file.read().context("4 read ++")? == 4, "read 4 again");

        Ok(())
    }
}
