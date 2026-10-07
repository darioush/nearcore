use borsh::{BorshDeserialize, BorshSerialize};
use std::io::{Read, Write};

/// Rows are written in frames rather than one record at a time:
///
/// ```text
/// [u32 little endian compressed length][zstd(borsh(Vec<T>))]  repeated
/// ```
///
/// Borsh records carry no length of their own, so something has to delimit
/// them. Framing many rows together also gives zstd a window wide enough to
/// notice that the same account ids repeat, which is where most of the saving
/// comes from. A reader can skip whole frames without decoding them, and a run
/// cut short loses only the frame being written.
const COMPRESSION_LEVEL: i32 = 3;

pub struct FrameWriter<W: Write> {
    out: W,
    rows_per_frame: usize,
    pending: Vec<u8>,
    pending_rows: usize,
}

impl<W: Write> FrameWriter<W> {
    pub fn new(out: W, rows_per_frame: usize) -> Self {
        Self { out, rows_per_frame, pending: Vec::new(), pending_rows: 0 }
    }

    pub fn write<T: BorshSerialize>(&mut self, row: &T) -> anyhow::Result<()> {
        borsh::to_writer(&mut self.pending, row)?;
        self.pending_rows += 1;
        if self.pending_rows >= self.rows_per_frame {
            self.flush_frame()?;
        }
        Ok(())
    }

    pub fn flush_frame(&mut self) -> anyhow::Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        // The row count leads the frame so a reader knows when to stop
        // deserializing without the borsh Vec header.
        let mut body = Vec::with_capacity(self.pending.len() + 8);
        body.extend_from_slice(&(self.pending_rows as u64).to_le_bytes());
        body.extend_from_slice(&self.pending);
        let compressed = zstd::encode_all(body.as_slice(), COMPRESSION_LEVEL)?;
        self.out.write_all(&(compressed.len() as u32).to_le_bytes())?;
        self.out.write_all(&compressed)?;
        self.pending.clear();
        self.pending_rows = 0;
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<()> {
        self.flush_frame()?;
        self.out.flush()?;
        Ok(())
    }
}

/// Reads frames back, yielding rows in the order they were written. A frame
/// that fails to decompress ends the iteration rather than erroring, so a file
/// from a run that was cut short still reads up to its last whole frame.
pub struct FrameReader<R: Read, T> {
    source: R,
    buffered: std::vec::IntoIter<T>,
    done: bool,
}

impl<R: Read, T: BorshDeserialize> FrameReader<R, T> {
    pub fn new(source: R) -> Self {
        Self { source, buffered: Vec::new().into_iter(), done: false }
    }

    fn read_frame(&mut self) -> Option<Vec<T>> {
        let mut length = [0u8; 4];
        if self.source.read_exact(&mut length).is_err() {
            return None;
        }
        let mut compressed = vec![0u8; u32::from_le_bytes(length) as usize];
        if self.source.read_exact(&mut compressed).is_err() {
            return None;
        }
        let body = zstd::decode_all(compressed.as_slice()).ok()?;
        let (count_bytes, mut rest) = body.split_at(8);
        let count = u64::from_le_bytes(count_bytes.try_into().ok()?);
        let mut rows = Vec::with_capacity(count as usize);
        for _ in 0..count {
            rows.push(T::deserialize(&mut rest).ok()?);
        }
        Some(rows)
    }
}

impl<R: Read, T: BorshDeserialize> Iterator for FrameReader<R, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        loop {
            if let Some(row) = self.buffered.next() {
                return Some(row);
            }
            if self.done {
                return None;
            }
            match self.read_frame() {
                Some(rows) => self.buffered = rows.into_iter(),
                None => {
                    self.done = true;
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone)]
    struct Row {
        height: u64,
        name: String,
    }

    fn rows(count: u64) -> Vec<Row> {
        (0..count)
            .map(|height| Row { height, name: format!("account{}.near", height % 7) })
            .collect()
    }

    #[test]
    fn rows_read_back_in_the_order_they_were_written() {
        let written = rows(2500);
        let mut buffer = Vec::new();
        let mut writer = FrameWriter::new(&mut buffer, 1000);
        for row in &written {
            writer.write(row).unwrap();
        }
        writer.finish().unwrap();

        let read: Vec<Row> = FrameReader::new(buffer.as_slice()).collect();
        assert_eq!(read, written);
    }

    #[test]
    fn repeated_account_ids_compress() {
        let written = rows(20_000);
        let mut buffer = Vec::new();
        let mut writer = FrameWriter::new(&mut buffer, 1000);
        for row in &written {
            writer.write(row).unwrap();
        }
        writer.finish().unwrap();

        let uncompressed: usize = written.iter().map(|row| 8 + 4 + row.name.len()).sum();
        assert!(
            buffer.len() * 4 < uncompressed,
            "framing {} rows gave {} bytes against {} uncompressed",
            written.len(),
            buffer.len(),
            uncompressed
        );
    }

    #[test]
    fn truncated_file_reads_up_to_its_last_whole_frame() {
        let written = rows(3000);
        let mut buffer = Vec::new();
        let mut writer = FrameWriter::new(&mut buffer, 1000);
        for row in &written {
            writer.write(row).unwrap();
        }
        writer.finish().unwrap();

        buffer.truncate(buffer.len() - 10);
        let read: Vec<Row> = FrameReader::new(buffer.as_slice()).collect();
        assert!(!read.is_empty(), "whole frames before the cut should still read");
        assert!(read.len() < written.len());
        assert_eq!(read[..], written[..read.len()]);
    }
}
