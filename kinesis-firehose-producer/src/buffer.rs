use crate::error::Error;
use bytes::Bytes;
use fehler::{throw, throws};
use rusoto_firehose::Record;

pub(crate) struct Buffer {
    buf: Vec<Record>,
    running_bytes: usize,
}

impl Buffer {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * Self::KIB;
    // limits: 500 records, each record up to 1,000 KiB, 4MiB total
    const MAX_RECORDS: usize = 500;
    const MAX_RECORD_BYTES: usize = 1_000 * Self::KIB;
    const MAX_BYTES: usize = 4 * Self::MIB;

    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::with_capacity(500),
            running_bytes: 0,
        }
    }

    pub(crate) fn is_overfull_with(&self, rec: &str) -> bool {
        let bs = rec.as_bytes().len();
        self.running_bytes + bs >= Self::MAX_BYTES || self.buf.len() + 1 >= Self::MAX_RECORDS
    }

    // member fn, not associated, because it breaks rust-analizer???
    #[throws]
    pub(crate) fn check_record_too_large(&self, rec: &str) {
        let bs = rec.as_bytes().len();
        let too_large = bs > Self::MAX_RECORD_BYTES;
        if too_large {
            throw!(Error::RecordTooLarge {
                size: rec.as_bytes().len(),
                record: rec.to_string()
            });
        }
    }

    // ASSUMES: this won't make us overfull -- ie the caller checked that first
    pub(crate) fn add_rec(&mut self, rec: String) {
        let bytes = Bytes::copy_from_slice(rec.as_bytes());
        self.running_bytes += bytes.len();
        self.buf.push(Record { data: bytes });
    }

    pub(crate) fn clear(&mut self) {
        self.buf.clear();
        self.running_bytes = 0;
    }

    pub(crate) fn as_owned_vec(&self) -> Vec<Record> {
        self.buf.clone()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}
