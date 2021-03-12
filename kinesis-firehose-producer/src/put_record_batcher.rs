use async_trait::async_trait;
use rusoto_core::RusotoError;
use rusoto_firehose::{
    KinesisFirehose, KinesisFirehoseClient, PutRecordBatchError, PutRecordBatchInput,
    PutRecordBatchOutput,
};

#[cfg(test)]
use rusoto_firehose::{PutRecordBatchResponseEntry, Record};

#[cfg(test)]
use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

#[async_trait]
pub trait PutRecordBatcher: Send + Sync {
    async fn _put_record_batch(
        &self,
        req: PutRecordBatchInput,
    ) -> Result<PutRecordBatchOutput, RusotoError<PutRecordBatchError>>;
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct MockPutRecordBatcher {
    pub(crate) buf: Arc<Mutex<RefCell<Vec<Record>>>>,
}

#[cfg(test)]
impl MockPutRecordBatcher {
    pub(crate) fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(RefCell::new(vec![]))),
        }
    }

    pub(crate) fn buf_ref(&self) -> Arc<Mutex<RefCell<Vec<Record>>>> {
        self.buf.clone()
    }
}

#[cfg(test)]
#[async_trait]
impl PutRecordBatcher for MockPutRecordBatcher {
    async fn _put_record_batch(
        &self,
        req: PutRecordBatchInput,
    ) -> Result<PutRecordBatchOutput, RusotoError<PutRecordBatchError>> {
        let records = req.records;

        let buf = self.buf.lock().expect("poisoned mutex");
        let mut buf = buf.borrow_mut();
        buf.append(&mut records.clone());

        // a nice successful response
        let rrs = records
            .iter()
            .map(|_r| PutRecordBatchResponseEntry {
                error_code: None,
                error_message: None,
                record_id: Some("wa".to_string()), // TODO: should be random str
            })
            .collect::<Vec<_>>();
        let resp = PutRecordBatchOutput {
            encrypted: None,
            failed_put_count: 0,
            request_responses: rrs,
        };
        Ok(resp)
    }
}

#[async_trait]
impl PutRecordBatcher for KinesisFirehoseClient {
    async fn _put_record_batch(
        &self,
        req: PutRecordBatchInput,
    ) -> Result<PutRecordBatchOutput, RusotoError<PutRecordBatchError>> {
        self.put_record_batch(req).await
    }
}
