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
    pub(crate) fail_times: Arc<Mutex<RefCell<usize>>>,
}

#[cfg(test)]
impl MockPutRecordBatcher {
    pub(crate) fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(RefCell::new(vec![]))),
            fail_times: Arc::new(Mutex::new(RefCell::new(0))),
        }
    }

    pub(crate) fn with_fail_times(ft: usize) -> Self {
        Self {
            buf: Arc::new(Mutex::new(RefCell::new(vec![]))),
            fail_times: Arc::new(Mutex::new(RefCell::new(ft))),
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

        let fail_times = self.fail_times.lock().expect("poisoned mutex");
        let mut fail_times = fail_times.borrow().clone();

        buf.append(&mut records.clone());

        // a nice successful response
        let rrs = records
            .iter()
            .map(|_r| {
                // TODO: realistic values
                if fail_times < 1 {
                    PutRecordBatchResponseEntry {
                        error_code: None,
                        error_message: None,
                        record_id: Some("wa".to_string()),
                    }
                } else {
                    fail_times -= 1;
                    PutRecordBatchResponseEntry {
                        error_code: Some("some error code".to_string()),
                        error_message: Some("some error message".to_string()),
                        record_id: Some("wa".to_string()),
                    }
                }
            })
            .collect::<Vec<_>>();
        let resp = PutRecordBatchOutput {
            encrypted: None,
            failed_put_count: rrs.iter().filter(|r| r.error_code.is_some()).count() as i64,
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
