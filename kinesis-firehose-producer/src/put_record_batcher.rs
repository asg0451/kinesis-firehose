use rusoto_core::{Region, RusotoError};
use rusoto_firehose::{
    KinesisFirehose, KinesisFirehoseClient, PutRecordBatchError, PutRecordBatchInput, Record,
};

trait PutRecordBatcher {
    async fn put_record_batch(
        &self,
        records: Vec<Record>,
    ) -> Result<PutRecordBatchOutput, PutRecordBatchError>;
}

struct MockPutRecordBatcher {
    buf: Vec<Record>,
}

impl PutRecordBatcher for MockPutRecordBatcher {
    async fn put_record_batch(
        &self,
        records: Vec<Record>,
    ) -> Result<PutRecordBatchOutput, PutRecordBatchError> {
        todo!()
    }
}
