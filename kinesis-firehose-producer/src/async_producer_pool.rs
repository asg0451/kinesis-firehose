use crate::async_producer::Producer;
use crate::error::Error;
use crate::put_record_batcher::PutRecordBatcher;
use fehler::throws;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;

#[cfg(test)]
use crate::put_record_batcher::{BufRef, MockPutRecordBatcher};
#[cfg(test)]
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum Message {
    Produce(String),
    Flush,
    Close,
}

#[derive(Debug)]
struct Member {
    tx: Sender<Message>,
    handle: JoinHandle<()>,
}

#[derive(Debug)]
pub struct AsyncPool {
    things: Vec<Member>,
    send_to_next: usize,
    err_watcher: JoinHandle<()>,
    shutdown: AtomicBool,
}

// TODO: tests
// TODO: doc about needing to call shutdown

impl AsyncPool {
    #[throws]
    pub async fn of_size(stream_name: String, size: usize) -> Self {
        Self::of_size_with_make_producer(stream_name, size, Producer::new).await?
    }

    #[throws]
    async fn of_size_with_make_producer<
        C: PutRecordBatcher + 'static,
        F: Fn(String) -> Result<Producer<C>, Error>,
    >(
        stream_name: String,
        size: usize,
        make_producer: F,
    ) -> Self {
        let (err_tx, mut err_rx) = channel::<Error>(1);
        let things = stream::iter(0..size)
            .then(|n| Self::create_member(stream_name.clone(), err_tx.clone(), n, &make_producer))
            .try_collect()
            .await?;
        // monitor err chan for errors. if there is one, panic
        // could attempt to flush or something, but at this point... just die
        let err_watcher = tokio::spawn(async move {
            log::trace!("err watcher set up");
            if let Some(err) = err_rx.recv().await {
                log::trace!("err watcher got something");
                panic!("error!: {:?}", err)
            }
            log::trace!("err watcher exiting");
        });
        Self {
            things,
            send_to_next: 0,
            err_watcher,
            shutdown: AtomicBool::new(false),
        }
    }

    #[throws]
    pub async fn produce(&mut self, rec: String) {
        if self.shutdown.load(Ordering::SeqCst) {
            panic!("pool has been shutdown");
        }
        let m = self.things.get(self.send_to_next).unwrap();
        m.tx.send(Message::Produce(rec)).await?;
        self.send_to_next = (self.send_to_next + 1) % self.things.len();
    }

    #[throws]
    pub async fn flush(&mut self) {
        for Member { tx, .. } in self.things.iter() {
            log::trace!("flushing member");
            tx.send(Message::Flush)
                .await
                .expect("failed to send flush message");
        }
    }

    #[throws]
    pub async fn shutdown(&mut self) {
        for Member { tx, handle, .. } in self.things.iter_mut() {
            log::trace!("closing member");
            tx.send(Message::Close)
                .await
                .expect("failed to send close message");
            tx.closed().await;
            handle.await?;
            log::trace!("closed member");
        }
        (&mut self.err_watcher).await?;
        self.shutdown.store(true, Ordering::SeqCst);
        log::trace!("shutdown");
    }

    #[throws]
    async fn create_member<
        C: PutRecordBatcher + 'static,
        F: Fn(String) -> Result<Producer<C>, Error>,
    >(
        stream_name: String,
        err_tx: Sender<Error>,
        num: usize,
        make_producer: F,
    ) -> Member {
        let (tx, mut rx) = channel(100);
        let mut p: Producer<C> = make_producer(stream_name)?;
        let handle = tokio::spawn(async move {
            log::trace!("creating producer {}", num);
            while let Some(msg) = rx.recv().await {
                log::trace!("producer {} got msg: {:?}", num, msg);
                if let Message::Produce(rec) = msg {
                    let res = p.produce(rec).await;
                    if let Err(err) = res {
                        err_tx.send(err).await.expect("failed to fail..");
                        break;
                    }
                } else if let Message::Flush = msg {
                    let res = p.flush().await;
                    if let Err(err) = res {
                        err_tx.send(err).await.expect("failed to fail..");
                        break;
                    }
                } else if let Message::Close = msg {
                    rx.close();
                    p.flush().await.expect("failed to flush while closing");
                    break;
                }
            }
        });

        Member { tx, handle }
    }

    #[cfg(test)]
    #[throws]
    async fn of_size_mocked(
        stream_name: String,
        size: usize,
        fail_times: i64,
    ) -> (Self, Arc<Mutex<Vec<BufRef>>>) {
        use std::sync::{Arc, Mutex};
        let bufs = Arc::new(Mutex::new(Vec::with_capacity(size)));
        let b2 = bufs.clone();
        let pool = Self::of_size_with_make_producer(stream_name, size, move |sn| {
            let bufs = b2.clone();
            let mut bufs = bufs.lock().unwrap();
            let mocker = MockPutRecordBatcher::with_fail_times(fail_times);
            let buf_ref = mocker.buf_ref();
            bufs.push(buf_ref);
            Producer::with_client(mocker, sn)
        })
        .await?;
        (pool, bufs)
    }
}

impl Drop for AsyncPool {
    fn drop(&mut self) {
        if !self.shutdown.load(Ordering::SeqCst) {
            log::error!("async pool being dropped without being shut down! data will be lost");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bufs_contents(buf_refs: Arc<Mutex<Vec<BufRef>>>) -> Vec<String> {
        let buf_refs = buf_refs.lock().unwrap();
        buf_refs
            .iter()
            .map(|b| {
                let buf = b.lock().unwrap();
                let buf = buf.borrow();
                let strs = buf
                    .clone()
                    .iter()
                    .map(|r| std::str::from_utf8(&r.data).unwrap().to_string())
                    .collect::<Vec<_>>();
                strs
            })
            .flatten()
            .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn it_works() {
        let (mut pool, buf_refs) = AsyncPool::of_size_mocked("mf-test-2".to_string(), 2, 0)
            .await
            .unwrap();

        pool.produce("hello".to_string()).await.unwrap();
        pool.produce("world".to_string()).await.unwrap();
        pool.shutdown().await.unwrap();

        let data = bufs_contents(buf_refs);
        assert_eq!(&data, &vec!["hello\n".to_string(), "world\n".to_string()])
    }

    #[should_panic]
    #[tokio::test]
    async fn it_panics_if_it_cant_flush() {
        let (mut pool, _buf_refs) = AsyncPool::of_size_mocked("mf-test-2".to_string(), 2, 11)
            .await
            .unwrap();

        pool.produce("hello".to_string()).await.unwrap();
        pool.produce("world".to_string()).await.unwrap();

        pool.shutdown().await.unwrap();
    }
}
