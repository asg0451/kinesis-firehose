use crate::async_producer::KinesisFirehoseProducer;
use crate::error::Error;
use fehler::throws;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;

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
        let (err_tx, mut err_rx) = channel::<Error>(1);
        let things = stream::iter(0..size)
            .then(|n| Self::create_thing(stream_name.clone(), err_tx.clone(), n))
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
    async fn create_thing(stream_name: String, err_tx: Sender<Error>, num: usize) -> Member {
        let (tx, mut rx) = channel(100);
        let mut p = KinesisFirehoseProducer::new(stream_name)?;
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
}

impl Drop for AsyncPool {
    fn drop(&mut self) {
        if !self.shutdown.load(Ordering::SeqCst) {
            log::error!("async pool being dropped without being shut down! data will be lost");
        }
    }
}
