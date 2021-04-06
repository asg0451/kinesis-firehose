use std::io::prelude::*;

use anyhow::Error;
use env_logger::Env;
use fehler::throws;
use structopt::StructOpt;

use kinesis_firehose_producer::async_producer_pool::AsyncProducerPool;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "kinesis_firehose_stdin_agent",
    about = "std -> kinesis firehose"
)]
struct Opt {
    #[structopt(short, long)]
    firehose_name: String,
    #[structopt(short, long)]
    num_producers: usize,
}

#[throws]
#[tokio::main]
async fn main() {
    let level_str = "info,kinesis_firehose_produce=trace";
    env_logger::Builder::from_env(Env::default().default_filter_or(level_str)).init();
    let opt = Opt::from_args();
    let mut pool = AsyncProducerPool::of_size(opt.firehose_name, opt.num_producers).await?;
    for line in std::io::stdin().lock().lines() {
        let line = line?;
        pool.produce(line).await?;
    }
    pool.shutdown().await?;
}
