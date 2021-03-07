use std::io::prelude::*;

use anyhow::Error;
use env_logger::Env;
use fehler::throws;
use structopt::StructOpt;

use kinesis_firehose_producer::Producer;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "kinesis_firehose_stdin_agent",
    about = "std -> kinesis firehose"
)]
struct Opt {
    #[structopt(short, long)]
    firehose_name: String,
}

#[throws]
fn main() {
    let level_str = "info,kinesis_firehose_produce=trace";
    env_logger::Builder::from_env(Env::default().default_filter_or(level_str)).init();
    let opt = Opt::from_args();
    let mut producer = Producer::new(opt.firehose_name)?;
    for line in std::io::stdin().lock().lines() {
        let line = line?;
        producer.produce(line)?;
    }
}
