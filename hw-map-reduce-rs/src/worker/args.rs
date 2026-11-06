use clap::Parser;

/// A MapReduce worker
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    // The worker does not take any arguments
}
