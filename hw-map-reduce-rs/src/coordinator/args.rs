use clap::Parser;

/// A MapReduce coordinator
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    // The coordinator does not take any arguments
}
