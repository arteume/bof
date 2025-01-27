mod bof;
use clap::{Subcommand,  Parser};
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(name = "BOF")]
#[command(about = "Box of Files: a tool for indexing files and directories", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands  {
    #[command(about = "Creates a directory .bof for indexing")]
    Init,

    #[command(arg_required_else_help = true)]
    #[command(about = "Index directories")]
    Index {
        #[arg(help = "Directories' paths")]
        paths: Vec<PathBuf>,
    },
}

fn main() {
    let args = Cli::parse();

    match args.command {
        Commands::Init => {
            if let Err(e) = bof::init() {
                println!("Error initializing: {}", e);
            }
        }
        Commands::Index { paths } => {
                if let Err(e) = bof::index_multiple_directories(paths) {
                    println!("Error indexing multiple directories: {}", e);
                }
        }
    }
}