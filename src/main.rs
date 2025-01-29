mod bof;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "BOF")]
#[command(about = "Box of Files: a tool for indexing files and directories", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, help = "Set the directory to save the index")]
    output_dir: Option<PathBuf>,
    #[arg(long, help = "Set paths to ignore while indexing")]
    ignore_paths: Vec<PathBuf>,
    #[arg(short = 'p', help = "Enable parallel processing")]
    parallel: Option<bool>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Creates a directory .bof for indexing")]
    Init,

    #[command(arg_required_else_help = true)]
    #[command(about = "Index directories")]
    Index {
        #[arg(help = "Directories' paths")]
        paths: Vec<PathBuf>,
    },
    #[command(arg_required_else_help = true)]
    #[command(about = "Update existing index")]
    Update {
        #[arg(help = "Directories' paths to update")]
        paths: Vec<PathBuf>,
    },
}

fn main() {
    let now = std::time::Instant::now();

    let mut config = bof::load_config();
    let args = Cli::parse();

    if let Some(parallel) = args.parallel {
        config.parallel = parallel;
    }

    if let Some(output_dir) = args.output_dir {
        config.output_dir = output_dir;
    }

    if !args.ignore_paths.is_empty() {
        config.ignore_paths.extend(args.ignore_paths);
    }

    match args.command {
        Commands::Init => {
            if let Err(e) = bof::init(&mut config) {
                println!("Error initializing: {}", e);
            }
        }
        Commands::Index { paths } => {
            if let Err(e) = bof::index_directories(paths, &config) {
                println!("Error indexing directories: {}", e);
            }
        }
        Commands::Update { paths } => {
            if let Err(e) = bof::update_directories(paths, &config) {
                println!("Error updating directories: {}", e);
            }
        }
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}
