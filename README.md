Box of Files: a tool for indexing files and directories

Usage: bof [OPTIONS] <COMMAND>

Commands:
  init    Creates a directory .bof for indexing
  index   Index directories
  update  Update existing index
  help    Print this message or the help of the given subcommand(s)

Options:
      --output-dir <OUTPUT_DIR>      Set the directory to save the index
      --ignore-paths <IGNORE_PATHS>  Set paths to ignore while indexing
  -p <PARALLEL>                      Enable parallel processing [possible values: true, false]
  -h, --help                         Print help
