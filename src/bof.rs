use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use std::{collections::HashMap, fs::{self, File, Metadata}, os::unix::fs::MetadataExt, path::{Path, PathBuf}, io::{self}};
use rayon::prelude::*;
use std::sync::{Arc, Mutex};

fn generate_key(ident: String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ident.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct BOFIndex {
    entries: Vec<BOFEntry>,
    inverse_table: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Serialize)]
struct BOFEntry {
    key: String,
    path: String,
    metadata: MetaData,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) enum MetaData {
    Directory(DirMetaData),
    File(FileMetaData)
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct FileMetaData {
    ctime: u64,
    mtime: u64,
    size: u64,
    inode: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DirMetaData {
    data: Vec<DirEntry>,
    inode: u64,
}

#[derive(Clone, Debug, Serialize)]
struct DirEntry {
    name: String,
    data: MetaData,
}

impl From<&Metadata> for FileMetaData {
    fn from(val: &Metadata) -> FileMetaData {
        use std::time::UNIX_EPOCH;
        Self {
            ctime: val.created()
            .map(|t| t.duration_since(UNIX_EPOCH).unwrap().as_secs())
            .unwrap(), // Should be supported in our system
            mtime: val.modified()
            .map(|t| t.duration_since(UNIX_EPOCH).unwrap().as_secs())
            .unwrap(), // Should be supported in our system
            size: val.len(),
            inode: val.ino(),
        }
    }
}

impl BOFIndex {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            inverse_table: HashMap::new(),
        }
    }

    fn add_entry(&mut self, path: &Path, key: String, metadata: &Metadata, dir_entries: Option<Vec<DirEntry>>) -> MetaData {
        let parent_dir = path.parent().unwrap_or_else(|| Path::new(".")).to_string_lossy().to_string();

        if metadata.is_file() {
            let metadata: FileMetaData = metadata.into();
            self.entries.push(BOFEntry { 
                key: key.clone(), path: path.to_string_lossy().to_string(), 
                metadata: MetaData::File(metadata.clone()) });
            self.inverse_table.entry(key).or_default().push( parent_dir);
            MetaData::File(metadata)
        }
        else {
        MetaData::Directory(DirMetaData {
            data: dir_entries.unwrap(), // Should be Some,
            inode: metadata.ino(),
        })}
    }
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct BOFConfig {
    #[serde(default = "BOFConfig::default_output_dir")]
    pub output_dir: PathBuf,
    #[serde(default = "BOFConfig::default_ignore_paths")]
    pub ignore_paths: Vec<PathBuf>,
    #[serde(default)]
    pub parallel: bool,
}

impl BOFConfig {
    fn default_output_dir() -> PathBuf {
        PathBuf::from(".bof")
    }
    fn default_ignore_paths() -> Vec<PathBuf> {
        vec![PathBuf::from(".git")]
    }
}

pub(crate) fn load_config() -> BOFConfig {
    let settings = config::Config::builder()
        .add_source(config::File::with_name("Config").required(false))
        .build()
        .unwrap();

   settings.try_deserialize::<BOFConfig>().unwrap_or_default()
}

pub(crate) fn init(config: &mut BOFConfig) -> io::Result<()> {
    fs::create_dir_all(&config.output_dir)?;
    println!("Initialized .bof directory at: {}", &config.output_dir.display());
    Ok(())
}

pub(crate) fn index(path: &Path, bof_index: &mut BOFIndex, config: &BOFConfig) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Path is not a directory"));
    }

    if config.ignore_paths.contains(&path.to_path_buf()) {
        println!("Skipping ignored path: {}", path.display());
        return Ok(MetaData::Directory(DirMetaData { data: Vec::new(), inode: metadata.ino() }))
    }

    let dir_key = generate_key(path.to_string_lossy().to_string());
    let mut dir_entries = DirMetaData {data: Vec::new(), inode: metadata.ino()};

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();

        if config.ignore_paths.contains(&path.to_path_buf()) {
            println!("Skipping ignored path: {}", path.display());
            return Ok(MetaData::Directory(DirMetaData { data: Vec::new(), inode: metadata.ino() }))
        }

        if metadata.is_file() {
            let key = generate_key(fs::read_to_string(&path)? + &name);
            let file_meta = bof_index.add_entry(&path, key, &metadata, None);
            dir_entries.data.push(DirEntry { name, data: file_meta });
        } else if metadata.is_dir() {
            let subdir_meta = index(&entry.path(), bof_index, config)?;
            dir_entries.data.push(DirEntry { name, data: subdir_meta });
        }
    }
    Ok(bof_index.add_entry(path, dir_key, &metadata, Some(dir_entries.data)))
}

pub(crate) fn index_parallel(path: &Path, bof_index: Arc<Mutex<BOFIndex>>, config: &BOFConfig) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Path is not a directory"));
    }

    if config.ignore_paths.contains(&path.to_path_buf()) {
        println!("Skipping ignored path: {}", path.display());
        return Ok(MetaData::Directory(DirMetaData { data: Vec::new(), inode: metadata.ino() }))
    }

    let dir_key = generate_key(path.to_string_lossy().to_string());
    let dir_entries = Arc::new(Mutex::new(Vec::new()));

    fs::read_dir(path)?
        .inspect(|entry| {
            if let Err(ref e) = entry {
                eprintln!("Invalid entry in directory {}: {}", path.display(), e);
            }
        })
        .filter_map(|e| e.ok())
        .par_bridge()
        .for_each(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let path = entry.path();
            let metadata = entry.metadata();

            if let Ok(metadata) = metadata {
                if config.ignore_paths.contains(&path.to_path_buf()) {
                    println!("Skipping ignored path: {}", path.display());
                    return;
                }

                if metadata.is_file() {
                    let key = generate_key(path.to_string_lossy().to_string());
                    let file_meta = {
                        let mut index = bof_index.lock().unwrap();
                        index.add_entry(&path, key, &metadata, None)
                    };
                    dir_entries.lock().unwrap().push(DirEntry { name, data: file_meta });
                } else if metadata.is_dir() {
                    if let Ok(subdir_meta) = index_parallel(&path, bof_index.clone(), config) {
                        dir_entries.lock().unwrap().push(DirEntry { name, data: subdir_meta });
                    }
                }
            }
        });

    let dir_entries = Arc::try_unwrap(dir_entries).unwrap().into_inner().unwrap();

    let meta_data = {
        let mut index = bof_index.lock().unwrap();
        index.add_entry(path, dir_key, &metadata, Some(dir_entries))
    };

    Ok(meta_data)
}

pub(crate) fn index_directories(paths: Vec<PathBuf>, config: &BOFConfig) -> io::Result<()> {
    let bof_indices = Arc::new(Mutex::new(Vec::new()));

    if config.parallel {
        paths.par_iter().for_each(|path| {
            let bof_index = BOFIndex::new();
            if let Err(e) = index_parallel(&path, Arc::new(Mutex::new(bof_index.clone())), config) {
                eprintln!("Error indexing directory {}: {}", path.display(), e);
            }

            bof_indices.lock().unwrap().push(bof_index);
        });
    } else {
        for path in paths {
            let mut bof_index = BOFIndex::new();
            index(&path, &mut bof_index, config)?;
            bof_indices.lock().unwrap().push(bof_index);
        }
    }

    let bof_indices_lock = bof_indices.lock().unwrap();
    save_indices(bof_indices_lock.clone(), config)
}

pub(crate) fn update_index(path: &Path, bof_index: &mut BOFIndex, config: &BOFConfig) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    let key = generate_key(path.to_string_lossy().to_string());

    // Check if the entry exists
    if let Some(entry) = bof_index.entries.iter_mut().find(|entry| entry.key == key) {
        if metadata.is_file() {
            let file_meta: FileMetaData = (&metadata).into();
            if let MetaData::File(existing_meta) = &entry.metadata {
                if file_meta.mtime > existing_meta.mtime {
                    // File has been modified
                    entry.metadata = MetaData::File(file_meta);
                }
            }
        } else if metadata.is_dir() {
            // For directories, recursively update sub-entries
            let sub_entries = fs::read_dir(path)?
                .map(|entry| entry.map(|e| e.path()))
                .collect::<Result<Vec<PathBuf>, io::Error>>()?;
            for sub_path in sub_entries {
                update_index(&sub_path, bof_index, config)?;
            }
        }
    } else {
        // Entry doesn't exist, add it to the index
        index(path, bof_index, config)?;
    }
    Ok(())
}

pub(crate) fn update_index_parallel(path: &Path, bof_index: Arc<Mutex<BOFIndex>>, config: &BOFConfig) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    let key = generate_key(path.to_string_lossy().to_string());

    // Check if the entry exists
    if let Some(entry) = bof_index.lock().unwrap().entries.iter_mut().find(|entry| entry.key == key) {
        if metadata.is_file() {
            let file_meta: FileMetaData = (&metadata).into();
            if let MetaData::File(existing_meta) = &entry.metadata {
                if file_meta.mtime > existing_meta.mtime {
                    // File has been modified, update it
                    entry.metadata = MetaData::File(file_meta);
                }
            }
        } else if metadata.is_dir() {
            // For directories, recursively update sub-entries in parallel
            let sub_entries = fs::read_dir(path)?
                .map(|entry| entry.map(|e| e.path()))
                .collect::<Result<Vec<PathBuf>, io::Error>>()?;

            // Parallel processing of subdirectory entries
            sub_entries.par_iter().for_each(|sub_path| {
                if let Err(e) = update_index_parallel(sub_path, bof_index.clone(), config) {
                    println!("Error updating index for {}: {}", sub_path.to_string_lossy(), e);
                }
            });
        }
    } else {
        // Entry doesn't exist, add it to the index
        let dir_key = generate_key(path.to_string_lossy().to_string());
        let dir_entries = Arc::new(Mutex::new(Vec::new()));
        let entries: Vec<_> = fs::read_dir(path)?
            .filter_map(|entry| entry.ok())
            .collect();

        // Parallel directory processing
        fs::read_dir(path)?
            .filter_map(|entry| entry.ok()) // Ignore invalid entries
            .par_bridge() // Converts the iterator into a parallel iterator
            .for_each(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                let path = entry.path();
                let metadata = entry.metadata();

                if let Ok(metadata) = metadata {
                    if metadata.is_file() {
                        // Process files
                        let key = generate_key(path.to_string_lossy().to_string());
                        let file_meta = {
                            let mut index = bof_index.lock().unwrap();
                            index.add_entry(&path, key, &metadata, None)
                        };
                        dir_entries.lock().unwrap().push(DirEntry { name, data: file_meta });
                    } else if metadata.is_dir() {
                        if let Ok(subdir_meta) = update_index_parallel(&path, bof_index.clone(), config) {
                            dir_entries.lock().unwrap().push(DirEntry { name, data: subdir_meta });
                        }
                    }
                }
            });

        let dir_entries = Arc::try_unwrap(dir_entries).unwrap().into_inner().unwrap();

        // Add directory entry after parallel processing
        let meta_data = {
            let mut index = bof_index.lock().unwrap();
            index.add_entry(path, dir_key, &metadata, Some(dir_entries))
        };

        return Ok(meta_data);
    }

    Ok(MetaData::Directory(DirMetaData { data: Vec::new(), inode: metadata.ino() }))
}

pub(crate) fn update_directories_parallel(
    paths: Vec<PathBuf>,
    bof_index: Arc<Mutex<BOFIndex>>,
    config: &BOFConfig,
) -> io::Result<()> {
    if config.parallel {
        // Parallelize directory updates
        paths.into_par_iter().for_each(|path| {
            if let Err(e) = update_index_parallel(&path, bof_index.clone(), config) {
                println!("Error updating index for {}: {}", path.to_string_lossy(), e);
            }
        });
    } else {
        // Sequential update if parallel is not enabled
        for path in paths {
            if let Err(e) = update_index(&path, &mut bof_index.lock().unwrap(), config) {
                println!("Error updating index for {}: {}", path.to_string_lossy(), e);
            }
        }
    }

    Ok(())
}

pub(crate) fn save_indices(bof_indices: Vec<BOFIndex>, config: &BOFConfig) -> io::Result<()> {
    let mut global_entries = Vec::new();
    let mut global_inverse_table = HashMap::new();

    for bof_index in bof_indices {
        global_entries.extend(bof_index.entries);
        for (key, value) in bof_index.inverse_table {
            global_inverse_table
                .entry(key)
                .or_insert_with(Vec::new)
                .extend(value); 
        }
    }

    let global_bof = BOFIndex { entries: global_entries, inverse_table: global_inverse_table };

    let file = File::create(&config.output_dir.join(PathBuf::from("index.json")))?;
    serde_json::to_writer_pretty(file, &global_bof)?;
    println!("BOF saved to {}/index.json", config.output_dir.display());

    Ok(())
}