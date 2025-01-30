use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{
    collections::HashMap,
    fs::{self, File, Metadata},
    io::{self},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

fn generate_key(ident: String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ident.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Debug)]
enum QueueItem {
    DirEntry(DirEntry),
    BOFEntry(BOFEntry),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct BOFIndex {
    entries: HashMap<PathBuf, BOFEntry>,
    inverse_table: HashMap<String, Vec<PathBuf>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BOFEntry {
    key: String,
    path: PathBuf,
    metadata: MetaData,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum MetaData {
    Directory(DirMetaData),
    File(FileMetaData),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct FileMetaData {
    ctime: SystemTime,
    mtime: SystemTime,
    size: u64,
    inode: u64,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub(crate) struct DirMetaData {
    data: Vec<DirEntry>,
    inode: u64,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
struct DirEntry {
    name: String,
    data: MetaData,
}

impl From<&Metadata> for FileMetaData {
    fn from(val: &Metadata) -> FileMetaData {
        Self {
            ctime: val.created().unwrap(),  // Should be supported in our system
            mtime: val.modified().unwrap(), // Should be supported in our system
            size: val.len(),
            inode: val.ino(),
        }
    }
}

impl BOFIndex {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            inverse_table: HashMap::new(),
        }
    }

    fn add_entry(
        &mut self,
        path: &Path,
        key: String,
        metadata: &Metadata,
        dir_entries: Option<Vec<DirEntry>>,
    ) -> MetaData {
        let parent_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_string_lossy()
            .to_string();

        if metadata.is_file() {
            let metadata: FileMetaData = metadata.into();
            self.entries.insert(
                path.to_path_buf(),
                BOFEntry {
                    key: key.clone(),
                    path: path.to_path_buf(),
                    metadata: MetaData::File(metadata.clone()),
                },
            );
            self.inverse_table
                .entry(key)
                .or_default()
                .push(parent_dir.into());
            MetaData::File(metadata)
        } else {
            MetaData::Directory(DirMetaData {
                data: dir_entries.unwrap(), // Should be Some,
                inode: metadata.ino(),
            })
        }
    }

    fn add_entry_meta(
        &mut self,
        path: &Path,
        key: String,
        metadata: &MetaData,
        dir_entries: Option<Vec<DirEntry>>,
    ) -> MetaData {
        let parent_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_string_lossy()
            .to_string();
        match metadata {
            MetaData::File(_) => {
                self.entries.insert(
                    path.to_path_buf(),
                    BOFEntry {
                        key: key.clone(),
                        path: path.to_path_buf(),
                        metadata: metadata.clone(),
                    },
                );
                self.inverse_table
                    .entry(key)
                    .or_default()
                    .push(parent_dir.into());
                metadata.clone()
            }
            MetaData::Directory(dir_meta) => {
                MetaData::Directory(DirMetaData {
                    data: dir_entries.unwrap(), // Should be Some,
                    inode: dir_meta.inode,
                })
            }
        }
    }

    fn update_entry(&mut self, path: &Path, key: String, metadata: &Metadata) -> MetaData {
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.1.path == path) {
            entry.1.key = key;
            entry.1.metadata = MetaData::File(metadata.into());
        }
        println!("Updated an entry {}", path.display());
        MetaData::File(metadata.into())
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
    println!(
        "Initialized .bof directory at: {}",
        &config.output_dir.display()
    );
    Ok(())
}

fn index(path: &Path, bof_index: &mut BOFIndex, config: &BOFConfig) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path is not a directory",
        ));
    }

    if config.ignore_paths.contains(&path.to_path_buf()) {
        println!("Skipping ignored path: {}", path.display());
        return Ok(MetaData::Directory(DirMetaData {
            data: Vec::new(),
            inode: metadata.ino(),
        }));
    }

    let dir_key = generate_key(path.to_string_lossy().to_string());
    let mut dir_entries = DirMetaData {
        data: Vec::new(),
        inode: metadata.ino(),
    };

    fs::read_dir(path)?
        .inspect(|entry| {
            if let Err(ref e) = entry {
                eprintln!("Invalid entry in directory {}: {}", path.display(), e);
            }
        })
        .filter_map(|e| e.ok())
        .for_each(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let path = entry.path();
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Failed to get metadata for {}: {}", path.display(), e);
                    return;
                }
            };

            if config.ignore_paths.contains(&path.to_path_buf()) {
                println!("Skipping ignored path: {}", path.display());
                return;
            }

            if metadata.is_file() {
                let key = match fs::read_to_string(&path) {
                    Ok(content) => generate_key(content + &name),
                    Err(e) => {
                        eprintln!("Failed to read file {}: {}", path.display(), e);
                        return;
                    }
                };
                let file_meta = bof_index.add_entry(&path, key, &metadata, None);
                dir_entries.data.push(DirEntry {
                    name,
                    data: file_meta,
                });
            } else if metadata.is_dir() {
                match index(&entry.path(), bof_index, config) {
                    Ok(subdir_meta) => dir_entries.data.push(DirEntry {
                        name,
                        data: subdir_meta,
                    }),
                    Err(e) => eprintln!("Failed to index directory {}: {}", path.display(), e),
                };
            } else {
                eprintln!("Neither file nor directory! {}", path.display());
                return;
            }
        });

    Ok(bof_index.add_entry(path, dir_key, &metadata, Some(dir_entries.data)))
}

fn index_parallel(
    path: &Path,
    bof_index: Arc<Mutex<BOFIndex>>,
    config: &BOFConfig,
) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path is not a directory",
        ));
    }

    if config.ignore_paths.contains(&path.to_path_buf()) {
        println!("Skipping ignored path: {}", path.display());
        return Ok(MetaData::Directory(DirMetaData {
            data: Vec::new(),
            inode: metadata.ino(),
        }));
    }

    let dir_key = generate_key(path.to_string_lossy().to_string());
    let queue = crossbeam_queue::SegQueue::new();

    let entries = fs::read_dir(path)?
        .inspect(|entry| {
            if let Err(ref e) = entry {
                eprintln!("Invalid entry in directory {}: {}", path.display(), e);
            }
        })
        .filter_map(|e| e.ok())
        .collect::<Vec<_>>();

    entries.par_iter().for_each(|entry| {
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Failed to get metadata for {}: {}", path.display(), e);
                return;
            }
        };

        if config.ignore_paths.contains(&path.to_path_buf()) {
            println!("Skipping ignored path: {}", path.display());
            return;
        }

        if metadata.is_file() {
            let key = match fs::read_to_string(&path) {
                Ok(content) => generate_key(content + &name),
                Err(e) => {
                    eprintln!("Failed to read file {}: {}", path.display(), e);
                    return;
                }
            };
            let file_meta = FileMetaData::from(&metadata);
            queue.push(QueueItem::DirEntry(DirEntry {
                name,
                data: MetaData::File(file_meta.clone()),
            }));

            let bof_entry = BOFEntry {
                key,
                path: path.clone(),
                metadata: MetaData::File(file_meta),
            };
            queue.push(QueueItem::BOFEntry(bof_entry));
        } else if metadata.is_dir() {
            match index_parallel(&path, bof_index.clone(), config) {
                Ok(subdir_meta) => queue.push(QueueItem::DirEntry(DirEntry {
                    name,
                    data: subdir_meta,
                })),
                Err(e) => eprintln!("Failed to index directory {}: {}", path.display(), e),
            };
        } else {
            eprintln!("Neither file nor directory! {}", path.display());
            return;
        }
    });

    let mut index_lock = bof_index.lock().unwrap();
    let mut dir_entries = Vec::new();

    while let Some(item) = queue.pop() {
        match item {
            QueueItem::DirEntry(entry) => dir_entries.push(entry),
            QueueItem::BOFEntry(bof_entry) => {
                index_lock.add_entry_meta(
                    &bof_entry.path,
                    bof_entry.key,
                    &bof_entry.metadata,
                    None,
                );
            }
        }
    }

    let meta_data = MetaData::Directory(DirMetaData {
        data: dir_entries.clone(),
        inode: metadata.ino(),
    });

    index_lock.add_entry(path, dir_key, &metadata, Some(dir_entries));
    Ok(meta_data)
}

pub(crate) fn index_directories(paths: Vec<PathBuf>, config: &BOFConfig) -> io::Result<()> {
    let bof_index = Arc::new(Mutex::new(BOFIndex::new()));

    if config.parallel {
        paths.par_iter().for_each(|path| {
            if let Err(e) = index_parallel(path, bof_index.clone(), config) {
                eprintln!("Error indexing directory {}: {}", path.display(), e);
            }
        });

        let bof_index_lock = bof_index.lock().unwrap();
        save_index((*bof_index_lock).clone(), config)
    } else {
        let mut bof_index = BOFIndex::new();
        for path in paths {
            index(&path, &mut bof_index, config)?;
        }
        save_index(bof_index, config)
    }
}

fn update_index(path: &Path, bof_index: &mut BOFIndex, config: &BOFConfig) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path is not a directory",
        ));
    }

    if config.ignore_paths.contains(&path.to_path_buf()) {
        println!("Skipping ignored path: {}", path.display());
        return Ok(MetaData::Directory(DirMetaData {
            data: Vec::new(),
            inode: metadata.ino(),
        }));
    }

    let dir_key = generate_key(path.to_string_lossy().to_string());
    let mut dir_entries = DirMetaData {
        data: Vec::new(),
        inode: metadata.ino(),
    };

    fs::read_dir(path)?
        .inspect(|entry| {
            if let Err(ref e) = entry {
                eprintln!("Invalid entry in directory {}: {}", path.display(), e);
            }
        })
        .filter_map(|e| e.ok())
        .for_each(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let path = entry.path();
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Failed to get metadata for {}: {}", path.display(), e);
                    return;
                }
            };

            match bof_index.entries.get_mut(&path) {
                Some(entry) => match &entry.metadata {
                    MetaData::Directory(_) => {
                        eprintln!("This entry is a directory! {}", path.display());
                    }
                    MetaData::File(file_meta) => {
                        if file_meta.mtime != metadata.modified().unwrap() {
                            let key = match fs::read_to_string(&path) {
                                Ok(content) => generate_key(content + &name),
                                Err(e) => {
                                    eprintln!("Failed to read file {}: {}", path.display(), e);
                                    return;
                                }
                            };
                            bof_index.update_entry(&path, key, &metadata);
                        }
                    }
                },
                None => {
                    if metadata.is_file() {
                        let key = match fs::read_to_string(&path) {
                            Ok(content) => generate_key(content + &name),
                            Err(e) => {
                                eprintln!("Failed to read file {}: {}", path.display(), e);
                                return;
                            }
                        };
                        let file_meta = bof_index.add_entry(&path, key, &metadata, None);
                        dir_entries.data.push(DirEntry {
                            name,
                            data: file_meta,
                        });
                    } else if metadata.is_dir() {
                        if let Ok(subdir_meta) = update_index(&path, &mut bof_index.clone(), config)
                        {
                            dir_entries.data.push(DirEntry {
                                name,
                                data: subdir_meta,
                            });
                        }
                    } else {
                        eprintln!("Neither file nor directory! {}", path.display());
                        return;
                    }
                }
            }
        });
    if let Some(entry) = bof_index.entries.iter().find(|entry| entry.1.path == path) {
        Ok(entry.1.metadata.clone())
    } else {
        Ok(bof_index.add_entry(path, dir_key, &metadata, Some(dir_entries.data)))
    }
}

fn update_index_parallel(
    path: &Path,
    bof_index: Arc<Mutex<BOFIndex>>,
    config: &BOFConfig,
) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path is not a directory",
        ));
    }

    if config.ignore_paths.contains(&path.to_path_buf()) {
        println!("Skipping ignored path: {}", path.display());
        return Ok(MetaData::Directory(DirMetaData {
            data: Vec::new(),
            inode: metadata.ino(),
        }));
    }

    let dir_key = generate_key(path.to_string_lossy().to_string());
    let queue = crossbeam_queue::SegQueue::new();

    let entries = fs::read_dir(path)?
        .inspect(|entry| {
            if let Err(ref e) = entry {
                eprintln!("Invalid entry in directory {}: {}", path.display(), e);
            }
        })
        .filter_map(|e| e.ok())
        .collect::<Vec<_>>();
    dbg!(&path);
    entries.par_iter().for_each(|entry| {
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Failed to get metadata for {}: {}", path.display(), e);
                return;
            }
        };

        let mut index_lock = bof_index.lock().unwrap();
        match index_lock.entries.get_mut(&path) {
            Some(entry) => match &entry.metadata {
                MetaData::Directory(_) => {
                    eprintln!("This entry is a directory! {}", path.display());
                    return;
                }
                MetaData::File(file_meta) => {
                    if file_meta.mtime != metadata.modified().unwrap() {
                        let key = match fs::read_to_string(&path) {
                            Ok(content) => generate_key(content + &name),
                            Err(e) => {
                                eprintln!("Failed to read file {}: {}", path.display(), e);
                                return;
                            }
                        };

                        index_lock.update_entry(&path, key, &metadata);
                    }
                }
            },
            None => {
                if metadata.is_file() {
                    let key = match fs::read_to_string(&path) {
                        Ok(content) => generate_key(content + &name),
                        Err(e) => {
                            eprintln!("Failed to read file {}: {}", path.display(), e);
                            return;
                        }
                    };
                    let file_meta = FileMetaData::from(&metadata);
                    queue.push(QueueItem::DirEntry(DirEntry {
                        name,
                        data: MetaData::File(file_meta.clone()),
                    }));

                    let bof_entry = BOFEntry {
                        key,
                        path: path.clone(),
                        metadata: MetaData::File(file_meta),
                    };
                    queue.push(QueueItem::BOFEntry(bof_entry));
                } else if metadata.is_dir() {
                    if let Ok(subdir_meta) = update_index_parallel(&path, bof_index.clone(), config)
                    {
                        queue.push(QueueItem::DirEntry(DirEntry {
                            name,
                            data: subdir_meta,
                        }));
                    }
                } else {
                    eprintln!("Neither file nor directory! {}", path.display());
                    return;
                }
            }
        }
    });

    let mut index_lock = bof_index.lock().unwrap();
    let mut dir_entries = Vec::new();

    while let Some(item) = queue.pop() {
        match item {
            QueueItem::DirEntry(entry) => dir_entries.push(entry),
            QueueItem::BOFEntry(bof_entry) => {
                index_lock.add_entry_meta(
                    &bof_entry.path,
                    bof_entry.key,
                    &bof_entry.metadata,
                    None,
                );
            }
        }
    }

    let meta_data = {
        let mut index = bof_index.lock().unwrap();
        if let Some(entry) = index.entries.iter().find(|entry| entry.1.path == path) {
            entry.1.metadata.clone()
        } else {
            index.add_entry(path, dir_key, &metadata, Some(dir_entries))
        }
    };

    Ok(meta_data)
}

pub(crate) fn update_directories(paths: Vec<PathBuf>, config: &BOFConfig) -> io::Result<()> {
    let mut existing_indices = load_indices(&config.output_dir)?;

    if config.parallel {
        paths.par_iter().for_each(|path| {
            if let Err(e) =
                update_index_parallel(path, Arc::new(Mutex::new(existing_indices.clone())), config)
            {
                eprintln!("Error updating directory {}: {}", path.display(), e);
            }
        });
        save_index(existing_indices, config)
    } else {
        let mut bof_indices = Vec::new();
        for path in paths {
            update_index(&path, &mut existing_indices, config)?;
            bof_indices.push(existing_indices.clone());
        }
        save_index(existing_indices, config)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IntBOFIndex {
    entries: Vec<BOFEntry>,
    inverse_table: HashMap<String, Vec<PathBuf>>,
}

pub(crate) fn save_index(bof_indices: BOFIndex, config: &BOFConfig) -> io::Result<()> {
    let file = File::create(config.output_dir.join(PathBuf::from("index.json")))?;
    serde_json::to_writer_pretty(
        file,
        &IntBOFIndex {
            entries: bof_indices.entries.values().cloned().collect::<Vec<_>>(),
            inverse_table: bof_indices.inverse_table,
        },
    )?;
    println!("BOF saved to {}/index.json", config.output_dir.display());

    Ok(())
}

pub fn load_indices(output_dir: &Path) -> io::Result<BOFIndex> {
    let path = output_dir.join("index.json");
    let file = File::open(path)?;

    let entries: IntBOFIndex = serde_json::from_reader(file)?;

    let entries_map: HashMap<PathBuf, BOFEntry> = entries
        .entries
        .into_iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect();

    Ok(BOFIndex {
        entries: entries_map,
        inverse_table: entries.inverse_table,
    })
}
