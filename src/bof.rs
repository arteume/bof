use std::fs::{self, File};
use std::io::{self};
use std::path::{Path, PathBuf};
use serde::Serialize;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;

fn generate_key(path: String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Serialize)]
pub(crate) enum MetaData {
    Directory(DirMetaData),
    File(FileMetaData)
}

#[derive(Debug, Serialize)]
struct BOFEntry {
    key: String,
    path: String,
    metadata: MetaData,
}


#[derive(Debug, Serialize, Clone)]
pub(crate) struct FileMetaData {
    ctime: u64,
    mtime: u64,
    size: u64,
    inode: u64,
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

#[derive(Debug, Serialize)]
pub(crate) struct DirMetaData {
    data: Vec<DirEntry>,
    inode: u64,
}

#[derive(Debug, Serialize)]
struct DirEntry {
    name: String,
    data: MetaData,
}

#[derive(Debug, Serialize)]
pub(crate) struct BOFIndex {
    entries: Vec<BOFEntry>,
    inverse_table: HashMap<String, Vec<String>>,
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
            self.entries.push(BOFEntry { key: key.clone(), path: path.to_string_lossy().to_string(), metadata: MetaData::File(metadata.clone()) });
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

pub(crate) fn init() -> io::Result<()> {
    fs::create_dir_all(".bof")
}

pub(crate) fn index(path: &Path, bof_index: &mut BOFIndex) -> io::Result<MetaData> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Path is not a directory"));
    }

    let dir_key = generate_key(path.to_string_lossy().to_string());
    let mut dir_entries = DirMetaData {data: Vec::new(), inode: metadata.ino()};
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();

        if metadata.is_file() {
            let key = generate_key(fs::read_to_string(&path)? + &name);
            let file_meta = bof_index.add_entry(&path, key, & metadata, None);
            dir_entries.data.push(DirEntry { name, data: file_meta });
        } else if metadata.is_dir() {
            let subdir_meta = index(&entry.path(), bof_index)?;
            dir_entries.data.push(DirEntry { name, data: subdir_meta });
        }
    }
    Ok(bof_index.add_entry(path, dir_key, &metadata, Some(dir_entries.data)))
}

pub(crate) fn save_index(bof_index: &BOFIndex) -> io::Result<()> {
    let index_path = Path::new(".bof/index.json");
    let file = File::create(index_path)?;
    serde_json::to_writer_pretty(file, bof_index)?;
    println!("Index saved to .bof/index.json");
    Ok(())
}

pub(crate) fn save_indices(bof_indices: Vec<BOFIndex>) -> io::Result<()> {
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
    let file = File::create(".bof/index.json")?;
    serde_json::to_writer_pretty(file, &global_bof)?;
    println!("Meta BOF saved to .bof/index.json");
    Ok(())
}

pub(crate) fn index_directory(path: &Path) -> io::Result<()> {
    let mut bof_index = BOFIndex::new();
    index(path, &mut bof_index)?;
    save_index(&bof_index)?;
    Ok(())
}

pub(crate) fn index_multiple_directories(paths: Vec<PathBuf>) -> io::Result<()> {
    let mut bof_indices = Vec::new();
    for path in paths {
        let mut bof_index = BOFIndex::new();
        index(&path, &mut bof_index)?;
        bof_indices.push(bof_index);
    }
    save_indices(bof_indices)?;
    Ok(())
}