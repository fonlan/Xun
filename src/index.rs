use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::ffi::c_void;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;
use rayon::prelude::*;
use windows::Win32::Foundation::{CloseHandle, GetLastError, WIN32_ERROR};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::{
    CREATE_USN_JOURNAL_DATA, FSCTL_CREATE_USN_JOURNAL, FSCTL_ENUM_USN_DATA,
    FSCTL_QUERY_USN_JOURNAL, FSCTL_READ_USN_JOURNAL, MFT_ENUM_DATA_V0, READ_USN_JOURNAL_DATA_V0,
    USN_JOURNAL_DATA_V0, USN_REASON_CLOSE, USN_REASON_FILE_CREATE, USN_REASON_FILE_DELETE,
    USN_REASON_RENAME_NEW_NAME, USN_REASON_RENAME_OLD_NAME, USN_RECORD_V2,
};

use crate::arena::{ByteArena, SliceRef};
use crate::model::{FileFlags, FileMeta, SearchResult, classify_path_kind};
use crate::win::{VolumeInfo, is_placeholder_attr, ntfs_volumes};
use crate::xlog;

const USN_BUFFER_SIZE: usize = 4 * 1024 * 1024;

#[derive(Default)]
struct TrieNode {
    children: HashMap<u8, usize>,
    postings: Vec<u32>,
}

#[derive(Default)]
struct PrefixTrie {
    nodes: Vec<TrieNode>,
}

impl PrefixTrie {
    fn new() -> Self {
        Self {
            nodes: vec![TrieNode::default()],
        }
    }

    fn insert(&mut self, key: &[u8], item_id: u32) {
        let mut node_idx = 0usize;
        for &ch in key.iter().take(256) {
            let next = if let Some(next) = self.nodes[node_idx].children.get(&ch).copied() {
                next
            } else {
                let created = self.nodes.len();
                self.nodes.push(TrieNode::default());
                self.nodes[node_idx].children.insert(ch, created);
                created
            };
            node_idx = next;
            self.nodes[node_idx].postings.push(item_id);
        }
    }

    fn search_prefix(&self, key: &[u8], limit: usize) -> Vec<u32> {
        if key.is_empty() {
            return Vec::new();
        }

        let mut node_idx = 0usize;
        for &ch in key.iter().take(256) {
            let Some(next) = self.nodes[node_idx].children.get(&ch).copied() else {
                return Vec::new();
            };
            node_idx = next;
        }

        self.nodes[node_idx]
            .postings
            .iter()
            .copied()
            .rev()
            .take(limit)
            .collect()
    }
}

struct IndexInner {
    arena: ByteArena,
    files: Vec<FileMeta>,
    trie: PrefixTrie,
    trigrams: HashMap<u32, Vec<u32>>,
    seen: HashMap<u64, u32>,
}

impl Default for IndexInner {
    fn default() -> Self {
        Self {
            arena: ByteArena::with_capacity(64 * 1024 * 1024),
            files: Vec::new(),
            trie: PrefixTrie::new(),
            trigrams: HashMap::new(),
            seen: HashMap::new(),
        }
    }
}

pub struct FileIndex {
    inner: Arc<RwLock<IndexInner>>,
}

impl FileIndex {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(IndexInner::default())),
        }
    }

    pub fn start_indexing(&self) -> Result<()> {
        xlog::info("start_indexing begin");
        let volumes = ntfs_volumes().context("failed to enumerate NTFS volumes")?;
        xlog::info(format!("ntfs_volumes found {} volume(s)", volumes.len()));
        if !volumes.is_empty() {
            let letters = volumes
                .iter()
                .map(|v| v.letter.to_string())
                .collect::<Vec<_>>()
                .join(",");
            xlog::info(format!("volumes={letters}"));
        }

        let snapshots = volumes
            .par_iter()
            .map(|volume| (volume.letter, scan_volume_from_mft(volume)))
            .collect::<Vec<(char, Result<VolumeSnapshot>)>>();

        {
            let mut indexed_count = 0usize;
            for (letter, snapshot) in snapshots {
                let snapshot = match snapshot {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        xlog::warn(format!("skip volume {letter}: {err:#}"));
                        continue;
                    }
                };

                xlog::info(format!(
                    "volume {letter} snapshot tree_nodes={}",
                    snapshot.tree.len()
                ));

                let mut inserted_this_volume = 0usize;
                let mut pending = Vec::with_capacity(4096);
                let started = Instant::now();

                let frns = snapshot.tree.keys().copied().collect::<Vec<_>>();
                let tree = &snapshot.tree;
                let mut path_cache = HashMap::<u64, Option<String>>::with_capacity(tree.len() / 2);

                for frn in frns {
                    let Some(path) = build_full_path_cached(letter, frn, tree, &mut path_cache)
                    else {
                        continue;
                    };
                    let flags = tree.get(&frn).map(|n| n.flags).unwrap_or_default();

                    pending.push(RawPath {
                        lower_path: path.to_ascii_lowercase(),
                        path,
                        flags,
                    });

                    if pending.len() >= 4096 {
                        let mut guard = self.inner.write();
                        for entry in pending.drain(..) {
                            push_entry(&mut guard, entry);
                            indexed_count += 1;
                            inserted_this_volume += 1;
                        }
                    }

                    if inserted_this_volume > 0 && inserted_this_volume % 200_000 == 0 {
                        xlog::info(format!(
                            "volume {letter} insert progress={} elapsed_ms={}",
                            inserted_this_volume,
                            started.elapsed().as_millis()
                        ));
                    }
                }

                if !pending.is_empty() {
                    let mut guard = self.inner.write();
                    for entry in pending.drain(..) {
                        push_entry(&mut guard, entry);
                        indexed_count += 1;
                        inserted_this_volume += 1;
                    }
                }

                xlog::info(format!(
                    "volume {letter} insert done={} elapsed_ms={}",
                    inserted_this_volume,
                    started.elapsed().as_millis()
                ));
            }

            let guard = self.inner.read();

            xlog::info(format!(
                "initial index ready: {} entries across {} NTFS volume(s)",
                indexed_count,
                volumes.len()
            ));
            xlog::info(format!("inner.files len={}", guard.files.len()));
        }

        xlog::info("spawning usn monitors");
        self.spawn_usn_monitors(volumes);
        xlog::info("start_indexing end");
        Ok(())
    }

    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let normalized = query.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Vec::new();
        }

        let needle = normalized.as_bytes();
        let needle_len = needle.len();
        let guard = self.inner.read();
        let mut out = Vec::new();
        let prefix_budget = if needle_len <= 1 {
            limit.saturating_mul(200)
        } else if needle_len == 2 {
            limit.saturating_mul(80)
        } else {
            limit.saturating_mul(16)
        };

        let mut seen = std::collections::HashSet::<u32>::new();
        let prefix_ids = guard
            .trie
            .search_prefix(needle, guard.files.len().min(prefix_budget));

        for id in prefix_ids {
            if !seen.insert(id) {
                continue;
            }

            let Some(meta) = guard.files.get(id as usize).copied() else {
                continue;
            };
            let lower = guard.arena.get(meta.lower_path);
            if !contains_subslice(lower, needle) {
                continue;
            }

            let path = decode_slice(&guard.arena, meta.path);
            out.push(SearchResult {
                kind: classify_path_kind(&path).to_string(),
                path,
            });
            if out.len() >= limit {
                return out;
            }
        }

        if needle_len <= 2 {
            return out;
        }

        let gram_keys = collect_trigram_keys(needle);
        if !gram_keys.is_empty() {
            let mut postings = gram_keys
                .iter()
                .filter_map(|key| guard.trigrams.get(key))
                .collect::<Vec<_>>();

            if postings.len() == gram_keys.len() {
                postings.sort_by_key(|list| list.len());
                if let Some(anchor) = postings.first() {
                    let needed = postings.len() as u16;
                    let mut counts = HashMap::<u32, u16>::with_capacity(anchor.len());
                    for &id in anchor.iter() {
                        counts.insert(id, 1);
                    }

                    for list in postings.iter().skip(1) {
                        for &id in list.iter() {
                            if let Some(count) = counts.get_mut(&id) {
                                *count += 1;
                            }
                        }
                    }

                    for &id in anchor.iter().rev() {
                        if out.len() >= limit {
                            break;
                        }
                        if !seen.insert(id) {
                            continue;
                        }
                        if counts.get(&id).copied().unwrap_or_default() != needed {
                            continue;
                        }

                        let Some(meta) = guard.files.get(id as usize).copied() else {
                            continue;
                        };
                        let lower = guard.arena.get(meta.lower_path);
                        if !contains_subslice(lower, needle) {
                            continue;
                        }

                        let path = decode_slice(&guard.arena, meta.path);
                        out.push(SearchResult {
                            kind: classify_path_kind(&path).to_string(),
                            path,
                        });
                    }
                }
            }
        }

        let path_like_query = needle.iter().any(|ch| matches!(*ch, b'\\' | b'/' | b':'));
        if out.len() < limit && path_like_query {
            for (idx, meta) in guard.files.iter().enumerate() {
                let id = idx as u32;
                if !seen.insert(id) {
                    continue;
                }

                let lower = guard.arena.get(meta.lower_path);
                if !contains_subslice(lower, needle) {
                    continue;
                }

                let path = decode_slice(&guard.arena, meta.path);
                out.push(SearchResult {
                    kind: classify_path_kind(&path).to_string(),
                    path,
                });
                if out.len() >= limit {
                    break;
                }
            }
        }

        out
    }

    pub fn len(&self) -> usize {
        self.inner.read().files.len()
    }

    fn spawn_usn_monitors(&self, volumes: Vec<VolumeInfo>) {
        let inner = self.inner.clone();

        thread::spawn(move || {
            xlog::info(format!(
                "usn monitor thread start, volumes={}",
                volumes.len()
            ));
            let mut monitors = volumes
                .into_iter()
                .filter_map(|volume| {
                    let letter = volume.letter;
                    let snapshot = match scan_volume_from_mft(&volume) {
                        Ok(snapshot) => snapshot,
                        Err(err) => {
                            xlog::warn(format!(
                                "monitor init skip volume {letter}: snapshot failed: {err:#}"
                            ));
                            return None;
                        }
                    };

                    let monitor = match UsnMonitor::new(volume, snapshot) {
                        Ok(monitor) => monitor,
                        Err(err) => {
                            xlog::warn(format!(
                                "monitor init skip volume {letter}: monitor create failed: {err:#}"
                            ));
                            return None;
                        }
                    };
                    xlog::info(format!("monitor attached volume {letter}"));
                    Some(monitor)
                })
                .collect::<Vec<_>>();

            xlog::info(format!("usn monitors active={}", monitors.len()));

            let mut tick = 0u64;

            loop {
                tick += 1;
                let mut total_changes = 0usize;
                for monitor in monitors.iter_mut() {
                    let letter = monitor.volume.letter;
                    let Ok(changes) = monitor.read_changes() else {
                        xlog::warn(format!("read_changes failed on volume {letter}"));
                        continue;
                    };
                    if changes.is_empty() {
                        continue;
                    }

                    xlog::info(format!("volume {letter} changes={}", changes.len()));

                    let mut guard = inner.write();
                    for entry in changes {
                        push_entry(&mut guard, entry);
                        total_changes += 1;
                    }
                }

                if total_changes > 0 || tick % 20 == 0 {
                    xlog::info(format!(
                        "usn tick={} applied_changes={} monitors={}",
                        tick,
                        total_changes,
                        monitors.len()
                    ));
                }

                thread::sleep(Duration::from_millis(500));
            }
        });
    }
}

#[derive(Clone)]
struct RawPath {
    path: String,
    lower_path: String,
    flags: FileFlags,
}

struct VolumeSnapshot {
    tree: HashMap<u64, Node>,
}

#[derive(Clone)]
struct Node {
    parent: Option<u64>,
    name: String,
    flags: FileFlags,
}

fn scan_volume_from_mft(volume: &VolumeInfo) -> Result<VolumeSnapshot> {
    xlog::info(format!(
        "scan_volume_from_mft begin volume {}",
        volume.letter
    ));
    ensure_usn_journal(volume)?;

    let snapshot = scan_volume_from_usn_enum(volume)?;
    xlog::info(format!(
        "scan_volume_from_mft end volume {} tree={}",
        volume.letter,
        snapshot.tree.len()
    ));
    Ok(snapshot)
}

fn scan_volume_from_usn_enum(volume: &VolumeInfo) -> Result<VolumeSnapshot> {
    xlog::info(format!(
        "scan_volume_from_usn_enum begin volume {}",
        volume.letter
    ));
    let handle = open_volume_handle(volume.letter)?;

    let mut enum_data = MFT_ENUM_DATA_V0 {
        StartFileReferenceNumber: 0,
        LowUsn: 0,
        HighUsn: i64::MAX,
    };

    let mut tree = HashMap::<u64, Node>::new();
    let mut out = vec![0u8; USN_BUFFER_SIZE];
    let mut page = 0usize;
    let mut total_records = 0usize;

    loop {
        let mut returned = 0u32;
        let res = unsafe {
            DeviceIoControl(
                handle,
                FSCTL_ENUM_USN_DATA,
                Some((&mut enum_data as *mut MFT_ENUM_DATA_V0).cast::<c_void>()),
                std::mem::size_of::<MFT_ENUM_DATA_V0>() as u32,
                Some(out.as_mut_ptr().cast::<c_void>()),
                out.len() as u32,
                Some(&mut returned),
                None,
            )
        };

        if let Err(err) = res {
            let last = unsafe { GetLastError() };
            if last == WIN32_ERROR(38) {
                xlog::info(format!(
                    "FSCTL_ENUM_USN_DATA reached end volume {} page={} records={}",
                    volume.letter, page, total_records
                ));
                break;
            }
            let _ = unsafe { CloseHandle(handle) };
            return Err(anyhow!(
                "FSCTL_ENUM_USN_DATA failed on {}: {:?}, last={:?}",
                volume.letter,
                err,
                last
            ));
        }

        if returned <= 8 {
            xlog::info(format!(
                "FSCTL_ENUM_USN_DATA short page volume {} page={} returned={}",
                volume.letter, page, returned
            ));
            break;
        }

        page += 1;
        if page == 1 || page % 32 == 0 {
            xlog::info(format!(
                "FSCTL_ENUM_USN_DATA page volume {} page={} returned={}",
                volume.letter, page, returned
            ));
        }

        let mut next = [0u8; 8];
        next.copy_from_slice(&out[..8]);
        enum_data.StartFileReferenceNumber = u64::from_le_bytes(next);

        let mut offset = 8usize;
        while offset + std::mem::size_of::<USN_RECORD_V2>() <= returned as usize {
            let rec = unsafe { &*(out[offset..].as_ptr().cast::<USN_RECORD_V2>()) };
            if rec.RecordLength == 0 {
                break;
            }

            let len = rec.RecordLength as usize;
            if offset + len > returned as usize {
                break;
            }

            if let Some(name) = usn_record_name(rec, &out[offset..offset + len]) {
                total_records += 1;
                let flags = FileFlags {
                    is_symlink: (rec.FileAttributes & 0x0000_0400) != 0,
                    is_hardlink: false,
                    is_placeholder: is_placeholder_attr(rec.FileAttributes),
                };

                tree.insert(
                    rec.FileReferenceNumber,
                    Node {
                        parent: Some(rec.ParentFileReferenceNumber),
                        name,
                        flags,
                    },
                );
            }

            offset += len;
        }
    }

    let _ = unsafe { CloseHandle(handle) };
    xlog::info(format!(
        "scan_volume_from_usn_enum parse done volume {} pages={} records={} tree={}",
        volume.letter,
        page,
        total_records,
        tree.len()
    ));

    Ok(VolumeSnapshot { tree })
}

fn build_full_path(letter: char, frn: u64, tree: &HashMap<u64, Node>) -> Option<String> {
    let mut parts = Vec::new();
    let mut cursor = Some(frn);
    let mut visited = HashSet::new();

    while let Some(current) = cursor {
        if !visited.insert(current) {
            break;
        }

        let Some(node) = tree.get(&current) else {
            break;
        };

        if !node.name.is_empty() && node.name != "." && node.name != ".." {
            parts.push(node.name.clone());
        }

        match node.parent {
            Some(parent) if parent != current => cursor = Some(parent),
            _ => break,
        }
    }

    if parts.is_empty() {
        return None;
    }

    parts.reverse();
    let rel = parts.join("\\");
    Some(format!("{}:\\{}", letter, rel.trim_start_matches('\\')))
}

fn build_full_path_cached(
    letter: char,
    frn: u64,
    tree: &HashMap<u64, Node>,
    cache: &mut HashMap<u64, Option<String>>,
) -> Option<String> {
    if let Some(cached) = cache.get(&frn) {
        return cached.clone();
    }

    let mut chain = Vec::<u64>::new();
    let mut cursor = Some(frn);
    let mut visited = HashSet::new();

    while let Some(current) = cursor {
        if !visited.insert(current) {
            break;
        }

        if let Some(cached) = cache.get(&current) {
            if let Some(base) = cached {
                let mut full = base.clone();
                for node_id in chain.iter().rev() {
                    let Some(node) = tree.get(node_id) else {
                        continue;
                    };
                    if !node.name.is_empty() && node.name != "." && node.name != ".." {
                        if !full.ends_with('\\') {
                            full.push('\\');
                        }
                        full.push_str(&node.name);
                    }
                }
                cache.insert(frn, Some(full.clone()));
                return Some(full);
            }
            break;
        }

        chain.push(current);

        let Some(node) = tree.get(&current) else {
            break;
        };
        match node.parent {
            Some(parent) if parent != current => cursor = Some(parent),
            _ => break,
        }
    }

    let mut parts = Vec::new();
    for node_id in chain.iter().rev() {
        let Some(node) = tree.get(node_id) else {
            continue;
        };
        if !node.name.is_empty() && node.name != "." && node.name != ".." {
            parts.push(node.name.as_str());
        }
    }

    if parts.is_empty() {
        cache.insert(frn, None);
        return None;
    }

    let full = format!("{}:\\{}", letter, parts.join("\\"));
    cache.insert(frn, Some(full.clone()));
    Some(full)
}

fn push_entry(inner: &mut IndexInner, entry: RawPath) {
    let key_hash = hash_key(entry.lower_path.as_bytes());
    if let Some(existing_id) = inner.seen.get(&key_hash).copied() {
        if let Some(existing_meta) = inner.files.get_mut(existing_id as usize) {
            let existing_lower = decode_slice(&inner.arena, existing_meta.lower_path);
            if existing_lower == entry.lower_path {
                let new_path = inner.arena.push(entry.path.as_bytes());
                let new_lower = inner.arena.push(entry.lower_path.as_bytes());
                existing_meta.path = new_path;
                existing_meta.lower_path = new_lower;
                existing_meta.flags = entry.flags;
                return;
            }
        }
    }

    let id = inner.files.len() as u32;
    let path_ref = inner.arena.push(entry.path.as_bytes());
    let lower_ref = inner.arena.push(entry.lower_path.as_bytes());

    inner.files.push(FileMeta {
        path: path_ref,
        lower_path: lower_ref,
        flags: entry.flags,
    });

    if let Some(name) = basename(&entry.lower_path) {
        if !name.is_empty() {
            let name_bytes = name.as_bytes();
            inner.trie.insert(name_bytes, id);

            for key in collect_trigram_keys(name_bytes) {
                inner.trigrams.entry(key).or_default().push(id);
            }
        }
    }

    inner.seen.insert(key_hash, id);
}

fn decode_slice(arena: &ByteArena, slice: SliceRef) -> String {
    String::from_utf8_lossy(arena.get(slice)).to_string()
}

fn basename(path: &str) -> Option<&str> {
    path.rsplit(['\\', '/']).next()
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn hash_key(value: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn collect_trigram_keys(bytes: &[u8]) -> Vec<u32> {
    if bytes.len() < 3 {
        return Vec::new();
    }

    let mut unique = HashSet::<u32>::with_capacity(bytes.len().saturating_sub(2));
    for window in bytes.windows(3) {
        unique.insert(trigram_key(window[0], window[1], window[2]));
    }

    unique.into_iter().collect()
}

#[inline]
fn trigram_key(a: u8, b: u8, c: u8) -> u32 {
    (a as u32) | ((b as u32) << 8) | ((c as u32) << 16)
}

fn ensure_usn_journal(volume: &VolumeInfo) -> Result<()> {
    let handle = open_volume_handle(volume.letter)?;
    let mut journal = USN_JOURNAL_DATA_V0::default();
    let mut bytes = 0u32;

    let query = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_QUERY_USN_JOURNAL,
            None,
            0,
            Some((&mut journal as *mut USN_JOURNAL_DATA_V0).cast::<c_void>()),
            std::mem::size_of::<USN_JOURNAL_DATA_V0>() as u32,
            Some(&mut bytes),
            None,
        )
    };

    if query.is_ok() {
        let _ = unsafe { CloseHandle(handle) };
        return Ok(());
    }

    let err = unsafe { GetLastError() };
    if err != WIN32_ERROR(1179) {
        let _ = unsafe { CloseHandle(handle) };
        return Err(anyhow!("FSCTL_QUERY_USN_JOURNAL failed: {err:?}"));
    }

    let create = CREATE_USN_JOURNAL_DATA {
        MaximumSize: 256 * 1024 * 1024,
        AllocationDelta: 64 * 1024 * 1024,
    };

    let create_result = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_CREATE_USN_JOURNAL,
            Some((&create as *const CREATE_USN_JOURNAL_DATA).cast::<c_void>()),
            std::mem::size_of::<CREATE_USN_JOURNAL_DATA>() as u32,
            None,
            0,
            Some(&mut bytes),
            None,
        )
    };
    let _ = unsafe { CloseHandle(handle) };
    create_result.context("FSCTL_CREATE_USN_JOURNAL failed")?;
    Ok(())
}

struct UsnMonitor {
    volume: VolumeInfo,
    handle: windows::Win32::Foundation::HANDLE,
    next_usn: i64,
    journal_id: u64,
    tree: HashMap<u64, Node>,
}

impl UsnMonitor {
    fn new(volume: VolumeInfo, snapshot: VolumeSnapshot) -> Result<Self> {
        let handle = open_volume_handle(volume.letter)?;

        let mut journal = USN_JOURNAL_DATA_V0::default();
        let mut bytes = 0u32;
        unsafe {
            DeviceIoControl(
                handle,
                FSCTL_QUERY_USN_JOURNAL,
                None,
                0,
                Some((&mut journal as *mut USN_JOURNAL_DATA_V0).cast::<c_void>()),
                std::mem::size_of::<USN_JOURNAL_DATA_V0>() as u32,
                Some(&mut bytes),
                None,
            )
        }
        .context("query journal in monitor failed")?;

        Ok(Self {
            volume,
            handle,
            next_usn: journal.NextUsn,
            journal_id: journal.UsnJournalID,
            tree: snapshot.tree,
        })
    }

    fn read_changes(&mut self) -> Result<Vec<RawPath>> {
        let mut req = READ_USN_JOURNAL_DATA_V0 {
            StartUsn: self.next_usn,
            ReasonMask: u32::MAX,
            ReturnOnlyOnClose: 0,
            Timeout: 0,
            BytesToWaitFor: 1,
            UsnJournalID: self.journal_id,
        };

        let mut buf = vec![0u8; USN_BUFFER_SIZE];
        let mut ret = 0u32;
        unsafe {
            DeviceIoControl(
                self.handle,
                FSCTL_READ_USN_JOURNAL,
                Some((&mut req as *mut READ_USN_JOURNAL_DATA_V0).cast::<c_void>()),
                std::mem::size_of::<READ_USN_JOURNAL_DATA_V0>() as u32,
                Some(buf.as_mut_ptr().cast::<c_void>()),
                buf.len() as u32,
                Some(&mut ret),
                None,
            )
        }
        .context("read journal failed")?;

        if ret < 8 {
            return Ok(Vec::new());
        }

        let mut next = [0u8; 8];
        next.copy_from_slice(&buf[..8]);
        self.next_usn = i64::from_le_bytes(next);

        let mut out = Vec::new();
        let mut offset = 8usize;

        while offset + std::mem::size_of::<USN_RECORD_V2>() <= ret as usize {
            let rec = unsafe { &*(buf[offset..].as_ptr().cast::<USN_RECORD_V2>()) };
            if rec.RecordLength == 0 {
                break;
            }

            let len = rec.RecordLength as usize;
            if offset + len > ret as usize {
                break;
            }

            let reason = rec.Reason;
            let frn = rec.FileReferenceNumber;
            let parent = rec.ParentFileReferenceNumber;
            let name = usn_record_name(rec, &buf[offset..offset + len]);

            if let Some(name) = name {
                if (reason & (USN_REASON_FILE_DELETE | USN_REASON_RENAME_OLD_NAME)) != 0 {
                    self.tree.remove(&frn);
                } else if (reason
                    & (USN_REASON_FILE_CREATE | USN_REASON_RENAME_NEW_NAME | USN_REASON_CLOSE))
                    != 0
                {
                    let existing_flags = self.tree.get(&frn).map(|n| n.flags).unwrap_or_default();
                    self.tree.insert(
                        frn,
                        Node {
                            parent: Some(parent),
                            name,
                            flags: existing_flags,
                        },
                    );

                    if let Some(path) = build_full_path(self.volume.letter, frn, &self.tree) {
                        let flags = self.tree.get(&frn).map(|n| n.flags).unwrap_or_default();
                        out.push(RawPath {
                            lower_path: path.to_ascii_lowercase(),
                            path,
                            flags,
                        });
                    }
                }
            }

            offset += len;
        }

        if !out.is_empty() {
            xlog::info(format!(
                "read_changes volume {} produced {} path(s)",
                self.volume.letter,
                out.len()
            ));
        }

        Ok(out)
    }
}

impl Drop for UsnMonitor {
    fn drop(&mut self) {
        let _ = unsafe { CloseHandle(self.handle) };
    }
}

fn usn_record_name(record: &USN_RECORD_V2, rec: &[u8]) -> Option<String> {
    let off = record.FileNameOffset as usize;
    let len = record.FileNameLength as usize;
    if len == 0 || off + len > rec.len() || len % 2 != 0 {
        return None;
    }

    let wide = rec[off..off + len]
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect::<Vec<_>>();
    Some(String::from_utf16_lossy(&wide))
}

fn open_volume_handle(letter: char) -> Result<windows::Win32::Foundation::HANDLE> {
    use windows::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FILE_FLAG_BACKUP_SEMANTICS, FILE_GENERIC_READ, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };
    use windows::core::PCWSTR;

    let path = format!(r"\\.\{}:", letter);
    let mut wide = path.encode_utf16().collect::<Vec<u16>>();
    wide.push(0);

    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            FILE_GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            None,
        )
    }
    .with_context(|| format!("open volume failed: {}", letter))?;

    if handle == INVALID_HANDLE_VALUE {
        return Err(anyhow!("invalid handle for volume {letter}"));
    }

    Ok(handle)
}
