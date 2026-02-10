use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::ffi::c_void;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;
use regex::RegexBuilder;
use windows::Win32::Foundation::{CloseHandle, GetLastError, WIN32_ERROR};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::{
    CREATE_USN_JOURNAL_DATA, FSCTL_CREATE_USN_JOURNAL, FSCTL_ENUM_USN_DATA,
    FSCTL_QUERY_USN_JOURNAL, FSCTL_READ_USN_JOURNAL, MFT_ENUM_DATA_V0, READ_USN_JOURNAL_DATA_V0,
    USN_JOURNAL_DATA_V0, USN_REASON_CLOSE, USN_REASON_FILE_CREATE, USN_REASON_FILE_DELETE,
    USN_REASON_RENAME_NEW_NAME, USN_REASON_RENAME_OLD_NAME, USN_RECORD_V2,
};

use crate::arena::{ByteArena, SliceRef};
use crate::model::{
    FileFlags, FileMeta, SearchQueryMode, SearchResult, classify_path_kind,
    decode_search_query_payload,
};
use crate::win::{VolumeInfo, is_placeholder_attr, ntfs_volumes};
use crate::xlog;

const USN_BUFFER_SIZE: usize = 4 * 1024 * 1024;

#[derive(Default)]
struct TrieNode {
    children: Vec<(u8, u32)>,
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
            let child_lookup = self.nodes[node_idx]
                .children
                .binary_search_by_key(&ch, |(key, _)| *key);
            let next = match child_lookup {
                Ok(position) => self.nodes[node_idx].children[position].1,
                Err(position) => {
                    let created = self.nodes.len() as u32;
                    self.nodes.push(TrieNode::default());
                    self.nodes[node_idx]
                        .children
                        .insert(position, (ch, created));
                    created
                }
            };
            node_idx = next as usize;
            self.nodes[node_idx].postings.push(item_id);
        }
    }

    fn search_prefix(&self, key: &[u8], limit: usize) -> Vec<u32> {
        if key.is_empty() {
            return Vec::new();
        }

        let mut node_idx = 0usize;
        for &ch in key.iter().take(256) {
            let Ok(position) = self.nodes[node_idx]
                .children
                .binary_search_by_key(&ch, |(key, _)| *key)
            else {
                return Vec::new();
            };
            node_idx = self.nodes[node_idx].children[position].1 as usize;
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
    initial_index_ready: AtomicBool,
}

impl FileIndex {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(IndexInner::default())),
            initial_index_ready: AtomicBool::new(false),
        }
    }

    pub fn start_indexing(&self) -> Result<()> {
        self.initial_index_ready.store(false, Ordering::Release);
        xlog::info("start_indexing begin");
        let volumes = ntfs_volumes().context("failed to enumerate NTFS volumes")?;
        let mut monitor_seeds = Vec::<MonitorSeed>::new();
        xlog::info(format!("ntfs_volumes found {} volume(s)", volumes.len()));
        if !volumes.is_empty() {
            let letters = volumes
                .iter()
                .map(|v| v.letter.to_string())
                .collect::<Vec<_>>()
                .join(",");
            xlog::info(format!("volumes={letters}"));
        }

        {
            let mut indexed_count = 0usize;
            for volume in &volumes {
                let letter = volume.letter;
                let snapshot = match scan_volume_from_mft(volume) {
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
                {
                    let mut inserted_this_volume = 0usize;
                    let mut pending = Vec::with_capacity(4096);
                    let started = Instant::now();

                    let tree = &snapshot.tree;
                    let cacheable_nodes = tree
                        .values()
                        .filter_map(|node| node.parent)
                        .collect::<HashSet<u64>>();
                    let mut path_cache = HashMap::<u64, String>::with_capacity(
                        (cacheable_nodes.len() / 2).max(1_024),
                    );

                    for frn in tree.keys().copied() {
                        let Some(path) = build_full_path_cached(
                            letter,
                            frn,
                            tree,
                            &cacheable_nodes,
                            &mut path_cache,
                        ) else {
                            continue;
                        };
                        let flags = tree.get(&frn).map(|n| n.flags).unwrap_or_default();

                        pending.push(RawPath { path, flags });

                        if pending.len() >= 4096 {
                            let mut guard = self.inner.write();
                            for entry in pending.drain(..) {
                                push_entry(&mut guard, entry);
                                indexed_count += 1;
                                inserted_this_volume += 1;
                            }
                        }

                        if inserted_this_volume > 0 && inserted_this_volume.is_multiple_of(200_000)
                        {
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

                    {
                        let guard = self.inner.read();
                        xlog::info(format!(
                            "memory stats after volume {letter}: files={} trie_nodes={} trigrams={} seen={} arena_len_mb={} arena_cap_mb={}",
                            guard.files.len(),
                            guard.trie.nodes.len(),
                            guard.trigrams.len(),
                            guard.seen.len(),
                            guard.arena.len() / (1024 * 1024),
                            guard.arena.capacity() / (1024 * 1024)
                        ));
                    }
                }

                monitor_seeds.push(MonitorSeed {
                    volume: *volume,
                    snapshot,
                });
                xlog::info(format!("monitor seed prepared volume {letter}"));
            }

            let guard = self.inner.read();

            xlog::info(format!(
                "initial index ready: {} entries across {} NTFS volume(s)",
                indexed_count,
                volumes.len()
            ));
            xlog::info(format!(
                "inner stats: files={} trie_nodes={} trigrams={} seen={} arena_len_mb={} arena_cap_mb={}",
                guard.files.len(),
                guard.trie.nodes.len(),
                guard.trigrams.len(),
                guard.seen.len(),
                guard.arena.len() / (1024 * 1024),
                guard.arena.capacity() / (1024 * 1024)
            ));
        }

        xlog::info("spawning usn monitors");
        self.spawn_usn_monitors(monitor_seeds);
        self.initial_index_ready.store(true, Ordering::Release);
        xlog::info("start_indexing end");
        Ok(())
    }

    pub fn search(&self, query_payload: &str, limit: usize) -> Vec<SearchResult> {
        let (mode, case_sensitive, full_path, query) = decode_search_query_payload(query_payload);
        match mode {
            SearchQueryMode::Wildcard => {
                self.search_wildcard(query, limit, case_sensitive, full_path)
            }
            SearchQueryMode::Regex => self.search_regex(query, limit, case_sensitive, full_path),
        }
    }

    fn search_wildcard(
        &self,
        query: &str,
        limit: usize,
        case_sensitive: bool,
        full_path: bool,
    ) -> Vec<SearchResult> {
        if case_sensitive {
            let pattern = query.trim();
            if pattern.is_empty() {
                return Vec::new();
            }

            if has_wildcard_operator(pattern) {
                return self.search_wildcard_pattern_case_sensitive(pattern, limit, full_path);
            }

            return self.search_substring_case_sensitive(pattern, limit, full_path);
        }

        let normalized = query.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Vec::new();
        }

        if has_wildcard_operator(normalized.as_str()) {
            return self.search_wildcard_pattern(normalized.as_str(), limit, full_path);
        }

        self.search_substring(normalized.as_str(), limit, full_path)
    }

    fn search_regex(
        &self,
        query: &str,
        limit: usize,
        case_sensitive: bool,
        full_path: bool,
    ) -> Vec<SearchResult> {
        let pattern = query.trim();
        if pattern.is_empty() {
            return Vec::new();
        }

        let regex = match RegexBuilder::new(pattern)
            .case_insensitive(!case_sensitive)
            .build()
        {
            Ok(regex) => regex,
            Err(err) => {
                xlog::warn(format!(
                    "regex query compile failed query={:?}: {err}",
                    pattern
                ));
                return Vec::new();
            }
        };

        let guard = self.inner.read();
        let mut out = Vec::new();
        for meta in guard.files.iter().rev() {
            if out.len() >= limit {
                break;
            }

            if case_sensitive {
                let path = decode_slice(&guard.arena, meta.path);
                let file_name = basename(path.as_str()).unwrap_or(path.as_str());
                if !regex.is_match(file_name) && (full_path && !regex.is_match(path.as_str())) {
                    continue;
                }

                out.push(SearchResult {
                    kind: classify_path_kind(&path).to_string(),
                    path,
                });
            } else {
                let path = std::str::from_utf8(guard.arena.get(meta.path)).unwrap_or_default();
                let file_name = basename(path).unwrap_or(path);
                if !regex.is_match(file_name) && (full_path && !regex.is_match(path)) {
                    continue;
                }

                out.push(SearchResult {
                    kind: classify_path_kind(path).to_string(),
                    path: path.to_string(),
                });
            }
        }

        out
    }

    fn search_wildcard_pattern_case_sensitive(
        &self,
        pattern: &str,
        limit: usize,
        full_path: bool,
    ) -> Vec<SearchResult> {
        let pattern_bytes = pattern.as_bytes();
        let guard = self.inner.read();
        let mut out = Vec::new();

        for meta in guard.files.iter().rev() {
            if out.len() >= limit {
                break;
            }

            let path = decode_slice(&guard.arena, meta.path);
            let path_bytes = path.as_bytes();
            let file_name = basename_bytes(path_bytes).unwrap_or(path_bytes);
            if !wildcard_is_match(pattern_bytes, file_name)
                && (full_path && !wildcard_is_match(pattern_bytes, path_bytes))
            {
                continue;
            }

            out.push(SearchResult {
                kind: classify_path_kind(&path).to_string(),
                path,
            });
        }

        out
    }

    fn search_wildcard_pattern(
        &self,
        pattern: &str,
        limit: usize,
        full_path: bool,
    ) -> Vec<SearchResult> {
        let pattern_bytes = pattern.as_bytes();
        let guard = self.inner.read();
        let mut out = Vec::new();

        for meta in guard.files.iter().rev() {
            if out.len() >= limit {
                break;
            }

            let path = guard.arena.get(meta.path);
            let file_name = basename_bytes(path).unwrap_or(path);
            if !wildcard_is_match_case_insensitive(pattern_bytes, file_name)
                && (full_path && !wildcard_is_match_case_insensitive(pattern_bytes, path))
            {
                continue;
            }

            let path = decode_slice(&guard.arena, meta.path);
            out.push(SearchResult {
                kind: classify_path_kind(&path).to_string(),
                path,
            });
        }

        out
    }

    fn search_substring_case_sensitive(
        &self,
        query: &str,
        limit: usize,
        full_path: bool,
    ) -> Vec<SearchResult> {
        let needle = query.as_bytes();
        let guard = self.inner.read();
        let mut out = Vec::new();

        for meta in guard.files.iter().rev() {
            if out.len() >= limit {
                break;
            }

            let path = decode_slice(&guard.arena, meta.path);
            let path_bytes = path.as_bytes();
            let file_name = basename_bytes(path_bytes).unwrap_or(path_bytes);
            if !contains_subslice(file_name, needle)
                && (full_path && !contains_subslice(path_bytes, needle))
            {
                continue;
            }

            out.push(SearchResult {
                kind: classify_path_kind(&path).to_string(),
                path,
            });
        }

        out
    }

    fn search_substring(
        &self,
        normalized: &str,
        limit: usize,
        full_path: bool,
    ) -> Vec<SearchResult> {
        let needle = normalized.as_bytes();
        let needle_len = needle.len();
        let guard = self.inner.read();
        let mut out = Vec::new();
        let prefix_budget = if full_path {
            0
        } else if needle_len <= 1 {
            limit.saturating_mul(200)
        } else if needle_len == 2 {
            limit.saturating_mul(80)
        } else {
            limit.saturating_mul(16)
        };

        let mut seen = std::collections::HashSet::<u32>::new();
        if full_path {
            for meta in guard.files.iter().rev() {
                if out.len() >= limit {
                    break;
                }

                let path = guard.arena.get(meta.path);
                if !contains_subslice_case_insensitive(path, needle) {
                    continue;
                }

                let path = decode_slice(&guard.arena, meta.path);
                out.push(SearchResult {
                    kind: classify_path_kind(&path).to_string(),
                    path,
                });
            }
        } else {
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
                let path = guard.arena.get(meta.path);
                let file_name = basename_bytes(path).unwrap_or(path);
                if !contains_subslice_case_insensitive(file_name, needle) {
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
                            let path = guard.arena.get(meta.path);
                            let file_name = basename_bytes(path).unwrap_or(path);
                            if !contains_subslice_case_insensitive(file_name, needle) {
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
        }

        out
    }

    pub fn len(&self) -> usize {
        self.inner.read().files.len()
    }

    pub fn is_initial_index_ready(&self) -> bool {
        self.initial_index_ready.load(Ordering::Acquire)
    }

    fn spawn_usn_monitors(&self, seeds: Vec<MonitorSeed>) {
        let inner = self.inner.clone();

        thread::spawn(move || {
            xlog::info(format!("usn monitor thread start, seeds={}", seeds.len()));

            let mut monitors = seeds
                .into_iter()
                .filter_map(|seed| {
                    let letter = seed.volume.letter;
                    let monitor = match UsnMonitor::new(seed.volume, seed.snapshot) {
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

                    let mut guard = inner.write();
                    for entry in changes {
                        push_entry(&mut guard, entry);
                        total_changes += 1;
                    }
                }

                if tick.is_multiple_of(120) {
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
    flags: FileFlags,
}

struct MonitorSeed {
    volume: VolumeInfo,
    snapshot: VolumeSnapshot,
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
        if page == 1 || page.is_multiple_of(32) {
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
    cacheable_nodes: &HashSet<u64>,
    cache: &mut HashMap<u64, String>,
) -> Option<String> {
    if let Some(cached) = cache.get(&frn) {
        return Some(cached.clone());
    }

    let mut chain = Vec::<u64>::new();
    let mut cursor = Some(frn);
    let mut visited = HashSet::new();

    while let Some(current) = cursor {
        if !visited.insert(current) {
            break;
        }

        if let Some(cached) = cache.get(&current) {
            let mut full = cached.clone();
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

            if cacheable_nodes.contains(&frn) {
                cache.insert(frn, full.clone());
            }
            return Some(full);
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
        return None;
    }

    let full = format!("{}:\\{}", letter, parts.join("\\"));
    if cacheable_nodes.contains(&frn) {
        cache.insert(frn, full.clone());
    }
    Some(full)
}

fn push_entry(inner: &mut IndexInner, entry: RawPath) {
    let key_hash = hash_key_ascii_lower(entry.path.as_bytes());
    if let Some(existing_id) = inner.seen.get(&key_hash).copied()
        && let Some(existing_meta) = inner.files.get_mut(existing_id as usize)
        && eq_ascii_case_insensitive(inner.arena.get(existing_meta.path), entry.path.as_bytes())
    {
        let new_path = inner.arena.push(entry.path.as_bytes());
        existing_meta.path = new_path;
        existing_meta.flags = entry.flags;
        return;
    }

    let id = inner.files.len() as u32;
    let path_ref = inner.arena.push(entry.path.as_bytes());

    inner.files.push(FileMeta {
        path: path_ref,
        flags: entry.flags,
    });

    if let Some(name) = basename(&entry.path)
        && !name.is_empty()
    {
        let lowered = name
            .as_bytes()
            .iter()
            .map(|&byte| ascii_lower(byte))
            .collect::<Vec<_>>();
        inner.trie.insert(&lowered, id);

        for key in collect_trigram_keys(&lowered) {
            inner.trigrams.entry(key).or_default().push(id);
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

fn basename_bytes(path: &[u8]) -> Option<&[u8]> {
    let mut start = 0usize;
    for (index, ch) in path.iter().enumerate() {
        if matches!(*ch, b'\\' | b'/') {
            start = index + 1;
        }
    }
    path.get(start..)
}

fn has_wildcard_operator(query: &str) -> bool {
    query.as_bytes().iter().any(|ch| matches!(*ch, b'*' | b'?'))
}

fn wildcard_is_match(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    let mut pattern_index = 0usize;
    let mut text_index = 0usize;
    let mut star_index: Option<usize> = None;
    let mut star_text_index = 0usize;

    while text_index < text.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == text[text_index])
        {
            pattern_index += 1;
            text_index += 1;
            continue;
        }

        if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_text_index = text_index;
            continue;
        }

        if let Some(star_pos) = star_index {
            pattern_index = star_pos + 1;
            star_text_index += 1;
            text_index = star_text_index;
            continue;
        }

        return false;
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
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

fn ascii_lower(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + 32
    } else {
        byte
    }
}

fn wildcard_is_match_case_insensitive(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    let mut pattern_index = 0usize;
    let mut text_index = 0usize;
    let mut star_index: Option<usize> = None;
    let mut star_text_index = 0usize;

    while text_index < text.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?'
                || pattern[pattern_index] == ascii_lower(text[text_index]))
        {
            pattern_index += 1;
            text_index += 1;
            continue;
        }

        if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_text_index = text_index;
            continue;
        }

        if let Some(star_pos) = star_index {
            pattern_index = star_pos + 1;
            star_text_index += 1;
            text_index = star_text_index;
            continue;
        }

        return false;
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

fn contains_subslice_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }

    haystack.windows(needle.len()).any(|window| {
        window
            .iter()
            .zip(needle.iter())
            .all(|(&lhs, &rhs)| ascii_lower(lhs) == rhs)
    })
}

fn hash_key_ascii_lower(value: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for &byte in value {
        ascii_lower(byte).hash(&mut hasher);
    }
    hasher.finish()
}

fn eq_ascii_case_insensitive(lhs: &[u8], rhs: &[u8]) -> bool {
    lhs.len() == rhs.len()
        && lhs
            .iter()
            .zip(rhs.iter())
            .all(|(&left, &right)| ascii_lower(left) == ascii_lower(right))
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
                        out.push(RawPath { path, flags });
                    }
                }
            }

            offset += len;
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
    if len == 0 || off + len > rec.len() || !len.is_multiple_of(2) {
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
