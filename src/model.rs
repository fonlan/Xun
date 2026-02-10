use crate::arena::SliceRef;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;

use once_cell::sync::Lazy;

const REGEX_QUERY_PREFIX: &str = "\u{1f}re\u{1f}";
const CASE_SENSITIVE_QUERY_PREFIX: &str = "\u{1f}cs\u{1f}";
const FULL_PATH_QUERY_PREFIX: &str = "\u{1f}fp\u{1f}";

const EXECUTABLE_EXTENSIONS: &[&str] = &["exe", "msi", "bat", "cmd", "com", "ps1", "vbs", "scr"];
const ARCHIVE_EXTENSIONS: &[&str] = &[
    "zip", "rar", "7z", "tar", "gz", "bz2", "xz", "tgz", "cab", "iso",
];
const TEXT_EXTENSIONS: &[&str] = &[
    "txt", "md", "markdown", "log", "ini", "cfg", "conf", "toml", "yaml", "yml", "json", "xml",
    "csv", "tsv", "rs", "py", "js", "ts", "jsx", "tsx", "java", "c", "cpp", "h", "hpp", "cs", "go",
    "php", "rb", "sh", "psm1",
];
const OFFICE_EXTENSIONS: &[&str] = &[
    "doc", "docx", "xls", "xlsx", "ppt", "pptx", "pdf", "wps", "et", "dps", "odt", "ods", "odp",
];
const AUDIO_EXTENSIONS: &[&str] = &[
    "mp3", "wav", "flac", "aac", "m4a", "ogg", "wma", "ape", "amr",
];
const VIDEO_EXTENSIONS: &[&str] = &[
    "mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "mpeg", "mpg", "ts",
];
const IMAGE_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "bmp", "webp", "svg", "ico", "tif", "tiff", "heic", "raw", "psd",
];

static EXTENSION_KIND_MAP: Lazy<HashMap<&'static str, FileTypeFilter>> =
    Lazy::new(build_extension_kind_map);

fn register_extensions(
    map: &mut HashMap<&'static str, FileTypeFilter>,
    kind: FileTypeFilter,
    extensions: &[&'static str],
) {
    for &ext in extensions {
        map.entry(ext).or_insert(kind);
    }
}

fn build_extension_kind_map() -> HashMap<&'static str, FileTypeFilter> {
    let mut map = HashMap::new();
    register_extensions(&mut map, FileTypeFilter::Executable, EXECUTABLE_EXTENSIONS);
    register_extensions(&mut map, FileTypeFilter::Archive, ARCHIVE_EXTENSIONS);
    register_extensions(&mut map, FileTypeFilter::Text, TEXT_EXTENSIONS);
    register_extensions(&mut map, FileTypeFilter::Office, OFFICE_EXTENSIONS);
    register_extensions(&mut map, FileTypeFilter::Audio, AUDIO_EXTENSIONS);
    register_extensions(&mut map, FileTypeFilter::Video, VIDEO_EXTENSIONS);
    register_extensions(&mut map, FileTypeFilter::Image, IMAGE_EXTENSIONS);
    map
}

#[derive(Clone, Copy, Debug)]
pub struct FileMeta {
    pub path: SliceRef,
    pub flags: FileFlags,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default)]
pub struct FileFlags {
    pub is_symlink: bool,
    pub is_hardlink: bool,
    pub is_placeholder: bool,
}

#[derive(Clone, Debug)]
pub struct SearchResult {
    pub path: String,
    pub kind: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SearchQueryMode {
    Wildcard,
    Regex,
}

pub fn encode_search_query_payload(
    query: &str,
    mode: SearchQueryMode,
    case_sensitive: bool,
    full_path: bool,
) -> String {
    let mut payload = String::new();
    if case_sensitive {
        payload.push_str(CASE_SENSITIVE_QUERY_PREFIX);
    }
    if full_path {
        payload.push_str(FULL_PATH_QUERY_PREFIX);
    }
    if mode == SearchQueryMode::Regex {
        payload.push_str(REGEX_QUERY_PREFIX);
    }
    payload.push_str(query);
    payload
}

pub fn decode_search_query_payload(payload: &str) -> (SearchQueryMode, bool, bool, &str) {
    let mut mode = SearchQueryMode::Wildcard;
    let mut case_sensitive = false;
    let mut full_path = false;
    let mut query = payload;

    loop {
        if let Some(stripped) = query.strip_prefix(CASE_SENSITIVE_QUERY_PREFIX) {
            case_sensitive = true;
            query = stripped;
            continue;
        }
        if let Some(stripped) = query.strip_prefix(FULL_PATH_QUERY_PREFIX) {
            full_path = true;
            query = stripped;
            continue;
        }
        if let Some(stripped) = query.strip_prefix(REGEX_QUERY_PREFIX) {
            mode = SearchQueryMode::Regex;
            query = stripped;
            continue;
        }
        break;
    }

    (mode, case_sensitive, full_path, query)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileTypeFilter {
    All,
    Executable,
    Archive,
    Text,
    Office,
    Audio,
    Video,
    Image,
    Other,
}

impl FileTypeFilter {
    pub fn from_index(index: i32) -> Self {
        match index {
            1 => Self::Executable,
            2 => Self::Archive,
            3 => Self::Text,
            4 => Self::Office,
            5 => Self::Audio,
            6 => Self::Video,
            7 => Self::Image,
            8 => Self::Other,
            _ => Self::All,
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Executable => "executable",
            Self::Archive => "archive",
            Self::Text => "text",
            Self::Office => "office",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Image => "image",
            Self::Other => "other",
        }
    }

    pub fn matches_kind(self, kind: &str) -> bool {
        matches!(self, Self::All) || kind == self.key()
    }
}

pub fn classify_path_kind(path: &str) -> &'static str {
    let Some(extension) = Path::new(path).extension().and_then(|ext| ext.to_str()) else {
        return FileTypeFilter::Other.key();
    };

    let ext_key = if extension.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(extension.to_ascii_lowercase())
    } else {
        Cow::Borrowed(extension)
    };

    EXTENSION_KIND_MAP
        .get(ext_key.as_ref())
        .copied()
        .unwrap_or(FileTypeFilter::Other)
        .key()
}
