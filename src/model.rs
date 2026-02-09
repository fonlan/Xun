use crate::arena::SliceRef;
use std::path::Path;

#[derive(Clone, Copy, Debug)]
pub struct FileMeta {
    pub path: SliceRef,
    pub lower_path: SliceRef,
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
    let extension = Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());

    let Some(ext) = extension.as_deref() else {
        return FileTypeFilter::Other.key();
    };

    if matches!(
        ext,
        "exe" | "msi" | "bat" | "cmd" | "com" | "ps1" | "vbs" | "scr"
    ) {
        return FileTypeFilter::Executable.key();
    }
    if matches!(
        ext,
        "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" | "tgz" | "cab" | "iso"
    ) {
        return FileTypeFilter::Archive.key();
    }
    if matches!(
        ext,
        "txt"
            | "md"
            | "markdown"
            | "log"
            | "ini"
            | "cfg"
            | "conf"
            | "toml"
            | "yaml"
            | "yml"
            | "json"
            | "xml"
            | "csv"
            | "tsv"
            | "rs"
            | "py"
            | "js"
            | "ts"
            | "jsx"
            | "tsx"
            | "java"
            | "c"
            | "cpp"
            | "h"
            | "hpp"
            | "cs"
            | "go"
            | "php"
            | "rb"
            | "sh"
            | "psm1"
    ) {
        return FileTypeFilter::Text.key();
    }
    if matches!(
        ext,
        "doc"
            | "docx"
            | "xls"
            | "xlsx"
            | "ppt"
            | "pptx"
            | "pdf"
            | "wps"
            | "et"
            | "dps"
            | "odt"
            | "ods"
            | "odp"
    ) {
        return FileTypeFilter::Office.key();
    }
    if matches!(
        ext,
        "mp3" | "wav" | "flac" | "aac" | "m4a" | "ogg" | "wma" | "ape" | "amr"
    ) {
        return FileTypeFilter::Audio.key();
    }
    if matches!(
        ext,
        "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm" | "m4v" | "mpeg" | "mpg" | "ts"
    ) {
        return FileTypeFilter::Video.key();
    }
    if matches!(
        ext,
        "png"
            | "jpg"
            | "jpeg"
            | "gif"
            | "bmp"
            | "webp"
            | "svg"
            | "ico"
            | "tif"
            | "tiff"
            | "heic"
            | "raw"
            | "psd"
    ) {
        return FileTypeFilter::Image.key();
    }

    FileTypeFilter::Other.key()
}
