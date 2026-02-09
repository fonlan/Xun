use std::ffi::OsStr;
use std::iter;
use std::os::windows::ffi::OsStrExt;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use windows::Win32::Foundation::{
    CloseHandle, ERROR_BROKEN_PIPE, ERROR_FILE_NOT_FOUND, ERROR_NO_DATA, ERROR_PIPE_NOT_CONNECTED,
    HANDLE, WIN32_ERROR,
};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_READ,
    FILE_SHARE_WRITE, OPEN_EXISTING, ReadFile, WriteFile,
};
use windows::Win32::System::Pipes::WaitNamedPipeW;
use windows::core::PCWSTR;

use crate::model::SearchResult;
use crate::xlog;

pub const SEARCH_PIPE_PATH: &str = r"\\.\pipe\xun.search.v1";

const SEARCH_REQUEST_OP: u8 = 1;
const SEARCH_RESPONSE_OK: u8 = 0;
const SEARCH_RESPONSE_ERROR: u8 = 1;
const FRAME_HEADER_BYTES: usize = 4;
const MAX_FRAME_BYTES: u32 = 8 * 1024 * 1024;
const PIPE_CONNECT_RETRY_TIMES: usize = 30;
const PIPE_CONNECT_RETRY_DELAY_MS: u64 = 120;
const PIPE_WAIT_TIMEOUT_MS: u32 = 1_000;
const SEARCH_RETRY_MAX_ATTEMPTS: usize = 3;
const SEARCH_RETRY_DELAY_MS: u64 = 40;

#[derive(Debug, Clone)]
pub struct SearchReply {
    pub index_len: usize,
    pub initial_index_ready: bool,
    pub results: Vec<SearchResult>,
}

pub struct IpcSession {
    handle: HANDLE,
}

impl Drop for IpcSession {
    fn drop(&mut self) {
        let _ = unsafe { CloseHandle(self.handle) };
    }
}

pub struct IpcClient {
    pipe_path_wide: Vec<u16>,
}

impl IpcClient {
    pub fn new() -> Self {
        Self {
            pipe_path_wide: to_wide(SEARCH_PIPE_PATH),
        }
    }

    pub fn search_with_session(
        &self,
        session: &mut Option<IpcSession>,
        query: &str,
        limit: usize,
    ) -> Result<SearchReply> {
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 1..=SEARCH_RETRY_MAX_ATTEMPTS {
            if session.is_none() {
                match self.connect_pipe() {
                    Ok(handle) => {
                        *session = Some(IpcSession { handle });
                    }
                    Err(err) => {
                        let retryable = is_retryable_pipe_error(&err);
                        let last_try = attempt == SEARCH_RETRY_MAX_ATTEMPTS;
                        if !retryable || last_try {
                            return Err(err);
                        }

                        xlog::warn(format!(
                            "ipc connect transient error, retrying attempt={}/{} query={:?}: {err:#}",
                            attempt, SEARCH_RETRY_MAX_ATTEMPTS, query
                        ));
                        last_error = Some(err);
                        thread::sleep(Duration::from_millis(SEARCH_RETRY_DELAY_MS));
                        continue;
                    }
                }
            }

            let handle = session
                .as_ref()
                .map(|s| s.handle)
                .ok_or_else(|| anyhow!("ipc session missing unexpectedly"))?;

            match self.search_once_on_handle(handle, query, limit) {
                Ok(reply) => {
                    if attempt > 1 {
                        xlog::info(format!(
                            "ipc search recovered after retry attempt={} query={:?}",
                            attempt, query
                        ));
                    }
                    return Ok(reply);
                }
                Err(err) => {
                    let retryable = is_retryable_pipe_error(&err);
                    if retryable {
                        *session = None;
                    }
                    let last_try = attempt == SEARCH_RETRY_MAX_ATTEMPTS;
                    if !retryable || last_try {
                        return Err(err);
                    }

                    xlog::warn(format!(
                        "ipc transient error, retrying attempt={}/{} query={:?}: {err:#}",
                        attempt, SEARCH_RETRY_MAX_ATTEMPTS, query
                    ));
                    last_error = Some(err);
                    thread::sleep(Duration::from_millis(SEARCH_RETRY_DELAY_MS));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("ipc search retry failed without details")))
    }

    pub fn check_initial_index_ready_quick(&self) -> Result<bool> {
        let opened = self.open_pipe_quick()?;
        let session = IpcSession { handle: opened };
        let reply = self
            .search_once_on_handle(session.handle, "", 1)
            .context("status probe search failed")?;
        Ok(reply.initial_index_ready)
    }

    fn search_once_on_handle(
        &self,
        pipe: HANDLE,
        query: &str,
        limit: usize,
    ) -> Result<SearchReply> {
        let request = IpcRequest::Search {
            query: query.to_string(),
            limit: limit.min(u32::MAX as usize) as u32,
        };
        let request_payload = encode_request(&request)?;

        let response_payload = (|| {
            write_framed(pipe, request_payload.as_slice())?;
            read_framed(pipe)
        })();

        let response = decode_response(response_payload?.as_slice())?;

        match response {
            IpcResponse::SearchOk {
                index_len,
                initial_index_ready,
                results,
            } => Ok(SearchReply {
                index_len: index_len as usize,
                initial_index_ready,
                results,
            }),
            IpcResponse::Error { message } => {
                Err(anyhow!("search service returned error: {message}"))
            }
        }
    }

    fn connect_pipe(&self) -> Result<HANDLE> {
        let mut last_error: Option<String> = None;

        for _ in 0..PIPE_CONNECT_RETRY_TIMES {
            let opened = unsafe {
                CreateFileW(
                    PCWSTR(self.pipe_path_wide.as_ptr()),
                    FILE_GENERIC_READ.0 | FILE_GENERIC_WRITE.0,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    None,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    None,
                )
            };

            match opened {
                Ok(handle) => return Ok(handle),
                Err(err) => {
                    last_error = Some(err.to_string());
                    let _ = unsafe {
                        WaitNamedPipeW(PCWSTR(self.pipe_path_wide.as_ptr()), PIPE_WAIT_TIMEOUT_MS)
                    };
                    thread::sleep(Duration::from_millis(PIPE_CONNECT_RETRY_DELAY_MS));
                }
            }
        }

        Err(anyhow!(
            "failed to connect search service pipe {}: {}",
            SEARCH_PIPE_PATH,
            last_error.unwrap_or_else(|| "unknown error".to_string())
        ))
    }

    fn open_pipe_quick(&self) -> Result<HANDLE> {
        let opened = unsafe {
            CreateFileW(
                PCWSTR(self.pipe_path_wide.as_ptr()),
                FILE_GENERIC_READ.0 | FILE_GENERIC_WRITE.0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                None,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                None,
            )
        }
        .context("CreateFileW failed")?;
        Ok(opened)
    }
}

#[derive(Debug)]
pub enum IpcRequest {
    Search { query: String, limit: u32 },
}

#[derive(Debug)]
pub enum IpcResponse {
    SearchOk {
        index_len: u64,
        initial_index_ready: bool,
        results: Vec<SearchResult>,
    },
    Error {
        message: String,
    },
}

pub fn encode_request(request: &IpcRequest) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(256);

    match request {
        IpcRequest::Search { query, limit } => {
            payload.push(SEARCH_REQUEST_OP);
            payload.extend_from_slice(limit.to_le_bytes().as_slice());
            push_string(&mut payload, query)?;
        }
    }

    Ok(payload)
}

pub fn decode_request(payload: &[u8]) -> Result<IpcRequest> {
    let mut cursor = 0usize;
    let op = read_u8(payload, &mut cursor)?;

    let request = match op {
        SEARCH_REQUEST_OP => {
            let limit = read_u32(payload, &mut cursor)?;
            let query = read_string(payload, &mut cursor)?;
            IpcRequest::Search { query, limit }
        }
        _ => {
            return Err(anyhow!("unsupported request opcode: {op}"));
        }
    };

    if cursor != payload.len() {
        return Err(anyhow!("request payload has trailing bytes"));
    }

    Ok(request)
}

pub fn encode_response(response: &IpcResponse) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(1024);

    match response {
        IpcResponse::SearchOk {
            index_len,
            initial_index_ready,
            results,
        } => {
            payload.push(SEARCH_RESPONSE_OK);
            payload.extend_from_slice(index_len.to_le_bytes().as_slice());
            payload.push(if *initial_index_ready { 1 } else { 0 });

            let count = u32::try_from(results.len())
                .map_err(|_| anyhow!("too many result rows: {}", results.len()))?;
            payload.extend_from_slice(count.to_le_bytes().as_slice());

            for row in results {
                push_string(&mut payload, row.kind.as_str())?;
                push_string(&mut payload, row.path.as_str())?;
            }
        }
        IpcResponse::Error { message } => {
            payload.push(SEARCH_RESPONSE_ERROR);
            push_string(&mut payload, message.as_str())?;
        }
    }

    Ok(payload)
}

pub fn decode_response(payload: &[u8]) -> Result<IpcResponse> {
    let mut cursor = 0usize;
    let status = read_u8(payload, &mut cursor)?;

    let response = match status {
        SEARCH_RESPONSE_OK => {
            let index_len = read_u64(payload, &mut cursor)?;
            let initial_index_ready = read_u8(payload, &mut cursor)? != 0;
            let count = read_u32(payload, &mut cursor)? as usize;
            let mut results = Vec::with_capacity(count);

            for _ in 0..count {
                let kind = read_string(payload, &mut cursor)?;
                let path = read_string(payload, &mut cursor)?;
                results.push(SearchResult { kind, path });
            }

            IpcResponse::SearchOk {
                index_len,
                initial_index_ready,
                results,
            }
        }
        SEARCH_RESPONSE_ERROR => {
            let message = read_string(payload, &mut cursor)?;
            IpcResponse::Error { message }
        }
        _ => {
            return Err(anyhow!("unsupported response opcode: {status}"));
        }
    };

    if cursor != payload.len() {
        return Err(anyhow!("response payload has trailing bytes"));
    }

    Ok(response)
}

pub fn read_framed(handle: HANDLE) -> Result<Vec<u8>> {
    let mut header = [0u8; FRAME_HEADER_BYTES];
    read_exact(handle, &mut header)?;

    let length = u32::from_le_bytes(header);
    if length == 0 {
        return Ok(Vec::new());
    }
    if length > MAX_FRAME_BYTES {
        return Err(anyhow!(
            "frame too large: {} bytes (max={MAX_FRAME_BYTES})",
            length
        ));
    }

    let mut payload = vec![0u8; length as usize];
    read_exact(handle, payload.as_mut_slice())?;
    Ok(payload)
}

pub fn write_framed(handle: HANDLE, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len())
        .map_err(|_| anyhow!("payload too large: {} bytes", payload.len()))?;

    let mut header_and_payload = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len());
    header_and_payload.extend_from_slice(length.to_le_bytes().as_slice());
    header_and_payload.extend_from_slice(payload);

    write_all(handle, header_and_payload.as_slice())
}

fn read_exact(handle: HANDLE, mut target: &mut [u8]) -> Result<()> {
    while !target.is_empty() {
        let mut read = 0u32;
        unsafe { ReadFile(handle, Some(target), Some(&mut read), None) }
            .context("ReadFile failed")?;

        if read == 0 {
            return Err(anyhow!("pipe closed before reading full frame"));
        }

        let advance = read as usize;
        if advance > target.len() {
            return Err(anyhow!("ReadFile returned invalid byte count"));
        }
        target = &mut target[advance..];
    }

    Ok(())
}

fn write_all(handle: HANDLE, mut source: &[u8]) -> Result<()> {
    while !source.is_empty() {
        let mut written = 0u32;
        unsafe { WriteFile(handle, Some(source), Some(&mut written), None) }
            .context("WriteFile failed")?;

        if written == 0 {
            return Err(anyhow!("pipe closed before writing full frame"));
        }

        let advance = written as usize;
        if advance > source.len() {
            return Err(anyhow!("WriteFile returned invalid byte count"));
        }
        source = &source[advance..];
    }

    Ok(())
}

fn push_string(payload: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| anyhow!("string too long for IPC frame: {} bytes", bytes.len()))?;
    payload.extend_from_slice(len.to_le_bytes().as_slice());
    payload.extend_from_slice(bytes);
    Ok(())
}

fn read_string(payload: &[u8], cursor: &mut usize) -> Result<String> {
    let len = read_u32(payload, cursor)? as usize;
    let bytes = read_bytes(payload, cursor, len)?;
    String::from_utf8(bytes.to_vec()).context("invalid utf-8 in IPC string")
}

fn read_u8(payload: &[u8], cursor: &mut usize) -> Result<u8> {
    let bytes = read_bytes(payload, cursor, 1)?;
    Ok(bytes[0])
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes = read_bytes(payload, cursor, 4)?;
    let mut chunk = [0u8; 4];
    chunk.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(chunk))
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> Result<u64> {
    let bytes = read_bytes(payload, cursor, 8)?;
    let mut chunk = [0u8; 8];
    chunk.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(chunk))
}

fn read_bytes<'a>(payload: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    if payload.len().saturating_sub(*cursor) < len {
        return Err(anyhow!("unexpected end of IPC payload"));
    }
    let start = *cursor;
    let end = start + len;
    *cursor = end;
    Ok(&payload[start..end])
}

fn to_wide(input: &str) -> Vec<u16> {
    OsStr::new(input)
        .encode_wide()
        .chain(iter::once(0))
        .collect()
}

pub fn is_service_unavailable_error(err: &anyhow::Error) -> bool {
    is_retryable_pipe_error(err)
}

fn is_retryable_pipe_error(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<windows::core::Error>())
        .filter_map(win32_error_from_windows_error)
        .any(|code| {
            code == ERROR_PIPE_NOT_CONNECTED
                || code == ERROR_BROKEN_PIPE
                || code == ERROR_NO_DATA
                || code == ERROR_FILE_NOT_FOUND
        })
}

fn win32_error_from_windows_error(err: &windows::core::Error) -> Option<WIN32_ERROR> {
    let code = err.code().0 as u32;
    if (code & 0xFFFF0000) == 0x80070000 {
        return Some(WIN32_ERROR(code & 0xFFFF));
    }
    None
}
