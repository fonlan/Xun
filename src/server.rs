use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use anyhow::{Context, Result, anyhow};
use windows::Win32::Foundation::{
    CloseHandle, ERROR_BROKEN_PIPE, ERROR_FAILED_SERVICE_CONTROLLER_CONNECT, ERROR_NO_DATA,
    ERROR_PIPE_CONNECTED, ERROR_SERVICE_ALREADY_RUNNING, ERROR_SERVICE_DOES_NOT_EXIST,
    ERROR_SERVICE_EXISTS, ERROR_SERVICE_MARKED_FOR_DELETE, ERROR_SERVICE_NOT_ACTIVE, GetLastError,
    HANDLE, HLOCAL, LocalFree, NO_ERROR,
};
use windows::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ, OPEN_EXISTING, PIPE_ACCESS_DUPLEX,
};
use windows::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, DisconnectNamedPipe, PIPE_READMODE_BYTE,
    PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
};
use windows::Win32::System::Services::{
    ChangeServiceConfigW, CloseServiceHandle, ControlService, CreateServiceW, DeleteService,
    ENUM_SERVICE_TYPE, OpenSCManagerW, OpenServiceW, QueryServiceStatus,
    RegisterServiceCtrlHandlerExW, SC_MANAGER_ALL_ACCESS, SERVICE_ACCEPT_SHUTDOWN,
    SERVICE_ACCEPT_STOP, SERVICE_ALL_ACCESS, SERVICE_AUTO_START, SERVICE_CHANGE_CONFIG,
    SERVICE_CONTROL_SHUTDOWN, SERVICE_CONTROL_STOP, SERVICE_ERROR, SERVICE_ERROR_NORMAL,
    SERVICE_NO_CHANGE, SERVICE_QUERY_STATUS, SERVICE_RUNNING, SERVICE_START, SERVICE_START_PENDING,
    SERVICE_STATUS, SERVICE_STATUS_HANDLE, SERVICE_STOP_PENDING, SERVICE_STOPPED,
    SERVICE_TABLE_ENTRYW, SERVICE_WIN32_OWN_PROCESS, SetServiceStatus, StartServiceCtrlDispatcherW,
    StartServiceW,
};
use windows::core::{PCWSTR, PWSTR, w};

use crate::index::FileIndex;
use crate::ipc::{
    IpcRequest, IpcResponse, SEARCH_PIPE_PATH, decode_request, encode_response, read_framed,
    write_framed,
};
use crate::xlog;

const SERVICE_NAME: &str = "XunService";
const SERVICE_DISPLAY_NAME: &str = "Xun Service";
const PIPE_BUFFER_BYTES: u32 = 256 * 1024;

static SHOULD_STOP: AtomicBool = AtomicBool::new(false);
static SERVICE_STATUS_HANDLE_RAW: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub fn run_server_mode() -> Result<()> {
    xlog::info("server mode selected");

    match run_as_service_dispatcher() {
        Ok(()) => {
            xlog::info("service dispatcher exited");
            Ok(())
        }
        Err(err) => {
            let win_err = match extract_win32_error(&err) {
                Some(code) => code,
                None => unsafe { GetLastError() },
            };
            if win_err == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT {
                xlog::warn("not launched by SCM, running as foreground server process (--server)");
                run_engine_loop(None)
            } else {
                Err(err).context("StartServiceCtrlDispatcherW failed")
            }
        }
    }
}

pub fn install_and_start_current_service() -> Result<()> {
    let exe = std::env::current_exe().context("failed to resolve current executable")?;
    let exe_path = exe
        .canonicalize()
        .unwrap_or(exe)
        .to_string_lossy()
        .into_owned();
    let bin_path = format!("\"{exe_path}\" --server");

    let scm = unsafe { OpenSCManagerW(None, None, SC_MANAGER_ALL_ACCESS) }
        .context("OpenSCManagerW failed")?;

    let service_name = to_wide(SERVICE_NAME);
    let display_name = to_wide(SERVICE_DISPLAY_NAME);
    let bin_path_w = to_wide(bin_path.as_str());

    let service = match unsafe {
        CreateServiceW(
            scm,
            PCWSTR(service_name.as_ptr()),
            PCWSTR(display_name.as_ptr()),
            SERVICE_CHANGE_CONFIG | SERVICE_QUERY_STATUS | SERVICE_START,
            SERVICE_WIN32_OWN_PROCESS,
            SERVICE_AUTO_START,
            SERVICE_ERROR_NORMAL,
            PCWSTR(bin_path_w.as_ptr()),
            PCWSTR::null(),
            None,
            PCWSTR::null(),
            PCWSTR::null(),
            PCWSTR::null(),
        )
    } {
        Ok(handle) => {
            xlog::info("service created successfully");
            handle
        }
        Err(err) => {
            if unsafe { GetLastError() } == ERROR_SERVICE_EXISTS {
                let existing = unsafe {
                    OpenServiceW(
                        scm,
                        PCWSTR(service_name.as_ptr()),
                        SERVICE_CHANGE_CONFIG | SERVICE_QUERY_STATUS | SERVICE_START,
                    )
                }
                .context("OpenServiceW(existing service) failed")?;

                unsafe {
                    ChangeServiceConfigW(
                        existing,
                        ENUM_SERVICE_TYPE(SERVICE_NO_CHANGE),
                        SERVICE_AUTO_START,
                        SERVICE_ERROR(SERVICE_NO_CHANGE),
                        PCWSTR(bin_path_w.as_ptr()),
                        PCWSTR::null(),
                        None,
                        PCWSTR::null(),
                        PCWSTR::null(),
                        PCWSTR::null(),
                        PCWSTR(display_name.as_ptr()),
                    )
                }
                .context("ChangeServiceConfigW failed")?;
                xlog::info("service already existed, configuration updated");
                existing
            } else {
                unsafe {
                    let _ = CloseServiceHandle(scm);
                }
                return Err(err).context("CreateServiceW failed");
            }
        }
    };

    let start_res = unsafe { StartServiceW(service, None) };
    if let Err(err) = start_res {
        if unsafe { GetLastError() } != ERROR_SERVICE_ALREADY_RUNNING {
            unsafe {
                let _ = CloseServiceHandle(service);
                let _ = CloseServiceHandle(scm);
            }
            return Err(err).context("StartServiceW failed");
        }
        xlog::info("service already running");
    } else {
        xlog::info("service started successfully");
    }

    unsafe {
        let _ = CloseServiceHandle(service);
        let _ = CloseServiceHandle(scm);
    }
    Ok(())
}

pub fn start_current_service() -> Result<()> {
    let scm = unsafe { OpenSCManagerW(None, None, SC_MANAGER_ALL_ACCESS) }
        .context("OpenSCManagerW failed")?;

    let service_name = to_wide(SERVICE_NAME);
    let service =
        match unsafe { OpenServiceW(scm, PCWSTR(service_name.as_ptr()), SERVICE_ALL_ACCESS) } {
            Ok(handle) => handle,
            Err(err) => {
                unsafe {
                    let _ = CloseServiceHandle(scm);
                }
                return Err(err).context("OpenServiceW failed");
            }
        };

    unsafe {
        ChangeServiceConfigW(
            service,
            ENUM_SERVICE_TYPE(SERVICE_NO_CHANGE),
            SERVICE_AUTO_START,
            SERVICE_ERROR(SERVICE_NO_CHANGE),
            PCWSTR::null(),
            PCWSTR::null(),
            None,
            PCWSTR::null(),
            PCWSTR::null(),
            PCWSTR::null(),
            PCWSTR::null(),
        )
    }
    .context("ChangeServiceConfigW(auto-start) failed")?;
    xlog::info("service startup type set to auto");

    let start_res = unsafe { StartServiceW(service, None) };
    if let Err(err) = start_res {
        if unsafe { GetLastError() } != ERROR_SERVICE_ALREADY_RUNNING {
            unsafe {
                let _ = CloseServiceHandle(service);
                let _ = CloseServiceHandle(scm);
            }
            return Err(err).context("StartServiceW failed");
        }
        xlog::info("service already running");
    } else {
        xlog::info("service started successfully");
    }

    unsafe {
        let _ = CloseServiceHandle(service);
        let _ = CloseServiceHandle(scm);
    }
    Ok(())
}

pub fn stop_current_service() -> Result<()> {
    let scm = unsafe { OpenSCManagerW(None, None, SC_MANAGER_ALL_ACCESS) }
        .context("OpenSCManagerW failed")?;

    let service_name = to_wide(SERVICE_NAME);
    let service =
        match unsafe { OpenServiceW(scm, PCWSTR(service_name.as_ptr()), SERVICE_ALL_ACCESS) } {
            Ok(handle) => handle,
            Err(err) => {
                let code = unsafe { GetLastError() };
                unsafe {
                    let _ = CloseServiceHandle(scm);
                }
                if code == ERROR_SERVICE_DOES_NOT_EXIST {
                    xlog::info("service does not exist, nothing to stop");
                    return Ok(());
                }
                return Err(err).context("OpenServiceW failed");
            }
        };

    let mut service_status = SERVICE_STATUS::default();
    if let Err(err) = unsafe { ControlService(service, SERVICE_CONTROL_STOP, &mut service_status) }
    {
        if unsafe { GetLastError() } != ERROR_SERVICE_NOT_ACTIVE {
            unsafe {
                let _ = CloseServiceHandle(service);
                let _ = CloseServiceHandle(scm);
            }
            return Err(err).context("ControlService(stop) failed");
        }
        xlog::info("service already stopped");
    }

    for _ in 0..30 {
        let mut status = SERVICE_STATUS::default();
        if unsafe { QueryServiceStatus(service, &mut status) }.is_err() {
            break;
        }
        if status.dwCurrentState == SERVICE_STOPPED {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    unsafe {
        let _ = CloseServiceHandle(service);
        let _ = CloseServiceHandle(scm);
    }
    xlog::info("service stopped successfully");
    Ok(())
}

pub fn uninstall_current_service() -> Result<()> {
    let scm = unsafe { OpenSCManagerW(None, None, SC_MANAGER_ALL_ACCESS) }
        .context("OpenSCManagerW failed")?;

    let service_name = to_wide(SERVICE_NAME);
    let service =
        match unsafe { OpenServiceW(scm, PCWSTR(service_name.as_ptr()), SERVICE_ALL_ACCESS) } {
            Ok(handle) => handle,
            Err(err) => {
                let code = unsafe { GetLastError() };
                unsafe {
                    let _ = CloseServiceHandle(scm);
                }
                if code == ERROR_SERVICE_DOES_NOT_EXIST {
                    xlog::info("service does not exist, nothing to uninstall");
                    return Ok(());
                }
                return Err(err).context("OpenServiceW failed");
            }
        };

    let mut service_status = SERVICE_STATUS::default();
    if let Err(err) = unsafe { ControlService(service, SERVICE_CONTROL_STOP, &mut service_status) }
    {
        if unsafe { GetLastError() } != ERROR_SERVICE_NOT_ACTIVE {
            unsafe {
                let _ = CloseServiceHandle(service);
                let _ = CloseServiceHandle(scm);
            }
            return Err(err).context("ControlService(stop) failed");
        }
    }

    for _ in 0..30 {
        let mut status = SERVICE_STATUS::default();
        if unsafe { QueryServiceStatus(service, &mut status) }.is_err() {
            break;
        }
        if status.dwCurrentState == SERVICE_STOPPED {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    if let Err(err) = unsafe { DeleteService(service) } {
        let code = unsafe { GetLastError() };
        unsafe {
            let _ = CloseServiceHandle(service);
            let _ = CloseServiceHandle(scm);
        }
        if code == ERROR_SERVICE_MARKED_FOR_DELETE {
            xlog::info("service already marked for deletion");
            return Ok(());
        }
        return Err(err).context("DeleteService failed");
    }

    unsafe {
        let _ = CloseServiceHandle(service);
        let _ = CloseServiceHandle(scm);
    }
    xlog::info("service deleted successfully");
    Ok(())
}

fn run_as_service_dispatcher() -> Result<()> {
    let mut name_wide = to_wide(SERVICE_NAME);
    let table = [
        SERVICE_TABLE_ENTRYW {
            lpServiceName: PWSTR(name_wide.as_mut_ptr()),
            lpServiceProc: Some(service_main),
        },
        SERVICE_TABLE_ENTRYW::default(),
    ];

    unsafe { StartServiceCtrlDispatcherW(table.as_ptr()) }
        .context("StartServiceCtrlDispatcherW returned failure")
}

unsafe extern "system" fn service_main(_num_args: u32, _args: *mut PWSTR) {
    let run_result = (|| -> Result<()> {
        xlog::info("service_main entered");
        SHOULD_STOP.store(false, Ordering::Release);

        let status_handle = unsafe {
            RegisterServiceCtrlHandlerExW(w!("XunService"), Some(service_ctrl_handler), None)
        }
        .context("RegisterServiceCtrlHandlerExW failed")?;
        SERVICE_STATUS_HANDLE_RAW.store(status_handle.0 as usize, Ordering::Release);

        set_service_status(
            status_handle,
            SERVICE_START_PENDING,
            0,
            NO_ERROR.0,
            1,
            3_000,
        )?;

        set_service_status(
            status_handle,
            SERVICE_RUNNING,
            SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN,
            NO_ERROR.0,
            0,
            0,
        )?;

        let run_res = run_engine_loop(Some(status_handle));

        let exit_code = if run_res.is_ok() { NO_ERROR.0 } else { 1 };
        set_service_status(status_handle, SERVICE_STOPPED, 0, exit_code, 0, 0)?;

        run_res
    })();

    if let Err(err) = run_result {
        xlog::error(format!("service_main failed: {err:#}"));
        if let Some(handle) = read_service_status_handle() {
            let _ = set_service_status(handle, SERVICE_STOPPED, 0, 1, 0, 0);
        }
    }
}

unsafe extern "system" fn service_ctrl_handler(
    control: u32,
    _event_type: u32,
    _event_data: *mut c_void,
    _context: *mut c_void,
) -> u32 {
    match control {
        SERVICE_CONTROL_STOP | SERVICE_CONTROL_SHUTDOWN => {
            xlog::info(format!("service control stop/shutdown received: {control}"));
            SHOULD_STOP.store(true, Ordering::Release);
            wake_pipe_listener();

            if let Some(handle) = read_service_status_handle() {
                let _ = set_service_status(
                    handle,
                    SERVICE_STOP_PENDING,
                    SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN,
                    NO_ERROR.0,
                    1,
                    2_000,
                );
            }

            NO_ERROR.0
        }
        _ => NO_ERROR.0,
    }
}

fn read_service_status_handle() -> Option<SERVICE_STATUS_HANDLE> {
    let raw = SERVICE_STATUS_HANDLE_RAW.load(Ordering::Acquire);
    if raw == 0 {
        return None;
    }

    Some(SERVICE_STATUS_HANDLE(raw as *mut c_void))
}

fn run_engine_loop(service_handle: Option<SERVICE_STATUS_HANDLE>) -> Result<()> {
    SHOULD_STOP.store(false, Ordering::Release);

    let index = Arc::new(FileIndex::new());
    let index_for_build = index.clone();
    thread::Builder::new()
        .name("xun-server-indexing".to_string())
        .spawn(move || {
            xlog::info("server indexing start");
            if let Err(err) = index_for_build.start_indexing() {
                xlog::error(format!("server start_indexing failed: {err:#}"));
                return;
            }
            xlog::info(format!(
                "server indexing complete, entries={}",
                index_for_build.len()
            ));
        })
        .context("failed to spawn server indexing thread")?;

    if let Some(handle) = service_handle {
        let _ = set_service_status(
            handle,
            SERVICE_RUNNING,
            SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN,
            NO_ERROR.0,
            0,
            0,
        );
    }

    run_pipe_server(index)
}

fn run_pipe_server(index: Arc<FileIndex>) -> Result<()> {
    xlog::info(format!("named pipe server starting at {SEARCH_PIPE_PATH}"));

    while !SHOULD_STOP.load(Ordering::Acquire) {
        let pipe = create_pipe_instance().context("create pipe instance failed")?;

        if let Err(err) = unsafe { ConnectNamedPipe(pipe, None) } {
            let code = unsafe { GetLastError() };
            if code != ERROR_PIPE_CONNECTED {
                let _ = unsafe { CloseHandle(pipe) };
                xlog::warn(format!("ConnectNamedPipe failed: {err}; code={}", code.0));
                continue;
            }
        }

        if SHOULD_STOP.load(Ordering::Acquire) {
            let _ = unsafe { DisconnectNamedPipe(pipe) };
            let _ = unsafe { CloseHandle(pipe) };
            break;
        }

        if let Err(err) = handle_pipe_client(pipe, index.as_ref()) {
            xlog::warn(format!("pipe client session failed: {err:#}"));
        }

        let _ = unsafe { DisconnectNamedPipe(pipe) };
        let _ = unsafe { CloseHandle(pipe) };
    }

    xlog::info("named pipe server stopped");
    Ok(())
}

fn handle_pipe_client(pipe: HANDLE, index: &FileIndex) -> Result<()> {
    loop {
        let request_payload = match read_framed(pipe) {
            Ok(payload) => payload,
            Err(err) => {
                let code = unsafe { GetLastError() };
                if code == ERROR_BROKEN_PIPE || code == ERROR_NO_DATA {
                    return Ok(());
                }
                return Err(err).context("read request frame failed");
            }
        };

        let response = match decode_request(request_payload.as_slice()) {
            Ok(IpcRequest::Search { query, limit }) => {
                let normalized_limit = limit.clamp(1, 2_000) as usize;
                let results = index.search(query.as_str(), normalized_limit);
                IpcResponse::SearchOk {
                    index_len: index.len() as u64,
                    initial_index_ready: index.is_initial_index_ready(),
                    results,
                }
            }
            Err(err) => IpcResponse::Error {
                message: format!("invalid request: {err}"),
            },
        };

        let response_payload = encode_response(&response).context("encode response failed")?;
        if let Err(err) = write_framed(pipe, response_payload.as_slice()) {
            let code = unsafe { GetLastError() };
            if code == ERROR_BROKEN_PIPE || code == ERROR_NO_DATA {
                return Ok(());
            }
            return Err(err).context("write response frame failed");
        }

        if SHOULD_STOP.load(Ordering::Acquire) {
            return Ok(());
        }
    }
}

fn create_pipe_instance() -> Result<HANDLE> {
    let security = build_pipe_security_attributes()?;
    let open_mode = PIPE_ACCESS_DUPLEX;
    let pipe_mode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS;

    let handle = unsafe {
        CreateNamedPipeW(
            PCWSTR(security.pipe_name_wide.as_ptr()),
            open_mode,
            pipe_mode,
            PIPE_UNLIMITED_INSTANCES,
            PIPE_BUFFER_BYTES,
            PIPE_BUFFER_BYTES,
            0,
            Some(&security.attributes),
        )
    };

    if handle.is_invalid() {
        return Err(anyhow!(
            "CreateNamedPipeW failed: {}",
            unsafe { GetLastError() }.0
        ));
    }

    Ok(handle)
}

fn extract_win32_error(err: &anyhow::Error) -> Option<windows::Win32::Foundation::WIN32_ERROR> {
    let code = err.downcast_ref::<windows::core::Error>()?.code().0 as u32;
    if (code & 0xFFFF0000) == 0x80070000 {
        return Some(windows::Win32::Foundation::WIN32_ERROR(code & 0xFFFF));
    }
    None
}

fn wake_pipe_listener() {
    let pipe_name_wide = to_wide(SEARCH_PIPE_PATH);
    let _ = thread::Builder::new()
        .name("xun-pipe-waker".to_string())
        .spawn(move || {
            let opened = unsafe {
                CreateFileW(
                    PCWSTR(pipe_name_wide.as_ptr()),
                    FILE_GENERIC_READ.0,
                    Default::default(),
                    None,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    None,
                )
            };

            if let Ok(handle) = opened {
                let _ = unsafe { CloseHandle(handle) };
            }
        });
}

struct PipeSecurity {
    attributes: SECURITY_ATTRIBUTES,
    descriptor: PSECURITY_DESCRIPTOR,
    pipe_name_wide: Vec<u16>,
}

impl Drop for PipeSecurity {
    fn drop(&mut self) {
        if !self.descriptor.is_invalid() {
            let _ = unsafe { LocalFree(HLOCAL(self.descriptor.0)) };
        }
    }
}

fn build_pipe_security_attributes() -> Result<PipeSecurity> {
    let mut descriptor = PSECURITY_DESCRIPTOR::default();
    unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            w!("D:(A;;GA;;;SY)(A;;GRGW;;;BA)(A;;GRGW;;;IU)"),
            SDDL_REVISION_1,
            &mut descriptor,
            None,
        )
    }
    .context("build pipe security descriptor failed")?;

    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: false.into(),
    };

    Ok(PipeSecurity {
        attributes,
        descriptor,
        pipe_name_wide: to_wide(SEARCH_PIPE_PATH),
    })
}

fn set_service_status(
    handle: SERVICE_STATUS_HANDLE,
    current_state: windows::Win32::System::Services::SERVICE_STATUS_CURRENT_STATE,
    controls_accepted: u32,
    win32_exit_code: u32,
    checkpoint: u32,
    wait_hint: u32,
) -> Result<()> {
    let status = SERVICE_STATUS {
        dwServiceType: SERVICE_WIN32_OWN_PROCESS,
        dwCurrentState: current_state,
        dwControlsAccepted: controls_accepted,
        dwWin32ExitCode: win32_exit_code,
        dwServiceSpecificExitCode: 0,
        dwCheckPoint: checkpoint,
        dwWaitHint: wait_hint,
    };

    unsafe { SetServiceStatus(handle, &status) }.context("SetServiceStatus failed")
}

fn to_wide(value: &str) -> Vec<u16> {
    use std::ffi::OsStr;
    use std::iter;
    use std::os::windows::ffi::OsStrExt;

    OsStr::new(value)
        .encode_wide()
        .chain(iter::once(0))
        .collect()
}
