use std::ffi::OsStr;
use std::ffi::c_void;
use std::iter;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result, anyhow};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use slint::{Image, Rgba8Pixel, SharedPixelBuffer};
use windows::Win32::Foundation::{
    CloseHandle, ERROR_CANCELLED, ERROR_FILE_NOT_FOUND, ERROR_SUCCESS, GetLastError, HANDLE, HWND,
    LPARAM, LRESULT, POINT, RECT, WPARAM,
};
use windows::Win32::Graphics::Gdi::{
    BI_RGB, BITMAPINFO, BITMAPINFOHEADER, CreateCompatibleDC, CreateDIBSection, DIB_RGB_COLORS,
    DeleteDC, DeleteObject, GetMonitorInfoW, HDC, MONITOR_DEFAULTTONEAREST, MONITORINFO,
    MonitorFromPoint, SelectObject,
};
use windows::Win32::Security::{GetTokenInformation, TOKEN_ELEVATION, TOKEN_QUERY, TokenElevation};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_OFFLINE, GetLogicalDrives,
    GetVolumeInformationW,
};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::System::Registry::{
    HKEY, HKEY_CURRENT_USER, KEY_QUERY_VALUE, KEY_SET_VALUE, REG_OPTION_NON_VOLATILE,
    REG_SAM_FLAGS, REG_SZ, REG_VALUE_TYPE, RegCloseKey, RegCreateKeyExW, RegDeleteValueW,
    RegOpenKeyExW, RegQueryValueExW, RegSetValueExW,
};
use windows::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
use windows::Win32::UI::HiDpi::{GetDpiForMonitor, MDT_EFFECTIVE_DPI};
use windows::Win32::UI::Input::KeyboardAndMouse::{
    GetAsyncKeyState, MOD_ALT, MOD_NOREPEAT, RegisterHotKey, UnregisterHotKey, VK_LBUTTON, VK_SPACE,
};
use windows::Win32::UI::Shell::{
    NIF_ICON, NIF_MESSAGE, NIF_TIP, NIM_ADD, NIM_DELETE, NOTIFYICONDATAW, SHFILEINFOW, SHGFI_ICON,
    SHGFI_LARGEICON, SHGFI_SMALLICON, SHGFI_USEFILEATTRIBUTES, SHGetFileInfoW, Shell_NotifyIconW,
    ShellExecuteW,
};
use windows::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CS_HREDRAW, CS_VREDRAW, CW_USEDEFAULT, CreatePopupMenu, CreateWindowExW,
    DI_NORMAL, DefWindowProcW, DestroyIcon, DestroyMenu, DestroyWindow, DispatchMessageW,
    DrawIconEx, GetCursorPos, GetMessageW, GetSystemMetrics, HICON, IDC_ARROW, IDI_APPLICATION,
    LoadCursorW, LoadIconW, MF_CHECKED, MF_STRING, MSG, PostQuitMessage, RegisterClassW,
    SM_CXSCREEN, SM_CYSCREEN, SPI_GETWORKAREA, SW_SHOWNORMAL, SendMessageW, SetForegroundWindow,
    SystemParametersInfoW, TPM_RETURNCMD, TPM_RIGHTBUTTON, TrackPopupMenu, TranslateMessage,
    WM_APP, WM_COMMAND, WM_DESTROY, WM_HOTKEY, WM_LBUTTONUP, WM_RBUTTONUP, WNDCLASSW,
    WS_OVERLAPPED,
};
use windows::core::{PCWSTR, w};

use crate::xlog;

const WM_TRAYICON: u32 = WM_APP + 0x77;
const HOTKEY_ID: i32 = 0x5151;
const TRAY_ID: u32 = 0x9A11;
const MENU_INSTALL_SERVICE_ID: usize = 1000;
const MENU_UNINSTALL_SERVICE_ID: usize = 1001;
const MENU_START_SERVICE_ID: usize = 1002;
const MENU_STOP_SERVICE_ID: usize = 1003;
const MENU_CLIENT_AUTOSTART_ID: usize = 1004;
const MENU_EXIT_ID: usize = 1005;
const APP_ICON_RESOURCE_ID: usize = 1;
const RUN_REG_SUBKEY: &str = "Software\\Microsoft\\Windows\\CurrentVersion\\Run";
const CLIENT_AUTOSTART_VALUE_NAME: &str = "XunClient";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunAsLaunchOutcome {
    Started,
    Cancelled,
}

struct OwnedRegKey(HKEY);

impl Drop for OwnedRegKey {
    fn drop(&mut self) {
        unsafe {
            let _ = RegCloseKey(self.0);
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct VolumeInfo {
    pub letter: char,
    pub _serial: u32,
}

pub fn assert_elevated() -> Result<()> {
    let _ = is_elevated()?;
    Ok(())
}

pub fn is_elevated() -> Result<bool> {
    unsafe {
        let mut token = HANDLE::default();
        OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token)
            .context("OpenProcessToken failed")?;

        let mut elevation = TOKEN_ELEVATION::default();
        let mut ret_len = 0u32;

        let result = GetTokenInformation(
            token,
            TokenElevation,
            Some((&mut elevation as *mut TOKEN_ELEVATION).cast()),
            std::mem::size_of::<TOKEN_ELEVATION>() as u32,
            &mut ret_len,
        );

        let _ = CloseHandle(token);
        result.context("GetTokenInformation(TokenElevation) failed")?;

        Ok(elevation.TokenIsElevated != 0)
    }
}

pub fn execute_or_open(input: &str) -> Result<()> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    if Path::new(trimmed).exists() {
        return shell_execute_open(trimmed, None);
    }

    let cmd = "cmd.exe";
    let args = format!("/C {trimmed}");
    shell_execute_open(cmd, Some(&args))
}

pub fn load_file_icon_image(path: &str, size_px: u32) -> Option<Image> {
    if path.trim().is_empty() {
        return None;
    }

    let icon_size = size_px.clamp(16, 64) as i32;
    let path_w = to_wide(path);
    let is_dir = std::fs::metadata(path)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false);
    let file_attrs = if is_dir {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_NORMAL
    };

    let mut shfi = SHFILEINFOW::default();
    let mut flags = SHGFI_ICON;
    flags |= if icon_size <= 16 {
        SHGFI_SMALLICON
    } else {
        SHGFI_LARGEICON
    };

    if !Path::new(path).exists() {
        flags |= SHGFI_USEFILEATTRIBUTES;
    }

    let call_ok = unsafe {
        SHGetFileInfoW(
            PCWSTR(path_w.as_ptr()),
            file_attrs,
            Some(&mut shfi),
            std::mem::size_of::<SHFILEINFOW>() as u32,
            flags,
        )
    };
    if call_ok == 0 || shfi.hIcon.0.is_null() {
        return None;
    }

    let icon = shfi.hIcon;
    let image = hicon_to_slint_image(icon, icon_size);
    let _ = unsafe { DestroyIcon(icon) };
    image
}

fn hicon_to_slint_image(icon: HICON, size_px: i32) -> Option<Image> {
    unsafe {
        let dc = CreateCompatibleDC(HDC::default());
        if dc.0.is_null() {
            return None;
        }

        let bitmap_info = BITMAPINFO {
            bmiHeader: BITMAPINFOHEADER {
                biSize: std::mem::size_of::<BITMAPINFOHEADER>() as u32,
                biWidth: size_px,
                biHeight: -size_px,
                biPlanes: 1,
                biBitCount: 32,
                biCompression: BI_RGB.0,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut bits: *mut c_void = std::ptr::null_mut();
        let bitmap = match CreateDIBSection(dc, &bitmap_info, DIB_RGB_COLORS, &mut bits, None, 0) {
            Ok(bitmap) => bitmap,
            Err(_) => {
                let _ = DeleteDC(dc);
                return None;
            }
        };

        if bits.is_null() {
            let _ = DeleteObject(bitmap);
            let _ = DeleteDC(dc);
            return None;
        }

        let old_bitmap = SelectObject(dc, bitmap);
        let draw_ok = DrawIconEx(dc, 0, 0, icon, size_px, size_px, 0, None, DI_NORMAL).is_ok();

        let image = if draw_ok {
            let pixel_count = (size_px * size_px) as usize;
            let bgra = std::slice::from_raw_parts(bits.cast::<u8>(), pixel_count * 4);
            let mut rgba = vec![0u8; pixel_count * 4];
            for (source, target) in bgra.chunks_exact(4).zip(rgba.chunks_exact_mut(4)) {
                target[0] = source[2];
                target[1] = source[1];
                target[2] = source[0];
                target[3] = source[3];
            }

            let shared = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(
                rgba.as_slice(),
                size_px as u32,
                size_px as u32,
            );
            Some(Image::from_rgba8(shared))
        } else {
            None
        };

        if !old_bitmap.0.is_null() {
            let _ = SelectObject(dc, old_bitmap);
        }
        let _ = DeleteObject(bitmap);
        let _ = DeleteDC(dc);

        image
    }
}

fn shell_execute_open(target: &str, args: Option<&str>) -> Result<()> {
    let target_w = to_wide(target);
    let args_w = args.map(to_wide);

    unsafe {
        let ret = ShellExecuteW(
            None,
            w!("open"),
            PCWSTR(target_w.as_ptr()),
            args_w
                .as_ref()
                .map(|v| PCWSTR(v.as_ptr()))
                .unwrap_or(PCWSTR::null()),
            PCWSTR::null(),
            SW_SHOWNORMAL,
        );

        if ret.0 as isize <= 32 {
            return Err(anyhow!("ShellExecute open failed, code={:?}", ret.0));
        }
    }

    Ok(())
}

fn launch_install_service() -> Result<RunAsLaunchOutcome> {
    launch_elevated_with_arg("--install-service")
}

fn launch_uninstall_service() -> Result<RunAsLaunchOutcome> {
    launch_elevated_with_arg("--uninstall-service")
}

fn launch_start_service() -> Result<RunAsLaunchOutcome> {
    launch_elevated_with_arg("--start-service")
}

pub fn launch_start_service_elevated() -> Result<RunAsLaunchOutcome> {
    launch_start_service()
}

fn launch_stop_service() -> Result<RunAsLaunchOutcome> {
    launch_elevated_with_arg("--stop-service")
}

fn launch_elevated_with_arg(arg: &str) -> Result<RunAsLaunchOutcome> {
    let exe = std::env::current_exe().context("failed to resolve current executable")?;
    let exe_w = to_wide(exe.to_string_lossy().as_ref());
    let args_w = to_wide(arg);

    unsafe {
        let ret = ShellExecuteW(
            None,
            w!("runas"),
            PCWSTR(exe_w.as_ptr()),
            PCWSTR(args_w.as_ptr()),
            PCWSTR::null(),
            SW_SHOWNORMAL,
        );

        if ret.0 as isize > 32 {
            return Ok(RunAsLaunchOutcome::Started);
        }

        let last_error = GetLastError();
        if ret.0 as isize == 5 || last_error == ERROR_CANCELLED {
            return Ok(RunAsLaunchOutcome::Cancelled);
        }

        Err(anyhow!(
            "ShellExecute runas failed, code={:?}, last_error={}",
            ret.0,
            last_error.0
        ))
    }
}

fn client_autostart_command() -> Result<String> {
    let exe = std::env::current_exe().context("failed to resolve current executable")?;
    Ok(format!("\"{}\"", exe.to_string_lossy()))
}

fn open_run_key(sam_desired: REG_SAM_FLAGS) -> Result<OwnedRegKey> {
    let subkey_w = to_wide(RUN_REG_SUBKEY);

    unsafe {
        let mut key = HKEY::default();
        let status = RegCreateKeyExW(
            HKEY_CURRENT_USER,
            PCWSTR(subkey_w.as_ptr()),
            0,
            PCWSTR::null(),
            REG_OPTION_NON_VOLATILE,
            sam_desired,
            None,
            &mut key,
            None,
        );

        if status != ERROR_SUCCESS {
            return Err(anyhow!("RegCreateKeyExW(HKCU\\Run) failed: {}", status.0));
        }

        Ok(OwnedRegKey(key))
    }
}

fn read_run_value(name: &str) -> Result<Option<String>> {
    let subkey_w = to_wide(RUN_REG_SUBKEY);
    let name_w = to_wide(name);

    unsafe {
        let mut key = HKEY::default();
        let open_status = RegOpenKeyExW(
            HKEY_CURRENT_USER,
            PCWSTR(subkey_w.as_ptr()),
            0,
            KEY_QUERY_VALUE,
            &mut key,
        );

        if open_status == ERROR_FILE_NOT_FOUND {
            return Ok(None);
        }
        if open_status != ERROR_SUCCESS {
            return Err(anyhow!(
                "RegOpenKeyExW(HKCU\\Run) failed: {}",
                open_status.0
            ));
        }

        let key = OwnedRegKey(key);
        let mut value_type = REG_VALUE_TYPE(0);
        let mut value_size = 0u32;
        let query_size_status = RegQueryValueExW(
            key.0,
            PCWSTR(name_w.as_ptr()),
            None,
            Some(&mut value_type),
            None,
            Some(&mut value_size),
        );

        if query_size_status == ERROR_FILE_NOT_FOUND {
            return Ok(None);
        }
        if query_size_status != ERROR_SUCCESS {
            return Err(anyhow!(
                "RegQueryValueExW(size) failed for {}: {}",
                name,
                query_size_status.0
            ));
        }

        if value_type != REG_SZ || value_size < 2 || !value_size.is_multiple_of(2) {
            return Ok(None);
        }

        let mut raw = vec![0u8; value_size as usize];
        let query_value_status = RegQueryValueExW(
            key.0,
            PCWSTR(name_w.as_ptr()),
            None,
            Some(&mut value_type),
            Some(raw.as_mut_ptr()),
            Some(&mut value_size),
        );

        if query_value_status != ERROR_SUCCESS {
            return Err(anyhow!(
                "RegQueryValueExW(data) failed for {}: {}",
                name,
                query_value_status.0
            ));
        }

        if value_type != REG_SZ || value_size < 2 || !value_size.is_multiple_of(2) {
            return Ok(None);
        }

        let utf16_len = value_size as usize / 2;
        let utf16 = raw
            .chunks_exact(2)
            .take(utf16_len)
            .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
            .collect::<Vec<_>>();
        let value = utf16_to_string(&utf16);
        Ok(Some(value))
    }
}

fn is_client_autostart_enabled() -> Result<bool> {
    let expected = client_autostart_command()?;
    let current = read_run_value(CLIENT_AUTOSTART_VALUE_NAME)?;
    Ok(current
        .map(|value| value.eq_ignore_ascii_case(expected.as_str()))
        .unwrap_or(false))
}

fn set_client_autostart_enabled(enabled: bool) -> Result<()> {
    let key = open_run_key(REG_SAM_FLAGS(KEY_SET_VALUE.0 | KEY_QUERY_VALUE.0))?;
    let value_name_w = to_wide(CLIENT_AUTOSTART_VALUE_NAME);

    unsafe {
        if enabled {
            let cmd = client_autostart_command()?;
            let cmd_w = to_wide(cmd.as_str());
            let value_data = std::slice::from_raw_parts(
                cmd_w.as_ptr().cast::<u8>(),
                cmd_w.len() * std::mem::size_of::<u16>(),
            );

            let status = RegSetValueExW(
                key.0,
                PCWSTR(value_name_w.as_ptr()),
                0,
                REG_SZ,
                Some(value_data),
            );
            if status != ERROR_SUCCESS {
                return Err(anyhow!(
                    "RegSetValueExW(HKCU\\Run\\{}) failed: {}",
                    CLIENT_AUTOSTART_VALUE_NAME,
                    status.0
                ));
            }
        } else {
            let status = RegDeleteValueW(key.0, PCWSTR(value_name_w.as_ptr()));
            if status != ERROR_SUCCESS && status != ERROR_FILE_NOT_FOUND {
                return Err(anyhow!(
                    "RegDeleteValueW(HKCU\\Run\\{}) failed: {}",
                    CLIENT_AUTOSTART_VALUE_NAME,
                    status.0
                ));
            }
        }
    }

    Ok(())
}

fn toggle_client_autostart() -> Result<bool> {
    let enabled = is_client_autostart_enabled()?;
    let next = !enabled;
    set_client_autostart_enabled(next)?;
    Ok(next)
}

pub fn ntfs_volumes() -> Result<Vec<VolumeInfo>> {
    unsafe {
        let drives = GetLogicalDrives();
        if drives == 0 {
            return Err(anyhow!("GetLogicalDrives failed: {}", GetLastError().0));
        }

        let mut out = Vec::new();
        for idx in 0..26u32 {
            if (drives & (1 << idx)) == 0 {
                continue;
            }

            let letter = (b'A' + idx as u8) as char;
            let root = format!("{letter}:\\");
            let root_w = to_wide(&root);

            let mut vol_name = [0u16; 260];
            let mut fs_name = [0u16; 64];
            let mut serial = 0u32;
            let mut max_component = 0u32;
            let mut flags = 0u32;

            let ok = GetVolumeInformationW(
                PCWSTR(root_w.as_ptr()),
                Some(&mut vol_name),
                Some(&mut serial),
                Some(&mut max_component),
                Some(&mut flags),
                Some(&mut fs_name),
            );

            if ok.is_err() {
                continue;
            }

            let fs = utf16_to_string(&fs_name);
            if fs.eq_ignore_ascii_case("NTFS") {
                xlog::info(format!(
                    "ntfs volume detected letter={} serial={} fs={}",
                    letter, serial, fs
                ));
                out.push(VolumeInfo {
                    letter,
                    _serial: serial,
                });
            }
        }

        xlog::info(format!("ntfs_volumes returning {} volume(s)", out.len()));
        Ok(out)
    }
}

pub fn is_placeholder_attr(attributes: u32) -> bool {
    const FILE_ATTRIBUTE_RECALL_ON_OPEN: u32 = 0x0004_0000;
    const FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS: u32 = 0x0040_0000;
    attributes
        & (FILE_ATTRIBUTE_OFFLINE.0
            | FILE_ATTRIBUTE_RECALL_ON_OPEN
            | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS)
        != 0
}

pub fn launcher_popup_position(window_width: i32, window_height: i32) -> (i32, i32) {
    if let Some(cursor) = cursor_position()
        && let Some(work_area) = monitor_work_area_for_point(cursor)
    {
        return popup_position_in_rect(work_area, window_width, window_height);
    }

    unsafe {
        let mut work_area = RECT::default();
        let has_work_area = SystemParametersInfoW(
            SPI_GETWORKAREA,
            0,
            Some((&mut work_area as *mut RECT).cast()),
            Default::default(),
        )
        .is_ok();

        let rect = if has_work_area {
            work_area
        } else {
            RECT {
                left: 0,
                top: 0,
                right: GetSystemMetrics(SM_CXSCREEN),
                bottom: GetSystemMetrics(SM_CYSCREEN),
            }
        };

        popup_position_in_rect(rect, window_width, window_height)
    }
}

pub fn monitor_scale_factor_for_cursor() -> Option<f32> {
    unsafe {
        let (x, y) = cursor_position()?;
        let monitor = MonitorFromPoint(POINT { x, y }, MONITOR_DEFAULTTONEAREST);
        if monitor.0.is_null() {
            return None;
        }

        let mut dpi_x = 96u32;
        let mut dpi_y = 96u32;
        if GetDpiForMonitor(monitor, MDT_EFFECTIVE_DPI, &mut dpi_x, &mut dpi_y).is_ok() && dpi_x > 0
        {
            return Some((dpi_x as f32) / 96.0);
        }

        None
    }
}

pub fn cursor_position() -> Option<(i32, i32)> {
    unsafe {
        let mut point = POINT::default();
        if GetCursorPos(&mut point).is_ok() {
            Some((point.x, point.y))
        } else {
            None
        }
    }
}

pub fn is_left_mouse_button_down() -> bool {
    unsafe { (GetAsyncKeyState(VK_LBUTTON.0 as i32) as u16 & 0x8000) != 0 }
}

fn monitor_work_area_for_point((x, y): (i32, i32)) -> Option<RECT> {
    unsafe {
        let monitor = MonitorFromPoint(POINT { x, y }, MONITOR_DEFAULTTONEAREST);
        if monitor.0.is_null() {
            return None;
        }

        let mut info = MONITORINFO {
            cbSize: std::mem::size_of::<MONITORINFO>() as u32,
            ..Default::default()
        };

        if GetMonitorInfoW(monitor, &mut info as *mut MONITORINFO).as_bool() {
            Some(info.rcWork)
        } else {
            None
        }
    }
}

fn popup_position_in_rect(rect: RECT, window_width: i32, window_height: i32) -> (i32, i32) {
    let left = rect.left;
    let top = rect.top;
    let right = rect.right;
    let bottom = rect.bottom;

    let work_width = (right - left).max(0);
    let work_height = (bottom - top).max(0);

    let x = left + ((work_width - window_width).max(0) / 2);
    let centered_y = top + ((work_height - window_height).max(0) / 2);
    let y_offset = ((work_height as f32) * 0.12) as i32;
    let y = (centered_y - y_offset).max(top);

    (x, y)
}

type ActivateCallback = Arc<dyn Fn() + Send + Sync + 'static>;
type ExitCallback = Arc<dyn Fn() + Send + Sync + 'static>;

struct CallbackState {
    on_activate: ActivateCallback,
    on_exit: ExitCallback,
}

static CALLBACKS: Lazy<Mutex<Option<CallbackState>>> = Lazy::new(|| Mutex::new(None));

pub struct ShellBridge {
    _thread: thread::JoinHandle<()>,
}

impl ShellBridge {
    pub fn start(on_activate: ActivateCallback, on_exit: ExitCallback) -> Result<Self> {
        {
            let mut guard = CALLBACKS.lock();
            *guard = Some(CallbackState {
                on_activate,
                on_exit,
            });
        }

        let handle = thread::Builder::new()
            .name("xun-shell-bridge".to_string())
            .spawn(move || {
                xlog::info("shell bridge thread started");
                if let Err(err) = run_shell_loop() {
                    xlog::error(format!("shell bridge terminated: {err:#}"));
                }
            })
            .context("failed to spawn shell bridge thread")?;

        Ok(Self { _thread: handle })
    }
}

fn run_shell_loop() -> Result<()> {
    let class_name = w!("XunShellBridgeClass");
    let wc = WNDCLASSW {
        style: CS_HREDRAW | CS_VREDRAW,
        lpfnWndProc: Some(shell_wnd_proc),
        hCursor: unsafe { LoadCursorW(None, IDC_ARROW) }.context("LoadCursorW failed")?,
        lpszClassName: class_name,
        ..Default::default()
    };

    let class_atom = unsafe { RegisterClassW(&wc) };
    if class_atom == 0 {
        return Err(anyhow!(
            "RegisterClassW failed: {}",
            unsafe { GetLastError() }.0
        ));
    }

    let hwnd = unsafe {
        CreateWindowExW(
            Default::default(),
            class_name,
            w!("Xun Shell Bridge"),
            WS_OVERLAPPED,
            CW_USEDEFAULT,
            CW_USEDEFAULT,
            0,
            0,
            None,
            None,
            None,
            None,
        )
    }
    .context("CreateWindowExW failed")?;

    unsafe { RegisterHotKey(hwnd, HOTKEY_ID, MOD_ALT | MOD_NOREPEAT, VK_SPACE.0 as u32) }
        .context("RegisterHotKey(Alt+Space) failed")?;
    xlog::info("RegisterHotKey Alt+Space success");

    add_tray_icon(hwnd)?;
    xlog::info("tray icon added");

    let mut msg = MSG::default();
    while unsafe { GetMessageW(&mut msg, None, 0, 0) }.as_bool() {
        let _ = unsafe { TranslateMessage(&msg) };
        unsafe { DispatchMessageW(&msg) };
    }

    Ok(())
}

unsafe extern "system" fn shell_wnd_proc(
    hwnd: HWND,
    msg: u32,
    wparam: WPARAM,
    lparam: LPARAM,
) -> LRESULT {
    match msg {
        WM_HOTKEY => {
            if wparam.0 as i32 == HOTKEY_ID {
                xlog::info("WM_HOTKEY Alt+Space received");
                fire_activate();
            }
            LRESULT(0)
        }
        WM_TRAYICON => {
            match lparam.0 as u32 {
                WM_LBUTTONUP => fire_activate(),
                WM_RBUTTONUP => show_tray_menu(hwnd),
                _ => {}
            }
            LRESULT(0)
        }
        WM_COMMAND => {
            match loword(wparam.0 as u32) as usize {
                MENU_INSTALL_SERVICE_ID => match launch_install_service() {
                    Ok(RunAsLaunchOutcome::Started) => {}
                    Ok(RunAsLaunchOutcome::Cancelled) => {
                        xlog::warn("install service cancelled by user at UAC prompt")
                    }
                    Err(err) => {
                        xlog::error(format!("launch install service command failed: {err:#}"));
                    }
                },
                MENU_UNINSTALL_SERVICE_ID => match launch_uninstall_service() {
                    Ok(RunAsLaunchOutcome::Started) => {}
                    Ok(RunAsLaunchOutcome::Cancelled) => {
                        xlog::warn("uninstall service cancelled by user at UAC prompt")
                    }
                    Err(err) => {
                        xlog::error(format!("launch uninstall service command failed: {err:#}"));
                    }
                },
                MENU_START_SERVICE_ID => match launch_start_service() {
                    Ok(RunAsLaunchOutcome::Started) => {}
                    Ok(RunAsLaunchOutcome::Cancelled) => {
                        xlog::warn("start service cancelled by user at UAC prompt")
                    }
                    Err(err) => {
                        xlog::error(format!("launch start service command failed: {err:#}"));
                    }
                },
                MENU_STOP_SERVICE_ID => match launch_stop_service() {
                    Ok(RunAsLaunchOutcome::Started) => {}
                    Ok(RunAsLaunchOutcome::Cancelled) => {
                        xlog::warn("stop service cancelled by user at UAC prompt")
                    }
                    Err(err) => {
                        xlog::error(format!("launch stop service command failed: {err:#}"));
                    }
                },
                MENU_CLIENT_AUTOSTART_ID => match toggle_client_autostart() {
                    Ok(enabled) => {
                        xlog::info(format!("client autostart toggled, enabled={enabled}"));
                    }
                    Err(err) => {
                        xlog::error(format!("toggle client autostart failed: {err:#}"));
                    }
                },
                MENU_EXIT_ID => {
                    fire_exit();
                    let _ = unsafe { DestroyWindow(hwnd) };
                }
                _ => {}
            }
            LRESULT(0)
        }
        WM_DESTROY => {
            let _ = unsafe { UnregisterHotKey(hwnd, HOTKEY_ID) };
            let _ = remove_tray_icon(hwnd);
            unsafe { PostQuitMessage(0) };
            LRESULT(0)
        }
        _ => unsafe { DefWindowProcW(hwnd, msg, wparam, lparam) },
    }
}

fn show_tray_menu(hwnd: HWND) {
    let menu = match unsafe { CreatePopupMenu() } {
        Ok(m) => m,
        Err(_) => return,
    };

    let autostart_enabled = match is_client_autostart_enabled() {
        Ok(value) => value,
        Err(err) => {
            xlog::error(format!("read client autostart state failed: {err:#}"));
            false
        }
    };

    let _ = unsafe {
        AppendMenuW(
            menu,
            MF_STRING,
            MENU_INSTALL_SERVICE_ID,
            w!("\u{5B89}\u{88C5}\u{7CFB}\u{7EDF}\u{670D}\u{52A1}"),
        )
    };
    let _ = unsafe {
        AppendMenuW(
            menu,
            MF_STRING,
            MENU_UNINSTALL_SERVICE_ID,
            w!("\u{5378}\u{8F7D}\u{7CFB}\u{7EDF}\u{670D}\u{52A1}"),
        )
    };
    let _ = unsafe {
        AppendMenuW(
            menu,
            MF_STRING,
            MENU_START_SERVICE_ID,
            w!("\u{542F}\u{52A8}\u{7CFB}\u{7EDF}\u{670D}\u{52A1}"),
        )
    };
    let _ = unsafe {
        AppendMenuW(
            menu,
            MF_STRING,
            MENU_STOP_SERVICE_ID,
            w!("\u{505C}\u{6B62}\u{7CFB}\u{7EDF}\u{670D}\u{52A1}"),
        )
    };
    let autostart_flags = if autostart_enabled {
        MF_STRING | MF_CHECKED
    } else {
        MF_STRING
    };
    let _ = unsafe {
        AppendMenuW(
            menu,
            autostart_flags,
            MENU_CLIENT_AUTOSTART_ID,
            w!("\u{5F00}\u{673A}\u{542F}\u{52A8}\u{5BA2}\u{6237}\u{7AEF}"),
        )
    };
    let _ = unsafe { AppendMenuW(menu, MF_STRING, MENU_EXIT_ID, w!("\u{9000}\u{51FA} Xun")) };

    let mut point = POINT::default();
    let _ = unsafe { GetCursorPos(&mut point) };
    let _ = unsafe { SetForegroundWindow(hwnd) };

    let selected = unsafe {
        TrackPopupMenu(
            menu,
            TPM_RETURNCMD | TPM_RIGHTBUTTON,
            point.x,
            point.y,
            0,
            hwnd,
            Some(&RECT::default()),
        )
    };

    let selected_id = selected.0 as usize;
    if selected_id != 0 {
        let _ = unsafe { SendMessageW(hwnd, WM_COMMAND, WPARAM(selected_id), LPARAM(0)) };
    }

    let _ = unsafe { DestroyMenu(menu) };
}

fn add_tray_icon(hwnd: HWND) -> Result<()> {
    let tray_icon = match load_app_icon() {
        Ok(icon) => icon,
        Err(err) => {
            xlog::warn(format!(
                "load embedded app icon failed, fallback to default icon: {err:#}"
            ));
            unsafe { LoadIconW(None, IDI_APPLICATION) }
                .context("LoadIconW(IDI_APPLICATION) failed")?
        }
    };

    let mut data = NOTIFYICONDATAW {
        cbSize: std::mem::size_of::<NOTIFYICONDATAW>() as u32,
        hWnd: hwnd,
        uID: TRAY_ID,
        uFlags: NIF_MESSAGE | NIF_ICON | NIF_TIP,
        uCallbackMessage: WM_TRAYICON,
        hIcon: tray_icon,
        ..Default::default()
    };

    let tip = to_wide("Xun Launcher");
    let max = data.szTip.len().min(tip.len());
    data.szTip[..max].copy_from_slice(&tip[..max]);

    let ok = unsafe { Shell_NotifyIconW(NIM_ADD, &data) };
    if !ok.as_bool() {
        return Err(anyhow!("Shell_NotifyIconW(NIM_ADD) failed"));
    }

    Ok(())
}

fn load_app_icon() -> Result<HICON> {
    let module = unsafe { GetModuleHandleW(PCWSTR::null()) }.context("GetModuleHandleW failed")?;
    let icon_resource = PCWSTR(APP_ICON_RESOURCE_ID as *const u16);
    unsafe { LoadIconW(module, icon_resource) }.context("LoadIconW(app icon resource) failed")
}

fn remove_tray_icon(hwnd: HWND) -> Result<()> {
    let data = NOTIFYICONDATAW {
        cbSize: std::mem::size_of::<NOTIFYICONDATAW>() as u32,
        hWnd: hwnd,
        uID: TRAY_ID,
        ..Default::default()
    };

    let ok = unsafe { Shell_NotifyIconW(NIM_DELETE, &data) };
    if !ok.as_bool() {
        return Err(anyhow!("Shell_NotifyIconW(NIM_DELETE) failed"));
    }

    Ok(())
}

fn fire_activate() {
    if let Some(cb) = CALLBACKS
        .lock()
        .as_ref()
        .map(|state| state.on_activate.clone())
    {
        cb();
    }
}

fn fire_exit() {
    if let Some(cb) = CALLBACKS.lock().as_ref().map(|state| state.on_exit.clone()) {
        cb();
    }
}

fn to_wide(value: &str) -> Vec<u16> {
    OsStr::new(value)
        .encode_wide()
        .chain(iter::once(0))
        .collect()
}

fn utf16_to_string(buf: &[u16]) -> String {
    let end = buf.iter().position(|ch| *ch == 0).unwrap_or(buf.len());
    String::from_utf16_lossy(&buf[..end])
}

fn loword(value: u32) -> u16 {
    (value & 0xFFFF) as u16
}
