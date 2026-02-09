use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Local};
use parking_lot::Mutex;
use slint::winit_030::{EventResult as WinitEventResult, WinitWindowAccessor, winit};
use slint::{Image, Model, ModelRc, PhysicalPosition, Timer, TimerMode, VecModel};

use crate::ipc::{IpcClient, IpcSession, is_service_unavailable_error};
use crate::model::{FileTypeFilter, SearchResult};
use crate::win::{
    ShellBridge, cursor_position, execute_or_open, is_left_mouse_button_down,
    launcher_popup_position, load_file_icon_image, monitor_scale_factor_for_cursor,
};
use crate::xlog;

slint::include_modules!();

const RESULT_ROW_HEIGHT_PX: f32 = 38.0;
const QUERY_DEBOUNCE_ONE_CHAR_MS: u64 = 180;
const QUERY_DEBOUNCE_TWO_CHARS_MS: u64 = 130;
const QUERY_DEBOUNCE_THREE_TO_FOUR_CHARS_MS: u64 = 90;
const QUERY_DEBOUNCE_LONG_QUERY_MS: u64 = 60;
const RESULT_ICON_SIZE_PX: u32 = 24;
const MAX_RESULT_ICON_CACHE: usize = 4096;

thread_local! {
    static RESULT_ICON_CACHE: RefCell<HashMap<String, Image>> = RefCell::new(HashMap::new());
}

pub struct XunApp {
    _ui: AppWindow,
    _bridge: ShellBridge,
    _outside_click_timer: Timer,
}

impl XunApp {
    pub fn new() -> Result<Self> {
        let ui = AppWindow::new()?;
        let ipc_client = Arc::new(IpcClient::new());
        let query_seq = Arc::new(AtomicU64::new(0));
        let (query_tx, query_rx) = mpsc::channel::<(u64, String)>();
        let drag_state = Arc::new(Mutex::new(None::<(i32, i32, i32, i32)>));
        let result_paths = Arc::new(Mutex::new(Vec::<String>::new()));
        let all_results = Arc::new(Mutex::new(Vec::<SearchResult>::new()));
        let last_dispatched_query = Arc::new(Mutex::new(None::<String>));

        let weak_for_worker = ui.as_weak();
        let ipc_client_for_worker = ipc_client.clone();
        let seq_for_worker = query_seq.clone();
        let result_paths_for_worker = result_paths.clone();
        let all_results_for_worker = all_results.clone();
        thread::Builder::new()
            .name("xun-query-worker".to_string())
            .spawn(move || {
                xlog::info("query worker thread started");
                let mut ipc_session: Option<IpcSession> = None;

                while let Ok((mut seq, mut query_text)) = query_rx.recv() {
                    let mut debounce_deadline =
                        Instant::now() + query_debounce_duration(query_text.as_str());
                    loop {
                        let now = Instant::now();
                        if now >= debounce_deadline {
                            break;
                        }

                        match query_rx
                            .recv_timeout(debounce_deadline.saturating_duration_since(now))
                        {
                            Ok((new_seq, new_query)) => {
                                seq = new_seq;
                                query_text = new_query;
                                debounce_deadline =
                                    Instant::now() + query_debounce_duration(query_text.as_str());
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => break,
                            Err(mpsc::RecvTimeoutError::Disconnected) => {
                                xlog::warn("query worker thread exiting: channel closed");
                                return;
                            }
                        }
                    }

                    while let Ok((new_seq, new_query)) = query_rx.try_recv() {
                        seq = new_seq;
                        query_text = new_query;
                    }

                    let started = Instant::now();
                    let reply = ipc_client_for_worker.search_with_session(
                        &mut ipc_session,
                        query_text.as_str(),
                        300,
                    );
                    let elapsed_ms = started.elapsed().as_millis();
                    let (results, index_len, initial_index_ready, service_unavailable) = match reply
                    {
                        Ok(reply) => (
                            reply.results,
                            reply.index_len,
                            reply.initial_index_ready,
                            false,
                        ),
                        Err(err) => {
                            let service_unavailable = is_service_unavailable_error(&err);
                            xlog::warn(format!(
                                "query worker ipc failed seq={} query={:?}: {err:#}",
                                seq, query_text
                            ));
                            (Vec::new(), 0, false, service_unavailable)
                        }
                    };

                    xlog::info(format!(
                        "query worker done seq={} query={:?} matches={} elapsed_ms={} index_len={}",
                        seq,
                        query_text,
                        results.len(),
                        elapsed_ms,
                        index_len
                    ));

                    let weak_for_apply = weak_for_worker.clone();
                    let seq_guard = seq_for_worker.clone();
                    let result_paths_for_apply = result_paths_for_worker.clone();
                    let all_results_for_apply = all_results_for_worker.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if seq_guard.load(Ordering::Acquire) != seq {
                            xlog::info(format!(
                                "query drop stale seq={} query={:?}",
                                seq, query_text
                            ));
                            return;
                        }

                        let Some(window) = weak_for_apply.upgrade() else {
                            xlog::warn("query apply ignored: window dropped");
                            return;
                        };

                        window.set_service_unavailable(service_unavailable);
                        window.set_initial_index_ready(initial_index_ready);

                        {
                            let mut guard = all_results_for_apply.lock();
                            *guard = results;
                        }

                        let selected_filter =
                            FileTypeFilter::from_index(window.get_selected_filter_index());
                        let guard = all_results_for_apply.lock();
                        let row_count = apply_results_for_filter(
                            &window,
                            &guard,
                            selected_filter,
                            &result_paths_for_apply,
                        );
                        xlog::info(format!(
                            "set_results applied seq={} total={} shown={} filter={}",
                            seq,
                            guard.len(),
                            row_count,
                            selected_filter.key()
                        ));
                    });
                }

                xlog::warn("query worker thread exiting: channel closed");
            })?;

        let weak = ui.as_weak();
        let seq_for_query = query_seq.clone();
        let query_tx_for_ui = query_tx.clone();
        let result_paths_for_query_clear = result_paths.clone();
        let all_results_for_query_clear = all_results.clone();
        let last_dispatched_query_for_ui = last_dispatched_query.clone();
        ui.on_query_changed(move |query| {
            let query_text = query.to_string();

            let Some(window) = weak.upgrade() else {
                xlog::warn("query_changed ignored: window dropped before dispatch");
                return;
            };

            if query_text.trim().is_empty() {
                window.set_results(ModelRc::new(VecModel::default()));
                window.set_selected_index(-1);
                window.set_total_match_count(0);
                window.set_results_viewport_y(0.0);
                result_paths_for_query_clear.lock().clear();
                all_results_for_query_clear.lock().clear();
                clear_result_icon_cache();
                *last_dispatched_query_for_ui.lock() = None;
                xlog::info("query_changed empty: cleared results");
                return;
            }

            {
                let mut guard = last_dispatched_query_for_ui.lock();
                if guard.as_deref() == Some(query_text.as_str()) {
                    xlog::info(format!(
                        "query_changed duplicate ignored query={:?}",
                        query_text
                    ));
                    return;
                }
                *guard = Some(query_text.clone());
            }

            let seq = seq_for_query.fetch_add(1, Ordering::AcqRel) + 1;

            xlog::info(format!("query dispatch seq={} query={:?}", seq, query_text));

            if query_tx_for_ui.send((seq, query_text)).is_err() {
                xlog::error("query dispatch failed: worker channel closed");
                *last_dispatched_query_for_ui.lock() = None;
            }
        });

        let weak = ui.as_weak();
        let all_results_for_filter = all_results.clone();
        let result_paths_for_filter = result_paths.clone();
        ui.on_filter_selected(move |index| {
            let Some(window) = weak.upgrade() else {
                return;
            };

            let normalized_index = index.clamp(0, 8);
            window.set_selected_filter_index(normalized_index);

            let selected_filter = FileTypeFilter::from_index(normalized_index);
            let guard = all_results_for_filter.lock();
            let shown = apply_results_for_filter(
                &window,
                &guard,
                selected_filter,
                &result_paths_for_filter,
            );

            xlog::info(format!(
                "filter_selected index={} filter={} total={} shown={}",
                normalized_index,
                selected_filter.key(),
                guard.len(),
                shown
            ));
        });

        let weak = ui.as_weak();
        let result_paths_for_submit = result_paths.clone();
        ui.on_submit(move |text| {
            let text = text.to_string();
            let Some(window) = weak.upgrade() else {
                return;
            };

            let selected = window.get_selected_index();
            if selected >= 0 {
                let selected_path = result_paths_for_submit
                    .lock()
                    .get(selected as usize)
                    .cloned();
                if let Some(path) = selected_path {
                    xlog::info(format!(
                        "submit selected result index={} path={}",
                        selected, path
                    ));
                    let _ = execute_or_open(&path);
                    let _ = window.hide();
                    xlog::info("window hidden after selected submit");
                    return;
                }
            }

            if text.trim().is_empty() {
                xlog::info("submit ignored: empty input");
                return;
            }

            xlog::info(format!("submit input={:?}", text));
            let _ = execute_or_open(&text);
            let _ = window.hide();
            xlog::info("window hidden after submit");
        });

        let weak = ui.as_weak();
        ui.on_item_activated(move |path| {
            xlog::info(format!("item_activated path={}", path));
            let _ = execute_or_open(path.as_str());
            if let Some(window) = weak.upgrade() {
                let _ = window.hide();
                xlog::info("window hidden after item activation");
            }
        });

        let weak = ui.as_weak();
        ui.on_esc_pressed(move || {
            if let Some(window) = weak.upgrade() {
                let _ = window.hide();
                xlog::info("window hidden by Esc");
            }
        });

        let weak = ui.as_weak();
        ui.window().on_winit_window_event(move |_window, event| {
            if matches!(event, winit::event::WindowEvent::Focused(false))
                && let Some(window) = weak.upgrade()
                && window.window().is_visible()
            {
                let _ = window.hide();
                xlog::info("window hidden by focus loss");
            }

            WinitEventResult::Propagate
        });

        let outside_click_last_down = Arc::new(Mutex::new(false));
        let outside_click_timer = Timer::default();
        let weak = ui.as_weak();
        outside_click_timer.start(TimerMode::Repeated, Duration::from_millis(20), move || {
            let Some(window) = weak.upgrade() else {
                return;
            };

            if !window.window().is_visible() {
                *outside_click_last_down.lock() = false;
                return;
            }

            let is_down = is_left_mouse_button_down();
            let mut last_down = outside_click_last_down.lock();
            if *last_down || !is_down {
                *last_down = is_down;
                return;
            }
            *last_down = true;

            let Some((cursor_x, cursor_y)) = cursor_position() else {
                return;
            };

            let position = window.window().position();
            let size = window.window().size();
            let right = position.x + size.width as i32;
            let bottom = position.y + size.height as i32;
            let inside = cursor_x >= position.x
                && cursor_x < right
                && cursor_y >= position.y
                && cursor_y < bottom;

            if !inside {
                let _ = window.hide();
                xlog::info("window hidden by outside click");
            }
        });

        let weak = ui.as_weak();
        ui.on_selection_up(move || {
            let Some(window) = weak.upgrade() else {
                return;
            };
            let len = window.get_results().row_count() as i32;
            if len <= 0 {
                window.set_selected_index(-1);
                window.set_results_viewport_y(0.0);
                return;
            }

            let current = window.get_selected_index();
            let next = if current <= 0 { len - 1 } else { current - 1 };
            window.set_selected_index(next);
            scroll_selected_row_into_view(&window, next);
        });

        let weak = ui.as_weak();
        ui.on_selection_down(move || {
            let Some(window) = weak.upgrade() else {
                return;
            };
            let len = window.get_results().row_count() as i32;
            if len <= 0 {
                window.set_selected_index(-1);
                window.set_results_viewport_y(0.0);
                return;
            }

            let current = window.get_selected_index();
            let next = if current < 0 || current >= len - 1 {
                0
            } else {
                current + 1
            };
            window.set_selected_index(next);
            scroll_selected_row_into_view(&window, next);
        });

        let weak = ui.as_weak();
        let drag_state_for_start = drag_state.clone();
        ui.on_drag_window_start(move || {
            let Some(window) = weak.upgrade() else {
                return;
            };
            let pos = window.window().position();
            let Some((cursor_x, cursor_y)) = cursor_position() else {
                return;
            };
            *drag_state_for_start.lock() = Some((cursor_x, cursor_y, pos.x, pos.y));
        });

        let weak = ui.as_weak();
        let drag_state_for_move = drag_state.clone();
        ui.on_drag_window_move(move || {
            let Some(window) = weak.upgrade() else {
                return;
            };
            let Some((start_x, start_y, window_x, window_y)) = *drag_state_for_move.lock() else {
                return;
            };

            let Some((cursor_x, cursor_y)) = cursor_position() else {
                return;
            };
            let dx = cursor_x - start_x;
            let dy = cursor_y - start_y;
            window
                .window()
                .set_position(PhysicalPosition::new(window_x + dx, window_y + dy));
        });

        let weak = ui.as_weak();
        let ipc_client_for_activate = ipc_client.clone();
        let all_results_for_activate = all_results.clone();
        let activate: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let weak = weak.clone();
            let ipc_client_for_activate = ipc_client_for_activate.clone();
            let all_results_for_activate = all_results_for_activate.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(window) = weak.upgrade() {
                    xlog::info("activate callback invoked");
                    window.set_query("".into());
                    window.set_results(ModelRc::new(VecModel::default()));
                    window.set_selected_index(-1);
                    window.set_selected_filter_index(0);
                    window.set_total_match_count(0);
                    window.set_results_viewport_y(0.0);
                    all_results_for_activate.lock().clear();
                    clear_result_icon_cache();

                    match ipc_client_for_activate.check_initial_index_ready_quick() {
                        Ok(initial_index_ready) => {
                            window.set_service_unavailable(false);
                            window.set_initial_index_ready(initial_index_ready);
                            xlog::info(format!(
                                "activate status refreshed service_unavailable=false initial_index_ready={initial_index_ready}"
                            ));
                        }
                        Err(err) => {
                            window.set_service_unavailable(true);
                            window.set_initial_index_ready(false);
                            xlog::warn(format!(
                                "activate status refresh failed, mark service unavailable: {err:#}"
                            ));
                        }
                    }

                    show_and_focus_launcher_window(&window);
                    xlog::info("window shown and query reset");
                }
            });
        });

        let exit: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {
            let _ = slint::invoke_from_event_loop(|| {
                slint::quit_event_loop().ok();
            });
        });

        let bridge = ShellBridge::start(activate, exit)?;

        ui.set_results(ModelRc::new(VecModel::default()));
        ui.set_selected_filter_index(0);
        ui.set_total_match_count(0);
        match ipc_client.check_initial_index_ready_quick() {
            Ok(initial_index_ready) => {
                ui.set_service_unavailable(false);
                ui.set_initial_index_ready(initial_index_ready);
            }
            Err(_) => {
                ui.set_service_unavailable(true);
                ui.set_initial_index_ready(false);
            }
        }
        center_popup_window(&ui);
        ui.show()?;
        show_and_focus_launcher_window(&ui);

        Ok(Self {
            _ui: ui,
            _bridge: bridge,
            _outside_click_timer: outside_click_timer,
        })
    }

    pub fn run(&self) -> Result<()> {
        slint::run_event_loop_until_quit()?;
        Ok(())
    }
}

fn apply_results_for_filter(
    window: &AppWindow,
    all_results: &[SearchResult],
    selected_filter: FileTypeFilter,
    result_paths: &Arc<Mutex<Vec<String>>>,
) -> usize {
    window.set_total_match_count(all_results.len() as i32);

    let filtered = all_results
        .iter()
        .filter(|item| selected_filter.matches_kind(item.kind.as_str()))
        .map(|item| ResultRow {
            icon: icon_for_path(item.path.as_str()),
            path: item.path.clone().into(),
            meta: format_result_meta(item.path.as_str()).into(),
        })
        .collect::<Vec<_>>();

    let paths = filtered
        .iter()
        .map(|item| item.path.to_string())
        .collect::<Vec<_>>();

    let row_count = filtered.len();
    window.set_results(ModelRc::new(VecModel::from(filtered)));
    window.set_selected_index(if row_count > 0 { 0 } else { -1 });
    window.set_results_viewport_y(0.0);
    *result_paths.lock() = paths;

    row_count
}

fn icon_for_path(path: &str) -> Image {
    RESULT_ICON_CACHE.with(|cache_cell| {
        let mut cache = cache_cell.borrow_mut();
        if let Some(image) = cache.get(path) {
            return image.clone();
        }

        let image = load_file_icon_image(path, RESULT_ICON_SIZE_PX).unwrap_or_default();
        if cache.len() >= MAX_RESULT_ICON_CACHE {
            cache.clear();
        }
        cache.insert(path.to_string(), image.clone());
        image
    })
}

fn clear_result_icon_cache() {
    RESULT_ICON_CACHE.with(|cache_cell| {
        cache_cell.borrow_mut().clear();
    });
}

fn format_result_meta(path: &str) -> String {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(_) => return "未知大小 · 未知时间".to_string(),
    };

    let size_text = format_size(metadata.len());
    let modified_text = metadata
        .modified()
        .ok()
        .map(format_modified_time)
        .unwrap_or_else(|| "未知时间".to_string());

    format!("{size_text} · {modified_text}")
}

fn format_size(bytes: u64) -> String {
    const SIZE_UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];

    let mut value = bytes as f64;
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index < SIZE_UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, SIZE_UNITS[unit_index])
    } else {
        format!("{value:.1} {}", SIZE_UNITS[unit_index])
    }
}

fn format_modified_time(modified: std::time::SystemTime) -> String {
    let local_time: DateTime<Local> = modified.into();
    local_time.format("%Y-%m-%d %H:%M").to_string()
}

fn scroll_selected_row_into_view(window: &AppWindow, selected_index: i32) {
    if selected_index < 0 {
        return;
    }

    let total_rows = window.get_results().row_count() as i32;
    if total_rows <= 0 || selected_index >= total_rows {
        return;
    }

    let visible_height = window.get_results_visible_height();
    if visible_height <= 0.0 {
        return;
    }

    let mut viewport_y = window.get_results_viewport_y();
    let item_top = viewport_y + selected_index as f32 * RESULT_ROW_HEIGHT_PX;
    if item_top < 0.0 {
        viewport_y += -item_top;
    }

    let item_bottom = item_top + RESULT_ROW_HEIGHT_PX;
    if item_bottom > visible_height {
        viewport_y -= item_bottom - visible_height;
    }

    let content_height = total_rows as f32 * RESULT_ROW_HEIGHT_PX;
    let min_viewport_y = (visible_height - content_height).min(0.0);
    let clamped_viewport_y = viewport_y.clamp(min_viewport_y, 0.0);
    window.set_results_viewport_y(clamped_viewport_y);
}

fn center_popup_window(window: &AppWindow) {
    let physical_size = window.window().size();
    let scale = window.window().scale_factor().max(0.1);
    let monitor_scale = monitor_scale_factor_for_cursor().unwrap_or(scale).max(0.1);

    let mut win_width = physical_size.width as i32;
    let mut win_height = physical_size.height as i32;

    let logical_fallback_w = (window.get_popup_width() * monitor_scale).round() as i32;
    let logical_fallback_h = (window.get_popup_height() * monitor_scale).round() as i32;

    if win_width < logical_fallback_w {
        win_width = logical_fallback_w;
    }
    if win_height < logical_fallback_h {
        win_height = logical_fallback_h;
    }

    if win_width <= 0 || win_height <= 0 {
        xlog::warn(format!(
            "skip center_popup_window due to non-positive size width={} height={}",
            win_width, win_height
        ));
        return;
    }

    let (x, y) = launcher_popup_position(win_width, win_height);
    window.window().set_position(PhysicalPosition::new(x, y));
    xlog::info(format!(
        "center_popup_window physical_size=({}, {}) window_scale={} monitor_scale={} -> pos=({}, {})",
        win_width, win_height, scale, monitor_scale, x, y
    ));
}

fn show_and_focus_launcher_window(window: &AppWindow) {
    if !window.window().is_visible() {
        let _ = window.show();
    }
    center_popup_window(window);

    window.window().with_winit_window(|winit_window| {
        winit_window.focus_window();
    });
    window.invoke_focus_query_input();
}

fn query_debounce_duration(query: &str) -> Duration {
    let char_count = query.trim().chars().count();
    let millis = match char_count {
        0 | 1 => QUERY_DEBOUNCE_ONE_CHAR_MS,
        2 => QUERY_DEBOUNCE_TWO_CHARS_MS,
        3 | 4 => QUERY_DEBOUNCE_THREE_TO_FOUR_CHARS_MS,
        _ => QUERY_DEBOUNCE_LONG_QUERY_MS,
    };
    Duration::from_millis(millis)
}
