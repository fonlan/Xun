use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_to_string, write};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use parking_lot::Mutex;
use slint::winit_030::{EventResult as WinitEventResult, WinitWindowAccessor, winit};
use slint::{Image, Model, ModelRc, PhysicalPosition, Timer, TimerMode, VecModel};

use crate::ipc::{IpcClient, IpcSession, is_service_unavailable_error};
use crate::model::{FileTypeFilter, SearchQueryMode, SearchResult, encode_search_query_payload};
use crate::win::{
    ResultItemContextMenuOutcome, ShellBridge, cursor_position, execute_or_open,
    execute_result_item_context_menu_action, is_left_mouse_button_down, launcher_popup_position,
    load_file_icon_image, monitor_scale_factor_for_cursor, show_result_item_context_menu,
};
use crate::xlog;

slint::include_modules!();

const RESULT_ROW_HEIGHT_PX: f32 = 38.0;
const QUERY_DEBOUNCE_ONE_CHAR_MS: u64 = 180;
const QUERY_DEBOUNCE_TWO_CHARS_MS: u64 = 130;
const QUERY_DEBOUNCE_THREE_TO_FOUR_CHARS_MS: u64 = 90;
const QUERY_DEBOUNCE_LONG_QUERY_MS: u64 = 60;
const RESULT_ICON_LOGICAL_SIZE_PX: u32 = 24;
const RESULT_ICON_MIN_SOURCE_SIZE_PX: u32 = 32;
const RESULT_ROW_FILL_BATCH_SIZE: usize = 16;
const RESULT_ROW_FILL_INITIAL_BATCH_SIZE: usize = 8;
const RESULT_ROW_FILL_INTERVAL_MS: u64 = 12;
const SEARCHING_ANIMATION_INTERVAL_MS: u64 = 20;
const SEARCHING_ANIMATION_STEP: f32 = 0.03;
const STARTUP_SERVICE_CONNECT_RETRY_TIMES: usize = 20;
const STARTUP_SERVICE_CONNECT_RETRY_DELAY_MS: u64 = 250;
const MAX_RESULT_ICON_CACHE: usize = 4096;
const MAX_RESULT_META_CACHE: usize = 4096;
const OPENED_ITEM_HISTORY_FILE_NAME: &str = "opened-items.v1";
const MAX_OPENED_ITEM_HISTORY: usize = 2048;

thread_local! {
    static RESULT_ICON_CACHE: RefCell<HashMap<String, Image>> = RefCell::new(HashMap::new());
    static RESULT_META_CACHE: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    static RESULT_ROW_FILL_STATE: RefCell<ResultRowFillState> = RefCell::new(ResultRowFillState::default());
}

#[derive(Clone)]
struct OpenedItemRecord {
    path: String,
    last_opened_ms: u128,
}

#[derive(Clone)]
struct QueryDispatch {
    seq: u64,
    display_query: String,
    encoded_query: String,
    mode: SearchQueryMode,
}

#[derive(Default)]
struct ResultRowFillState {
    next_index: usize,
    icon_source_size_px: u32,
    paths: Vec<String>,
    model: Option<ModelRc<ResultRow>>,
}

type ResultRowBatch = (ModelRc<ResultRow>, u32, usize, Vec<String>, usize);

#[derive(Default)]
struct OpenedItemHistory {
    records: HashMap<String, OpenedItemRecord>,
}

impl OpenedItemHistory {
    fn load() -> Self {
        match Self::try_load() {
            Ok(history) => history,
            Err(err) => {
                xlog::warn(format!("load opened item history failed: {err:#}"));
                Self::default()
            }
        }
    }

    fn open_rank(&self, path: &str) -> u128 {
        let key = normalize_opened_item_key(path);
        self.records
            .get(key.as_str())
            .map(|record| record.last_opened_ms)
            .unwrap_or(0)
    }

    fn record_open(&mut self, path: &str) -> Result<()> {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return Ok(());
        }

        let key = normalize_opened_item_key(trimmed);
        self.records.insert(
            key,
            OpenedItemRecord {
                path: trimmed.to_string(),
                last_opened_ms: now_unix_millis(),
            },
        );
        self.persist()
    }

    fn try_load() -> Result<Self> {
        let path = opened_item_history_path();
        let content = match read_to_string(path.as_path()) {
            Ok(content) => content,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("read opened item history failed: {}", path.display())
                });
            }
        };

        let mut records: HashMap<String, OpenedItemRecord> = HashMap::new();
        for (line_index, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }

            let Some((timestamp_text, item_path)) = line.split_once('|') else {
                xlog::warn(format!(
                    "ignore malformed opened item history line {}",
                    line_index + 1
                ));
                continue;
            };

            let Ok(last_opened_ms) = timestamp_text.trim().parse::<u128>() else {
                xlog::warn(format!(
                    "ignore opened item history line {} due to invalid timestamp",
                    line_index + 1
                ));
                continue;
            };

            let item_path = item_path.trim();
            if item_path.is_empty() {
                continue;
            }

            let key = normalize_opened_item_key(item_path);
            match records.get(&key) {
                Some(existing) if existing.last_opened_ms >= last_opened_ms => {}
                _ => {
                    records.insert(
                        key,
                        OpenedItemRecord {
                            path: item_path.to_string(),
                            last_opened_ms,
                        },
                    );
                }
            }
        }

        let mut history = Self { records };
        history.trim_to_limit();
        Ok(history)
    }

    fn persist(&mut self) -> Result<()> {
        self.trim_to_limit();

        let path = opened_item_history_path();
        if let Some(parent) = path.parent() {
            create_dir_all(parent)
                .with_context(|| format!("create config dir failed: {}", parent.display()))?;
        }

        let mut records = self.records.values().cloned().collect::<Vec<_>>();
        records.sort_by(|left, right| right.last_opened_ms.cmp(&left.last_opened_ms));

        let mut serialized = String::new();
        for record in records {
            serialized.push_str(record.last_opened_ms.to_string().as_str());
            serialized.push('|');
            serialized.push_str(record.path.as_str());
            serialized.push('\n');
        }

        write(path.as_path(), serialized)
            .with_context(|| format!("write opened item history failed: {}", path.display()))
    }

    fn trim_to_limit(&mut self) {
        if self.records.len() <= MAX_OPENED_ITEM_HISTORY {
            return;
        }

        let mut records = self
            .records
            .drain()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        records.sort_by(|left, right| right.last_opened_ms.cmp(&left.last_opened_ms));
        records.truncate(MAX_OPENED_ITEM_HISTORY);

        for record in records {
            self.records
                .insert(normalize_opened_item_key(record.path.as_str()), record);
        }
    }
}

pub struct XunApp {
    _ui: AppWindow,
    _bridge: ShellBridge,
    _outside_click_timer: Timer,
    _result_row_fill_timer: Timer,
    _searching_indicator_timer: Timer,
}

impl XunApp {
    pub fn new(startup_service_starting: bool) -> Result<Self> {
        let ui = AppWindow::new()?;
        let ipc_client = Arc::new(IpcClient::new());
        let query_seq = Arc::new(AtomicU64::new(0));
        let (query_tx, query_rx) = mpsc::channel::<QueryDispatch>();
        let drag_state = Arc::new(Mutex::new(None::<(i32, i32, i32, i32)>));
        let result_paths = Arc::new(Mutex::new(Vec::<String>::new()));
        let all_results = Arc::new(Mutex::new(Vec::<SearchResult>::new()));
        let last_dispatched_query = Arc::new(Mutex::new(None::<(SearchQueryMode, String)>));
        let opened_item_history = Arc::new(Mutex::new(OpenedItemHistory::load()));

        let result_row_fill_timer = Timer::default();
        result_row_fill_timer.start(
            TimerMode::Repeated,
            Duration::from_millis(RESULT_ROW_FILL_INTERVAL_MS),
            move || {
                let _ = fill_result_rows_batch(RESULT_ROW_FILL_BATCH_SIZE);
            },
        );

        let weak_for_searching_animation = ui.as_weak();
        let searching_indicator_timer = Timer::default();
        searching_indicator_timer.start(
            TimerMode::Repeated,
            Duration::from_millis(SEARCHING_ANIMATION_INTERVAL_MS),
            move || {
                let Some(window) = weak_for_searching_animation.upgrade() else {
                    return;
                };

                if !window.get_searching() {
                    if window.get_searching_progress() != 0.0 {
                        window.set_searching_progress(0.0);
                    }
                    return;
                }

                let mut next = window.get_searching_progress() + SEARCHING_ANIMATION_STEP;
                if next >= 1.0 {
                    next -= 1.0;
                }
                window.set_searching_progress(next);
            },
        );

        let weak_for_worker = ui.as_weak();
        let ipc_client_for_worker = ipc_client.clone();
        let seq_for_worker = query_seq.clone();
        let result_paths_for_worker = result_paths.clone();
        let all_results_for_worker = all_results.clone();
        let opened_item_history_for_worker = opened_item_history.clone();
        thread::Builder::new()
            .name("xun-query-worker".to_string())
            .spawn(move || {
                xlog::info("query worker thread started");
                let mut ipc_session: Option<IpcSession> = None;

                while let Ok(mut dispatch) = query_rx.recv() {
                    let mut debounce_deadline =
                        Instant::now() + query_debounce_duration(dispatch.display_query.as_str());
                    loop {
                        let now = Instant::now();
                        if now >= debounce_deadline {
                            break;
                        }

                        match query_rx
                            .recv_timeout(debounce_deadline.saturating_duration_since(now))
                        {
                            Ok(new_dispatch) => {
                                dispatch = new_dispatch;
                                debounce_deadline =
                                    Instant::now() + query_debounce_duration(dispatch.display_query.as_str());
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => break,
                            Err(mpsc::RecvTimeoutError::Disconnected) => {
                                xlog::warn("query worker thread exiting: channel closed");
                                return;
                            }
                        }
                    }

                    while let Ok(new_dispatch) = query_rx.try_recv() {
                        dispatch = new_dispatch;
                    }

                    let started = Instant::now();
                    let reply = ipc_client_for_worker.search_with_session(
                        &mut ipc_session,
                        dispatch.encoded_query.as_str(),
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
                                "query worker ipc failed seq={} mode={} query={:?}: {err:#}",
                                dispatch.seq,
                                search_query_mode_label(dispatch.mode),
                                dispatch.display_query
                            ));
                            (Vec::new(), 0, false, service_unavailable)
                        }
                    };

                    xlog::info(format!(
                        "query worker done seq={} mode={} query={:?} matches={} elapsed_ms={} index_len={}",
                        dispatch.seq,
                        search_query_mode_label(dispatch.mode),
                        dispatch.display_query,
                        results.len(),
                        elapsed_ms,
                        index_len
                    ));

                    let weak_for_apply = weak_for_worker.clone();
                    let seq_guard = seq_for_worker.clone();
                    let result_paths_for_apply = result_paths_for_worker.clone();
                    let all_results_for_apply = all_results_for_worker.clone();
                    let opened_item_history_for_apply = opened_item_history_for_worker.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if seq_guard.load(Ordering::Acquire) != dispatch.seq {
                            xlog::info(format!(
                                "query drop stale seq={} mode={} query={:?}",
                                dispatch.seq,
                                search_query_mode_label(dispatch.mode),
                                dispatch.display_query
                            ));
                            return;
                        }

                        let Some(window) = weak_for_apply.upgrade() else {
                            xlog::warn("query apply ignored: window dropped");
                            return;
                        };

                        stop_searching_indicator(&window);
                        window.set_service_starting(false);
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
                            &opened_item_history_for_apply,
                        );
                        xlog::info(format!(
                            "set_results applied seq={} total={} shown={} filter={}",
                            dispatch.seq,
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
                stop_searching_indicator(&window);
                window.set_results(ModelRc::new(VecModel::default()));
                window.set_selected_index(-1);
                window.set_total_match_count(0);
                window.set_results_viewport_y(0.0);
                result_paths_for_query_clear.lock().clear();
                all_results_for_query_clear.lock().clear();
                clear_result_icon_cache();
                clear_result_meta_cache();
                clear_result_row_fill_state();
                *last_dispatched_query_for_ui.lock() = None;
                xlog::info("query_changed empty: cleared results");
                return;
            }

            let mode = if window.get_regex_mode() {
                SearchQueryMode::Regex
            } else {
                SearchQueryMode::Wildcard
            };

            {
                let mut guard = last_dispatched_query_for_ui.lock();
                if guard.as_ref() == Some(&(mode, query_text.clone())) {
                    xlog::info(format!(
                        "query_changed duplicate ignored mode={} query={:?}",
                        search_query_mode_label(mode),
                        query_text
                    ));
                    return;
                }
                *guard = Some((mode, query_text.clone()));
            }

            let seq = seq_for_query.fetch_add(1, Ordering::AcqRel) + 1;
            let dispatch = QueryDispatch {
                seq,
                display_query: query_text.clone(),
                encoded_query: encode_search_query_payload(query_text.as_str(), mode),
                mode,
            };

            xlog::info(format!(
                "query dispatch seq={} mode={} query={:?}",
                seq,
                search_query_mode_label(mode),
                query_text
            ));

            start_searching_indicator(&window);
            if query_tx_for_ui.send(dispatch).is_err() {
                xlog::error("query dispatch failed: worker channel closed");
                stop_searching_indicator(&window);
                *last_dispatched_query_for_ui.lock() = None;
            }
        });

        let weak = ui.as_weak();
        let seq_for_mode_toggle = query_seq.clone();
        let query_tx_for_mode_toggle = query_tx.clone();
        let last_dispatched_query_for_mode_toggle = last_dispatched_query.clone();
        ui.on_regex_mode_toggled(move |enabled| {
            let Some(window) = weak.upgrade() else {
                return;
            };

            let mode = if enabled {
                SearchQueryMode::Regex
            } else {
                SearchQueryMode::Wildcard
            };
            let query_text = window.get_query().to_string();
            xlog::info(format!(
                "query mode toggled mode={} query={:?}",
                search_query_mode_label(mode),
                query_text
            ));

            if query_text.trim().is_empty() {
                stop_searching_indicator(&window);
                *last_dispatched_query_for_mode_toggle.lock() = None;
                return;
            }

            {
                let mut guard = last_dispatched_query_for_mode_toggle.lock();
                if guard.as_ref() == Some(&(mode, query_text.clone())) {
                    return;
                }
                *guard = Some((mode, query_text.clone()));
            }

            let seq = seq_for_mode_toggle.fetch_add(1, Ordering::AcqRel) + 1;
            let dispatch = QueryDispatch {
                seq,
                display_query: query_text.clone(),
                encoded_query: encode_search_query_payload(query_text.as_str(), mode),
                mode,
            };

            xlog::info(format!(
                "query redispatch by mode toggle seq={} mode={} query={:?}",
                seq,
                search_query_mode_label(mode),
                query_text
            ));

            start_searching_indicator(&window);
            if query_tx_for_mode_toggle.send(dispatch).is_err() {
                xlog::error("query redispatch by mode toggle failed: worker channel closed");
                stop_searching_indicator(&window);
                *last_dispatched_query_for_mode_toggle.lock() = None;
            }
        });

        let weak = ui.as_weak();
        let all_results_for_filter = all_results.clone();
        let result_paths_for_filter = result_paths.clone();
        let opened_item_history_for_filter = opened_item_history.clone();
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
                &opened_item_history_for_filter,
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
        let opened_item_history_for_submit = opened_item_history.clone();
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
                    if let Err(err) = execute_or_open(&path) {
                        xlog::warn(format!(
                            "submit selected execute_or_open failed path={}: {err:#}",
                            path
                        ));
                    } else {
                        record_opened_item(&opened_item_history_for_submit, path.as_str());
                    }
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
            if let Err(err) = execute_or_open(&text) {
                xlog::warn(format!(
                    "submit execute_or_open failed input={:?}: {err:#}",
                    text
                ));
            } else {
                record_opened_item(&opened_item_history_for_submit, text.as_str());
            }
            let _ = window.hide();
            xlog::info("window hidden after submit");
        });

        let weak = ui.as_weak();
        let opened_item_history_for_activate = opened_item_history.clone();
        ui.on_item_activated(move |path| {
            xlog::info(format!("item_activated path={}", path));
            if let Err(err) = execute_or_open(path.as_str()) {
                xlog::warn(format!(
                    "item_activated execute_or_open failed path={}: {err:#}",
                    path
                ));
            } else {
                record_opened_item(&opened_item_history_for_activate, path.as_str());
            }
            if let Some(window) = weak.upgrade() {
                let _ = window.hide();
                xlog::info("window hidden after item activation");
            }
        });

        let weak = ui.as_weak();
        let result_paths_for_context_menu = result_paths.clone();
        let opened_item_history_for_context_menu = opened_item_history.clone();
        ui.on_item_context_menu_requested(move |index| {
            let Some(window) = weak.upgrade() else {
                return;
            };

            let normalized_index = index.max(0) as usize;
            let selected_path = result_paths_for_context_menu
                .lock()
                .get(normalized_index)
                .cloned();
            let Some(path) = selected_path else {
                xlog::warn(format!(
                    "item context menu ignored: index out of range index={index}"
                ));
                return;
            };

            match show_result_item_context_menu() {
                Ok(Some(action)) => {
                    xlog::info(format!(
                        "item context menu action selected index={} path={} action={:?}",
                        index, path, action
                    ));
                    match execute_result_item_context_menu_action(path.as_str(), action) {
                        Ok(ResultItemContextMenuOutcome::Completed) => {
                            let should_hide_window = matches!(
                                action,
                                crate::win::ResultItemContextMenuAction::Open
                                    | crate::win::ResultItemContextMenuAction::OpenAsAdmin
                                    | crate::win::ResultItemContextMenuAction::OpenDirectory
                            );
                            let should_record_opened_item = matches!(
                                action,
                                crate::win::ResultItemContextMenuAction::Open
                                    | crate::win::ResultItemContextMenuAction::OpenAsAdmin
                            );

                            if should_record_opened_item {
                                record_opened_item(
                                    &opened_item_history_for_context_menu,
                                    path.as_str(),
                                );
                            }

                            if should_hide_window {
                                let _ = window.hide();
                                xlog::info("window hidden after context menu action");
                            }
                        }
                        Ok(ResultItemContextMenuOutcome::CancelledByUser) => {
                            xlog::info(format!(
                                "item context menu action cancelled by user index={} path={} action={:?}",
                                index, path, action
                            ));
                        }
                        Err(err) => {
                            xlog::warn(format!(
                                "item context menu action failed index={} path={} action={:?}: {err:#}",
                                index, path, action
                            ));
                        }
                    }
                }
                Ok(None) => {
                    xlog::info(format!(
                        "item context menu dismissed index={} path={}",
                        index, path
                    ));
                }
                Err(err) => {
                    xlog::warn(format!(
                        "show item context menu failed index={} path={}: {err:#}",
                        index, path
                    ));
                }
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
                    stop_searching_indicator(&window);
                    window.set_regex_mode(false);
                    window.set_results(ModelRc::new(VecModel::default()));
                    window.set_selected_index(-1);
                    window.set_selected_filter_index(0);
                    window.set_total_match_count(0);
                    window.set_results_viewport_y(0.0);
                    all_results_for_activate.lock().clear();
                    clear_result_icon_cache();
                    clear_result_meta_cache();
                    clear_result_row_fill_state();

                    let previous_initial_index_ready = window.get_initial_index_ready();
                    match ipc_client_for_activate.check_initial_index_ready_quick() {
                        Ok(initial_index_ready) => {
                            window.set_service_starting(false);
                            window.set_service_unavailable(false);
                            window.set_initial_index_ready(initial_index_ready);
                            xlog::info(format!(
                                "activate status refreshed service_unavailable=false initial_index_ready={initial_index_ready}"
                            ));
                        }
                        Err(err) => {
                            let service_unavailable = is_service_unavailable_error(&err);
                            window.set_service_starting(false);
                            window.set_service_unavailable(service_unavailable);
                            if service_unavailable {
                                window.set_initial_index_ready(false);
                                xlog::warn(format!(
                                    "activate status refresh failed, mark service unavailable: {err:#}"
                                ));
                            } else {
                                window.set_initial_index_ready(previous_initial_index_ready);
                                xlog::warn(format!(
                                    "activate status refresh failed but service seems available, keep previous initial_index_ready={previous_initial_index_ready}: {err:#}"
                                ));
                            }
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
        ui.set_searching(false);
        ui.set_searching_progress(0.0);
        ui.set_selected_filter_index(0);
        ui.set_total_match_count(0);
        if startup_service_starting {
            ui.set_service_starting(true);
            ui.set_service_unavailable(false);
            ui.set_initial_index_ready(false);
        } else {
            match ipc_client.check_initial_index_ready_quick() {
                Ok(initial_index_ready) => {
                    ui.set_service_starting(false);
                    ui.set_service_unavailable(false);
                    ui.set_initial_index_ready(initial_index_ready);
                }
                Err(err) => {
                    let service_unavailable = is_service_unavailable_error(&err);
                    ui.set_service_starting(false);
                    ui.set_service_unavailable(service_unavailable);
                    ui.set_initial_index_ready(false);
                    xlog::warn(format!(
                        "startup status refresh failed service_unavailable={service_unavailable}: {err:#}"
                    ));
                }
            }
        }
        center_popup_window(&ui);
        ui.show()?;
        show_and_focus_launcher_window(&ui);

        if startup_service_starting {
            let weak_for_startup_probe = ui.as_weak();
            let ipc_client_for_startup_probe = ipc_client.clone();
            thread::Builder::new()
                .name("xun-startup-status-probe".to_string())
                .spawn(move || {
                    let (service_unavailable, initial_index_ready, delayed_err) =
                        match check_initial_index_ready_with_retry(
                            ipc_client_for_startup_probe.as_ref(),
                        ) {
                            Ok(initial_index_ready) => {
                                (false, Some(initial_index_ready), None::<String>)
                            }
                            Err(err) => {
                                let service_unavailable = is_service_unavailable_error(&err);
                                (service_unavailable, None, Some(format!("{err:#}")))
                            }
                        };

                    let _ = slint::invoke_from_event_loop(move || {
                        let Some(window) = weak_for_startup_probe.upgrade() else {
                            return;
                        };

                        window.set_service_starting(false);
                        window.set_service_unavailable(service_unavailable);

                        if let Some(initial_index_ready) = initial_index_ready {
                            window.set_initial_index_ready(initial_index_ready);
                        } else {
                            window.set_initial_index_ready(false);
                        }

                        if let Some(err) = delayed_err {
                            xlog::warn(format!(
                                "startup delayed status refresh failed service_unavailable={}: {err}",
                                service_unavailable
                            ));
                        }
                    });
                })
                .context("spawn startup status probe thread failed")?;
        }

        Ok(Self {
            _ui: ui,
            _bridge: bridge,
            _outside_click_timer: outside_click_timer,
            _result_row_fill_timer: result_row_fill_timer,
            _searching_indicator_timer: searching_indicator_timer,
        })
    }

    pub fn run(&self) -> Result<()> {
        slint::run_event_loop_until_quit()?;
        Ok(())
    }
}

fn check_initial_index_ready_with_retry(ipc_client: &IpcClient) -> Result<bool> {
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=STARTUP_SERVICE_CONNECT_RETRY_TIMES {
        match ipc_client.check_initial_index_ready_quick() {
            Ok(initial_index_ready) => {
                xlog::info(format!(
                    "startup status probe connected after retry attempt={attempt}/{STARTUP_SERVICE_CONNECT_RETRY_TIMES}"
                ));
                return Ok(initial_index_ready);
            }
            Err(err) => {
                let service_unavailable = is_service_unavailable_error(&err);
                xlog::warn(format!(
                    "startup status probe failed attempt={attempt}/{STARTUP_SERVICE_CONNECT_RETRY_TIMES} service_unavailable={service_unavailable}: {err:#}"
                ));

                if attempt < STARTUP_SERVICE_CONNECT_RETRY_TIMES {
                    thread::sleep(Duration::from_millis(
                        STARTUP_SERVICE_CONNECT_RETRY_DELAY_MS,
                    ));
                }
                last_error = Some(err);
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("startup status probe failed without details")))
}

fn apply_results_for_filter(
    window: &AppWindow,
    all_results: &[SearchResult],
    selected_filter: FileTypeFilter,
    result_paths: &Arc<Mutex<Vec<String>>>,
    opened_item_history: &Arc<Mutex<OpenedItemHistory>>,
) -> usize {
    window.set_total_match_count(all_results.len() as i32);

    let mut filtered = {
        let history = opened_item_history.lock();
        all_results
            .iter()
            .enumerate()
            .filter(|(_, item)| selected_filter.matches_kind(item.kind.as_str()))
            .map(|(source_index, item)| (history.open_rank(item.path.as_str()), source_index, item))
            .collect::<Vec<_>>()
    };

    filtered.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

    let icon_source_size_px = result_icon_source_size_for_window(window);
    let mut rows = Vec::with_capacity(filtered.len());
    let mut paths = Vec::with_capacity(filtered.len());
    for (_, _, item) in filtered {
        let path = item.path.clone();
        rows.push(ResultRow {
            icon: Image::default(),
            path: path.as_str().into(),
            meta: "".into(),
        });
        paths.push(path);
    }

    let row_count = rows.len();
    let model = ModelRc::new(VecModel::from(rows));
    window.set_results(model.clone());
    window.set_selected_index(if row_count > 0 { 0 } else { -1 });
    window.set_results_viewport_y(0.0);
    *result_paths.lock() = paths.clone();

    RESULT_ROW_FILL_STATE.with(|state_cell| {
        let mut state = state_cell.borrow_mut();
        state.next_index = 0;
        state.icon_source_size_px = icon_source_size_px;
        state.paths = paths;
        state.model = Some(model);
    });
    let _ = fill_result_rows_batch(RESULT_ROW_FILL_INITIAL_BATCH_SIZE);

    row_count
}

fn fill_result_rows_batch(batch_size: usize) -> usize {
    let Some((model, icon_source_size_px, start_index, batch_paths, total_count)) =
        next_result_row_batch(batch_size)
    else {
        return 0;
    };

    let batch_count = batch_paths.len();
    for (offset, path) in batch_paths.iter().enumerate() {
        model.set_row_data(
            start_index + offset,
            ResultRow {
                icon: icon_for_path(path.as_str(), icon_source_size_px),
                path: path.as_str().into(),
                meta: meta_for_path(path.as_str()).into(),
            },
        );
    }

    let next_index = start_index + batch_count;
    if next_index >= total_count {
        xlog::info(format!("result rows fully rendered count={total_count}"));
    }

    batch_count
}

fn next_result_row_batch(batch_size: usize) -> Option<ResultRowBatch> {
    if batch_size == 0 {
        return None;
    }

    RESULT_ROW_FILL_STATE.with(|state_cell| {
        let mut state = state_cell.borrow_mut();
        let model = state.model.clone()?;
        if state.next_index >= state.paths.len() {
            return None;
        }

        let start_index = state.next_index;
        let end_index = (start_index + batch_size).min(state.paths.len());
        state.next_index = end_index;

        let batch_paths = state.paths[start_index..end_index].to_vec();
        Some((
            model,
            state.icon_source_size_px,
            start_index,
            batch_paths,
            state.paths.len(),
        ))
    })
}

fn clear_result_row_fill_state() {
    RESULT_ROW_FILL_STATE.with(|state_cell| {
        let mut state = state_cell.borrow_mut();
        state.next_index = 0;
        state.icon_source_size_px = 0;
        state.paths.clear();
        state.model = None;
    });
}

fn record_opened_item(opened_item_history: &Arc<Mutex<OpenedItemHistory>>, path: &str) {
    let mut history = opened_item_history.lock();
    if let Err(err) = history.record_open(path) {
        xlog::warn(format!("record opened item failed path={}: {err:#}", path));
    }
}

fn opened_item_history_path() -> PathBuf {
    std::env::var_os("APPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("Xun")
        .join("config")
        .join(OPENED_ITEM_HISTORY_FILE_NAME)
}

fn normalize_opened_item_key(path: &str) -> String {
    path.trim().replace('/', "\\").to_ascii_lowercase()
}

fn now_unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn icon_for_path(path: &str, icon_source_size_px: u32) -> Image {
    RESULT_ICON_CACHE.with(|cache_cell| {
        let mut cache = cache_cell.borrow_mut();
        let cache_key = result_icon_cache_key(path, icon_source_size_px);
        if let Some(image) = cache.get(cache_key.as_str()) {
            return image.clone();
        }

        let image = load_file_icon_image(path, icon_source_size_px).unwrap_or_default();
        if cache.len() >= MAX_RESULT_ICON_CACHE {
            cache.clear();
        }
        cache.insert(cache_key, image.clone());
        image
    })
}

fn result_icon_source_size_for_window(window: &AppWindow) -> u32 {
    let window_scale = window.window().scale_factor().max(0.1);
    let monitor_scale = monitor_scale_factor_for_cursor()
        .unwrap_or(window_scale)
        .max(0.1);
    let target_physical_size = ((RESULT_ICON_LOGICAL_SIZE_PX as f32) * monitor_scale).ceil() as u32;
    let preferred_size = target_physical_size.max(RESULT_ICON_MIN_SOURCE_SIZE_PX);

    match preferred_size {
        0..=16 => 16,
        17..=24 => 24,
        25..=32 => 32,
        33..=48 => 48,
        _ => 64,
    }
}

fn result_icon_cache_key(path: &str, icon_source_size_px: u32) -> String {
    format!("{icon_source_size_px}|{path}")
}

fn clear_result_icon_cache() {
    RESULT_ICON_CACHE.with(|cache_cell| {
        cache_cell.borrow_mut().clear();
    });
}

fn meta_for_path(path: &str) -> String {
    RESULT_META_CACHE.with(|cache_cell| {
        let mut cache = cache_cell.borrow_mut();
        if let Some(meta) = cache.get(path) {
            return meta.clone();
        }

        let meta = format_result_meta(path);
        if cache.len() >= MAX_RESULT_META_CACHE {
            cache.clear();
        }
        cache.insert(path.to_string(), meta.clone());
        meta
    })
}

fn clear_result_meta_cache() {
    RESULT_META_CACHE.with(|cache_cell| {
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

fn search_query_mode_label(mode: SearchQueryMode) -> &'static str {
    match mode {
        SearchQueryMode::Wildcard => "wildcard",
        SearchQueryMode::Regex => "regex",
    }
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

fn start_searching_indicator(window: &AppWindow) {
    window.set_searching(true);
    let progress = window.get_searching_progress();
    if !(0.0..1.0).contains(&progress) {
        window.set_searching_progress(0.0);
    }
}

fn stop_searching_indicator(window: &AppWindow) {
    window.set_searching(false);
    if window.get_searching_progress() != 0.0 {
        window.set_searching_progress(0.0);
    }
}
