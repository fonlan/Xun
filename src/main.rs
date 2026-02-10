#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod app;
mod arena;
mod index;
mod ipc;
mod model;
mod server;
mod win;
mod xlog;

use anyhow::Context;
use app::XunApp;
use server::{StartInstalledServiceOutcome, StopInstalledServiceOutcome};
use win::RunAsLaunchOutcome;

fn ensure_mode_elevated(mode_flag: &str) -> anyhow::Result<()> {
    win::assert_elevated().context("failed to check admin privilege")?;
    if !win::is_elevated()? {
        return Err(anyhow::anyhow!(
            "{mode_flag} mode requires elevated/admin privilege"
        ));
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    xlog::init_session();
    xlog::info("bootstrap start");

    let mut install_service_mode = false;
    let mut uninstall_service_mode = false;
    let mut start_service_mode = false;
    let mut stop_service_mode = false;
    let mut server_mode = false;

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--install-service" => install_service_mode = true,
            "--uninstall-service" => uninstall_service_mode = true,
            "--start-service" => start_service_mode = true,
            "--stop-service" => stop_service_mode = true,
            "--server" => server_mode = true,
            _ => {}
        }
    }

    if install_service_mode {
        xlog::info("startup mode: install-service");

        ensure_mode_elevated("--install-service")?;

        server::install_and_start_current_service()?;
        xlog::info("install-service completed");
        return Ok(());
    }

    if uninstall_service_mode {
        xlog::info("startup mode: uninstall-service");

        ensure_mode_elevated("--uninstall-service")?;

        server::uninstall_current_service()?;
        xlog::info("uninstall-service completed");
        return Ok(());
    }

    if start_service_mode {
        xlog::info("startup mode: start-service");

        ensure_mode_elevated("--start-service")?;

        server::start_current_service()?;
        xlog::info("start-service completed");
        return Ok(());
    }

    if stop_service_mode {
        xlog::info("startup mode: stop-service");

        ensure_mode_elevated("--stop-service")?;

        server::stop_current_service()?;
        xlog::info("stop-service completed");
        return Ok(());
    }

    if server_mode {
        xlog::info("startup mode: server");

        ensure_mode_elevated("--server")?;

        return server::run_server_mode();
    }

    xlog::info("startup mode: client");
    let mut startup_service_starting = false;
    match server::start_installed_service_if_stopped() {
        Ok(StartInstalledServiceOutcome::NotInstalled)
        | Ok(StartInstalledServiceOutcome::AlreadyRunning) => {}
        Ok(StartInstalledServiceOutcome::Started) => {
            startup_service_starting = true;
        }
        Ok(StartInstalledServiceOutcome::RequiresElevation) => {
            match win::launch_start_service_elevated() {
                Ok(RunAsLaunchOutcome::Started) => {
                    startup_service_starting = true;
                }
                Ok(RunAsLaunchOutcome::Cancelled) => {
                    xlog::warn("用户取消提权启动");
                }
                Err(err) => {
                    xlog::warn(format!(
                        "startup auto-start requires elevation, runas trigger failed: {err:#}"
                    ));
                }
            }
        }
        Err(err) => {
            xlog::warn(format!(
                "startup auto-start service failed, continue as client only: {err:#}"
            ));
        }
    }
    let app = XunApp::new(startup_service_starting)?;
    xlog::info("client app constructed, entering run loop");
    let run_result = app.run();

    match server::stop_installed_service_if_running() {
        Ok(StopInstalledServiceOutcome::NotInstalled)
        | Ok(StopInstalledServiceOutcome::AlreadyStopped)
        | Ok(StopInstalledServiceOutcome::Stopped) => {}
        Ok(StopInstalledServiceOutcome::RequiresElevation) => {
            match win::launch_stop_service_elevated() {
                Ok(RunAsLaunchOutcome::Started) => {}
                Ok(RunAsLaunchOutcome::Cancelled) => {
                    xlog::warn("shutdown auto-stop cancelled by user at UAC prompt")
                }
                Err(err) => {
                    xlog::warn(format!(
                        "shutdown auto-stop requires elevation, runas trigger failed: {err:#}"
                    ));
                }
            }
        }
        Err(err) => {
            xlog::warn(format!(
                "shutdown auto-stop service failed, continue client shutdown: {err:#}"
            ));
        }
    }

    run_result
}
