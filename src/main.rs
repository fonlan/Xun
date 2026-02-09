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
use server::StartInstalledServiceOutcome;
use win::RunAsLaunchOutcome;

fn main() -> anyhow::Result<()> {
    xlog::init_session();
    xlog::info("bootstrap start");

    let args = std::env::args().collect::<Vec<_>>();
    let install_service_mode = args.iter().skip(1).any(|arg| arg == "--install-service");
    let uninstall_service_mode = args.iter().skip(1).any(|arg| arg == "--uninstall-service");
    let start_service_mode = args.iter().skip(1).any(|arg| arg == "--start-service");
    let stop_service_mode = args.iter().skip(1).any(|arg| arg == "--stop-service");
    let server_mode = args.iter().skip(1).any(|arg| arg == "--server");

    if install_service_mode {
        xlog::info("startup mode: install-service");

        win::assert_elevated().context("failed to check admin privilege")?;
        if !win::is_elevated()? {
            return Err(anyhow::anyhow!(
                "--install-service mode requires elevated/admin privilege"
            ));
        }

        server::install_and_start_current_service()?;
        xlog::info("install-service completed");
        return Ok(());
    }

    if uninstall_service_mode {
        xlog::info("startup mode: uninstall-service");

        win::assert_elevated().context("failed to check admin privilege")?;
        if !win::is_elevated()? {
            return Err(anyhow::anyhow!(
                "--uninstall-service mode requires elevated/admin privilege"
            ));
        }

        server::uninstall_current_service()?;
        xlog::info("uninstall-service completed");
        return Ok(());
    }

    if start_service_mode {
        xlog::info("startup mode: start-service");

        win::assert_elevated().context("failed to check admin privilege")?;
        if !win::is_elevated()? {
            return Err(anyhow::anyhow!(
                "--start-service mode requires elevated/admin privilege"
            ));
        }

        server::start_current_service()?;
        xlog::info("start-service completed");
        return Ok(());
    }

    if stop_service_mode {
        xlog::info("startup mode: stop-service");

        win::assert_elevated().context("failed to check admin privilege")?;
        if !win::is_elevated()? {
            return Err(anyhow::anyhow!(
                "--stop-service mode requires elevated/admin privilege"
            ));
        }

        server::stop_current_service()?;
        xlog::info("stop-service completed");
        return Ok(());
    }

    if server_mode {
        xlog::info("startup mode: server");

        win::assert_elevated().context("failed to check admin privilege")?;
        if !win::is_elevated()? {
            return Err(anyhow::anyhow!(
                "--server mode requires elevated/admin privilege"
            ));
        }

        return server::run_server_mode();
    }

    xlog::info("startup mode: client");
    match server::start_installed_service_if_stopped() {
        Ok(StartInstalledServiceOutcome::NotInstalled)
        | Ok(StartInstalledServiceOutcome::AlreadyRunning)
        | Ok(StartInstalledServiceOutcome::Started) => {}
        Ok(StartInstalledServiceOutcome::RequiresElevation) => {
            match win::launch_start_service_elevated() {
                Ok(RunAsLaunchOutcome::Started) => {}
                Ok(RunAsLaunchOutcome::Cancelled) => {
                    xlog::warn("startup auto-start cancelled by user at UAC prompt")
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
    let app = XunApp::new()?;
    xlog::info("client app constructed, entering run loop");
    app.run()
}
