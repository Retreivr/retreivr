use std::fs;
use std::path::PathBuf;
use std::process::Command;

use tauri::{AppHandle, Manager};

fn app_support_dir(app: &AppHandle) -> PathBuf {
    app.path()
        .app_data_dir()
        .expect("failed to resolve app data dir")
        .join("Retreivr")
}

fn compose_path(app: &AppHandle) -> PathBuf {
    app_support_dir(app).join("compose.yaml")
}

#[tauri::command]
fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tauri::command]
fn compose_exists(app: AppHandle) -> bool {
    compose_path(&app).exists()
}

#[tauri::command]
fn container_running(app: AppHandle) -> bool {
    if !compose_path(&app).exists() {
        return false;
    }

    Command::new("docker")
        .args(["compose", "ps", "-q"])
        .current_dir(app_support_dir(&app))
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false)
}

#[tauri::command]
fn install_retreivr(app: AppHandle) -> Result<(), String> {
    let dir = app_support_dir(&app);
    let compose = compose_path(&app);

    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    if !compose.exists() {
        let content = r#"
services:
  retreivr:
    image: ghcr.io/retreivr/retreivr:latest
    container_name: retreivr
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      - "./config:/config"
      - "./downloads:/downloads"
"#;
        fs::write(&compose, content.trim_start()).map_err(|e| e.to_string())?;
    }

    Command::new("docker")
        .args(["compose", "up", "-d"])
        .current_dir(&dir)
        .status()
        .map_err(|e| e.to_string())?;

    Ok(())
}

#[tauri::command]
fn stop_retreivr(app: AppHandle) -> Result<(), String> {
    Command::new("docker")
        .args(["compose", "down"])
        .current_dir(app_support_dir(&app))
        .status()
        .map_err(|e| e.to_string())?;

    Ok(())
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            docker_available,
            compose_exists,
            container_running,
            install_retreivr,
            stop_retreivr
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}