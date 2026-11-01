//! The HSFT desktop app. The interface lives in `ui/`, and this connects it to `hsft-core`.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;

use hsft_core::{Event, FriendInfo, Network, Node};
use serde::Serialize;
use tauri::menu::{Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Emitter, Manager, State, WindowEvent};

struct App {
    node: Node,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Overview {
    name: String,
    download_dir: String,
    friends: Vec<FriendInfo>,
}

fn download_dir(node: &Node) -> PathBuf {
    node.settings()
        .download_dir
        .or_else(dirs::download_dir)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn message(error: anyhow::Error) -> String {
    format!("{error:#}")
}

#[tauri::command]
fn overview(app: State<App>) -> Overview {
    Overview {
        name: app.node.settings().name,
        download_dir: download_dir(&app.node).display().to_string(),
        friends: app.node.friends(),
    }
}

#[tauri::command]
async fn create_invite(app: State<'_, App>) -> Result<String, String> {
    app.node.create_invite().await.map_err(message)
}

#[tauri::command]
async fn join_friend(app: State<'_, App>, code: String) -> Result<FriendInfo, String> {
    app.node.join(&code).await.map_err(message)
}

#[tauri::command]
fn remove_friend(app: State<App>, id: String) -> Result<(), String> {
    app.node.remove_friend(&id).map_err(message)
}

#[tauri::command]
async fn send_files(
    app: State<'_, App>,
    friend: String,
    paths: Vec<PathBuf>,
) -> Result<u64, String> {
    app.node.send(&friend, paths).map_err(message)
}

#[tauri::command]
fn respond(app: State<App>, transfer: u64, accept: bool) {
    let dest = accept.then(|| download_dir(&app.node));
    app.node.respond(transfer, dest);
}

#[tauri::command]
fn cancel(app: State<App>, transfer: u64) {
    app.node.cancel(transfer);
}

#[tauri::command]
fn set_name(app: State<App>, name: String) -> Result<(), String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("Enter a name your friends will recognise.".into());
    }
    let mut settings = app.node.settings();
    settings.name = name.chars().take(64).collect();
    app.node.update_settings(settings).map_err(message)
}

#[tauri::command]
fn set_download_dir(app: State<App>, path: PathBuf) -> Result<(), String> {
    let mut settings = app.node.settings();
    settings.download_dir = Some(path);
    app.node.update_settings(settings).map_err(message)
}

#[tauri::command]
fn reveal(path: PathBuf) -> Result<(), String> {
    tauri_plugin_opener::reveal_item_in_dir(path).map_err(|e| e.to_string())
}

fn show_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

fn build_tray(app: &tauri::App) -> tauri::Result<()> {
    let open = MenuItem::with_id(app, "open", "Open HSFT", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&open, &quit])?;
    let mut tray = TrayIconBuilder::new()
        .tooltip("HSFT")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| match event.id.as_ref() {
            "open" => show_window(app),
            "quit" => app.exit(0),
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                show_window(tray.app_handle());
            }
        });
    if let Some(icon) = app.default_window_icon() {
        tray = tray.icon(icon.clone());
    }
    tray.build(app)?;
    Ok(())
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("HSFT_LOG"))
        .with_writer(std::io::stderr)
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _, _| show_window(app)))
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            overview,
            create_invite,
            join_friend,
            remove_friend,
            send_files,
            respond,
            cancel,
            set_name,
            set_download_dir,
            reveal,
        ])
        .on_window_event(|window, event| {
            // Friends can only reach this PC while the app runs, so closing keeps it in the tray.
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .setup(|app| {
            let data_dir = std::env::var_os("HSFT_DATA")
                .map(PathBuf::from)
                .or_else(|| dirs::data_dir().map(|dir| dir.join("HSFT")))
                .ok_or_else(|| anyhow::anyhow!("no data folder"))?;
            let (node, mut events) =
                tauri::async_runtime::block_on(Node::start(&data_dir, Network::Internet))?;
            app.manage(App { node });
            build_tray(app)?;

            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                while let Some(event) = events.recv().await {
                    if matches!(event, Event::Offer { .. }) {
                        show_window(&handle);
                    }
                    let _ = handle.emit("hsft", event);
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("HSFT failed to start");
}
