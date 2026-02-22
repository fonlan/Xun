# Xun

Xun 是一个基于 Rust + Slint 的 Windows 本地文件检索与启动器。  
客户端通过 `Alt+Space` 和托盘唤起搜索窗口；后台服务维护 NTFS 索引，并通过命名管道 `\\.\pipe\xun.search.v1` 提供查询。

## 核心功能

- 全局快捷键：`Alt+Space`
- 托盘菜单：安装/卸载服务、开机启动、退出
- NTFS 索引：MFT 全量 + USN 增量更新
- 查询模式：通配符（`*`/`?`）与正则，支持 `Full`（全路径）和 `Aa`（大小写）
- 服务联动：客户端启动自动尝试拉起服务，退出自动尝试停服（必要时触发 UAC）
- 结果增强：打开历史置顶、类型筛选、右键快捷操作（打开/复制/剪切等）

## 运行要求

- Windows（依赖 Win32 API）
- NTFS 卷（索引能力基于 MFT/USN）
- Rust stable（开发构建时）

## 快速开始

### 普通使用

1. 启动 `xun.exe`。
2. 在托盘菜单中选择“安装系统服务”（首次使用）。
3. 使用 `Alt+Space` 唤起搜索窗口。

### 开发调试

```powershell
cargo run
```

按需安装服务：

```powershell
cargo run -- --install-service
```

## 命令行参数

- `--install-service`：安装并启动系统服务（需管理员）
- `--uninstall-service`：卸载系统服务（需管理员）
- `--start-service`：启动已安装服务（需管理员）
- `--stop-service`：停止已安装服务（需管理员）
- `--server`：以服务端模式运行（需管理员）
- `--client-autostart`：客户端开机自启模式（仅驻留托盘，不主动显示主界面）

## 开发命令

```powershell
cargo check
cargo fmt
cargo clippy --all-targets -- -D warnings
```

## 日志与数据

- 客户端日志：`%AppData%\Xun\logs\xun-YYYY-MM-DD.log`
- 服务端日志：`%ProgramData%\Xun\logs\xun-YYYY-MM-DD.log`
- 日志保留：自动清理，仅保留最近 7 天
- 打开历史：`%AppData%\Xun\config\opened-items.v1`

## 常见排查

- 无搜索结果时先检查服务是否已安装并运行。
- 确认目标目录位于 NTFS 分区。
- 确认当前权限可访问对应卷与 USN。

## 主要目录

```text
src/main.rs    启动入口与参数解析
src/app.rs     Slint UI 与交互
src/server.rs  Windows 服务生命周期
src/index.rs   索引构建与检索
src/ipc.rs     命名管道协议
src/win.rs     Win32 桥接
src/xlog.rs    日志系统
ui/app.slint   界面定义
```
