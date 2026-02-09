#[cfg(target_os = "windows")]
use std::env;
#[cfg(target_os = "windows")]
use std::fs::File;
#[cfg(target_os = "windows")]
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=ui/app.slint");
    println!("cargo:rerun-if-changed=xun.png");
    slint_build::compile("ui/app.slint").expect("failed to compile Slint UI");

    #[cfg(target_os = "windows")]
    configure_windows_icon().expect("failed to embed Windows icon resource");
}

#[cfg(target_os = "windows")]
fn configure_windows_icon() -> Result<(), Box<dyn std::error::Error>> {
    use ico::{IconDir, IconDirEntry, IconImage, ResourceType};
    use image::imageops::FilterType;

    let source = image::open("xun.png")?;
    let mut icon_dir = IconDir::new(ResourceType::Icon);

    for size in [16, 24, 32, 48, 64, 128, 256] {
        let resized = source
            .resize_exact(size, size, FilterType::Lanczos3)
            .into_rgba8();
        let icon_image = IconImage::from_rgba_data(size, size, resized.into_raw());
        icon_dir.add_entry(IconDirEntry::encode(&icon_image)?);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let icon_path = out_dir.join("xun.ico");
    let icon_file = File::create(&icon_path)?;
    icon_dir.write(icon_file)?;

    let mut res = winres::WindowsResource::new();
    res.set_icon_with_id(icon_path.to_string_lossy().as_ref(), "1");
    res.compile()?;
    Ok(())
}
