use std::{env, fs, path::PathBuf};

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }
    let directory = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let root = directory.parent().unwrap().parent().unwrap();
    let icon = root.join("assets/zipover.ico");
    let manifest = root.join("assets/windows/app.manifest");
    println!("cargo:rerun-if-changed={}", icon.display());
    println!("cargo:rerun-if-changed={}", manifest.display());
    let resource = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("zipover.rc");
    fs::write(
        &resource,
        format!(
            "1 ICON \"{}\"\n1 24 \"{}\"\n",
            icon.display().to_string().replace('\\', "/"),
            manifest.display().to_string().replace('\\', "/")
        ),
    )
    .unwrap();
    embed_resource::compile(&resource, embed_resource::NONE)
        .manifest_required()
        .expect("无法嵌入 Windows 图标和应用清单");
}
