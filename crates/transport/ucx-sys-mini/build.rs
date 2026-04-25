//! Build script — runs bindgen against `<ucp/api/ucp.h>` and emits
//! `$OUT_DIR/bindings.rs`. Whitelist matches spec §13.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");

    // Probe libucx via pkg-config. If absent, emit an empty bindings.rs so the
    // crate compiles to an effectively-empty stub. Downstream that opts into
    // `autumn-transport/ucx` will fail loudly (unresolved symbols) — which is
    // the correct signal: install libucx-dev. Downstream that does NOT enable
    // the feature won't reference any symbol, so a stub is fine and keeps
    // `cargo build --workspace` working on hosts without UCX.
    let lib = match pkg_config::probe_library("ucx") {
        Ok(lib) => lib,
        Err(e) => {
            println!(
                "cargo:warning=libucx not found via pkg-config ({e}); ucx-sys-mini built as empty stub. Install libucx-dev to enable --features ucx."
            );
            std::fs::write(&out_path, b"").expect("write empty bindings.rs stub");
            return;
        }
    };

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Generate fewer files / less noise: only the ucp/ucs symbols we need.
        // We keep the regex broad enough to pick up all the structs/enums those
        // functions touch — bindgen will follow type closures automatically.
        // ucp_init in the header is a static inline that bindgen skips;
        // we call ucp_init_version directly with the macro-defined version.
        .allowlist_function("ucp_init_version")
        .allowlist_function("ucp_cleanup")
        .allowlist_function("ucp_config_read")
        .allowlist_function("ucp_config_release")
        .allowlist_function("ucp_context_print_info")
        .allowlist_function("ucp_worker_create")
        .allowlist_function("ucp_worker_destroy")
        .allowlist_function("ucp_worker_get_efd")
        .allowlist_function("ucp_worker_arm")
        .allowlist_function("ucp_worker_progress")
        .allowlist_function("ucp_ep_create")
        .allowlist_function("ucp_ep_close_nbx")
        .allowlist_function("ucp_listener_create")
        .allowlist_function("ucp_listener_destroy")
        .allowlist_function("ucp_listener_query")
        .allowlist_function("ucp_conn_request_query")
        .allowlist_function("ucp_stream_send_nbx")
        .allowlist_function("ucp_stream_recv_nbx")
        .allowlist_function("ucp_request_cancel")
        .allowlist_function("ucp_request_check_status")
        .allowlist_function("ucp_request_free")
        .allowlist_function("ucs_status_string")
        // Pull in the constants we reference (UCP_FEATURE_*, UCP_*_FIELD_*,
        // UCP_OP_ATTR_*, UCS_OK / UCS_INPROGRESS / etc.) and any enum we need.
        .allowlist_var("UCP_.*")
        .allowlist_var("UCS_.*")
        .allowlist_type("ucp_.*")
        .allowlist_type("ucs_status.*")
        .allowlist_type("ucs_thread_mode.*")
        // C bitfield enums map cleanly to a single Rust constified-enum module
        // without needing #[repr(C)] gymnastics on the Rust side.
        .default_enum_style(bindgen::EnumVariation::ModuleConsts)
        .layout_tests(false);

    for inc in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", inc.display()));
    }

    // libclang on this build host doesn't ship a clang resource directory
    // (no `clang` CLI installed, only libclang.so), so it fails to find
    // freestanding headers like <stddef.h>. Point it at gcc's include dir
    // explicitly. If clang is installed, this is harmless (the system clang
    // headers take precedence anyway).
    for guess in [
        "/usr/lib/gcc/x86_64-linux-gnu/13/include",
        "/usr/lib/gcc/x86_64-linux-gnu/12/include",
        "/usr/lib/gcc/x86_64-linux-gnu/11/include",
    ] {
        if std::path::Path::new(guess).exists() {
            builder = builder.clang_arg(format!("-I{guess}"));
            break;
        }
    }

    let bindings = builder
        .generate()
        .expect("bindgen failed against <ucp/api/ucp.h>");

    bindings
        .write_to_file(&out_path)
        .expect("write bindings.rs");
}
