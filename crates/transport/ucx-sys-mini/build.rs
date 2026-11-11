//! Regenerates `src/bindings.rs` from `<ucp/api/ucp.h>` when built with
//! `--features regen` (needs libclang and the UCX headers via pkg-config).
//! Ordinary builds use the checked-in file and need neither.
//!
//! The bindings load libucp at run time (`dynamic_library_name`), so nothing
//! links against UCX: one build serves hosts with and without it.

fn main() {
    #[cfg(feature = "regen")]
    regen();
}

#[cfg(feature = "regen")]
fn regen() {
    use std::path::PathBuf;

    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");

    let lib = pkg_config::Config::new()
        .cargo_metadata(false)
        .probe("ucx")
        .expect("regen needs the UCX headers (pkg-config ucx)");

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .dynamic_library_name("Ucp")
        .dynamic_link_require_all(true)
        // ucp_init in the header is a static inline that bindgen skips;
        // we call ucp_init_version directly with the macro-defined version.
        .allowlist_function("ucp_init_version")
        .allowlist_function("ucp_get_version")
        .allowlist_function("ucp_cleanup")
        .allowlist_function("ucp_config_read")
        .allowlist_function("ucp_config_release")
        .allowlist_function("ucp_context_print_info")
        .allowlist_function("ucp_worker_create")
        .allowlist_function("ucp_worker_destroy")
        .allowlist_function("ucp_worker_get_efd")
        .allowlist_function("ucp_worker_arm")
        .allowlist_function("ucp_worker_signal")
        .allowlist_function("ucp_worker_progress")
        .allowlist_function("ucp_ep_create")
        .allowlist_function("ucp_ep_flush_nbx")
        .allowlist_function("ucp_ep_close_nbx")
        .allowlist_function("ucp_ep_query")
        .allowlist_function("ucp_listener_create")
        .allowlist_function("ucp_listener_destroy")
        .allowlist_function("ucp_listener_query")
        .allowlist_function("ucp_conn_request_query")
        .allowlist_function("ucp_stream_send_nbx")
        .allowlist_function("ucp_stream_recv_nbx")
        .allowlist_function("ucp_stream_recv_data_nb")
        .allowlist_function("ucp_stream_data_release")
        .allowlist_function("ucp_worker_set_am_recv_handler")
        .allowlist_function("ucp_am_send_nbx")
        .allowlist_function("ucp_am_recv_data_nbx")
        .allowlist_function("ucp_am_data_release")
        .allowlist_function("ucp_mem_map")
        .allowlist_function("ucp_mem_unmap")
        .allowlist_function("ucp_request_cancel")
        .allowlist_function("ucp_request_check_status")
        .allowlist_function("ucp_request_free")
        .allowlist_function("ucs_status_string")
        // The constants we reference (UCP_FEATURE_*, UCP_*_FIELD_*,
        // UCP_OP_ATTR_*, UCS_OK / UCS_INPROGRESS / etc.) and the types the
        // functions touch.
        .allowlist_var("UCP_.*")
        .allowlist_var("UCS_.*")
        .allowlist_type("ucp_.*")
        .allowlist_type("ucs_status.*")
        .allowlist_type("ucs_thread_mode.*")
        .default_enum_style(bindgen::EnumVariation::ModuleConsts)
        .layout_tests(false);

    for inc in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", inc.display()));
    }
    // A libclang without its resource directory cannot find <stddef.h>;
    // gcc's include dir has it.
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

    let out = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/bindings.rs");
    builder
        .generate()
        .expect("bindgen failed against <ucp/api/ucp.h>")
        .write_to_file(&out)
        .expect("write src/bindings.rs");
}
