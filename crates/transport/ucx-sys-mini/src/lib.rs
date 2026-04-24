#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(deref_nullptr)]

//! Generated FFI bindings for UCP/UCX.
//!
//! All declarations come from `bindgen` over `<ucp/api/ucp.h>` (UCX 1.16);
//! the symbol whitelist is in `build.rs` and matches spec §13.
//!
//! Re-exports `libc` so consumers (`autumn-transport`) don't need a separate
//! dep for the `sockaddr` / `sockaddr_storage` types that UCX takes by pointer.

pub use libc;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
