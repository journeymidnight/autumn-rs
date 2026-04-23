use autumn_transport::{decide, Decision};

fn with_env<R>(key: &str, value: Option<&str>, f: impl FnOnce() -> R) -> R {
    let prev = std::env::var(key).ok();
    match value {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
    let r = f();
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
    r
}

#[test]
fn unset_env_defaults_to_tcp_when_probe_returns_none() {
    with_env("AUTUMN_TRANSPORT", None, || {
        assert_eq!(decide(), Decision::Tcp);
    });
}

#[test]
fn explicit_tcp_returns_tcp() {
    with_env("AUTUMN_TRANSPORT", Some("tcp"), || {
        assert_eq!(decide(), Decision::Tcp);
    });
}

#[test]
fn auto_returns_tcp_when_probe_returns_none() {
    with_env("AUTUMN_TRANSPORT", Some("auto"), || {
        assert_eq!(decide(), Decision::Tcp);
    });
}

#[test]
#[should_panic(expected = "invalid AUTUMN_TRANSPORT")]
fn invalid_env_panics() {
    with_env("AUTUMN_TRANSPORT", Some("garbage"), || {
        let _ = decide();
    });
}

// Without the ucx feature, probe_ucx() always returns None, so forced ucx
// must panic.
#[cfg(not(feature = "ucx"))]
#[test]
#[should_panic(expected = "AUTUMN_TRANSPORT=ucx but UCX unavailable")]
fn forced_ucx_without_feature_panics() {
    with_env("AUTUMN_TRANSPORT", Some("ucx"), || {
        let _ = decide();
    });
}
