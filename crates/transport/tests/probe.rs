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

// These two tests intentionally only run WITHOUT the ucx feature: with
// the feature on (and an RDMA-capable host), probe_ucx() returns
// Some(Ucx), so unset/auto pick UCX, not TCP. The "pick UCX when
// available" behaviour is covered by `auto_picks_ucx_when_rdma_present`
// below.
#[cfg(not(feature = "ucx"))]
#[test]
fn unset_env_defaults_to_tcp_when_no_ucx() {
    with_env("AUTUMN_TRANSPORT", None, || {
        assert_eq!(decide(), Decision::Tcp);
    });
}

#[cfg(not(feature = "ucx"))]
#[test]
fn auto_returns_tcp_when_no_ucx() {
    with_env("AUTUMN_TRANSPORT", Some("auto"), || {
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

// With the ucx feature on a host that has RDMA (this build host has
// 10× mlx5 HCAs verified by scripts/check_roce.sh), the probe should
// pick UCX for AUTUMN_TRANSPORT=ucx and AUTUMN_TRANSPORT=auto.
#[cfg(feature = "ucx")]
#[test]
fn forced_ucx_with_rdma_returns_ucx() {
    with_env("AUTUMN_TRANSPORT", Some("ucx"), || {
        assert_eq!(decide(), Decision::Ucx);
    });
}

#[cfg(feature = "ucx")]
#[test]
fn auto_picks_ucx_when_rdma_present() {
    with_env("AUTUMN_TRANSPORT", Some("auto"), || {
        assert_eq!(decide(), Decision::Ucx);
    });
}
