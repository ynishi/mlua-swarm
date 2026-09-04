//! Shared HTTP client construction — L0 access-token pass-through (GH #101).
//!
//! When `MSE_ACCESS_TOKEN` is set, every HTTP request this binary makes to
//! the server must carry it as the `X-MSE-Access-Token` header (the L0
//! perimeter; vocabulary: `mse://guides/auth-token-model`). Centralizing
//! the `reqwest` client construction here means no call site can forget
//! the header: [`client_builder`] bakes it into the client's default
//! headers, and the WebSocket upgrade (which bypasses `reqwest`) takes it
//! from [`access_token_header`]. Unset env ⇒ no header ⇒ byte-identical
//! behavior to a server without a perimeter.

use reqwest::header::{HeaderMap, HeaderValue};

/// Header carrying the L0 access token — the server crate's constant is
/// the single source (a drifted duplicate would send the old header and
/// collect 401s with nothing pinning the two equal).
pub use mlua_swarm_server::ACCESS_TOKEN_HEADER;

/// The `X-MSE-Access-Token` header value from `MSE_ACCESS_TOKEN`, if set.
///
/// Marked sensitive so `reqwest`/`http` debug output redacts it. Empty or
/// non-header-safe values are treated as unset.
pub fn access_token_header() -> Option<HeaderValue> {
    let token = std::env::var("MSE_ACCESS_TOKEN").ok()?;
    if token.is_empty() {
        return None;
    }
    let mut value = HeaderValue::from_str(&token).ok()?;
    value.set_sensitive(true);
    Some(value)
}

/// A `reqwest::ClientBuilder` with the L0 header (when configured) as a
/// default header. Call sites chain their own timeouts and `build()`.
///
/// Redirects are disabled: reqwest strips only `Authorization`/cookie-class
/// headers on a cross-host redirect, so a followed 302 would re-send the
/// custom access-token header to the redirect target. No mse endpoint
/// issues redirects, so `Policy::none()` is behavior-preserving.
pub fn client_builder() -> reqwest::ClientBuilder {
    let mut headers = HeaderMap::new();
    if let Some(value) = access_token_header() {
        headers.insert(ACCESS_TOKEN_HEADER, value);
    }
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .default_headers(headers)
}

/// The loopback address `mse serve` binds to when nothing says otherwise.
/// Kept as a whole base URL rather than a `host:port` literal, because the
/// scheme is part of "where the server is" — see [`Endpoint`].
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:7777";

/// Where the server is — resolved once, in one place.
///
/// Every HTTP call this binary makes used to answer that question twice, in
/// two incompatible ways: the `bind` tool arguments carried a `host:port`
/// and each call site wrote `format!("http://{bind}/…")`, while the operator
/// client read `MSE_HTTP` as a whole base URL. The first shape cannot name
/// an `https` server at all — a scheme written into `bind` comes back out as
/// `http://https://…` — so half the tools could reach a TLS-terminated
/// deployment and half could not, and no error said so.
///
/// Resolution order, in full:
///
/// 1. the `bind` argument, when given and non-empty;
/// 2. otherwise `MSE_HTTP`, when set and non-empty;
/// 3. otherwise [`DEFAULT_BASE_URL`].
///
/// A value that already names a scheme is used as given; a bare `host:port`
/// gets `http://`, which is what every historical caller passed and meant.
pub struct Endpoint {
    base: String,
    source: EndpointSource,
}

/// Which of the three layers supplied the endpoint.
///
/// Knowing *where* a tool connected is half the answer; the other half is
/// **why there**, because that names the thing a reader would go change.
/// The failure this exists for looked like a vacant operator seat three
/// hops downstream, and the cause was that one call had an argument and
/// another fell through to the environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointSource {
    /// An explicit `bind` argument on the tool call.
    Argument,
    /// The `MSE_HTTP` environment variable.
    Env,
    /// Nothing said, so [`DEFAULT_BASE_URL`].
    Default,
}

impl EndpointSource {
    /// Human-facing name of the thing that would have to change to point
    /// somewhere else.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Argument => "bind argument",
            Self::Env => "MSE_HTTP",
            Self::Default => "loopback default",
        }
    }
}

impl Endpoint {
    /// Resolves the endpoint from an optional `bind` argument, falling back
    /// to `MSE_HTTP` and then to [`DEFAULT_BASE_URL`].
    pub fn resolve(bind: Option<&str>) -> Self {
        let (raw, source) = bind
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| (value.to_owned(), EndpointSource::Argument))
            .or_else(|| {
                std::env::var("MSE_HTTP")
                    .ok()
                    .map(|value| value.trim().to_owned())
                    .filter(|value| !value.is_empty())
                    .map(|value| (value, EndpointSource::Env))
            })
            .unwrap_or_else(|| (DEFAULT_BASE_URL.to_owned(), EndpointSource::Default));

        // "Already names a scheme" is the question, not "is it http or
        // https" — a caller naming some other scheme owns that choice.
        let based = if raw.contains("://") {
            raw
        } else {
            format!("http://{raw}")
        };

        Self {
            base: based.trim_end_matches('/').to_owned(),
            source,
        }
    }

    /// Which layer supplied this endpoint.
    pub fn source(&self) -> EndpointSource {
        self.source
    }

    /// Joins an absolute path (`/v1/doctor`) onto the resolved base.
    pub fn url(&self, path: &str) -> String {
        debug_assert!(
            path.starts_with('/'),
            "Endpoint::url takes an absolute path, got {path:?}"
        );
        format!("{}{}", self.base, path)
    }

    /// The resolved base URL, scheme included, without a trailing slash.
    pub fn base(&self) -> &str {
        &self.base
    }
}

/// The only route prefix a caller-supplied path may name.
pub const API_PATH_PREFIX: &str = "/v1/";

/// Checks a caller-supplied API path before it is joined onto an
/// [`Endpoint`].
///
/// The endpoint itself comes from configuration the caller never supplies,
/// so a path is the only thing a caller controls — which makes it the only
/// thing that could turn a convenience escape hatch into a request
/// forwarder aimed at an arbitrary host. Rejected: anything outside
/// `/v1/`, anything that names a scheme or a host, parent-directory
/// traversal, and control characters.
pub fn validate_api_path(path: &str) -> Result<(), String> {
    if !path.starts_with(API_PATH_PREFIX) {
        return Err(format!(
            "path must start with {API_PATH_PREFIX:?} (got {path:?})"
        ));
    }
    if path.contains("://") {
        return Err(format!(
            "path must not name a scheme — the caller does not choose the host (got {path:?})"
        ));
    }
    if path.starts_with("//") {
        return Err(format!("a leading // is a host, not a path (got {path:?})"));
    }
    if path
        .split(['/', '?', '#'])
        .any(|segment| segment == ".." || segment == ".")
    {
        return Err(format!(
            "path must not traverse out of {API_PATH_PREFIX:?} (got {path:?})"
        ));
    }
    if let Some(bad) = path
        .chars()
        .find(|c| c.is_whitespace() || c.is_control() || *c == '\\')
    {
        return Err(format!("path must not contain {bad:?} (got {path:?})"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! This module owns two behaviors that every HTTP call in the binary
    //! inherits — whether the access-token header is attached, and whether a
    //! redirect is followed — and until now neither was pinned by a test.
    //! That is not a cosmetic gap: `Policy::none()` is what turns an
    //! http→https `301` into a non-success status, which is how a healthy
    //! server came to be reported as `up: false`. The policy is correct and
    //! stays; these tests exist so that it stays *deliberately*, and so that
    //! the header contract is not silently lost in a refactor.

    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

    /// `MSE_ACCESS_TOKEN` is process-global state, and cargo runs tests in
    /// one process across many threads. Every test that touches the variable
    /// takes this lock, so they serialize against each other instead of
    /// reading a value another test is midway through setting.
    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Sets an environment variable for the duration of a test and restores
    /// whatever was there before, so a developer running the suite with the
    /// variable exported in their shell gets the same result as CI.
    struct EnvVar {
        name: &'static str,
        previous: Option<String>,
    }

    impl EnvVar {
        fn set(name: &'static str, value: Option<&str>) -> Self {
            let previous = std::env::var(name).ok();
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
            Self { name, previous }
        }
    }

    impl Drop for EnvVar {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(v) => std::env::set_var(self.name, v),
                None => std::env::remove_var(self.name),
            }
        }
    }

    /// Shorthand for the token variable, which most of these tests move.
    struct TokenEnv(#[allow(dead_code)] EnvVar);

    impl TokenEnv {
        fn set(value: Option<&str>) -> Self {
            Self(EnvVar::set("MSE_ACCESS_TOKEN", value))
        }
    }

    // ─── API path allow-list ───────────────────────────────────────────────
    //
    // The escape hatch that lets a caller reach a route no tool wraps yet
    // must not become a general-purpose request forwarder. The endpoint is
    // resolved from configuration the caller never supplies, so the only
    // thing a caller controls is the path — and that is what these bound.

    #[test]
    fn api_path_accepts_v1_routes() {
        for path in ["/v1/doctor", "/v1/healthz", "/v1/runs/R-1/trace?latest=50"] {
            assert!(validate_api_path(path).is_ok(), "should accept {path}");
        }
    }

    #[test]
    fn api_path_rejects_routes_outside_v1() {
        for path in ["/admin", "/", "/v2/doctor", "/v1", "v1/doctor"] {
            assert!(
                validate_api_path(path).is_err(),
                "should reject {path} — only /v1/ routes are reachable"
            );
        }
    }

    #[test]
    fn api_path_rejects_an_absolute_url() {
        for path in [
            "http://evil.example/v1/doctor",
            "https://evil.example/v1/doctor",
            "/v1/../../http://evil.example",
        ] {
            assert!(
                validate_api_path(path).is_err(),
                "should reject {path} — the caller does not choose the host"
            );
        }
    }

    #[test]
    fn api_path_rejects_a_protocol_relative_path() {
        assert!(
            validate_api_path("//evil.example/v1/doctor").is_err(),
            "a leading // is a host, not a path"
        );
    }

    #[test]
    fn api_path_rejects_parent_directory_traversal() {
        for path in ["/v1/../admin", "/v1/runs/../../admin", "/v1/a/../../b"] {
            assert!(
                validate_api_path(path).is_err(),
                "should reject {path} — traversal escapes the allow-list"
            );
        }
        assert!(
            validate_api_path("/v1/runs/R-..-1").is_ok(),
            "a literal '..' inside a segment is not traversal"
        );
    }

    #[test]
    fn api_path_rejects_control_characters_and_whitespace() {
        for path in ["/v1/doc tor", "/v1/doctor\n", "/v1/doc\ttor", "/v1/\u{0}x"] {
            assert!(
                validate_api_path(path).is_err(),
                "should reject {path:?} — request smuggling surface"
            );
        }
    }

    // ─── Endpoint resolution ───────────────────────────────────────────────
    //
    // The defect these pin: `bind` was modelled as `host:port` and every URL
    // was built with `format!("http://{bind}/…")`, so there was no way to
    // name an https server — writing a scheme into `bind` produced
    // `http://https://…`. Meanwhile the operator client read `MSE_HTTP`, a
    // whole base URL. One process, two types for "where the server is".

    #[test]
    fn endpoint_accepts_an_https_base_url() {
        let endpoint = Endpoint::resolve(Some("https://example.com"));
        assert_eq!(
            endpoint.url("/v1/doctor"),
            "https://example.com/v1/doctor",
            "a base URL that already names its scheme must be used as given"
        );
    }

    #[test]
    fn endpoint_accepts_an_http_base_url_without_doubling_the_scheme() {
        let endpoint = Endpoint::resolve(Some("http://example.com:7777"));
        assert_eq!(
            endpoint.url("/v1/healthz"),
            "http://example.com:7777/v1/healthz"
        );
    }

    #[test]
    fn endpoint_completes_the_scheme_for_a_bare_host_port() {
        let endpoint = Endpoint::resolve(Some("127.0.0.1:7777"));
        assert_eq!(
            endpoint.url("/v1/doctor"),
            "http://127.0.0.1:7777/v1/doctor",
            "the historical host:port shape must keep working unchanged"
        );
    }

    #[test]
    fn endpoint_tolerates_a_trailing_slash_on_the_base() {
        let endpoint = Endpoint::resolve(Some("https://example.com/"));
        assert_eq!(
            endpoint.url("/v1/doctor"),
            "https://example.com/v1/doctor",
            "a trailing slash must not produce a doubled separator"
        );
    }

    /// "Where are we connected?" was answerable; "why there?" was not, and
    /// that is the question a split between a local run and a remote
    /// operator session actually poses. Three possible answers, so the
    /// value says which one it was.
    #[test]
    fn endpoint_reports_that_an_argument_chose_the_target() {
        let _lock = env_lock();
        let _env = EnvVar::set("MSE_HTTP", Some("https://from-env.example"));
        let endpoint = Endpoint::resolve(Some("https://from-arg.example"));
        assert_eq!(endpoint.base(), "https://from-arg.example");
        assert_eq!(endpoint.source(), EndpointSource::Argument);
    }

    #[test]
    fn endpoint_reports_that_the_environment_chose_the_target() {
        let _lock = env_lock();
        let _env = EnvVar::set("MSE_HTTP", Some("https://from-env.example"));
        let endpoint = Endpoint::resolve(None);
        assert_eq!(endpoint.base(), "https://from-env.example");
        assert_eq!(endpoint.source(), EndpointSource::Env);
    }

    #[test]
    fn endpoint_reports_that_nothing_chose_the_target() {
        let _lock = env_lock();
        let _env = EnvVar::set("MSE_HTTP", None);
        let endpoint = Endpoint::resolve(None);
        assert_eq!(endpoint.base(), DEFAULT_BASE_URL);
        assert_eq!(endpoint.source(), EndpointSource::Default);
    }

    #[test]
    fn endpoint_source_renders_the_thing_a_reader_would_go_change() {
        assert_eq!(EndpointSource::Argument.as_str(), "bind argument");
        assert_eq!(EndpointSource::Env.as_str(), "MSE_HTTP");
        assert_eq!(EndpointSource::Default.as_str(), "loopback default");
    }

    #[test]
    fn endpoint_falls_back_to_mse_http_when_no_bind_is_given() {
        let _lock = env_lock();
        let _env = EnvVar::set("MSE_HTTP", Some("https://example.com"));
        assert_eq!(
            Endpoint::resolve(None).url("/v1/doctor"),
            "https://example.com/v1/doctor",
            "the bind path and the operator path must resolve to one server"
        );
    }

    #[test]
    fn endpoint_falls_back_to_loopback_when_neither_bind_nor_env_is_set() {
        let _lock = env_lock();
        let _env = EnvVar::set("MSE_HTTP", None);
        assert_eq!(
            Endpoint::resolve(None).url("/v1/healthz"),
            "http://127.0.0.1:7777/v1/healthz",
            "the historical default must survive"
        );
    }

    #[test]
    fn endpoint_ignores_an_empty_mse_http() {
        let _lock = env_lock();
        let _env = EnvVar::set("MSE_HTTP", Some(""));
        assert_eq!(
            Endpoint::resolve(None).url("/v1/healthz"),
            "http://127.0.0.1:7777/v1/healthz",
            "an empty value is 'unset', not an empty base URL"
        );
    }

    #[test]
    fn access_token_header_is_none_when_the_env_is_unset() {
        let _lock = env_lock();
        let _env = TokenEnv::set(None);
        assert!(access_token_header().is_none());
    }

    #[test]
    fn access_token_header_is_none_when_the_env_is_empty() {
        let _lock = env_lock();
        let _env = TokenEnv::set(Some(""));
        assert!(
            access_token_header().is_none(),
            "an empty token is 'no perimeter', not a header with an empty value"
        );
    }

    #[test]
    fn access_token_header_is_marked_sensitive() {
        let _lock = env_lock();
        let _env = TokenEnv::set(Some("secret-value"));
        let header = access_token_header().expect("a non-empty token yields a header");
        assert!(
            header.is_sensitive(),
            "the header must be marked sensitive so reqwest/http debug output redacts it"
        );
    }

    #[test]
    fn access_token_header_is_none_when_the_value_is_not_header_safe() {
        let _lock = env_lock();
        let _env = TokenEnv::set(Some("bad\nvalue"));
        assert!(
            access_token_header().is_none(),
            "a value that cannot be a header must be dropped, not panic the caller"
        );
    }

    /// Spawns a stub that records the access-token header of whatever
    /// request reaches `/probe`, and returns `(base_url, recorded)`.
    async fn spawn_header_recorder() -> (String, Arc<Mutex<Option<Option<String>>>>) {
        use axum::extract::State;
        use axum::http::HeaderMap as AxumHeaderMap;
        use axum::routing::get;
        use axum::Router;

        let recorded: Arc<Mutex<Option<Option<String>>>> = Arc::new(Mutex::new(None));
        let router = Router::new()
            .route(
                "/probe",
                get(
                    |State(sink): State<Arc<Mutex<Option<Option<String>>>>>,
                     headers: AxumHeaderMap| async move {
                        let seen = headers
                            .get(ACCESS_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_owned);
                        *sink.lock().expect("sink lock") = Some(seen);
                        "ok"
                    },
                ),
            )
            .with_state(Arc::clone(&recorded));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        (format!("http://{addr}"), recorded)
    }

    #[tokio::test]
    async fn client_builder_attaches_the_access_token_header_to_requests() {
        // The client bakes the header in at build time, so the env only has
        // to be set while building — never while awaiting.
        let client = {
            let _lock = env_lock();
            let _env = TokenEnv::set(Some("tok-abc"));
            client_builder().build().expect("build client")
        };

        let (base, recorded) = spawn_header_recorder().await;
        let response = client
            .get(format!("{base}/probe"))
            .send()
            .await
            .expect("request reaches the stub");
        assert!(response.status().is_success());

        let seen = recorded.lock().expect("sink lock").clone();
        assert_eq!(
            seen,
            Some(Some("tok-abc".to_string())),
            "every request from this client must carry the L0 header"
        );
    }

    #[tokio::test]
    async fn client_builder_omits_the_header_when_the_env_is_unset() {
        let client = {
            let _lock = env_lock();
            let _env = TokenEnv::set(None);
            client_builder().build().expect("build client")
        };

        let (base, recorded) = spawn_header_recorder().await;
        client
            .get(format!("{base}/probe"))
            .send()
            .await
            .expect("request reaches the stub");

        let seen = recorded.lock().expect("sink lock").clone();
        assert_eq!(
            seen,
            Some(None),
            "no token configured must mean no header — byte-identical to a server without a perimeter"
        );
    }

    /// The behavior that produced the `up: false` report on a healthy
    /// server. It is deliberate — following a cross-host redirect would
    /// re-send the access token to the redirect target — so this test pins
    /// it rather than arguing with it. What must change is elsewhere: the
    /// caller has to be able to say `https` in the first place, and has to
    /// report the `301` instead of collapsing it to `false`.
    #[tokio::test]
    async fn client_builder_does_not_follow_redirects() {
        use axum::http::{header, StatusCode};
        use axum::routing::get;
        use axum::Router;

        let target_hit = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&target_hit);
        let router = Router::new()
            .route(
                "/redirect",
                get(|| async {
                    (
                        StatusCode::MOVED_PERMANENTLY,
                        [(header::LOCATION, "/target")],
                    )
                }),
            )
            .route(
                "/target",
                get(move || {
                    let flag = Arc::clone(&flag);
                    async move {
                        flag.store(true, Ordering::SeqCst);
                        "arrived"
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let client = client_builder().build().expect("build client");
        let response = client
            .get(format!("http://{addr}/redirect"))
            .send()
            .await
            .expect("request reaches the stub");

        assert_eq!(
            response.status(),
            reqwest::StatusCode::MOVED_PERMANENTLY,
            "the 301 must surface to the caller, not be followed away"
        );
        assert!(
            !target_hit.load(Ordering::SeqCst),
            "the redirect target must never be requested — that is what keeps the \
             access token from reaching another host"
        );
    }
}
