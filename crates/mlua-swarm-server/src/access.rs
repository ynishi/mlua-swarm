//! L0 perimeter — the access-token gate (GH #101).
//!
//! The credential model has three layers (full vocabulary:
//! `mse://guides/auth-token-model`):
//!
//! - **L0 perimeter** (this module): "may you talk to this server at all?"
//!   One static secret, presented as the `X-MSE-Access-Token` header on
//!   every request. Deliberately **not** `Authorization` — that header is
//!   already the L1 identity channel (worker `CapToken` / `wh-` handle on
//!   `/v1/worker/*`, operator session token on `/v1/operators/*`), and a
//!   header whose meaning changes per route is a misconfiguration source.
//! - **L1 identity**: operator session token / worker `CapToken` /
//!   `wh-` handle — unchanged by this module.
//! - **L2 capability**: role × verb gate + scopes + seat check —
//!   server-side, unchanged by this module.
//!
//! ## Placement contract
//!
//! The gate must wrap the **fully merged** router — in `mse serve` that is
//! the `app` in `serve.rs` *after* the last `.merge()` (issues /
//! blueprints / enhance-log / enhance-settings / doctor ride in there).
//! Wrapping only `build_router_full*`'s return value silently leaves those
//! five sub-routers outside the perimeter; the integration tests in
//! `tests/access_gate.rs` pin one route per merged sub-router to a 401.
//!
//! ## Fail-closed startup
//!
//! [`validate_bind_security`] is the boot-time rule: binding a
//! non-loopback address without an access token refuses to start (the
//! caller turns the `Err` into process exit). Loopback binds keep working
//! with no token — the gate is simply not installed. `0.0.0.0` / `[::]`
//! count as non-loopback (`IpAddr::is_loopback`).
//!
//! ## Comparison discipline
//!
//! Both sides are SHA-256'd (fixed length — no length leak) and compared
//! with `subtle::ConstantTimeEq`. The presented header value is never
//! logged; a failed check is a bare 401 with no detail.

use std::net::SocketAddr;

use axum::{
    extract::Request,
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Router,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Header carrying the L0 access token. Lower-case (http crate canonical form).
pub const ACCESS_TOKEN_HEADER: &str = "x-mse-access-token";

/// The only route exempt from L0 — platform health checks can't send
/// custom headers. Its body stays minimal for the same reason.
const HEALTHZ_PATH: &str = "/v1/healthz";

/// L0 gate: holds the SHA-256 digest of the configured access token, never
/// the token itself (same at-rest discipline as the operator session
/// token's sid/digest split and the `CapToken` nonce/fingerprint split).
#[derive(Clone)]
pub struct AccessGate {
    expected_digest: [u8; 32],
}

impl AccessGate {
    /// Build a gate from the configured token value.
    pub fn new(token: &str) -> Self {
        Self {
            expected_digest: Sha256::digest(token.as_bytes()).into(),
        }
    }

    /// Constant-time check of a presented header value. `None` (header
    /// absent) is always a refusal.
    pub fn check(&self, presented: Option<&str>) -> bool {
        let Some(presented) = presented else {
            return false;
        };
        let presented_digest: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        presented_digest.ct_eq(&self.expected_digest).into()
    }
}

/// Wrap `router` with the L0 middleware when a token is configured.
///
/// `None` returns the router untouched — the loopback / no-token
/// configuration is byte-identical to today's behavior. Callers apply this
/// to the **fully merged** app (see the module doc's placement contract).
pub fn apply_access_gate(router: Router, gate: Option<AccessGate>) -> Router {
    match gate {
        None => router,
        // 32-byte digest, plain memcpy per request — no Arc needed.
        Some(gate) => router.layer(axum::middleware::from_fn(
            move |req: Request, next: Next| {
                let gate = gate.clone();
                async move { require_access_token(gate, req, next).await }
            },
        )),
    }
}

/// The middleware body: healthz exemption, then constant-time header check.
async fn require_access_token(gate: AccessGate, req: Request, next: Next) -> Response {
    if req.method() == Method::GET && req.uri().path() == HEALTHZ_PATH {
        return next.run(req).await;
    }
    let presented = req
        .headers()
        .get(ACCESS_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok());
    if gate.check(presented) {
        next.run(req).await
    } else {
        // Bare 401, no body detail: the perimeter does not explain itself.
        StatusCode::UNAUTHORIZED.into_response()
    }
}

/// Boot-time fail-closed rule (see module doc).
///
/// Returns `Err` when the bind is non-loopback and no access token is
/// configured (the caller refuses to start), and a list of warnings
/// otherwise — currently one: non-loopback with an unpinned
/// `token_secret`, which is an availability hazard (every restart
/// invalidates outstanding CapTokens), not a confidentiality one.
pub fn validate_bind_security(
    bind: &SocketAddr,
    has_access_token: bool,
    has_pinned_token_secret: bool,
) -> Result<Vec<String>, String> {
    if bind.ip().is_loopback() {
        return Ok(Vec::new());
    }
    if !has_access_token {
        return Err(format!(
            "refusing to start: bind {bind} is not loopback and no access token is configured. \
             Set `access_token` in the config file, MSE_ACCESS_TOKEN, or --access-token \
             (see mse://guides/auth-token-model), or bind to 127.0.0.1/[::1]."
        ));
    }
    let mut warnings = Vec::new();
    if !has_pinned_token_secret {
        warnings.push(format!(
            "bind {bind} is not loopback but token_secret is unpinned: it is regenerated \
             every boot, so a restart invalidates all outstanding worker CapTokens. Pin it \
             via config or --token-secret (see mse://guides/auth-token-model)."
        ));
    }
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request as HttpRequest, routing::get};
    use tower::ServiceExt;

    fn app(gate: Option<AccessGate>) -> Router {
        let router = Router::new()
            .route("/v1/healthz", get(|| async { "ok" }))
            .route("/v1/status", get(|| async { "status" }));
        apply_access_gate(router, gate)
    }

    async fn hit(router: Router, path: &str, header: Option<&str>) -> StatusCode {
        let mut req = HttpRequest::builder().uri(path);
        if let Some(v) = header {
            req = req.header(ACCESS_TOKEN_HEADER, v);
        }
        let res = router
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        res.status()
    }

    #[tokio::test]
    async fn no_gate_leaves_everything_open() {
        assert_eq!(hit(app(None), "/v1/status", None).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn gate_rejects_missing_and_wrong_token() {
        let gate = Some(AccessGate::new("s3cret"));
        assert_eq!(
            hit(app(gate.clone()), "/v1/status", None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            hit(app(gate), "/v1/status", Some("wrong")).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn gate_accepts_correct_token() {
        let gate = Some(AccessGate::new("s3cret"));
        assert_eq!(
            hit(app(gate), "/v1/status", Some("s3cret")).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn healthz_is_exempt() {
        let gate = Some(AccessGate::new("s3cret"));
        assert_eq!(hit(app(gate), "/v1/healthz", None).await, StatusCode::OK);
    }

    #[test]
    fn check_is_none_safe_and_exact() {
        let g = AccessGate::new("tok");
        assert!(!g.check(None));
        assert!(!g.check(Some("to")));
        assert!(!g.check(Some("tok ")));
        assert!(g.check(Some("tok")));
    }

    #[test]
    fn loopback_bind_needs_nothing() {
        let b: SocketAddr = "127.0.0.1:7777".parse().unwrap();
        assert_eq!(validate_bind_security(&b, false, false), Ok(Vec::new()));
        let b6: SocketAddr = "[::1]:7777".parse().unwrap();
        assert_eq!(validate_bind_security(&b6, false, false), Ok(Vec::new()));
    }

    #[test]
    fn non_loopback_without_token_refuses() {
        let b: SocketAddr = "0.0.0.0:7777".parse().unwrap();
        assert!(validate_bind_security(&b, false, true).is_err());
        let b6: SocketAddr = "[::]:7777".parse().unwrap();
        assert!(validate_bind_security(&b6, false, true).is_err());
    }

    #[test]
    fn non_loopback_with_token_warns_on_unpinned_secret() {
        let b: SocketAddr = "0.0.0.0:7777".parse().unwrap();
        let warnings = validate_bind_security(&b, true, false).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("token_secret"));
        assert_eq!(validate_bind_security(&b, true, true), Ok(Vec::new()));
    }
}
