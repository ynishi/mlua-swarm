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
