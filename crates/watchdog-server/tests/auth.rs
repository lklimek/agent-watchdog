//! MCP/API Bearer authentication tests.

use watchdog_server::{BearerAuthError, BearerAuthenticator, MAX_AUTHORIZATION_BYTES};

#[test]
fn bearer_authentication_fails_closed_without_reflecting_secret() {
    let auth = BearerAuthenticator::new("correct-secret").expect("token should be valid");

    assert!(auth.authorize(Some(b"Bearer correct-secret")));
    assert!(!auth.authorize(None));
    assert!(!auth.authorize(Some(b"")));
    assert!(!auth.authorize(Some(b"correct-secret")));
    assert!(!auth.authorize(Some(b"bearer correct-secret")));
    assert!(!auth.authorize(Some(b"Bearer wrong-secret")));
    assert!(!auth.authorize(Some(&vec![b'x'; MAX_AUTHORIZATION_BYTES + 1])));
    assert!(!format!("{auth:?}").contains("correct-secret"));
}

#[test]
fn invalid_configured_tokens_are_rejected() {
    assert!(matches!(
        BearerAuthenticator::new(""),
        Err(BearerAuthError::Empty)
    ));
    assert!(matches!(
        BearerAuthenticator::new("x".repeat(MAX_AUTHORIZATION_BYTES)),
        Err(BearerAuthError::TooLong)
    ));
    assert!(matches!(
        BearerAuthenticator::new("line\nbreak"),
        Err(BearerAuthError::InvalidCharacters)
    ));
}
