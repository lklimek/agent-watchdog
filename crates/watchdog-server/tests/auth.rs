//! MCP/API Bearer authentication tests.

use watchdog_server::{
    BasicAuthError, BasicAuthenticator, BearerAuthError, BearerAuthenticator,
    MAX_AUTHORIZATION_BYTES,
};

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

#[test]
fn basic_authentication_is_exact_bounded_and_redacted() {
    let auth =
        BasicAuthenticator::new("watchdog", "correct-secret").expect("credentials should be valid");

    assert!(auth.authorize(Some(b"Basic d2F0Y2hkb2c6Y29ycmVjdC1zZWNyZXQ=")));
    assert!(!auth.authorize(None));
    assert!(!auth.authorize(Some(b"basic d2F0Y2hkb2c6Y29ycmVjdC1zZWNyZXQ=")));
    assert!(!auth.authorize(Some(b"Basic d2F0Y2hkb2c6d3Jvbmc=")));
    assert!(!auth.authorize(Some(&vec![b'x'; MAX_AUTHORIZATION_BYTES + 1])));
    assert!(!format!("{auth:?}").contains("correct-secret"));
}

#[test]
fn invalid_basic_auth_configuration_is_rejected() {
    assert!(matches!(
        BasicAuthenticator::new("", "password"),
        Err(BasicAuthError::Empty)
    ));
    assert!(matches!(
        BasicAuthenticator::new("user:name", "password"),
        Err(BasicAuthError::UsernameContainsColon)
    ));
    assert!(matches!(
        BasicAuthenticator::new("user", &"x".repeat(MAX_AUTHORIZATION_BYTES)),
        Err(BasicAuthError::TooLong)
    ));
}
