//! Secret redaction contracts.

use secrecy::ExposeSecret;
use watchdog_domain::SecretText;

#[test]
fn secret_debug_output_is_redacted() {
    let secret = SecretText::new("credential-value");
    let debug = format!("{secret:?}");

    assert!(!debug.contains(secret.expose_secret()));
    assert!(debug.contains("REDACTED"));
}
