use super::*;

#[test]
fn sslmode_is_parsed_from_the_dsn() {
    const CASES: &[(&str, Option<SslMode>)] = &[
        ("postgres://h/db", None),
        ("postgres://h/db?sslmode=disable", Some(SslMode::Disable)),
        ("postgres://h/db?a=1&SSLMODE=Verify-Full", Some(SslMode::VerifyFull)),
        ("postgres://h/db?&sslmode=verify_ca", Some(SslMode::VerifyCa)),
        ("postgres://h/db?sslmode", None),
        ("postgres://h/db?sslmode=weird", None),
    ];
    for (dsn, expected) in CASES {
        assert_eq!(sslmode_from_url(dsn), *expected, "dsn: {dsn}");
    }
}

/// Every TLS attempt verifies the certificate (and, but for `verify-ca`, the
/// host name), so no mode ever accepts an unverified server; `prefer` and
/// `allow` fall back between plain text and verified TLS in libpq's order.
#[test]
fn sslmode_selects_the_transports_in_order() {
    const FULL: Transport = Transport::Tls { verify_host: true };
    let cases: &[(SslMode, &[Transport])] = &[
        (SslMode::Disable, &[Transport::Plain]),
        (SslMode::Allow, &[Transport::Plain, FULL]),
        (SslMode::Prefer, &[FULL, Transport::Plain]),
        (SslMode::Require, &[FULL]),
        (SslMode::VerifyCa, &[Transport::Tls { verify_host: false }]),
        (SslMode::VerifyFull, &[FULL]),
    ];
    for (mode, expected) in cases {
        assert_eq!(transports(*mode), *expected, "{mode:?}");
    }
}

#[test]
fn an_unparseable_dsn_fails_before_any_connection() {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let error = runtime.block_on(connect_with_sslmode("not a url")).unwrap_err();
    assert_ne!(error.to_string(), "");
}
