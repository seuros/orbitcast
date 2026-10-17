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
