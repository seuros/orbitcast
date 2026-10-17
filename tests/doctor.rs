//! `orbitcast doctor` and `orbitcast --version`, driven through the real
//! binary: the gem runs `doctor --json` to verify an overridden binary, and
//! the release smoke compares it with the manifest.
//!
//! Every invocation starts from an empty environment so the developer's shell
//! (`ORBITCAST_*`, `MS_*`) never leaks into an expectation.

use std::process::{Command, Output};

use serde_json::Value;

fn orbitcast() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orbitcast"));
    command.env_clear();
    command
}

fn run(command: &mut Command) -> Output {
    command.output().expect("the orbitcast binary runs")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

fn json(output: &Output) -> Value {
    serde_json::from_str(&stdout(output)).unwrap_or_else(|err| {
        panic!("doctor --json must print one JSON object: {err}\n{}", stdout(output))
    })
}

/// A loopback port nothing listens on: bound, read, released.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[test]
fn version_prints_exactly_name_and_version() {
    let output = run(orbitcast().arg("--version"));
    assert!(output.status.success());
    assert_eq!(stdout(&output), format!("orbitcast {}\n", env!("CARGO_PKG_VERSION")));
}

#[test]
fn doctor_json_with_no_configuration_at_all() {
    // What the gem's binary verification sees: no Rails, no database, no
    // environment. Exit 0, one JSON object, the fields it compares.
    let output = run(orbitcast().args(["doctor", "--json"]));
    assert!(output.status.success(), "doctor diagnoses, it does not gate: {output:?}");

    let report = json(&output);
    assert_eq!(report["version"], env!("CARGO_PKG_VERSION"));
    assert!(report["target"].as_str().unwrap().contains('-'), "{}", report["target"]);
    assert!(report["envelope_versions"].as_array().unwrap().contains(&Value::from(1)));
    let features: Vec<&str> =
        report["features"].as_array().unwrap().iter().map(|f| f.as_str().unwrap()).collect();
    assert_ne!(features.len(), 0);
    assert!(features.iter().all(|f| ["postgres", "memory"].contains(f)), "{features:?}");

    assert_eq!(report["database"]["status"], "not_configured");
    assert_eq!(report["validation"]["ok"], false, "no transport is reported, not fatal");
    assert!(report["transports"]["listen_addr"].is_null());
    assert!(report["transports"]["docking"].is_null());
    assert_eq!(report["keys"]["streams"]["configured"], false);
    assert_eq!(report["keys"]["turbo"]["enabled"], false);
}

#[test]
fn doctor_reports_an_unreachable_database_without_the_password_and_exits_zero() {
    let port = closed_port();
    let url = format!("postgres://orbitcast:hunter2-secret@127.0.0.1:{port}/x");

    for json_flag in [true, false] {
        let mut command = orbitcast();
        command.arg("doctor");
        if json_flag {
            command.arg("--json");
        }
        command
            .env("ORBITCAST_DATABASE_URL", &url)
            .env("ORBITCAST_NAMESPACE", "x")
            .env("ORBITCAST_LISTEN_ADDR", "127.0.0.1:0")
            .env("ORBITCAST_STREAMS_SECRET_BASE64", "c3RyZWFtcy1rZXk=")
            .env("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64", "b2xkLWtleQ==,b2xkZXIta2V5")
            .env("ORBITCAST_ALLOWED_ORIGINS", "https://App.Example/,https://b.example");
        let output = run(&mut command);
        assert!(output.status.success(), "{output:?}");
        let text = stdout(&output);
        assert!(!text.contains("hunter2"), "password leaked:\n{text}");
        assert!(!text.contains("streams-key") && !text.contains("old-key"), "key leaked:\n{text}");
        assert!(text.contains("[REDACTED]"), "{text}");

        if json_flag {
            let report = json(&output);
            assert_eq!(report["namespace"], "x");
            assert_eq!(
                report["database"]["url"],
                format!("postgres://orbitcast:[REDACTED]@127.0.0.1:{port}/x")
            );
            let status = report["database"]["status"].as_str().unwrap();
            // A postgres build probes; a memory-only build says it cannot.
            assert!(["unreachable", "unsupported_by_build"].contains(&status), "{status}");
            assert!(report["database"]["error"].is_string());
            assert_eq!(report["keys"]["streams"]["configured"], true);
            assert_eq!(report["keys"]["streams"]["previous_keys"], 2);
            assert_eq!(
                report["origins"],
                serde_json::json!(["https://app.example", "https://b.example"])
            );
            assert_eq!(report["transports"]["listen_addr"], "127.0.0.1:0");
        } else {
            assert!(
                text.starts_with(&format!("orbitcast {} (", env!("CARGO_PKG_VERSION"))),
                "{text}"
            );
            assert!(text.contains("configured (+2 previous)"), "{text}");
        }
    }
}

#[test]
fn doctor_exits_two_when_the_configuration_cannot_be_parsed() {
    let output = run(orbitcast()
        .args(["doctor", "--json"])
        .env("ORBITCAST_STREAMS_SECRET_BASE64", "!!!not-base64!!!"));
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let report = json(&output);
    assert_eq!(report["version"], env!("CARGO_PKG_VERSION"));
    assert!(report["config_error"].as_str().unwrap().contains("ORBITCAST_STREAMS_SECRET_BASE64"));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cannot be parsed"), "{stderr}");
}

#[test]
fn doctor_reports_validation_failures_without_gating() {
    // Public streams without the insecure acknowledgement: refused by
    // validation, reported by doctor, exit 0.
    let output = run(orbitcast()
        .args(["doctor", "--json"])
        .env("ORBITCAST_LISTEN_ADDR", "127.0.0.1:0")
        .env("ORBITCAST_NAMESPACE", "x")
        .env("ORBITCAST_DATABASE_URL", format!("postgres://u:p@127.0.0.1:{}/x", closed_port()))
        .env("ORBITCAST_PUBLIC_STREAMS", "true"));
    assert!(output.status.success());
    let report = json(&output);
    assert_eq!(report["validation"]["ok"], false);
    assert!(
        report["validation"]["error"].as_str().unwrap().contains("public_streams_insecure"),
        "{}",
        report["validation"]
    );
    assert_eq!(report["keys"]["public_streams"]["enabled"], true);
    assert_eq!(report["keys"]["public_streams"]["insecure_acknowledged"], false);
}
