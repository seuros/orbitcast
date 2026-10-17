//! Records the exact Rust target triple the binary is compiled for, so
//! `orbitcast doctor` can report it (the gem compares it against the platform
//! manifest). `TARGET` is what
//! cargo hands every build
//! script; `std::env::consts` cannot reproduce a full triple (vendor and ABI
//! are not exposed), which is why this is a build script and not a constant.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let target = std::env::var("TARGET").expect("cargo always sets TARGET for build scripts");
    println!("cargo:rustc-env=ORBITCAST_TARGET={target}");
}
