//! Build script for `isa-l-sys`.
//!
//! Locates Intel ISA-L (the static or dynamic library + `erasure_code.h`)
//! and runs `bindgen` to generate Rust FFI bindings.
//!
//! Library discovery order:
//!   1. `pkg-config --libs libisal` if pkg-config is on PATH and finds it
//!   2. environment variables `ISA_L_INCLUDE_DIR` + `ISA_L_LIB_DIR`
//!   3. fallback: standard system paths (`/usr/include`, `/usr/local/include`,
//!      `/opt/homebrew/include`, `/home/linuxbrew/.linuxbrew/include`)
//!
//! On any platform that ships ISA-L as `pkg-config libisal`, the default
//! path works.

use std::env;
use std::path::PathBuf;

fn main() {
    let mut include_dirs: Vec<PathBuf> = Vec::new();

    // 1. pkg-config. Prefer static linking — avoids rpath plumbing
    //    through every downstream crate's test/bin executable.
    let pkg_config_result = pkg_config::Config::new()
        .atleast_version("2.31")
        .statik(true)
        .probe("libisal");

    match pkg_config_result {
        Ok(lib) => {
            for inc in &lib.include_paths {
                include_dirs.push(inc.clone());
                println!("cargo:include={}", inc.display());
            }
            for link in &lib.link_paths {
                println!("cargo:rustc-link-search=native={}", link.display());
                // Embed each link path as an rpath so the test/runtime
                // executable can find libisal.so without LD_LIBRARY_PATH.
                println!("cargo:rustc-link-arg=-Wl,-rpath,{}", link.display());
            }
            // pkg-config emits the rustc-link-lib lines for us via its
            // higher-level API; nothing more to do for linking here.
        }
        Err(e) => {
            if cfg!(feature = "strict-pkg-config") {
                panic!("pkg-config failed and strict mode is on: {e}");
            }
            eprintln!("isa-l-sys: pkg-config did not find libisal: {e}");
            eprintln!("isa-l-sys: falling back to env vars / system paths");

            // 2. env vars
            if let Ok(inc) = env::var("ISA_L_INCLUDE_DIR") {
                include_dirs.push(PathBuf::from(inc));
            }
            if let Ok(libdir) = env::var("ISA_L_LIB_DIR") {
                println!("cargo:rustc-link-search=native={libdir}");
            }

            // 3. common system paths
            for p in [
                "/usr/include",
                "/usr/local/include",
                "/opt/homebrew/include",
                "/home/linuxbrew/.linuxbrew/include",
            ] {
                let pb = PathBuf::from(p);
                if pb.exists() {
                    include_dirs.push(pb);
                }
            }

            // Force the link line even without pkg-config.
            println!("cargo:rustc-link-lib=isal");
        }
    }

    // bindgen
    let header = locate_header(&include_dirs).unwrap_or_else(|| {
        panic!(
            "could not locate `isa-l/erasure_code.h` in any of: {:?}",
            include_dirs
        )
    });

    let mut builder = bindgen::Builder::default()
        .header(header.to_string_lossy().into_owned())
        .clang_arg("-D_GNU_SOURCE")
        .allowlist_function("ec_.*")
        .allowlist_function("gf_.*")
        .allowlist_var("MMAX|KMAX")
        .layout_tests(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    for inc in &include_dirs {
        builder = builder.clang_arg(format!("-I{}", inc.display()));
    }

    let bindings = builder.generate().expect("bindgen failed");

    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"))
        .join("isa_l_bindings.rs");
    bindings
        .write_to_file(&out_path)
        .expect("could not write bindings");

    println!("cargo:rerun-if-changed={}", header.display());
    println!("cargo:rerun-if-env-changed=ISA_L_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=ISA_L_LIB_DIR");
}

fn locate_header(include_dirs: &[PathBuf]) -> Option<PathBuf> {
    for inc in include_dirs {
        for candidate in [
            inc.join("isa-l").join("erasure_code.h"),
            inc.join("isa-l/erasure_code.h"),
            inc.join("erasure_code.h"),
        ] {
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}
