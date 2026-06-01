//! Pinned EC golden vectors: lock the exact parity bytes our ISA-L Cauchy
//! encoder produces for fixed inputs.
//!
//! These bytes are a deterministic function of ISA-L's Cauchy generator matrix
//! (`gf_gen_cauchy1_matrix`) plus GF(2^8) arithmetic, so they are stable across
//! ISA-L versions. They serve two purposes:
//!  1. Regression guard — any change that alters EC output bytes fails here.
//!  2. The reference for byte-equivalence with Apache Ozone's *native* coder,
//!     which is ISA-L (the same Cauchy matrix) via JNI. The Java cross-check
//!     procedure (and the matrix-construction caveat for the pure-Java coder)
//!     lives in `ec-equivalence/README.md`.
//!
//! Inputs are deterministic so a Java dumper can reproduce them exactly:
//! shard `i`, byte `j` = `(i*37 + j*5 + 1) mod 256`, for `len = 32`.

use isa_l_safe::{EcConfig, Encoder};

fn make_data(k: usize, len: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| (0..len).map(|j| ((i * 37 + j * 5 + 1) & 0xff) as u8).collect())
        .collect()
}

fn encode_hex(k: usize, p: usize, len: usize) -> Vec<String> {
    let enc = Encoder::new(EcConfig { data: k, parity: p }).unwrap();
    let data = make_data(k, len);
    let drefs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut par = vec![vec![0u8; len]; p];
    {
        let mut prefs: Vec<&mut [u8]> = par.iter_mut().map(|v| v.as_mut_slice()).collect();
        enc.encode(len, &drefs, &mut prefs).unwrap();
    }
    par.iter()
        .map(|pp| pp.iter().map(|b| format!("{b:02x}")).collect())
        .collect()
}

#[test]
fn golden_rs_3_2() {
    assert_eq!(
        encode_hex(3, 2, 32),
        vec![
            "acc9bf35ba08b5d8b8c540c1560f9aef64212746c95bd650503d1372f58c397c",
            "796ba7e52f9547ab36751f7140ee86c4ae47195399d8e1058892c0751dab3891",
        ]
    );
}

#[test]
fn golden_rs_6_3() {
    assert_eq!(
        encode_hex(6, 3, 32),
        vec![
            "d43aa7ed7782130104b998feedae73bc17f16ccdc5d34dfd0b58fb855899e747",
            "65d4f4513e658f40417af210b00c24bd759d56c69183d2cb31d17131817a7b19",
            "d9c367a7f7d8f0f24bda852938ed6c3c16aed0e71b72678b2dfe57832b5efb86",
        ]
    );
}

#[test]
fn golden_rs_10_4() {
    assert_eq!(
        encode_hex(10, 4, 32),
        vec![
            "6c902341f408522e1d13888c75a9638dce2e1853ee119ccbd300c091dce343d1",
            "b763d587016499ab07026f7a11bbf9a28ae00579c2a2d2b84e5302a65021299e",
            "c1dea334e288549c54aa6efd534f980b5fd4c943b2e5fe506bd1f1a4c9faf04e",
            "482894defb0a343284b7bd522c51c907ab1edf9987aed1e85a0d3c033f893255",
        ]
    );
}
