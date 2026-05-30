//! bindgen-generated FFI to Intel ISA-L (Reed-Solomon erasure coding).
//!
//! See: notetaker/Projects/Apache Ozone/S3 Gateway Rust/
//!      2026-05-23 EC Implementation Spec ISA-L FFI.md

#![allow(unsafe_code, non_camel_case_types, non_upper_case_globals, non_snake_case)]
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/isa_l_bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    /// Cauchy generator matrix for k=2, p=2: 4 rows x 2 cols (matrix of bytes).
    /// `gf_gen_cauchy1_matrix` is the foundation for byte-equivalence with the
    /// Java path; if this call links and the data array is non-zero, the FFI
    /// is wired correctly.
    #[test]
    fn cauchy_matrix_2_2_compiles_and_links() {
        const K: usize = 2;
        const P: usize = 2;
        let mut matrix = [0u8; (K + P) * K];
        unsafe {
            gf_gen_cauchy1_matrix(matrix.as_mut_ptr(), (K + P) as i32, K as i32);
        }
        // The top KxK rows of the encoding matrix are the identity matrix:
        // [1, 0]
        // [0, 1]
        assert_eq!(matrix[0], 1);
        assert_eq!(matrix[1], 0);
        assert_eq!(matrix[2], 0);
        assert_eq!(matrix[3], 1);
        // The bottom P rows are the Cauchy parity matrix; must not be all zeros.
        let parity = &matrix[(K * K)..];
        assert!(parity.iter().any(|&b| b != 0), "parity rows all zero");
    }
}
