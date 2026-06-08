//! Generated gRPC types for the three Ozone Rust services.
//!
//! `tonic-build` runs in `build.rs` and emits one module per proto package.
//! See `proto/` for the source `.proto` files and the Phase 3 Wire Protocol
//! RFC in the notetaker vault.

#![allow(clippy::all, missing_docs)]

/// Datanode <-> Gateway protocol (package `org.apache.ozone.dn.v1`).
pub mod dn {
    pub mod v1 {
        tonic::include_proto!("org.apache.ozone.dn.v1");
    }
}

/// OM <-> Rust Gateway protocol (package `org.apache.ozone.om.gw.v1`).
pub mod om {
    pub mod gw {
        pub mod v1 {
            tonic::include_proto!("org.apache.ozone.om.gw.v1");
        }
    }
}

/// SCM <-> Rust Datanode protocol (package `org.apache.ozone.scm.dn.v1`).
pub mod scm {
    pub mod dn {
        pub mod v1 {
            tonic::include_proto!("org.apache.ozone.scm.dn.v1");
        }
    }
}

/// Real Apache Ozone SCM <-> Datanode protocol, vendored VERBATIM from
/// apache/ozone master (`StorageContainerDatanodeProtocolProtos` + `hdds.proto`,
/// package `hadoop.hdds`). This is the compliant wire contract the Rust datanode
/// speaks so the real SCM needs only a thin gRPC transport adapter.
pub mod hadoop {
    pub mod hdds {
        tonic::include_proto!("hadoop.hdds");
    }
}

pub mod conv;
