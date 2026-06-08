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

/// Real Apache Ozone protocols, vendored VERBATIM from apache/ozone master. The
/// submodules mirror the proto packages so the generated cross-package references
/// (`super::hdds::…`, `super::common::…`) resolve:
/// - [`hadoop::hdds`] — `hdds.proto` (`StorageContainerDatanodeProtocolProtos` shares
///   it); the datanode<->SCM wire contract.
/// - [`hadoop::ozone`] — `OmClientProtocol.proto` (`OzoneManagerProtocolProtos`); the
///   OM client wire contract (`OzoneManagerService.submitRequest(OMRequest)`) the Rust
///   S3 gateway speaks so a real OM needs only its existing gRPC transport.
/// - [`hadoop::common`] — `Security.proto` (`SecurityProtos`), imported by the OM proto.
pub mod hadoop {
    pub mod hdds {
        tonic::include_proto!("hadoop.hdds");
    }
    pub mod ozone {
        tonic::include_proto!("hadoop.ozone");
    }
    pub mod common {
        tonic::include_proto!("hadoop.common");
    }
}

pub mod conv;
