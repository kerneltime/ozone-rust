//! B0 compliance smoke test for the VENDORED real Apache Ozone SCM<->datanode
//! protos. Proves the generated types round-trip on the wire and that the
//! EC-critical encodings match the real contract EXACTLY: the byte-per-index
//! `missingContainerIndexes`, the `SCMCommandProto.Type` enum values (the enum
//! value 11 is distinct from the message field tag 12), and the
//! `ContainerReplicaProto.State` ordinals that SCM's replica-count math switches on.

use ozone_grpc_types::hadoop::hdds as oz;
use prost::Message;

#[test]
fn reconstruct_ec_command_roundtrips_with_byte_per_index() {
    let cmd = oz::ReconstructEcContainersCommandProto {
        container_id: 42,
        sources: Vec::new(),
        targets: Vec::new(),
        // One RAW byte per 1-based EC slot (not varints, not text, not a bitmap):
        // exactly how SCM's integers2ByteString packs it.
        missing_container_indexes: vec![1u8, 4u8],
        ec_replication_config: oz::EcReplicationConfig {
            data: 3,
            parity: 2,
            codec: "rs".to_string(),
            ec_chunk_size: 1024,
        },
        cmd_id: 7,
    };
    let back =
        oz::ReconstructEcContainersCommandProto::decode(&cmd.encode_to_vec()[..]).unwrap();
    assert_eq!(
        back.missing_container_indexes,
        vec![1u8, 4u8],
        "byte-per-index missingContainerIndexes must survive the wire verbatim"
    );
    assert_eq!(back.container_id, 42);
    assert_eq!(back.cmd_id, 7);
    assert_eq!(back.ec_replication_config.data, 3);
    assert_eq!(back.ec_replication_config.codec, "rs");
}

#[test]
fn scm_command_type_enum_matches_real_ozone() {
    use oz::scm_command_proto::Type;
    // Enum VALUE 11 (distinct from the message field tag 12 — must not conflate).
    assert_eq!(Type::ReconstructEcContainersCommand as i32, 11);
    assert_eq!(Type::CloseContainerCommand as i32, 3);
    assert_eq!(Type::DeleteContainerCommand as i32, 4);
    assert_eq!(Type::UnknownScmCommand as i32, 0);
}

#[test]
fn container_replica_state_enum_matches_real_ozone() {
    use oz::container_replica_proto::State;
    // The exact ordinals SCM's ECContainerReplicaCount switches on; a replica is a
    // reconstruction source only when CLOSED(4).
    assert_eq!(State::Open as i32, 1);
    assert_eq!(State::Closed as i32, 4);
    assert_eq!(State::Unhealthy as i32, 5);
    assert_eq!(State::Invalid as i32, 6);
    assert_eq!(State::Deleted as i32, 7);
}

#[test]
fn datanode_request_routes_by_cmd_type() {
    let req = oz::ScmDatanodeRequest {
        cmd_type: oz::Type::SendHeartbeat as i32,
        trace_id: None,
        get_version_request: None,
        register_request: None,
        send_heartbeat_request: Some(oz::ScmHeartbeatRequestProto {
            datanode_details: oz::DatanodeDetailsProto::default(),
            ..Default::default()
        }),
    };
    let back = oz::ScmDatanodeRequest::decode(&req.encode_to_vec()[..]).unwrap();
    assert_eq!(back.cmd_type, oz::Type::SendHeartbeat as i32);
    assert!(back.send_heartbeat_request.is_some());
    assert!(back.register_request.is_none());
}
