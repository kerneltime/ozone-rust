//! B0 compliance smoke test for the VENDORED real Apache Ozone OM client proto
//! (`OmClientProtocol.proto`, package `hadoop.ozone`). Proves the generated types
//! round-trip on the wire and that the contract the Rust S3 gateway will depend on
//! holds EXACTLY: the `Type` discriminators that multiplex the single
//! `submitRequest(OMRequest)`, the 1-based `Status` enum (success is `status == OK`,
//! not a zero default), the envelope's required `clientId`, and the EC write-path
//! shape where `KeyArgs` carries the EC config and each `KeyLocation` names a block
//! (container+local id) and its datanode `Pipeline`. Cross-package references
//! (`hadoop.ozone` -> `hadoop.hdds` / `hadoop.common`) are exercised by construction.

use ozone_grpc_types::hadoop::hdds as hdds;
use ozone_grpc_types::hadoop::ozone as oz;
use prost::Message;

#[test]
fn om_type_enum_matches_real_ozone() {
    // The exact discriminators that route submitRequest server-side. Wrong values
    // would dispatch the wrong handler. (Numbers are 1-based and sparse.)
    assert_eq!(oz::Type::CreateBucket as i32, 21);
    assert_eq!(oz::Type::InfoBucket as i32, 22);
    assert_eq!(oz::Type::DeleteBucket as i32, 24);
    assert_eq!(oz::Type::ListBuckets as i32, 25);
    assert_eq!(oz::Type::CreateKey as i32, 31);
    assert_eq!(oz::Type::LookupKey as i32, 32);
    assert_eq!(oz::Type::DeleteKey as i32, 34);
    assert_eq!(oz::Type::ListKeys as i32, 35);
    assert_eq!(oz::Type::CommitKey as i32, 36);
    assert_eq!(oz::Type::AllocateBlock as i32, 37);
    assert_eq!(oz::Type::InitiateMultiPartUpload as i32, 45);
    // The S3 read entrypoint (distinct from LookupKey, the FS path).
    assert_eq!(oz::Type::GetKeyInfo as i32, 111);
}

#[test]
fn om_status_enum_is_one_based() {
    // Success is OK(1), NOT a zero default — the client must check `status == OK`,
    // because a default-constructed/zero status is NOT success.
    assert_eq!(oz::Status::Ok as i32, 1);
    assert_eq!(oz::Status::BucketNotFound as i32, 8);
    assert_eq!(oz::Status::KeyNotFound as i32, 12);
    assert_eq!(oz::Status::BucketAlreadyExists as i32, 10);
    assert_eq!(oz::Status::VolumeNotFound as i32, 3);
}

#[test]
fn om_request_envelope_routes_by_cmd_type() {
    // The single submitRequest is multiplexed by cmdType; the matching typed
    // sub-request rides alongside. clientId is REQUIRED (non-Option) on the wire.
    let req = oz::OmRequest {
        cmd_type: oz::Type::CreateKey as i32,
        client_id: "rust-s3g-0001".to_string(),
        version: Some(1),
        create_key_request: Some(oz::CreateKeyRequest {
            key_args: oz::KeyArgs {
                volume_name: "s3v".to_string(),
                bucket_name: "bkt".to_string(),
                key_name: "obj".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }),
        // Security-off auth: the principal the trusted proxy attests.
        s3_authentication: Some(oz::S3Authentication {
            access_id: Some("the-principal".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let back = oz::OmRequest::decode(&req.encode_to_vec()[..]).unwrap();
    assert_eq!(back.cmd_type, oz::Type::CreateKey as i32);
    assert_eq!(back.client_id, "rust-s3g-0001");
    assert!(back.create_key_request.is_some(), "the typed sub-request rides the envelope");
    assert!(back.commit_key_request.is_none(), "other sub-requests stay unset");
    assert_eq!(
        back.s3_authentication.unwrap().access_id.as_deref(),
        Some("the-principal"),
        "the attested principal survives the wire"
    );
    let ka = back.create_key_request.unwrap().key_args;
    assert_eq!(ka.volume_name, "s3v");
    assert_eq!(ka.key_name, "obj");
}

#[test]
fn ec_write_path_shape_roundtrips() {
    // The write path: KeyArgs carries the EC config (cross-package hadoop.hdds), and
    // each KeyLocation names a block (container+local id) plus the datanode Pipeline
    // the gateway writes shards to. This is the bridge from OM to the Track-2 data
    // plane.
    let ka = oz::KeyArgs {
        volume_name: "s3v".to_string(),
        bucket_name: "bkt".to_string(),
        key_name: "obj".to_string(),
        data_size: Some(1024),
        r#type: Some(hdds::ReplicationType::Ec as i32),
        ec_replication_config: Some(hdds::EcReplicationConfig {
            data: 3,
            parity: 2,
            codec: "rs".to_string(),
            ec_chunk_size: 1048576,
        }),
        key_locations: vec![oz::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 7,
                    local_id: 99,
                },
                block_commit_sequence_id: None,
            },
            offset: 0,
            length: 512,
            pipeline: Some(hdds::Pipeline::default()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let back = oz::KeyArgs::decode(&ka.encode_to_vec()[..]).unwrap();
    let ec = back.ec_replication_config.expect("EC config rides KeyArgs");
    assert_eq!(ec.data, 3);
    assert_eq!(ec.parity, 2);
    assert_eq!(ec.codec, "rs");
    assert_eq!(back.r#type, Some(hdds::ReplicationType::Ec as i32));
    let loc = &back.key_locations[0];
    assert_eq!(loc.block_id.container_block_id.container_id, 7);
    assert_eq!(loc.block_id.container_block_id.local_id, 99);
    assert_eq!(loc.length, 512);
    assert!(loc.pipeline.is_some(), "each block names its datanode pipeline");
}
