//! A compliant fake Ozone Manager speaking the REAL Apache Ozone
//! `OzoneManagerService.submitRequest(OMRequest) -> OMResponse` contract
//! (vendored `ozone_grpc_types::hadoop::ozone`), the OM-side COMPLIANCE harness
//! for the Rust S3 gateway's `OzoneOmClient`.
//!
//! This mirrors the bespoke [`crate::fake_om::FakeOm`] in-memory semantics — a
//! bucket set, a key map keyed by `(volume, bucket, key)`, monotonic
//! `local_id`/`client_id` counters, and exactly ONE statically configured EC
//! pipeline where `datanodes[i]` holds EC slot `i + 1` — but emits the ACTUAL OM
//! wire shapes (`OmRequest`/`OmResponse` multiplexed by `cmdType`, `KeyInfo`,
//! `KeyLocation`, `hdds::Pipeline`) instead of the bespoke `om::gw::v1` ones.
//!
//! # What this fixture is and is NOT
//! - It is enough OM to drive the gateway's CORE path: bucket head/create/delete/
//!   list and the key write/read/delete cycle (`CreateKey` -> `AllocateBlock`* ->
//!   `CommitKey`, then `GetKeyInfo` / `DeleteKey`). Multipart and `ListKeys` are
//!   OUT OF SCOPE here and return `INVALID_REQUEST` like any other unimplemented
//!   `cmdType` (no panic).
//! - It does NO auth checking: with Ozone security off the gateway attests the
//!   principal via `s3Authentication.accessId` and the OM trusts it. The fixture
//!   records the request verbatim so a test can assert the principal was sent,
//!   but it never rejects on auth.
//! - It performs NO placement, exclusion-list handling, or failover: every block
//!   lands on the one configured pipeline.
//!
//! # Invariants (mirrors of `FakeOm`'s, restated for the real proto)
//! - W3 EC slot fidelity: the emitted [`hdds::Pipeline`] carries
//!   `member_replica_indexes` PARALLEL to `members`, with
//!   `member_replica_indexes[i] == i + 1`, because `datanodes[i]` holds EC slot
//!   `i + 1`. A mismatch silently swaps data/parity shards on read.
//! - W2 commit lists what was written: `CommitKey` stores the request's
//!   `keyArgs.key_locations` VERBATIM (the actual written blocks/lengths), never
//!   a re-derived pre-allocation, so a later `GetKeyInfo` returns the real
//!   layout.
//! - Monotonic ids: `next_local_id` (block local id) and `next_client_id`
//!   (the `CreateKey` open-session id) each start at 1 and only increase;
//!   `next_container_id` starts at 1 and increases per `CreateKey` so distinct
//!   keys do not collide on `(container_id, local_id)`.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use ozone_grpc_types::hadoop::hdds as hdds;
use ozone_grpc_types::hadoop::ozone as oz;
use oz::ozone_manager_service_server::{OzoneManagerService, OzoneManagerServiceServer};

/// Fixed pipeline id reported in every emitted [`hdds::Pipeline`]. A single
/// non-HA pipeline needs only a stable identifier.
const PIPELINE_ID: &str = "pl-1";

/// Fixed object creation/modification time (2021-01-01T00:00:00Z in ms). `KeyInfo`
/// marks `creation_time`/`modification_time` as proto2 `required`, so they MUST be
/// set; a constant non-zero value keeps timestamp-derived behavior (Last-Modified,
/// date-conditional requests) deterministic yet exercisable.
const FAKE_OBJECT_TIME_MS: u64 = 1_609_459_200_000;

/// The one static EC pipeline a [`CompliantOm`] allocates from.
///
/// The datanode order is load-bearing: `datanodes[i]` holds EC replica slot
/// `i + 1` (1-based; data shards `1..=k`, parity `k+1..=k+p`). Each
/// [`hdds::DatanodeDetailsProto`] must carry a `REPLICATION` port (the name the
/// Rust datanode advertises to SCM in Track 2) whose value is that datanode's
/// data port, so the gateway can dial it for shard I/O.
pub struct CompliantOmPipeline {
    /// Ordered datanodes; position `i` (0-based) owns EC replica slot `i + 1`.
    pub datanodes: Vec<hdds::DatanodeDetailsProto>,
    /// Erasure-coding parameters of this pipeline (real wire form).
    pub ec: hdds::EcReplicationConfig,
}

/// Mutable in-memory OM state, guarded by a single `parking_lot::Mutex`.
///
/// A single coarse lock is intentional: this is a test fixture, contention is
/// irrelevant, and one lock keeps the dispatch handlers trivially correct under
/// concurrent gateway calls.
struct OmState {
    /// Buckets that exist, keyed by `(volume, bucket)`. Backs
    /// CreateBucket/InfoBucket/DeleteBucket/ListBuckets.
    buckets: HashSet<(String, String)>,
    /// Committed keys, addressed by `(volume, bucket, key)`. The stored
    /// [`oz::KeyInfo`] carries the ACTUAL committed locations (W2).
    keys: HashMap<(String, String, String), oz::KeyInfo>,
    /// Next block `local_id` to hand out; first allocation returns 1.
    next_local_id: i64,
    /// Next `CreateKey` open-session id (the response `id`); first call returns 1.
    next_client_id: u64,
    /// Next container id to hand out; bumped once per `CreateKey` so blocks of
    /// distinct keys do not collide on `(container_id, local_id)`. First key uses 1.
    next_container_id: i64,
}

/// A compliant fake OM. Transfer ownership to a tonic server via
/// [`CompliantOm::into_server`]; clone [`CompliantOm::record`] BEFORE that to
/// retain the inspection handle.
pub struct CompliantOm {
    /// Every [`oz::OmRequest`] received, in arrival order (decoded real proto).
    /// Lets a test assert the envelope the client built (cmdType, clientId,
    /// `s3Authentication.accessId`, the typed sub-request).
    record: Arc<Mutex<Vec<oz::OmRequest>>>,
    state: Mutex<OmState>,
    pipeline: CompliantOmPipeline,
}

impl CompliantOm {
    /// Create a fake OM that allocates every block from `pipeline`.
    pub fn new(pipeline: CompliantOmPipeline) -> Self {
        Self {
            record: Arc::new(Mutex::new(Vec::new())),
            state: Mutex::new(OmState {
                buckets: HashSet::new(),
                keys: HashMap::new(),
                next_local_id: 1,
                next_client_id: 1,
                next_container_id: 1,
            }),
            pipeline,
        }
    }

    /// Handle to inspect every [`oz::OmRequest`] the client sent, in arrival
    /// order. Clone BEFORE [`Self::into_server`] (which consumes `self`).
    pub fn record(&self) -> Arc<Mutex<Vec<oz::OmRequest>>> {
        self.record.clone()
    }

    /// Consume into a tonic server for `Server::add_service`.
    pub fn into_server(self) -> OzoneManagerServiceServer<Self> {
        OzoneManagerServiceServer::new(self)
    }

    /// Build the static pipeline message: the configured datanodes plus the
    /// parallel `member_replica_indexes` where index `i` is `i + 1`.
    ///
    /// W3: `member_replica_indexes` is PARALLEL to `members`, and
    /// `member_replica_indexes[i] == i + 1` because `datanodes[i]` holds EC slot
    /// `i + 1`. The gateway maps shard `s` to the member whose index is `s`.
    fn pipeline_message(&self) -> hdds::Pipeline {
        let n = self.pipeline.datanodes.len() as u32;
        hdds::Pipeline {
            members: self.pipeline.datanodes.clone(),
            member_replica_indexes: (1..=n).collect(),
            ec_replication_config: Some(self.pipeline.ec.clone()),
            r#type: Some(hdds::ReplicationType::Ec as i32),
            id: hdds::PipelineId {
                id: Some(PIPELINE_ID.to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Allocate one block on the configured pipeline within container
    /// `container_id`, bumping `next_local_id`. `offset`/`length` are 0 (the OM
    /// pre-allocates the block; the gateway fills real lengths on commit).
    fn alloc_block(&self, container_id: i64) -> oz::KeyLocation {
        let local_id = {
            let mut st = self.state.lock();
            let id = st.next_local_id;
            st.next_local_id += 1;
            id
        };
        oz::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id,
                    local_id,
                },
                ..Default::default()
            },
            offset: 0,
            length: 0,
            pipeline: Some(self.pipeline_message()),
            ..Default::default()
        }
    }

    /// Build an `OmResponse` envelope: the echoed `cmd_type`, `success = true`,
    /// and `status = OK`. Callers set exactly the one relevant sub-response.
    fn ok_response(cmd_type: i32) -> oz::OmResponse {
        oz::OmResponse {
            cmd_type,
            status: oz::Status::Ok as i32,
            success: Some(true),
            ..Default::default()
        }
    }

    /// Build a non-OK `OmResponse`: echoed `cmd_type`, `success = false`, and the
    /// given 1-based `status`. No sub-response is set (W1: a non-OK status must
    /// stand on its own, never carry a stale success body).
    fn err_response(cmd_type: i32, status: oz::Status, message: &str) -> oz::OmResponse {
        oz::OmResponse {
            cmd_type,
            status: status as i32,
            success: Some(false),
            message: Some(message.to_string()),
            ..Default::default()
        }
    }
}

#[tonic::async_trait]
impl OzoneManagerService for CompliantOm {
    async fn submit_request(
        &self,
        req: Request<oz::OmRequest>,
    ) -> Result<Response<oz::OmResponse>, Status> {
        let req = req.into_inner();
        let cmd_type = req.cmd_type;
        self.record.lock().push(req.clone());

        let resp = match oz::Type::try_from(cmd_type) {
            Ok(oz::Type::CreateBucket) => {
                // CreateBucketRequest carries the full BucketInfo; the
                // (volume, bucket) names live on it.
                let info = req
                    .create_bucket_request
                    .map(|r| r.bucket_info)
                    .ok_or_else(|| Status::invalid_argument("missing createBucketRequest"))?;
                self.state
                    .lock()
                    .buckets
                    .insert((info.volume_name, info.bucket_name));
                let mut r = Self::ok_response(cmd_type);
                r.create_bucket_response = Some(oz::CreateBucketResponse {});
                r
            }
            Ok(oz::Type::InfoBucket) => {
                let ib = req
                    .info_bucket_request
                    .ok_or_else(|| Status::invalid_argument("missing infoBucketRequest"))?;
                let exists = self
                    .state
                    .lock()
                    .buckets
                    .contains(&(ib.volume_name.clone(), ib.bucket_name.clone()));
                if exists {
                    let mut r = Self::ok_response(cmd_type);
                    r.info_bucket_response = Some(oz::InfoBucketResponse {
                        bucket_info: Some(oz::BucketInfo {
                            volume_name: ib.volume_name,
                            bucket_name: ib.bucket_name,
                            is_version_enabled: false,
                            storage_type: hdds::StorageTypeProto::Disk as i32,
                            ..Default::default()
                        }),
                    });
                    r
                } else {
                    Self::err_response(cmd_type, oz::Status::BucketNotFound, "bucket not found")
                }
            }
            Ok(oz::Type::DeleteBucket) => {
                let db = req
                    .delete_bucket_request
                    .ok_or_else(|| Status::invalid_argument("missing deleteBucketRequest"))?;
                self.state
                    .lock()
                    .buckets
                    .remove(&(db.volume_name, db.bucket_name));
                let mut r = Self::ok_response(cmd_type);
                r.delete_bucket_response = Some(oz::DeleteBucketResponse {});
                r
            }
            Ok(oz::Type::ListBuckets) => {
                let lb = req
                    .list_buckets_request
                    .ok_or_else(|| Status::invalid_argument("missing listBucketsRequest"))?;
                let st = self.state.lock();
                let mut bucket_info: Vec<oz::BucketInfo> = st
                    .buckets
                    .iter()
                    .filter(|(v, _)| *v == lb.volume_name)
                    .map(|(v, b)| oz::BucketInfo {
                        volume_name: v.clone(),
                        bucket_name: b.clone(),
                        is_version_enabled: false,
                        storage_type: hdds::StorageTypeProto::Disk as i32,
                        ..Default::default()
                    })
                    .collect();
                bucket_info.sort_by(|a, b| a.bucket_name.cmp(&b.bucket_name));
                let mut r = Self::ok_response(cmd_type);
                r.list_buckets_response = Some(oz::ListBucketsResponse { bucket_info });
                r
            }
            Ok(oz::Type::CreateKey) => {
                let ka = req
                    .create_key_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing createKeyRequest"))?;
                let (client_id, container_id) = {
                    let mut st = self.state.lock();
                    let cid = st.next_client_id;
                    st.next_client_id += 1;
                    let cont = st.next_container_id;
                    st.next_container_id += 1;
                    (cid, cont)
                };
                let location = self.alloc_block(container_id);
                // Echo the EC config the request asked for, else the pipeline's.
                let ec = ka
                    .ec_replication_config
                    .clone()
                    .or_else(|| Some(self.pipeline.ec.clone()));
                let key_info = oz::KeyInfo {
                    volume_name: ka.volume_name,
                    bucket_name: ka.bucket_name,
                    key_name: ka.key_name,
                    data_size: ka.data_size.unwrap_or(0),
                    r#type: hdds::ReplicationType::Ec as i32,
                    key_location_list: vec![oz::KeyLocationList {
                        version: Some(0),
                        key_locations: vec![location],
                        ..Default::default()
                    }],
                    creation_time: FAKE_OBJECT_TIME_MS,
                    modification_time: FAKE_OBJECT_TIME_MS,
                    ec_replication_config: ec,
                    ..Default::default()
                };
                let mut r = Self::ok_response(cmd_type);
                r.create_key_response = Some(oz::CreateKeyResponse {
                    key_info: Some(key_info),
                    id: Some(client_id),
                    open_version: Some(0),
                });
                r
            }
            Ok(oz::Type::AllocateBlock) => {
                let abr = req
                    .allocate_block_request
                    .ok_or_else(|| Status::invalid_argument("missing allocateBlockRequest"))?;
                // The same pipeline; a fresh monotonic block in the key's container.
                let container_id = abr
                    .key_args
                    .key_locations
                    .first()
                    .map(|l| l.block_id.container_block_id.container_id)
                    .unwrap_or(1);
                let location = self.alloc_block(container_id);
                let mut r = Self::ok_response(cmd_type);
                r.allocate_block_response = Some(oz::AllocateBlockResponse {
                    key_location: Some(location),
                });
                r
            }
            Ok(oz::Type::CommitKey) => {
                let ka = req
                    .commit_key_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing commitKeyRequest"))?;
                // W2: keep the ACTUAL written block ids/lengths/offsets verbatim.
                // The client commits with pipeline-less locations (the OM owns
                // placement, so a commit need only name block_id + length). The
                // real OM re-associates each committed block with its pipeline so
                // a later GetKeyInfo returns the full location (members +
                // member_replica_indexes + EC) the gateway reads shards from.
                // Mirror that: preserve the committed extent, re-attach the
                // pipeline.
                let tuple = (
                    ka.volume_name.clone(),
                    ka.bucket_name.clone(),
                    ka.key_name.clone(),
                );
                let ec = ka
                    .ec_replication_config
                    .clone()
                    .or_else(|| Some(self.pipeline.ec.clone()));
                let committed: Vec<oz::KeyLocation> = ka
                    .key_locations
                    .into_iter()
                    .map(|mut loc| {
                        loc.pipeline = Some(self.pipeline_message());
                        loc
                    })
                    .collect();
                let key_info = oz::KeyInfo {
                    volume_name: ka.volume_name,
                    bucket_name: ka.bucket_name,
                    key_name: ka.key_name,
                    data_size: ka.data_size.unwrap_or(0),
                    r#type: hdds::ReplicationType::Ec as i32,
                    key_location_list: vec![oz::KeyLocationList {
                        version: Some(0),
                        key_locations: committed,
                        ..Default::default()
                    }],
                    creation_time: FAKE_OBJECT_TIME_MS,
                    modification_time: FAKE_OBJECT_TIME_MS,
                    metadata: ka.metadata,
                    ec_replication_config: ec,
                    ..Default::default()
                };
                self.state.lock().keys.insert(tuple, key_info);
                let mut r = Self::ok_response(cmd_type);
                r.commit_key_response = Some(oz::CommitKeyResponse {});
                r
            }
            Ok(oz::Type::GetKeyInfo) => {
                let ka = req
                    .get_key_info_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing getKeyInfoRequest"))?;
                let tuple = (ka.volume_name, ka.bucket_name, ka.key_name);
                let found = self.state.lock().keys.get(&tuple).cloned();
                match found {
                    Some(key_info) => {
                        let mut r = Self::ok_response(cmd_type);
                        r.get_key_info_response = Some(oz::GetKeyInfoResponse {
                            key_info: Some(key_info),
                            ..Default::default()
                        });
                        r
                    }
                    None => Self::err_response(cmd_type, oz::Status::KeyNotFound, "key not found"),
                }
            }
            Ok(oz::Type::DeleteKey) => {
                let ka = req
                    .delete_key_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing deleteKeyRequest"))?;
                let tuple = (ka.volume_name, ka.bucket_name, ka.key_name);
                self.state.lock().keys.remove(&tuple);
                let mut r = Self::ok_response(cmd_type);
                r.delete_key_response = Some(oz::DeleteKeyResponse::default());
                r
            }
            // Anything else (multipart, list_keys, FS ops, ...) is out of scope
            // for this fixture: report INVALID_REQUEST rather than panic.
            _ => Self::err_response(
                cmd_type,
                oz::Status::InvalidRequest,
                "cmdType not implemented by CompliantOm",
            ),
        };

        Ok(Response::new(resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oz::ozone_manager_service_client::OzoneManagerServiceClient as Client;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    /// A 3+2 RS pipeline with five distinct datanodes (slots 1..=5), each with a
    /// REPLICATION port the gateway would dial.
    fn test_pipeline() -> CompliantOmPipeline {
        let ec = hdds::EcReplicationConfig {
            data: 3,
            parity: 2,
            codec: "rs".to_string(),
            ec_chunk_size: 1024 * 1024,
        };
        let datanodes = (0..5u32)
            .map(|i| hdds::DatanodeDetailsProto {
                uuid: Some(format!("dn-{i}")),
                ip_address: format!("10.0.0.{}", i + 1),
                host_name: format!("host-{i}"),
                ports: vec![hdds::Port {
                    name: "REPLICATION".to_string(),
                    value: 19864 + i,
                }],
                ..Default::default()
            })
            .collect();
        CompliantOmPipeline { datanodes, ec }
    }

    /// Build an `OmRequest` envelope for `cmd_type` with a recognizable principal,
    /// so dispatch and the recorded `s3Authentication.accessId` are exercised.
    fn envelope(cmd_type: oz::Type) -> oz::OmRequest {
        oz::OmRequest {
            cmd_type: cmd_type as i32,
            client_id: "test-client".to_string(),
            version: Some(1),
            s3_authentication: Some(oz::S3Authentication {
                access_id: Some("the-principal".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn key_args(key: &str) -> oz::KeyArgs {
        oz::KeyArgs {
            volume_name: "s3v".to_string(),
            bucket_name: "bkt".to_string(),
            key_name: key.to_string(),
            ..Default::default()
        }
    }

    async fn serve(om: CompliantOm) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(om.into_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .ok();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn dispatch_roundtrips_over_real_service() {
        // Drives the real OzoneManagerService.submitRequest over a tonic channel
        // (not just inherent-method calls), proving the cmdType dispatch wiring.
        let om = CompliantOm::new(test_pipeline());
        let record = om.record();
        let mut client = Client::connect(serve(om).await).await.unwrap();

        // CreateBucket then InfoBucket: the bucket must now report as existing.
        let mut create = envelope(oz::Type::CreateBucket);
        create.create_bucket_request = Some(oz::CreateBucketRequest {
            bucket_info: oz::BucketInfo {
                volume_name: "s3v".to_string(),
                bucket_name: "bkt".to_string(),
                is_version_enabled: false,
                storage_type: hdds::StorageTypeProto::Disk as i32,
                ..Default::default()
            },
        });
        let cresp = client.submit_request(create).await.unwrap().into_inner();
        assert_eq!(cresp.status, oz::Status::Ok as i32);
        assert!(cresp.create_bucket_response.is_some());

        let mut info = envelope(oz::Type::InfoBucket);
        info.info_bucket_request = Some(oz::InfoBucketRequest {
            volume_name: "s3v".to_string(),
            bucket_name: "bkt".to_string(),
        });
        let iresp = client.submit_request(info).await.unwrap().into_inner();
        assert_eq!(iresp.status, oz::Status::Ok as i32);
        assert_eq!(
            iresp
                .info_bucket_response
                .unwrap()
                .bucket_info
                .unwrap()
                .bucket_name,
            "bkt"
        );

        // The fixture recorded both envelopes with the attested principal.
        let rec = record.lock();
        assert_eq!(rec.len(), 2);
        assert_eq!(
            rec[0].s3_authentication.as_ref().unwrap().access_id.as_deref(),
            Some("the-principal")
        );
    }

    #[tokio::test]
    async fn info_bucket_missing_is_bucket_not_found() {
        let om = CompliantOm::new(test_pipeline());
        let mut req = envelope(oz::Type::InfoBucket);
        req.info_bucket_request = Some(oz::InfoBucketRequest {
            volume_name: "s3v".to_string(),
            bucket_name: "absent".to_string(),
        });
        let resp = om
            .submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, oz::Status::BucketNotFound as i32);
        assert_eq!(resp.success, Some(false));
        assert!(
            resp.info_bucket_response.is_none(),
            "a non-OK status must not carry a sub-response (W1)"
        );
    }

    #[tokio::test]
    async fn create_key_returns_client_id_and_identity_replica_indexes() {
        let om = CompliantOm::new(test_pipeline());
        let mut req = envelope(oz::Type::CreateKey);
        req.create_key_request = Some(oz::CreateKeyRequest {
            key_args: key_args("obj"),
            ..Default::default()
        });
        let resp = om
            .submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, oz::Status::Ok as i32);
        let ck = resp.create_key_response.unwrap();
        assert_eq!(ck.id, Some(1), "first CreateKey open-session id is 1");

        let ki = ck.key_info.unwrap();
        assert_eq!(ki.key_location_list.len(), 1, "one pre-allocated block group");
        let locs = &ki.key_location_list[0].key_locations;
        assert_eq!(locs.len(), 1, "exactly one block pre-allocated");
        let pipe = locs[0].pipeline.as_ref().unwrap();
        assert_eq!(pipe.members.len(), 5, "k+p members");
        // W3: member_replica_indexes is parallel to members and == 1..=5.
        assert_eq!(pipe.member_replica_indexes, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn commit_then_get_key_info_round_trips() {
        let om = CompliantOm::new(test_pipeline());

        // Commit a key with one concrete written block (container 7, local 42).
        let written = oz::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 7,
                    local_id: 42,
                },
                ..Default::default()
            },
            offset: 0,
            length: 4096,
            ..Default::default()
        };
        let mut commit = envelope(oz::Type::CommitKey);
        let mut ka = key_args("obj");
        ka.data_size = Some(4096);
        ka.key_locations = vec![written];
        ka.metadata = vec![hdds::KeyValue {
            key: "ETAG".to_string(),
            value: Some("deadbeef".to_string()),
        }];
        commit.commit_key_request = Some(oz::CommitKeyRequest {
            key_args: ka,
            client_id: 1,
            ..Default::default()
        });
        let cresp = om
            .submit_request(Request::new(commit))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cresp.status, oz::Status::Ok as i32);
        assert!(cresp.commit_key_response.is_some());

        // GetKeyInfo must return the committed size + the verbatim written block.
        let mut get = envelope(oz::Type::GetKeyInfo);
        get.get_key_info_request = Some(oz::GetKeyInfoRequest {
            key_args: key_args("obj"),
            ..Default::default()
        });
        let gresp = om
            .submit_request(Request::new(get))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(gresp.status, oz::Status::Ok as i32);
        let ki = gresp.get_key_info_response.unwrap().key_info.unwrap();
        assert_eq!(ki.data_size, 4096);
        let locs = &ki.key_location_list[0].key_locations;
        assert_eq!(locs.len(), 1);
        // W2: the stored location is exactly what was committed.
        assert_eq!(locs[0].block_id.container_block_id.container_id, 7);
        assert_eq!(locs[0].block_id.container_block_id.local_id, 42);
        assert_eq!(locs[0].length, 4096);
        assert_eq!(
            ki.metadata
                .iter()
                .find(|kv| kv.key == "ETAG")
                .and_then(|kv| kv.value.as_deref()),
            Some("deadbeef")
        );
    }

    #[tokio::test]
    async fn get_key_info_missing_is_key_not_found() {
        let om = CompliantOm::new(test_pipeline());
        let mut req = envelope(oz::Type::GetKeyInfo);
        req.get_key_info_request = Some(oz::GetKeyInfoRequest {
            key_args: key_args("nope"),
            ..Default::default()
        });
        let resp = om
            .submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, oz::Status::KeyNotFound as i32);
        assert_eq!(resp.success, Some(false));
        assert!(resp.get_key_info_response.is_none());
    }

    #[tokio::test]
    async fn allocate_block_local_ids_are_monotonic() {
        let om = CompliantOm::new(test_pipeline());
        let mut last = 0i64;
        for _ in 0..4 {
            let mut req = envelope(oz::Type::AllocateBlock);
            req.allocate_block_request = Some(oz::AllocateBlockRequest {
                key_args: key_args("obj"),
                client_id: 1,
                ..Default::default()
            });
            let resp = om
                .submit_request(Request::new(req))
                .await
                .unwrap()
                .into_inner();
            let id = resp
                .allocate_block_response
                .unwrap()
                .key_location
                .unwrap()
                .block_id
                .container_block_id
                .local_id;
            assert!(id > last, "local_id {id} must exceed previous {last}");
            last = id;
        }
    }
}
