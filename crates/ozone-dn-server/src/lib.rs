//! `DatanodeGatewayService` server: the datanode's gateway-facing data plane.
//!
//! Implements the tonic-generated service trait over the storage *traits*
//! ([`MetaStore`] for block/container metadata, [`ChunkStore`] for chunk bytes).
//! It is deliberately implementation-agnostic — the binary injects the concrete
//! fjall + filesystem stores. The datanode performs NO erasure coding: each EC
//! shard arrives as an ordinary chunk whose [`ozone_types::BlockId`] carries the
//! replica slot, so the gateway (which owns the GF math) writes shards with
//! plain [`pb::WriteChunkRequest`]s.
//!
//! # Scope
//! Container lifecycle, block put/get/delete/list, and chunk write/read are
//! fully implemented. The optional `PutECStripe`/`ReadECStripe` fan-out helpers
//! are intentionally unimplemented (`Status::unimplemented`): the design folds
//! EC shard writes into the normal chunk path, and these RPCs are flagged as an
//! open question in the wire-protocol RFC. The gateway must not call them.

#![forbid(unsafe_code)]

pub mod repair;
pub mod scm_compliant;
pub mod scrub;
pub use scm_compliant::{CompliantScmError, CompliantScmRegistration};

use std::pin::Pin;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use ozone_grpc_types::conv::{container_state_to_wire, ConversionError};
use ozone_grpc_types::dn::v1 as pb;
use ozone_grpc_types::dn::v1::datanode_gateway_service_server::{
    DatanodeGatewayService, DatanodeGatewayServiceServer,
};
use ozone_storage::{checksum, ChunkStore, MetaStore, StorageError};
use ozone_types::{
    BlockData, BlockId, ChunkInfo, ContainerId, ContainerInfo, ContainerState, EcReplicationConfig,
};

/// Frame size for streaming chunk reads back to the gateway.
const READ_FRAME_BYTES: usize = 64 * 1024;

/// Default page size for `ListBlocks` when the request leaves it unset.
const DEFAULT_LIST_PAGE: usize = 1000;

/// The datanode service. Cheap to clone (everything is behind `Arc`).
#[derive(Clone)]
pub struct DatanodeService {
    meta: Arc<dyn MetaStore>,
    chunks: Arc<dyn ChunkStore>,
    server_major: u32,
    server_minor: u32,
    server_build: String,
}

impl DatanodeService {
    /// Build a service over the given metadata and chunk stores.
    pub fn new(meta: Arc<dyn MetaStore>, chunks: Arc<dyn ChunkStore>) -> Self {
        Self {
            meta,
            chunks,
            server_major: 1,
            server_minor: 0,
            server_build: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Wrap the service in the tonic server for `Server::add_service`.
    pub fn into_server(self) -> DatanodeGatewayServiceServer<Self> {
        DatanodeGatewayServiceServer::new(self)
    }
}

// ---- error mapping ----

/// Map a storage error onto the closest gRPC status code.
fn storage_status(e: StorageError) -> Status {
    match e {
        StorageError::ContainerNotFound(_) | StorageError::BlockNotFound(_) => {
            Status::not_found(e.to_string())
        }
        StorageError::ContainerExists(_) => Status::already_exists(e.to_string()),
        StorageError::ContainerNotOpen(..) => Status::failed_precondition(e.to_string()),
        StorageError::Checksum(_) => Status::data_loss(e.to_string()),
        StorageError::Io(_) | StorageError::Meta(_) | StorageError::Corrupt(_) => {
            Status::internal(e.to_string())
        }
    }
}

/// Map a wire-decode failure onto `InvalidArgument`.
fn conv_status(e: ConversionError) -> Status {
    Status::invalid_argument(e.to_string())
}

/// A required nested message was absent on the wire.
fn missing(field: &str) -> Status {
    Status::invalid_argument(format!("missing required field: {field}"))
}

/// Wrap the `state` enum in the wire `ContainerState` message.
fn wire_state(state: ContainerState) -> pb::ContainerState {
    pb::ContainerState {
        state: container_state_to_wire(state),
    }
}

/// Slice `data` into frames for the read-chunk response stream. An empty chunk
/// yields a single empty terminal frame so the client always sees one message.
fn frame(data: Bytes) -> Vec<Bytes> {
    if data.is_empty() {
        return vec![Bytes::new()];
    }
    let mut out = Vec::with_capacity(data.len().div_ceil(READ_FRAME_BYTES));
    let mut off = 0;
    while off < data.len() {
        let end = (off + READ_FRAME_BYTES).min(data.len());
        out.push(data.slice(off..end));
        off = end;
    }
    out
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl DatanodeGatewayService for DatanodeService {
    async fn get_version(
        &self,
        _req: Request<pb::VersionRequest>,
    ) -> Result<Response<pb::VersionResponse>, Status> {
        Ok(Response::new(pb::VersionResponse {
            server_major: self.server_major,
            server_minor: self.server_minor,
            server_build: self.server_build.clone(),
        }))
    }

    async fn create_container(
        &self,
        req: Request<pb::CreateContainerRequest>,
    ) -> Result<Response<pb::CreateContainerResponse>, Status> {
        let req = req.into_inner();
        let cid: ContainerId = req
            .container_id
            .ok_or_else(|| missing("container_id"))?
            .into();
        let ec: EcReplicationConfig = req
            .ec_config
            .ok_or_else(|| missing("ec_config"))?
            .try_into()
            .map_err(conv_status)?;
        self.meta
            .create_container(ContainerInfo::new_open(cid, ec))
            .await
            .map_err(storage_status)?;
        Ok(Response::new(pb::CreateContainerResponse {
            state: Some(wire_state(ContainerState::Open)),
        }))
    }

    async fn close_container(
        &self,
        req: Request<pb::CloseContainerRequest>,
    ) -> Result<Response<pb::CloseContainerResponse>, Status> {
        let cid: ContainerId = req
            .into_inner()
            .container_id
            .ok_or_else(|| missing("container_id"))?
            .into();
        self.meta
            .set_container_state(cid, ContainerState::Closed)
            .await
            .map_err(storage_status)?;
        let info = self
            .meta
            .get_container(cid)
            .await
            .map_err(storage_status)?
            .ok_or_else(|| Status::not_found(format!("container {cid} vanished during close")))?;
        Ok(Response::new(pb::CloseContainerResponse {
            state: Some(wire_state(info.state)),
            used_bytes: info.used_bytes,
            block_count: info.block_count,
        }))
    }

    async fn delete_container(
        &self,
        req: Request<pb::DeleteContainerRequest>,
    ) -> Result<Response<pb::DeleteContainerResponse>, Status> {
        let cid: ContainerId = req
            .into_inner()
            .container_id
            .ok_or_else(|| missing("container_id"))?
            .into();
        // Metadata first, then bytes: a crash between the two leaves orphaned
        // chunk files (reclaimable by a scrubber) rather than dangling metadata
        // pointing at deleted bytes.
        self.meta
            .delete_container(cid)
            .await
            .map_err(storage_status)?;
        self.chunks
            .delete_container(cid)
            .await
            .map_err(storage_status)?;
        Ok(Response::new(pb::DeleteContainerResponse {}))
    }

    async fn get_container_info(
        &self,
        req: Request<pb::GetContainerInfoRequest>,
    ) -> Result<Response<pb::GetContainerInfoResponse>, Status> {
        let cid: ContainerId = req
            .into_inner()
            .container_id
            .ok_or_else(|| missing("container_id"))?
            .into();
        let info = self
            .meta
            .get_container(cid)
            .await
            .map_err(storage_status)?
            .ok_or_else(|| Status::not_found(format!("container {cid} not found")))?;
        Ok(Response::new(pb::GetContainerInfoResponse {
            container_id: Some(cid.into()),
            state: Some(wire_state(info.state)),
            used_bytes: info.used_bytes,
            block_count: info.block_count,
            ec_config: info.ec_config.map(Into::into),
        }))
    }

    async fn put_block(
        &self,
        req: Request<pb::PutBlockRequest>,
    ) -> Result<Response<pb::PutBlockResponse>, Status> {
        let block_data = req
            .into_inner()
            .block_data
            .ok_or_else(|| missing("block_data"))?;
        let block: BlockData = block_data.try_into().map_err(conv_status)?;
        let bcsi = self.meta.put_block(&block).await.map_err(storage_status)?;
        Ok(Response::new(pb::PutBlockResponse { bcsi_id: bcsi }))
    }

    async fn get_block(
        &self,
        req: Request<pb::GetBlockRequest>,
    ) -> Result<Response<pb::GetBlockResponse>, Status> {
        let id: BlockId = req
            .into_inner()
            .block_id
            .ok_or_else(|| missing("block_id"))?
            .try_into()
            .map_err(conv_status)?;
        match self.meta.get_block(&id).await.map_err(storage_status)? {
            Some(bd) => Ok(Response::new(pb::GetBlockResponse {
                block_data: Some(bd.into()),
            })),
            None => Err(Status::not_found(format!("block {id} not found"))),
        }
    }

    async fn delete_block(
        &self,
        req: Request<pb::DeleteBlockRequest>,
    ) -> Result<Response<pb::DeleteBlockResponse>, Status> {
        let id: BlockId = req
            .into_inner()
            .block_id
            .ok_or_else(|| missing("block_id"))?
            .try_into()
            .map_err(conv_status)?;
        // Remove chunk bytes for every chunk the block records, then the
        // metadata. Both steps are idempotent, so a retry after a partial
        // failure converges.
        if let Some(bd) = self.meta.get_block(&id).await.map_err(storage_status)? {
            for ci in &bd.chunks {
                self.chunks
                    .delete_chunk(&id, ci)
                    .await
                    .map_err(storage_status)?;
            }
        }
        self.meta.delete_block(&id).await.map_err(storage_status)?;
        Ok(Response::new(pb::DeleteBlockResponse {}))
    }

    type ListBlocksStream = BoxStream<pb::ListBlocksResponse>;

    async fn list_blocks(
        &self,
        req: Request<pb::ListBlocksRequest>,
    ) -> Result<Response<Self::ListBlocksStream>, Status> {
        let req = req.into_inner();
        let cid: ContainerId = req
            .container_id
            .ok_or_else(|| missing("container_id"))?
            .into();
        let page_size = if req.page_size == 0 {
            DEFAULT_LIST_PAGE
        } else {
            req.page_size as usize
        };
        let meta = self.meta.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut start = req.start_local_id;
            loop {
                match meta.list_blocks(cid, start, page_size).await {
                    Ok(page) => {
                        let next = page.next_local_id;
                        let resp = pb::ListBlocksResponse {
                            blocks: page.blocks.into_iter().map(Into::into).collect(),
                            next_local_id: next.unwrap_or(0),
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            return; // client dropped the stream
                        }
                        match next {
                            Some(n) => start = n,
                            None => return,
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(storage_status(e))).await;
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn write_chunk(
        &self,
        req: Request<Streaming<pb::WriteChunkRequest>>,
    ) -> Result<Response<pb::WriteChunkResponse>, Status> {
        let mut stream = req.into_inner();
        let mut header: Option<pb::WriteChunkHeader> = None;
        let mut buf = BytesMut::new();
        // The header rides on the first message only; every message may carry
        // payload, and `last` terminates the stream.
        while let Some(msg) = stream.message().await? {
            if header.is_none() {
                header = msg.header;
            }
            buf.extend_from_slice(&msg.payload);
            if msg.last {
                break;
            }
        }
        let header = header.ok_or_else(|| missing("WriteChunkHeader"))?;
        let block: BlockId = header
            .block_id
            .ok_or_else(|| missing("WriteChunkHeader.block_id"))?
            .try_into()
            .map_err(conv_status)?;
        let chunk: ChunkInfo = header
            .chunk_info
            .ok_or_else(|| missing("WriteChunkHeader.chunk_info"))?
            .try_into()
            .map_err(conv_status)?;
        let data = buf.freeze();
        let bytes_written = data.len() as u64;
        // Verify integrity at ingress when the gateway supplied checksums.
        if let Some(cd) = &chunk.checksum_data {
            checksum::verify(&data, cd).map_err(|e| Status::data_loss(e.to_string()))?;
        }
        self.chunks
            .write_chunk(&block, &chunk, data)
            .await
            .map_err(storage_status)?;
        Ok(Response::new(pb::WriteChunkResponse { bytes_written }))
    }

    type ReadChunkStream = BoxStream<pb::ReadChunkResponse>;

    // The stream item is `Result<_, tonic::Status>`, and `Status` is a large
    // error type dictated by tonic; boxing it is not possible across the stream
    // boundary, so the large-Err lint is moot here.
    #[allow(clippy::result_large_err)]
    async fn read_chunk(
        &self,
        req: Request<pb::ReadChunkRequest>,
    ) -> Result<Response<Self::ReadChunkStream>, Status> {
        let req = req.into_inner();
        let block: BlockId = req
            .block_id
            .ok_or_else(|| missing("block_id"))?
            .try_into()
            .map_err(conv_status)?;
        let chunk: ChunkInfo = req
            .chunk_info
            .ok_or_else(|| missing("chunk_info"))?
            .try_into()
            .map_err(conv_status)?;
        let data = self
            .chunks
            .read_chunk(&block, &chunk)
            .await
            .map_err(storage_status)?;
        if req.verify {
            // Prefer a caller-supplied checksum; otherwise verify the bytes we
            // read against the checksum recorded for this chunk at PutBlock time.
            // This makes a corrupted-on-disk shard surface as DataLoss so the
            // gateway can degrade to an EC reconstruct instead of returning bad
            // bytes (the gateway's read requests carry no checksum of their own).
            let expected = match &chunk.checksum_data {
                Some(cd) => Some(cd.clone()),
                None => self
                    .meta
                    .get_block(&block)
                    .await
                    .map_err(storage_status)?
                    .and_then(|bd| {
                        bd.chunks
                            .into_iter()
                            .find(|c| c.chunk_name == chunk.chunk_name)
                    })
                    .and_then(|c| c.checksum_data),
            };
            if let Some(cd) = expected {
                checksum::verify(&data, &cd).map_err(|e| Status::data_loss(e.to_string()))?;
            }
        }
        let frames = frame(data);
        let last_idx = frames.len() - 1;
        let stream = tokio_stream::iter(frames.into_iter().enumerate().map(move |(i, f)| {
            Ok(pb::ReadChunkResponse {
                payload: f.to_vec(),
                last: i == last_idx,
            })
        }));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn put_ec_stripe(
        &self,
        _req: Request<Streaming<pb::PutEcStripeRequest>>,
    ) -> Result<Response<pb::PutEcStripeResponse>, Status> {
        Err(Status::unimplemented(
            "PutECStripe is not served; EC shards are written via WriteChunk per replica slot",
        ))
    }

    type ReadECStripeStream = BoxStream<pb::ReadEcStripeResponse>;

    async fn read_ec_stripe(
        &self,
        _req: Request<pb::ReadEcStripeRequest>,
    ) -> Result<Response<Self::ReadECStripeStream>, Status> {
        Err(Status::unimplemented(
            "ReadECStripe is not served; EC shards are read via ReadChunk per replica slot",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozone_fjall_store::FjallMetaStore;
    use ozone_storage::FileChunkStore;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn svc() -> (DatanodeService, std::path::PathBuf) {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("ozone-dn-svc-{}-{}", std::process::id(), n));
        let meta = Arc::new(FjallMetaStore::open(root.join("meta")).unwrap());
        let chunks = Arc::new(FileChunkStore::new(root.join("data")));
        (DatanodeService::new(meta, chunks), root)
    }

    #[tokio::test]
    async fn version_and_container_lifecycle() {
        let (s, root) = svc();
        let v = s
            .get_version(Request::new(pb::VersionRequest {
                client_major: 1,
                client_minor: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(v.server_major, 1);

        let ec = pb::EcReplicationConfig::from(EcReplicationConfig::RS_6_3_1MIB);
        s.create_container(Request::new(pb::CreateContainerRequest {
            container_id: Some(pb::ContainerId { id: 1 }),
            ec_config: Some(ec.clone()),
            lease_id: String::new(),
            request_id: "r1".into(),
        }))
        .await
        .unwrap();

        // Duplicate create is AlreadyExists.
        let dup = s
            .create_container(Request::new(pb::CreateContainerRequest {
                container_id: Some(pb::ContainerId { id: 1 }),
                ec_config: Some(ec),
                lease_id: String::new(),
                request_id: "r2".into(),
            }))
            .await;
        assert_eq!(dup.unwrap_err().code(), tonic::Code::AlreadyExists);

        let info = s
            .get_container_info(Request::new(pb::GetContainerInfoRequest {
                container_id: Some(pb::ContainerId { id: 1 }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(info.block_count, 0);
        assert!(info.ec_config.is_some());

        tokio::fs::remove_dir_all(&root).await.ok();
    }

    #[tokio::test]
    async fn put_then_get_block() {
        let (s, root) = svc();
        s.create_container(Request::new(pb::CreateContainerRequest {
            container_id: Some(pb::ContainerId { id: 5 }),
            ec_config: Some(EcReplicationConfig::RS_3_2_1MIB.into()),
            lease_id: String::new(),
            request_id: "r".into(),
        }))
        .await
        .unwrap();

        let bd = BlockData::new(BlockId::ec(
            ContainerId(5),
            ozone_types::LocalId(1),
            ozone_types::ReplicaIndex::new(1),
        ));
        let put = s
            .put_block(Request::new(pb::PutBlockRequest {
                block_data: Some(bd.into()),
                eof: true,
                request_id: "r".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(put.bcsi_id, 1);

        let got = s
            .get_block(Request::new(pb::GetBlockRequest {
                block_id: Some(pb::BlockId {
                    container_id: 5,
                    local_id: 1,
                    replica_index: 1,
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.block_data.unwrap().block_id.unwrap().local_id, 1);

        // Missing block is NotFound.
        let miss = s
            .get_block(Request::new(pb::GetBlockRequest {
                block_id: Some(pb::BlockId {
                    container_id: 5,
                    local_id: 999,
                    replica_index: 1,
                }),
            }))
            .await;
        assert_eq!(miss.unwrap_err().code(), tonic::Code::NotFound);

        tokio::fs::remove_dir_all(&root).await.ok();
    }
}
