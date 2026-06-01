//! Domain-typed, gateway-side client for `DatanodeGatewayService`.
//!
//! Unlike the thin wire-typed `ozone-scm-client`/`ozone-om-client`, this client
//! speaks `ozone-types` domain values ([`BlockId`], [`ChunkInfo`], [`Bytes`],
//! ...) because the gateway is its primary caller and works in those types. It
//! converts to/from the prost wire types internally via `ozone-grpc-types`.
//!
//! `NotFound` is surfaced as `Ok(None)` for the lookup methods
//! ([`DnClient::get_block`], [`DnClient::get_container_info`]); every other
//! gRPC status is an `Err`.
//!
//! The client holds a cloneable tonic [`Channel`]; clone the `DnClient` freely
//! to fan requests across tasks (the channel multiplexes over one HTTP/2
//! connection).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tonic::transport::Channel;

use ozone_grpc_types::conv::{container_state_from_wire, ConversionError};
use ozone_grpc_types::dn::v1 as pb;
use ozone_grpc_types::dn::v1::datanode_gateway_service_client::DatanodeGatewayServiceClient;
use ozone_types::{
    BlockData, BlockId, ChunkInfo, ContainerId, ContainerInfo, ContainerState, EcReplicationConfig,
};

/// Payload frame size for streaming chunk writes. Keeps individual gRPC
/// messages well under the default 4 MiB max while letting a chunk exceed it.
const WRITE_FRAME_BYTES: usize = 256 * 1024;

/// Errors from the datanode client.
#[derive(Debug, Error)]
pub enum DnClientError {
    /// Establishing the transport connection failed.
    #[error(transparent)]
    Connect(#[from] tonic::transport::Error),
    /// The RPC returned a non-OK status (other than the `NotFound` that lookup
    /// methods fold into `Ok(None)`).
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
    /// A response message failed wire->domain decoding.
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    /// A response was missing a field the protocol requires.
    #[error("missing field in response: {0}")]
    MissingField(&'static str),
}

/// Connected datanode client.
#[derive(Clone)]
pub struct DnClient {
    inner: DatanodeGatewayServiceClient<Channel>,
}

impl DnClient {
    /// Connect to a datanode endpoint (e.g. `http://host:port`). Single attempt;
    /// the caller owns any retry policy.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, DnClientError> {
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|e| DnClientError::Rpc(tonic::Status::invalid_argument(e.to_string())))?
            .connect()
            .await?;
        Ok(Self::from_channel(channel))
    }

    /// Wrap a pre-built/shared channel.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: DatanodeGatewayServiceClient::new(channel),
        }
    }

    /// Datanode version handshake: `(major, minor, build)`.
    pub async fn get_version(&mut self) -> Result<(u32, u32, String), DnClientError> {
        let r = self
            .inner
            .get_version(pb::VersionRequest {
                client_major: 1,
                client_minor: 0,
            })
            .await?
            .into_inner();
        Ok((r.server_major, r.server_minor, r.server_build))
    }

    /// Create an EC container, returning its resulting state.
    pub async fn create_container(
        &mut self,
        id: ContainerId,
        ec: EcReplicationConfig,
    ) -> Result<ContainerState, DnClientError> {
        let r = self
            .inner
            .create_container(pb::CreateContainerRequest {
                container_id: Some(id.into()),
                ec_config: Some(ec.into()),
                lease_id: String::new(),
                request_id: String::new(),
            })
            .await?
            .into_inner();
        let state = r.state.ok_or(DnClientError::MissingField("state"))?;
        Ok(container_state_from_wire(state.state)?)
    }

    /// Close a container, returning `(state, used_bytes, block_count)`.
    pub async fn close_container(
        &mut self,
        id: ContainerId,
    ) -> Result<(ContainerState, u64, u64), DnClientError> {
        let r = self
            .inner
            .close_container(pb::CloseContainerRequest {
                container_id: Some(id.into()),
                request_id: String::new(),
            })
            .await?
            .into_inner();
        let state =
            container_state_from_wire(r.state.ok_or(DnClientError::MissingField("state"))?.state)?;
        Ok((state, r.used_bytes, r.block_count))
    }

    /// Delete a container (metadata + chunk bytes).
    pub async fn delete_container(&mut self, id: ContainerId) -> Result<(), DnClientError> {
        self.inner
            .delete_container(pb::DeleteContainerRequest {
                container_id: Some(id.into()),
                force: false,
                request_id: String::new(),
            })
            .await?;
        Ok(())
    }

    /// Fetch container info, or `None` if it does not exist.
    pub async fn get_container_info(
        &mut self,
        id: ContainerId,
    ) -> Result<Option<ContainerInfo>, DnClientError> {
        match self
            .inner
            .get_container_info(pb::GetContainerInfoRequest {
                container_id: Some(id.into()),
            })
            .await
        {
            Ok(resp) => {
                let r = resp.into_inner();
                let state = container_state_from_wire(
                    r.state.ok_or(DnClientError::MissingField("state"))?.state,
                )?;
                let ec_config = r.ec_config.map(TryInto::try_into).transpose()?;
                Ok(Some(ContainerInfo {
                    container_id: id,
                    state,
                    used_bytes: r.used_bytes,
                    block_count: r.block_count,
                    ec_config,
                }))
            }
            Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
            Err(s) => Err(s.into()),
        }
    }

    /// Commit a block's metadata, returning the container's new `bcsi`. Set
    /// `eof` on the EC stripe-trailing block so the datanode records the
    /// block-group length correctly.
    pub async fn put_block(&mut self, block: &BlockData, eof: bool) -> Result<u64, DnClientError> {
        let r = self
            .inner
            .put_block(pb::PutBlockRequest {
                block_data: Some(block.clone().into()),
                eof,
                request_id: String::new(),
            })
            .await?
            .into_inner();
        Ok(r.bcsi_id)
    }

    /// Fetch a block's metadata, or `None` if absent.
    pub async fn get_block(&mut self, id: &BlockId) -> Result<Option<BlockData>, DnClientError> {
        match self
            .inner
            .get_block(pb::GetBlockRequest {
                block_id: Some((*id).into()),
            })
            .await
        {
            Ok(resp) => {
                let bd = resp
                    .into_inner()
                    .block_data
                    .ok_or(DnClientError::MissingField("block_data"))?;
                Ok(Some(bd.try_into()?))
            }
            Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
            Err(s) => Err(s.into()),
        }
    }

    /// Delete a block (metadata + its chunk bytes). Idempotent on the server.
    pub async fn delete_block(&mut self, id: &BlockId) -> Result<(), DnClientError> {
        self.inner
            .delete_block(pb::DeleteBlockRequest {
                block_id: Some((*id).into()),
                request_id: String::new(),
            })
            .await?;
        Ok(())
    }

    /// List a container's blocks from `start_local_id`, following pagination to
    /// completion and returning all blocks. `page_size` is the per-RPC page (0
    /// lets the server pick its default).
    pub async fn list_blocks(
        &mut self,
        container: ContainerId,
        start_local_id: u64,
        page_size: u32,
    ) -> Result<Vec<BlockData>, DnClientError> {
        let mut stream = self
            .inner
            .list_blocks(pb::ListBlocksRequest {
                container_id: Some(container.into()),
                start_local_id,
                page_size,
            })
            .await?
            .into_inner();
        let mut out = Vec::new();
        while let Some(page) = stream.message().await? {
            for bd in page.blocks {
                out.push(bd.try_into()?);
            }
        }
        Ok(out)
    }

    /// Stream `data` to the datanode as the bytes of `(block, chunk)`, returning
    /// the bytes written. The payload is framed so a single chunk may exceed the
    /// gRPC max message size.
    pub async fn write_chunk(
        &mut self,
        block: &BlockId,
        chunk: &ChunkInfo,
        data: Bytes,
    ) -> Result<u64, DnClientError> {
        let header = pb::WriteChunkHeader {
            block_id: Some((*block).into()),
            chunk_info: Some(chunk.clone().into()),
            request_id: String::new(),
        };
        let mut msgs: Vec<pb::WriteChunkRequest> = Vec::new();
        if data.is_empty() {
            msgs.push(pb::WriteChunkRequest {
                header: Some(header),
                payload: Vec::new(),
                last: true,
            });
        } else {
            let mut off = 0;
            let mut first = true;
            while off < data.len() {
                let end = (off + WRITE_FRAME_BYTES).min(data.len());
                msgs.push(pb::WriteChunkRequest {
                    header: if first { Some(header.clone()) } else { None },
                    payload: data.slice(off..end).to_vec(),
                    last: end == data.len(),
                });
                first = false;
                off = end;
            }
        }
        let r = self
            .inner
            .write_chunk(tokio_stream::iter(msgs))
            .await?
            .into_inner();
        Ok(r.bytes_written)
    }

    /// Read the full bytes of `(block, chunk)`. With `verify`, the datanode
    /// recomputes and checks the chunk's recorded checksums before returning.
    pub async fn read_chunk(
        &mut self,
        block: &BlockId,
        chunk: &ChunkInfo,
        verify: bool,
    ) -> Result<Bytes, DnClientError> {
        let mut stream = self
            .inner
            .read_chunk(pb::ReadChunkRequest {
                block_id: Some((*block).into()),
                chunk_info: Some(chunk.clone().into()),
                verify,
            })
            .await?
            .into_inner();
        let mut buf = BytesMut::new();
        while let Some(msg) = stream.message().await? {
            buf.extend_from_slice(&msg.payload);
            if msg.last {
                break;
            }
        }
        Ok(buf.freeze())
    }
}
