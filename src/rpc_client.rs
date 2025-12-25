use std::time::Duration;

use anyhow::Context;
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::codec::ProstCodec;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::rpc::anycable::{CommandMessage, CommandResponse, ConnectionRequest, ConnectionResponse,
    DisconnectRequest, DisconnectResponse};

pub struct AnyCableRpc {
    channel: Channel,
    timeout: Option<Duration>,
}

impl AnyCableRpc {
    pub async fn connect(addr: &str, timeout: Option<Duration>) -> anyhow::Result<Self> {
        let endpoint = Endpoint::from_shared(normalize_rpc_addr(addr))?
            .connect_timeout(Duration::from_secs(5));
        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("failed to connect to AnyCable RPC at {}", addr))?;

        Ok(Self { channel, timeout })
    }

    pub async fn connect_request(
        &self,
        request: ConnectionRequest,
    ) -> anyhow::Result<ConnectionResponse> {
        self.unary(
            request,
            "/anycable.RPC/Connect",
        )
        .await
    }

    pub async fn command(&self, message: CommandMessage) -> anyhow::Result<CommandResponse> {
        self.unary(
            message,
            "/anycable.RPC/Command",
        )
        .await
    }

    pub async fn disconnect(
        &self,
        request: DisconnectRequest,
    ) -> anyhow::Result<DisconnectResponse> {
        self.unary(
            request,
            "/anycable.RPC/Disconnect",
        )
        .await
    }

    async fn unary<Req, Res>(&self, message: Req, path: &'static str) -> anyhow::Result<Res>
    where
        Req: prost::Message + 'static,
        Res: prost::Message + Default + 'static,
    {
        let mut grpc = Grpc::new(self.channel.clone());
        grpc
            .ready()
            .await
            .context("AnyCable RPC service not ready")?;
        let mut request = Request::new(message);
        if let Some(timeout) = self.timeout {
            request.set_timeout(timeout);
        }
        let response = grpc
            .unary(request, PathAndQuery::from_static(path), ProstCodec::default())
            .await?
            .into_inner();
        Ok(response)
    }
}

fn normalize_rpc_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}
