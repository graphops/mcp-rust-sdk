use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, request::Parts},
    response::{
        Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::{Sink, SinkExt, Stream};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, PollSender};
use tracing::Instrument;

use crate::{
    RoleServer, Service,
    model::ClientJsonRpcMessage,
    service::{RxJsonRpcMessage, TxJsonRpcMessage, serve_directly_with_ct},
    transport::common::axum::{DEFAULT_AUTO_PING_INTERVAL, SessionId, session_id},
    state_store::{MemoryStateStore, StateStore, SseConnectionData},
};

type TxStore = Arc<tokio::sync::RwLock<HashMap<SessionId, tokio::sync::mpsc::Sender<ClientJsonRpcMessage>>>>;
pub type TransportReceiver = ReceiverStream<RxJsonRpcMessage<RoleServer>>;

#[derive(Clone)]
struct App {
    txs: TxStore,
    transport_tx: tokio::sync::mpsc::UnboundedSender<SseServerTransport>,
    post_path: Arc<str>,
    sse_ping_interval: Duration,
    // Optional state store for horizontal scaling
    state_store: Option<MemoryStateStore>,
    server_instance_id: String,
}

impl App {
    pub fn new(
        post_path: String,
        sse_ping_interval: Duration,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<SseServerTransport>,
    ) {
        let (transport_tx, transport_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                txs: Default::default(),
                transport_tx,
                post_path: post_path.into(),
                sse_ping_interval,
                state_store: None,
                server_instance_id: format!("sse-server-{}", uuid::Uuid::new_v4()),
            },
            transport_rx,
        )
    }

    pub fn with_state_store(mut self, state_store: MemoryStateStore) -> Self {
        self.state_store = Some(state_store);
        self
    }

    async fn register_session(&self, session_id: &SessionId, sender: tokio::sync::mpsc::Sender<ClientJsonRpcMessage>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Always register in local TxStore for backward compatibility
        self.txs.write().await.insert(session_id.clone(), sender);

        // Also register in state store if available
        if let Some(ref state_store) = self.state_store {
            let connection_data = SseConnectionData {
                created_at: std::time::SystemTime::now(),
                last_ping: std::time::SystemTime::now(),
                ping_interval: self.sse_ping_interval,
                server_instance_id: self.server_instance_id.clone(),
            };
            state_store.register_sse_connection(session_id, &connection_data).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }
        Ok(())
    }

    async fn unregister_session(&self, session_id: &SessionId) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Remove from local TxStore
        self.txs.write().await.remove(session_id);

        // Also remove from state store if available
        if let Some(ref state_store) = self.state_store {
            state_store.remove_sse_connection(session_id).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }
        Ok(())
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostEventQuery {
    pub session_id: String,
}

async fn post_event_handler(
    State(app): State<App>,
    Query(PostEventQuery { session_id }): Query<PostEventQuery>,
    parts: Parts,
    Json(mut message): Json<ClientJsonRpcMessage>,
) -> Result<StatusCode, StatusCode> {
    tracing::debug!(session_id, ?parts, ?message, "new client message");
    let tx = {
        let rg = app.txs.read().await;
        rg.get(session_id.as_str())
            .ok_or(StatusCode::NOT_FOUND)?
            .clone()
    };
    message.insert_extension(parts);
    if tx.send(message).await.is_err() {
        tracing::error!("send message error");
        return Err(StatusCode::GONE);
    }
    Ok(StatusCode::ACCEPTED)
}

async fn sse_handler(
    State(app): State<App>,
    parts: Parts,
) -> Result<Sse<impl Stream<Item = Result<Event, io::Error>>>, Response<String>> {
    let session = session_id();
    tracing::info!(%session, ?parts, "sse connection");
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    use tokio_util::sync::PollSender;
    let (from_client_tx, from_client_rx) = tokio::sync::mpsc::channel(64);
    let (to_client_tx, to_client_rx) = tokio::sync::mpsc::channel(64);
    let to_client_tx_clone = to_client_tx.clone();

    // Register session with both local store and optional state store
    if let Err(e) = app.register_session(&session, from_client_tx).await {
        tracing::error!(%session, error = %e, "Failed to register session");
        let mut response = Response::new("Failed to register session".to_string());
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        return Err(response);
    }
    let session = session.clone();
    let stream = ReceiverStream::new(from_client_rx);
    let sink = PollSender::new(to_client_tx);
    let transport = SseServerTransport {
        stream,
        sink,
        session_id: session.clone(),
        tx_store: app.txs.clone(),
    };
    let transport_send_result = app.transport_tx.send(transport);
    if transport_send_result.is_err() {
        tracing::warn!("send transport out error");
        let mut response =
            Response::new("fail to send out transport, it seems server is closed".to_string());
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        return Err(response);
    }
    let post_path = app.post_path.as_ref();
    let ping_interval = app.sse_ping_interval;
    let stream = futures::stream::once(futures::future::ok(
        Event::default()
            .event("endpoint")
            .data(format!("{post_path}?sessionId={session}")),
    ))
    .chain(ReceiverStream::new(to_client_rx).map(|message| {
        match serde_json::to_string(&message) {
            Ok(bytes) => Ok(Event::default().event("message").data(&bytes)),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }));

    let app_clone = app.clone();
    tokio::spawn(async move {
        // Wait for connection closure
        to_client_tx_clone.closed().await;

        // Clean up session from both local store and state store
        let session_id = session.clone();
        if let Err(e) = app_clone.unregister_session(&session_id).await {
            tracing::error!(%session_id, error = %e, "Failed to unregister session");
        } else {
            tracing::debug!(%session_id, "Closed session and cleaned up resources");
        }
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(ping_interval)))
}

pub struct SseServerTransport {
    stream: ReceiverStream<RxJsonRpcMessage<RoleServer>>,
    sink: PollSender<TxJsonRpcMessage<RoleServer>>,
    session_id: SessionId,
    tx_store: TxStore,
}

impl Sink<TxJsonRpcMessage<RoleServer>> for SseServerTransport {
    type Error = io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.sink
            .poll_ready_unpin(cx)
            .map_err(std::io::Error::other)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: TxJsonRpcMessage<RoleServer>,
    ) -> Result<(), Self::Error> {
        self.sink
            .start_send_unpin(item)
            .map_err(std::io::Error::other)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.sink
            .poll_flush_unpin(cx)
            .map_err(std::io::Error::other)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let inner_close_result = self
            .sink
            .poll_close_unpin(cx)
            .map_err(std::io::Error::other);
        if inner_close_result.is_ready() {
            let session_id = self.session_id.clone();
            let tx_store = self.tx_store.clone();
            tokio::spawn(async move {
                tx_store.write().await.remove(&session_id);
            });
        }
        inner_close_result
    }
}

impl Stream for SseServerTransport {
    type Item = RxJsonRpcMessage<RoleServer>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::StreamExt;
        self.stream.poll_next_unpin(cx)
    }
}

#[derive(Debug, Clone)]
pub struct SseServerConfig {
    pub bind: SocketAddr,
    pub sse_path: String,
    pub post_path: String,
    pub ct: CancellationToken,
    pub sse_keep_alive: Option<Duration>,
    pub state_store: Option<MemoryStateStore>,
}

impl SseServerConfig {
    pub fn with_state_store(mut self, state_store: MemoryStateStore) -> Self {
        self.state_store = Some(state_store);
        self
    }
}

#[derive(Debug)]
pub struct SseServer {
    transport_rx: tokio::sync::mpsc::UnboundedReceiver<SseServerTransport>,
    pub config: SseServerConfig,
}

impl SseServer {
    pub async fn serve(bind: SocketAddr) -> io::Result<Self> {
        Self::serve_with_config(SseServerConfig {
            bind,
            sse_path: "/sse".to_string(),
            post_path: "/message".to_string(),
            ct: CancellationToken::new(),
            sse_keep_alive: None,
            state_store: None,
        })
        .await
    }
    pub async fn serve_with_config(config: SseServerConfig) -> io::Result<Self> {
        let (sse_server, service) = Self::new(config);
        let listener = tokio::net::TcpListener::bind(sse_server.config.bind).await?;
        let ct = sse_server.config.ct.child_token();
        let server = axum::serve(listener, service).with_graceful_shutdown(async move {
            ct.cancelled().await;
            tracing::info!("sse server cancelled");
        });
        tokio::spawn(
            async move {
                if let Err(e) = server.await {
                    tracing::error!(error = %e, "sse server shutdown with error");
                }
            }
            .instrument(tracing::info_span!("sse-server", bind_address = %sse_server.config.bind)),
        );
        Ok(sse_server)
    }

    /// Warning: This function creates a new SseServer instance with the provided configuration.
    /// `App.post_path` may be incorrect if using `Router` as an embedded router.
    pub fn new(config: SseServerConfig) -> (SseServer, Router) {
        let (mut app, transport_rx) = App::new(
            config.post_path.clone(),
            config.sse_keep_alive.unwrap_or(DEFAULT_AUTO_PING_INTERVAL),
        );
        
        // Configure state store if provided
        if let Some(state_store) = config.state_store.clone() {
            app = app.with_state_store(state_store);
        }
        
        let router = Router::new()
            .route(&config.sse_path, get(sse_handler))
            .route(&config.post_path, post(post_event_handler))
            .with_state(app);

        let server = SseServer {
            transport_rx,
            config,
        };

        (server, router)
    }

    pub fn with_service<S, F>(mut self, service_provider: F) -> CancellationToken
    where
        S: Service<RoleServer>,
        F: Fn() -> S + Send + 'static,
    {
        use crate::service::ServiceExt;
        let ct = self.config.ct.clone();
        tokio::spawn(async move {
            while let Some(transport) = self.next_transport().await {
                let service = service_provider();
                let ct = self.config.ct.child_token();
                tokio::spawn(async move {
                    let server = service
                        .serve_with_ct(transport, ct)
                        .await
                        .map_err(std::io::Error::other)?;
                    server.waiting().await?;
                    tokio::io::Result::Ok(())
                });
            }
        });
        ct
    }

    /// This allows you to skip the initialization steps for incoming request.
    pub fn with_service_directly<S, F>(mut self, service_provider: F) -> CancellationToken
    where
        S: Service<RoleServer>,
        F: Fn() -> S + Send + 'static,
    {
        let ct = self.config.ct.clone();
        tokio::spawn(async move {
            while let Some(transport) = self.next_transport().await {
                let service = service_provider();
                let ct = self.config.ct.child_token();
                tokio::spawn(async move {
                    let server = serve_directly_with_ct(service, transport, None, ct).await;
                    server.waiting().await?;
                    tokio::io::Result::Ok(())
                });
            }
        });
        ct
    }

    pub fn cancel(&self) {
        self.config.ct.cancel();
    }

    pub async fn next_transport(&mut self) -> Option<SseServerTransport> {
        self.transport_rx.recv().await
    }
}

impl Stream for SseServer {
    type Item = SseServerTransport;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.transport_rx.poll_recv(cx)
    }
}
