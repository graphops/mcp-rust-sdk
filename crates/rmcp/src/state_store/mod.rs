//! State store abstraction for horizontal scaling support.
//!
//! This module provides traits and implementations for externalizing session state
//! to enable horizontal scaling of MCP servers behind load balancers.

use std::{collections::HashSet, time::SystemTime};

use serde::{Deserialize, Serialize};

use async_trait::async_trait;

use crate::model::RequestId;

#[cfg(any(feature = "transport-streamable-http-server", feature = "transport-sse-server"))]
use crate::transport::common::axum::SessionId;

#[cfg(feature = "transport-streamable-http-server-session")]
use crate::transport::streamable_http_server::session::{ResourceKey, ServerSessionMessage};

// Fallback types when transport features are not enabled
#[cfg(not(any(feature = "transport-streamable-http-server", feature = "transport-sse-server")))]
pub type SessionId = String;

#[cfg(not(feature = "transport-streamable-http-server-session"))]
#[derive(Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResourceKey {
    McpRequestId(RequestId),
    ProgressToken(String),
}

#[cfg(not(feature = "transport-streamable-http-server-session"))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ServerSessionMessage {
    pub event_id: EventId,
    pub message: std::sync::Arc<serde_json::Value>,
}

#[cfg(not(feature = "transport-streamable-http-server-session"))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EventId {
    pub http_request_id: Option<HttpRequestId>,
    pub index: usize,
}

pub mod config;
pub mod memory;
#[cfg(feature = "state-store-redis")]
pub mod redis;

/// HTTP request identifier used for routing
pub type HttpRequestId = u64;

/// State store abstraction for session persistence
#[async_trait]
pub trait StateStore: Send + Sync + Clone {
    type Error: std::error::Error + Send + Sync + 'static;

    // Session lifecycle management
    async fn create_session(&self, session_id: &SessionId) -> Result<(), Self::Error>;
    async fn delete_session(&self, session_id: &SessionId) -> Result<(), Self::Error>;
    async fn session_exists(&self, session_id: &SessionId) -> Result<bool, Self::Error>;
    async fn update_session_ttl(&self, session_id: &SessionId, ttl_seconds: u64) -> Result<(), Self::Error>;

    // Session handle registry (for streamable HTTP)
    async fn register_session_handle(&self, session_id: &SessionId, handle_data: &SessionHandleData) -> Result<(), Self::Error>;
    async fn get_session_handle(&self, session_id: &SessionId) -> Result<Option<SessionHandleData>, Self::Error>;
    async fn remove_session_handle(&self, session_id: &SessionId) -> Result<(), Self::Error>;

    // SSE connection state
    async fn register_sse_connection(&self, session_id: &SessionId, connection_data: &SseConnectionData) -> Result<(), Self::Error>;
    async fn get_sse_connection(&self, session_id: &SessionId) -> Result<Option<SseConnectionData>, Self::Error>;
    async fn remove_sse_connection(&self, session_id: &SessionId) -> Result<(), Self::Error>;

    // HTTP request routing state
    async fn store_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId, route_data: &RouteData) -> Result<(), Self::Error>;
    async fn get_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId) -> Result<Option<RouteData>, Self::Error>;
    async fn remove_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId) -> Result<(), Self::Error>;
    async fn list_tx_routes(&self, session_id: &SessionId) -> Result<Vec<(HttpRequestId, RouteData)>, Self::Error>;

    // Resource routing state
    async fn store_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey, http_request_id: HttpRequestId) -> Result<(), Self::Error>;
    async fn get_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey) -> Result<Option<HttpRequestId>, Self::Error>;
    async fn remove_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey) -> Result<(), Self::Error>;
    async fn list_resource_routes(&self, session_id: &SessionId) -> Result<Vec<(ResourceKey, HttpRequestId)>, Self::Error>;

    // Message caching for session replay
    async fn cache_message(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, message: &ServerSessionMessage) -> Result<(), Self::Error>;
    async fn get_cached_messages(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, from_index: usize) -> Result<Vec<ServerSessionMessage>, Self::Error>;
    async fn trim_cache(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, max_size: usize) -> Result<(), Self::Error>;

    // Service request tracking
    async fn store_request_responder(&self, service_id: &str, request_id: &RequestId, responder_data: &ResponderData) -> Result<(), Self::Error>;
    async fn get_request_responder(&self, service_id: &str, request_id: &RequestId) -> Result<Option<ResponderData>, Self::Error>;
    async fn remove_request_responder(&self, service_id: &str, request_id: &RequestId) -> Result<(), Self::Error>;

    // Cancellation token tracking
    async fn store_cancellation_token(&self, service_id: &str, request_id: &RequestId, token_data: &CancellationTokenData) -> Result<(), Self::Error>;
    async fn get_cancellation_token(&self, service_id: &str, request_id: &RequestId) -> Result<Option<CancellationTokenData>, Self::Error>;
    async fn remove_cancellation_token(&self, service_id: &str, request_id: &RequestId) -> Result<(), Self::Error>;
}

/// HTTP request routing data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteData {
    pub resources: HashSet<ResourceKey>,
    pub capacity: usize,
    pub created_at: SystemTime,
}

/// Session handle metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionHandleData {
    pub created_at: SystemTime,
    pub last_activity: SystemTime,
    pub channel_capacity: usize,
}

/// SSE connection metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SseConnectionData {
    pub created_at: SystemTime,
    pub last_ping: SystemTime,
    pub ping_interval: std::time::Duration,
}

/// Service request responder metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponderData {
    pub created_at: SystemTime,
    pub timeout: Option<std::time::Duration>,
    // Note: Cannot serialize actual responder channel, requires reconnection logic
}

/// Cancellation token state
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancellationTokenData {
    pub created_at: SystemTime,
    pub is_cancelled: bool,
    pub cancel_reason: Option<String>,
}

// Re-export implementations
pub use config::{StateStoreConfig, StateStoreConfigBuilder};
pub use memory::MemoryStateStore;
#[cfg(feature = "state-store-redis")]
pub use redis::{RedisConfig, RedisStateStore};