//! In-memory state store implementation for backward compatibility and testing.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::SystemTime,
};

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::*;

/// In-memory state store implementation
///
/// This provides the same behavior as the original in-memory storage but through
/// the StateStore trait interface. Suitable for single-instance deployments
/// and testing.
#[derive(Clone, Debug)]
pub struct MemoryStateStore {
    inner: Arc<MemoryStateStoreInner>,
}

#[derive(Debug)]
struct MemoryStateStoreInner {
    // Session data
    sessions: RwLock<HashMap<SessionId, SessionData>>,
    
    // Session handles
    session_handles: RwLock<HashMap<SessionId, SessionHandleData>>,
    
    // SSE connections
    sse_connections: RwLock<HashMap<SessionId, SseConnectionData>>,
    
    // Service request tracking
    request_responders: RwLock<HashMap<String, HashMap<RequestId, ResponderData>>>,
    cancellation_tokens: RwLock<HashMap<String, HashMap<RequestId, CancellationTokenData>>>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct SessionData {
    created_at: SystemTime,
    last_activity: SystemTime,
    tx_routes: HashMap<HttpRequestId, RouteData>,
    resource_routes: HashMap<ResourceKey, HttpRequestId>,
    message_cache: HashMap<Option<HttpRequestId>, VecDeque<ServerSessionMessage>>,
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryStateStoreError {
    #[error("Session not found: {0}")]
    SessionNotFound(String),
    #[error("Route not found")]
    RouteNotFound,
    #[error("Resource not found")]
    ResourceNotFound,
    #[error("Responder not found")]
    ResponderNotFound,
    #[error("Cancellation token not found")]
    CancellationTokenNotFound,
}

impl MemoryStateStore {
    /// Create a new in-memory state store
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MemoryStateStoreInner {
                sessions: RwLock::new(HashMap::new()),
                session_handles: RwLock::new(HashMap::new()),
                sse_connections: RwLock::new(HashMap::new()),
                request_responders: RwLock::new(HashMap::new()),
                cancellation_tokens: RwLock::new(HashMap::new()),
            }),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    type Error = MemoryStateStoreError;

    async fn create_session(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        let now = SystemTime::now();
        
        sessions.insert(session_id.clone(), SessionData {
            created_at: now,
            last_activity: now,
            tx_routes: HashMap::new(),
            resource_routes: HashMap::new(),
            message_cache: HashMap::new(),
        });
        
        Ok(())
    }

    async fn delete_session(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        sessions.remove(session_id);
        
        // Clean up related data
        self.inner.session_handles.write().await.remove(session_id);
        self.inner.sse_connections.write().await.remove(session_id);
        
        Ok(())
    }

    async fn session_exists(&self, session_id: &SessionId) -> Result<bool, Self::Error> {
        let sessions = self.inner.sessions.read().await;
        Ok(sessions.contains_key(session_id))
    }

    async fn update_session_ttl(&self, session_id: &SessionId, _ttl_seconds: u64) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_activity = SystemTime::now();
            Ok(())
        } else {
            Err(MemoryStateStoreError::SessionNotFound(session_id.to_string()))
        }
    }

    async fn register_session_handle(&self, session_id: &SessionId, handle_data: &SessionHandleData) -> Result<(), Self::Error> {
        let mut handles = self.inner.session_handles.write().await;
        handles.insert(session_id.clone(), handle_data.clone());
        Ok(())
    }

    async fn get_session_handle(&self, session_id: &SessionId) -> Result<Option<SessionHandleData>, Self::Error> {
        let handles = self.inner.session_handles.read().await;
        Ok(handles.get(session_id).cloned())
    }

    async fn remove_session_handle(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut handles = self.inner.session_handles.write().await;
        handles.remove(session_id);
        Ok(())
    }

    async fn register_sse_connection(&self, session_id: &SessionId, connection_data: &SseConnectionData) -> Result<(), Self::Error> {
        let mut connections = self.inner.sse_connections.write().await;
        connections.insert(session_id.clone(), connection_data.clone());
        Ok(())
    }

    async fn get_sse_connection(&self, session_id: &SessionId) -> Result<Option<SseConnectionData>, Self::Error> {
        let connections = self.inner.sse_connections.read().await;
        Ok(connections.get(session_id).cloned())
    }

    async fn remove_sse_connection(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut connections = self.inner.sse_connections.write().await;
        connections.remove(session_id);
        Ok(())
    }

    async fn store_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId, route_data: &RouteData) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.tx_routes.insert(http_request_id, route_data.clone());
            session.last_activity = SystemTime::now();
            Ok(())
        } else {
            Err(MemoryStateStoreError::SessionNotFound(session_id.to_string()))
        }
    }

    async fn get_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId) -> Result<Option<RouteData>, Self::Error> {
        let sessions = self.inner.sessions.read().await;
        if let Some(session) = sessions.get(session_id) {
            Ok(session.tx_routes.get(&http_request_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn remove_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.tx_routes.remove(&http_request_id);
            session.last_activity = SystemTime::now();
        }
        Ok(())
    }

    async fn list_tx_routes(&self, session_id: &SessionId) -> Result<Vec<(HttpRequestId, RouteData)>, Self::Error> {
        let sessions = self.inner.sessions.read().await;
        if let Some(session) = sessions.get(session_id) {
            Ok(session.tx_routes.iter().map(|(k, v)| (*k, v.clone())).collect())
        } else {
            Ok(Vec::new())
        }
    }

    async fn store_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey, http_request_id: HttpRequestId) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.resource_routes.insert(resource_key.clone(), http_request_id);
            session.last_activity = SystemTime::now();
            Ok(())
        } else {
            Err(MemoryStateStoreError::SessionNotFound(session_id.to_string()))
        }
    }

    async fn get_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey) -> Result<Option<HttpRequestId>, Self::Error> {
        let sessions = self.inner.sessions.read().await;
        if let Some(session) = sessions.get(session_id) {
            Ok(session.resource_routes.get(resource_key).copied())
        } else {
            Ok(None)
        }
    }

    async fn remove_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.resource_routes.remove(resource_key);
            session.last_activity = SystemTime::now();
        }
        Ok(())
    }

    async fn list_resource_routes(&self, session_id: &SessionId) -> Result<Vec<(ResourceKey, HttpRequestId)>, Self::Error> {
        let sessions = self.inner.sessions.read().await;
        if let Some(session) = sessions.get(session_id) {
            Ok(session.resource_routes.iter().map(|(k, v)| (k.clone(), *v)).collect())
        } else {
            Ok(Vec::new())
        }
    }

    async fn cache_message(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, message: &ServerSessionMessage) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            let cache = session.message_cache.entry(http_request_id).or_insert_with(VecDeque::new);
            cache.push_back(message.clone());
            session.last_activity = SystemTime::now();
            Ok(())
        } else {
            Err(MemoryStateStoreError::SessionNotFound(session_id.to_string()))
        }
    }

    async fn get_cached_messages(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, from_index: usize) -> Result<Vec<ServerSessionMessage>, Self::Error> {
        let sessions = self.inner.sessions.read().await;
        if let Some(session) = sessions.get(session_id) {
            if let Some(cache) = session.message_cache.get(&http_request_id) {
                Ok(cache.iter().skip(from_index).cloned().collect())
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    async fn trim_cache(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, max_size: usize) -> Result<(), Self::Error> {
        let mut sessions = self.inner.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            if let Some(cache) = session.message_cache.get_mut(&http_request_id) {
                while cache.len() > max_size {
                    cache.pop_front();
                }
            }
            session.last_activity = SystemTime::now();
        }
        Ok(())
    }

    async fn store_request_responder(&self, service_id: &str, request_id: &RequestId, responder_data: &ResponderData) -> Result<(), Self::Error> {
        let mut responders = self.inner.request_responders.write().await;
        let service_responders = responders.entry(service_id.to_string()).or_insert_with(HashMap::new);
        service_responders.insert(request_id.clone(), responder_data.clone());
        Ok(())
    }

    async fn get_request_responder(&self, service_id: &str, request_id: &RequestId) -> Result<Option<ResponderData>, Self::Error> {
        let responders = self.inner.request_responders.read().await;
        if let Some(service_responders) = responders.get(service_id) {
            Ok(service_responders.get(request_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn remove_request_responder(&self, service_id: &str, request_id: &RequestId) -> Result<(), Self::Error> {
        let mut responders = self.inner.request_responders.write().await;
        if let Some(service_responders) = responders.get_mut(service_id) {
            service_responders.remove(request_id);
        }
        Ok(())
    }

    async fn store_cancellation_token(&self, service_id: &str, request_id: &RequestId, token_data: &CancellationTokenData) -> Result<(), Self::Error> {
        let mut tokens = self.inner.cancellation_tokens.write().await;
        let service_tokens = tokens.entry(service_id.to_string()).or_insert_with(HashMap::new);
        service_tokens.insert(request_id.clone(), token_data.clone());
        Ok(())
    }

    async fn get_cancellation_token(&self, service_id: &str, request_id: &RequestId) -> Result<Option<CancellationTokenData>, Self::Error> {
        let tokens = self.inner.cancellation_tokens.read().await;
        if let Some(service_tokens) = tokens.get(service_id) {
            Ok(service_tokens.get(request_id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn remove_cancellation_token(&self, service_id: &str, request_id: &RequestId) -> Result<(), Self::Error> {
        let mut tokens = self.inner.cancellation_tokens.write().await;
        if let Some(service_tokens) = tokens.get_mut(service_id) {
            service_tokens.remove(request_id);
        }
        Ok(())
    }
}