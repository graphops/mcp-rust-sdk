//! Redis state store implementation for horizontal scaling.

use std::time::Duration;

use async_trait::async_trait;
use ::redis::{aio::ConnectionManager, AsyncCommands, Client, RedisError};
use serde::{Deserialize, Serialize};

use super::*;

/// Redis state store configuration
#[derive(Clone, Debug)]
pub struct RedisConfig {
    /// Redis connection URL(s) - supports single instance and cluster
    pub urls: Vec<String>,
    /// Key prefix for namespacing
    pub key_prefix: String,
    /// Default TTL for sessions in seconds
    pub session_ttl: Duration,
    /// Maximum number of cached messages per channel
    pub cache_capacity: usize,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            urls: vec!["redis://127.0.0.1:6379".to_string()],
            key_prefix: "rmcp".to_string(),
            session_ttl: Duration::from_secs(3600), // 1 hour
            cache_capacity: 1000,
        }
    }
}

/// Redis state store implementation
#[derive(Clone)]
pub struct RedisStateStore {
    connection: ConnectionManager,
    config: RedisConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum RedisStateStoreError {
    #[error("Redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("Session not found: {0}")]
    SessionNotFound(String),
}

impl RedisStateStore {
    /// Create a new Redis state store
    pub async fn new(config: RedisConfig) -> Result<Self, RedisStateStoreError> {
        let client = Client::open(config.urls[0].as_str())?;
        let connection = ConnectionManager::new(client).await?;

        Ok(Self {
            connection,
            config,
        })
    }

    fn session_key(&self, session_id: &SessionId) -> String {
        format!("{}:session:{}:meta", self.config.key_prefix, session_id)
    }

    fn session_handle_key(&self, session_id: &SessionId) -> String {
        format!("{}:session:{}:handle", self.config.key_prefix, session_id)
    }

    fn sse_connection_key(&self, session_id: &SessionId) -> String {
        format!("{}:sse:{}:connection", self.config.key_prefix, session_id)
    }

    fn sse_connections_key(&self) -> String {
        format!("{}:sse:connections", self.config.key_prefix)
    }

    fn tx_routes_key(&self, session_id: &SessionId) -> String {
        format!("{}:session:{}:tx_routes", self.config.key_prefix, session_id)
    }

    fn resource_routes_key(&self, session_id: &SessionId) -> String {
        format!("{}:session:{}:resource_routes", self.config.key_prefix, session_id)
    }

    fn cache_key(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>) -> String {
        match http_request_id {
            Some(id) => format!("{}:session:{}:cache:request:{}", self.config.key_prefix, session_id, id),
            None => format!("{}:session:{}:cache:common", self.config.key_prefix, session_id),
        }
    }

    fn responders_key(&self, service_id: &str) -> String {
        format!("{}:service:{}:responders", self.config.key_prefix, service_id)
    }

    fn tokens_key(&self, service_id: &str) -> String {
        format!("{}:service:{}:cancellation_tokens", self.config.key_prefix, service_id)
    }

    async fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, RedisStateStoreError> {
        bincode::serialize(value).map_err(RedisStateStoreError::Serialization)
    }

    async fn deserialize<T: for<'de> Deserialize<'de>>(&self, data: Vec<u8>) -> Result<T, RedisStateStoreError> {
        bincode::deserialize(&data).map_err(RedisStateStoreError::Serialization)
    }
}

#[async_trait]
impl StateStore for RedisStateStore {
    type Error = RedisStateStoreError;

    async fn create_session(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.session_key(session_id);
        let session_meta = SessionMeta {
            created_at: SystemTime::now(),
            last_activity: SystemTime::now(),
        };
        
        let data = self.serialize(&session_meta).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        let _: () = conn.set_ex(key, data, ttl.try_into().unwrap_or(3600)).await?;
        Ok(())
    }

    async fn delete_session(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        
        // Delete all session-related keys
        let keys = vec![
            self.session_key(session_id),
            self.session_handle_key(session_id),
            self.sse_connection_key(session_id),
            self.tx_routes_key(session_id),
            self.resource_routes_key(session_id),
            self.cache_key(session_id, None),
        ];
        
        // Also clean up request-specific caches
        let cache_pattern = format!("{}:session:{}:cache:request:*", self.config.key_prefix, session_id);
        let cache_keys: Vec<String> = conn.keys(&cache_pattern).await?;
        
        let mut all_keys = keys;
        all_keys.extend(cache_keys);
        
        if !all_keys.is_empty() {
            let _: () = conn.del(all_keys).await?;
        }
        
        Ok(())
    }

    async fn session_exists(&self, session_id: &SessionId) -> Result<bool, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.session_key(session_id);
        let exists: bool = conn.exists(&key).await?;
        Ok(exists)
    }

    async fn update_session_ttl(&self, session_id: &SessionId, ttl_seconds: u64) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.session_key(session_id);
        
        // Update session metadata with new activity time
        if let Ok(data) = conn.get::<_, Vec<u8>>(&key).await {
            if let Ok(mut session_meta) = self.deserialize::<SessionMeta>(data).await {
                session_meta.last_activity = SystemTime::now();
                let updated_data = self.serialize(&session_meta).await?;
                let _: () = conn.set_ex(&key, updated_data, ttl_seconds).await?;
            }
        }
        
        Ok(())
    }

    async fn register_session_handle(&self, session_id: &SessionId, handle_data: &SessionHandleData) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.session_handle_key(session_id);
        let data = self.serialize(handle_data).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        let _: () = conn.set_ex(key, data, ttl.try_into().unwrap_or(3600)).await?;
        Ok(())
    }

    async fn get_session_handle(&self, session_id: &SessionId) -> Result<Option<SessionHandleData>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.session_handle_key(session_id);
        
        match conn.get::<_, Vec<u8>>(&key).await {
            Ok(data) => Ok(Some(self.deserialize(data).await?)),
            Err(_) => Ok(None),
        }
    }

    async fn remove_session_handle(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.session_handle_key(session_id);
        let _: () = conn.del(&key).await?;
        Ok(())
    }

    async fn register_sse_connection(&self, session_id: &SessionId, connection_data: &SseConnectionData) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let individual_key = self.sse_connection_key(session_id);
        let connections_hash_key = self.sse_connections_key();
        let data = self.serialize(connection_data).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        // Store individual connection
        let _: () = conn.set_ex(&individual_key, &data, ttl.try_into().unwrap_or(3600)).await?;
        
        // Also store in connections hash for discovery
        let session_key = self.serialize(session_id).await?;
        let _: () = conn.hset(&connections_hash_key, session_key, &data).await?;
        let _: () = conn.expire(&connections_hash_key, ttl as i64).await?;
        
        Ok(())
    }

    async fn get_sse_connection(&self, session_id: &SessionId) -> Result<Option<SseConnectionData>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.sse_connection_key(session_id);
        
        match conn.get::<_, Vec<u8>>(&key).await {
            Ok(data) => Ok(Some(self.deserialize(data).await?)),
            Err(_) => Ok(None),
        }
    }

    async fn remove_sse_connection(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let individual_key = self.sse_connection_key(session_id);
        let connections_hash_key = self.sse_connections_key();
        let session_key = self.serialize(session_id).await?;
        
        // Remove individual connection
        let _: () = conn.del(&individual_key).await?;
        
        // Also remove from connections hash
        let _: () = conn.hdel(&connections_hash_key, session_key).await?;
        
        Ok(())
    }

    async fn store_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId, route_data: &RouteData) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.tx_routes_key(session_id);
        let data = self.serialize(route_data).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        let _: () = conn.hset(&key, http_request_id, data).await?;
        let _: () = conn.expire(&key, ttl as i64).await?;
        Ok(())
    }

    async fn get_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId) -> Result<Option<RouteData>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.tx_routes_key(session_id);
        
        match conn.hget::<_, _, Vec<u8>>(&key, http_request_id).await {
            Ok(data) => Ok(Some(self.deserialize(data).await?)),
            Err(_) => Ok(None),
        }
    }

    async fn remove_tx_route(&self, session_id: &SessionId, http_request_id: HttpRequestId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.tx_routes_key(session_id);
        let _: () = conn.hdel(&key, http_request_id).await?;
        Ok(())
    }

    async fn list_tx_routes(&self, session_id: &SessionId) -> Result<Vec<(HttpRequestId, RouteData)>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.tx_routes_key(session_id);
        
        let hash: std::collections::HashMap<String, Vec<u8>> = conn.hgetall(&key).await?;
        let mut routes = Vec::new();
        
        for (id_str, data) in hash {
            if let Ok(id) = id_str.parse::<HttpRequestId>() {
                if let Ok(route_data) = self.deserialize(data).await {
                    routes.push((id, route_data));
                }
            }
        }
        
        Ok(routes)
    }

    async fn store_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey, http_request_id: HttpRequestId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.resource_routes_key(session_id);
        let resource_key_str = self.serialize(resource_key).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        let _: () = conn.hset(&key, resource_key_str, http_request_id).await?;
        let _: () = conn.expire(&key, ttl as i64).await?;
        Ok(())
    }

    async fn get_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey) -> Result<Option<HttpRequestId>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.resource_routes_key(session_id);
        let resource_key_str = self.serialize(resource_key).await?;
        
        match conn.hget::<_, _, HttpRequestId>(&key, resource_key_str).await {
            Ok(id) => Ok(Some(id)),
            Err(_) => Ok(None),
        }
    }

    async fn remove_resource_route(&self, session_id: &SessionId, resource_key: &ResourceKey) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.resource_routes_key(session_id);
        let resource_key_str = self.serialize(resource_key).await?;
        
        let _: () = conn.hdel(&key, resource_key_str).await?;
        Ok(())
    }

    async fn list_resource_routes(&self, session_id: &SessionId) -> Result<Vec<(ResourceKey, HttpRequestId)>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.resource_routes_key(session_id);
        
        let hash: std::collections::HashMap<Vec<u8>, HttpRequestId> = conn.hgetall(&key).await?;
        let mut routes = Vec::new();
        
        for (key_data, id) in hash {
            if let Ok(resource_key) = self.deserialize(key_data).await {
                routes.push((resource_key, id));
            }
        }
        
        Ok(routes)
    }

    async fn cache_message(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, message: &ServerSessionMessage) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.cache_key(session_id, http_request_id);
        let data = self.serialize(message).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        // Add to list and trim to capacity
        let _: () = conn.lpush(&key, data).await?;
        let _: () = conn.ltrim(&key, 0, (self.config.cache_capacity - 1) as isize).await?;
        let _: () = conn.expire(&key, ttl as i64).await?;
        
        Ok(())
    }

    async fn get_cached_messages(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, from_index: usize) -> Result<Vec<ServerSessionMessage>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.cache_key(session_id, http_request_id);
        
        let data_list: Vec<Vec<u8>> = conn.lrange(&key, from_index as isize, -1).await?;
        let mut messages = Vec::new();
        
        for data in data_list {
            if let Ok(message) = self.deserialize(data).await {
                messages.push(message);
            }
        }
        
        // Reverse because Redis lists are LIFO
        messages.reverse();
        Ok(messages)
    }

    async fn trim_cache(&self, session_id: &SessionId, http_request_id: Option<HttpRequestId>, max_size: usize) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.cache_key(session_id, http_request_id);
        
        let _: () = conn.ltrim(&key, 0, (max_size - 1) as isize).await?;
        Ok(())
    }

    async fn store_request_responder(&self, service_id: &str, request_id: &RequestId, responder_data: &ResponderData) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.responders_key(service_id);
        let request_key = self.serialize(request_id).await?;
        let data = self.serialize(responder_data).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        let _: () = conn.hset(&key, request_key, data).await?;
        let _: () = conn.expire(&key, ttl as i64).await?;
        Ok(())
    }

    async fn get_request_responder(&self, service_id: &str, request_id: &RequestId) -> Result<Option<ResponderData>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.responders_key(service_id);
        let request_key = self.serialize(request_id).await?;
        
        match conn.hget::<_, _, Vec<u8>>(&key, request_key).await {
            Ok(data) => Ok(Some(self.deserialize(data).await?)),
            Err(_) => Ok(None),
        }
    }

    async fn remove_request_responder(&self, service_id: &str, request_id: &RequestId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.responders_key(service_id);
        let request_key = self.serialize(request_id).await?;
        
        let _: () = conn.hdel(&key, request_key).await?;
        Ok(())
    }

    async fn store_cancellation_token(&self, service_id: &str, request_id: &RequestId, token_data: &CancellationTokenData) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.tokens_key(service_id);
        let request_key = self.serialize(request_id).await?;
        let data = self.serialize(token_data).await?;
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        let _: () = conn.hset(&key, request_key, data).await?;
        let _: () = conn.expire(&key, ttl as i64).await?;
        Ok(())
    }

    async fn get_cancellation_token(&self, service_id: &str, request_id: &RequestId) -> Result<Option<CancellationTokenData>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.tokens_key(service_id);
        let request_key = self.serialize(request_id).await?;
        
        match conn.hget::<_, _, Vec<u8>>(&key, request_key).await {
            Ok(data) => Ok(Some(self.deserialize(data).await?)),
            Err(_) => Ok(None),
        }
    }

    async fn remove_cancellation_token(&self, service_id: &str, request_id: &RequestId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.tokens_key(service_id);
        let request_key = self.serialize(request_id).await?;
        
        let _: () = conn.hdel(&key, request_key).await?;
        Ok(())
    }

    async fn find_session_instance(&self, session_id: &SessionId) -> Result<Option<String>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.sse_connections_key();
        let session_key = self.serialize(session_id).await?;
        
        match conn.hget::<_, _, Vec<u8>>(&key, session_key).await {
            Ok(data) => {
                let connection_data: SseConnectionData = self.deserialize(data).await?;
                Ok(Some(connection_data.server_instance_id))
            },
            Err(_) => Ok(None),
        }
    }

    async fn list_active_sessions(&self) -> Result<Vec<(SessionId, String)>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.sse_connections_key();
        
        let hash: std::collections::HashMap<Vec<u8>, Vec<u8>> = conn.hgetall(&key).await?;
        let mut sessions = Vec::new();
        
        for (session_data, connection_data) in hash {
            if let (Ok(session_id), Ok(connection)) = (
                self.deserialize::<SessionId>(session_data).await,
                self.deserialize::<SseConnectionData>(connection_data).await,
            ) {
                sessions.push((session_id, connection.server_instance_id));
            }
        }
        
        Ok(sessions)
    }

    async fn list_sessions_by_instance(&self, server_instance_id: &str) -> Result<Vec<SessionId>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.sse_connections_key();
        
        let hash: std::collections::HashMap<Vec<u8>, Vec<u8>> = conn.hgetall(&key).await?;
        let mut sessions = Vec::new();
        
        for (session_data, connection_data) in hash {
            if let (Ok(session_id), Ok(connection)) = (
                self.deserialize::<SessionId>(session_data).await,
                self.deserialize::<SseConnectionData>(connection_data).await,
            ) {
                if connection.server_instance_id == server_instance_id {
                    sessions.push(session_id);
                }
            }
        }
        
        Ok(sessions)
    }

    async fn update_session_heartbeat(&self, session_id: &SessionId) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let individual_key = self.sse_connection_key(session_id);
        let connections_hash_key = self.sse_connections_key();
        let session_key = self.serialize(session_id).await?;
        
        // Get current connection data
        match conn.get::<_, Vec<u8>>(&individual_key).await {
            Ok(data) => {
                let mut connection_data: SseConnectionData = self.deserialize(data).await?;
                connection_data.last_ping = std::time::SystemTime::now();
                
                // Update both individual key and hash
                let updated_data = self.serialize(&connection_data).await?;
                let ttl = self.config.session_ttl.as_secs() as usize;
                
                let _: () = conn.set_ex(&individual_key, &updated_data, ttl.try_into().unwrap_or(3600)).await?;
                let _: () = conn.hset(&connections_hash_key, session_key, &updated_data).await?;
                let _: () = conn.expire(&connections_hash_key, ttl as i64).await?;
            },
            Err(_) => {
                // Session not found, ignore
            }
        }
        
        Ok(())
    }

    async fn is_session_healthy(&self, session_id: &SessionId, max_idle_duration: std::time::Duration) -> Result<bool, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.sse_connection_key(session_id);
        
        match conn.get::<_, Vec<u8>>(&key).await {
            Ok(data) => {
                let connection_data: SseConnectionData = self.deserialize(data).await?;
                let now = std::time::SystemTime::now();
                
                if let Ok(elapsed) = now.duration_since(connection_data.last_ping) {
                    Ok(elapsed < max_idle_duration)
                } else {
                    Ok(false) // Invalid time, consider unhealthy
                }
            },
            Err(_) => Ok(false), // Session not found, consider unhealthy
        }
    }

    async fn cleanup_stale_sessions(&self, max_idle_duration: std::time::Duration) -> Result<Vec<SessionId>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = self.sse_connections_key();
        let now = std::time::SystemTime::now();
        
        let hash: std::collections::HashMap<Vec<u8>, Vec<u8>> = conn.hgetall(&key).await?;
        let mut stale_sessions = Vec::new();
        
        for (session_data, connection_data) in hash {
            if let (Ok(session_id), Ok(connection)) = (
                self.deserialize::<SessionId>(session_data).await,
                self.deserialize::<SseConnectionData>(connection_data).await,
            ) {
                let is_stale = if let Ok(elapsed) = now.duration_since(connection.last_ping) {
                    elapsed >= max_idle_duration
                } else {
                    true // Invalid time, consider stale
                };
                
                if is_stale {
                    // Remove from both hash and individual key
                    let session_key = self.serialize(&session_id).await?;
                    let individual_key = self.sse_connection_key(&session_id);
                    let _: () = conn.hdel(&key, &session_key).await?;
                    let _: () = conn.del(&individual_key).await?;
                    stale_sessions.push(session_id);
                }
            }
        }
        
        Ok(stale_sessions)
    }

    async fn mark_instance_unhealthy(&self, server_instance_id: &str) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let key = format!("{}:instance_health", self.config.key_prefix);
        let ttl = self.config.session_ttl.as_secs() as usize;
        
        // Get or create health data
        let health_data = match conn.hget::<_, _, Vec<u8>>(&key, server_instance_id).await {
            Ok(data) => {
                let mut health: InstanceHealthData = self.deserialize(data).await?;
                health.is_healthy = false;
                health.failed_health_checks += 1;
                health.last_heartbeat = std::time::SystemTime::now();
                health
            },
            Err(_) => {
                InstanceHealthData {
                    server_instance_id: server_instance_id.to_string(),
                    last_heartbeat: std::time::SystemTime::now(),
                    is_healthy: false,
                    failed_health_checks: 1,
                }
            }
        };
        
        let data = self.serialize(&health_data).await?;
        let _: () = conn.hset(&key, server_instance_id, data).await?;
        let _: () = conn.expire(&key, ttl as i64).await?;
        
        Ok(())
    }

    async fn get_healthy_instances(&self) -> Result<Vec<String>, Self::Error> {
        let mut conn = self.connection.clone();
        let key = format!("{}:instance_health", self.config.key_prefix);
        
        let hash: std::collections::HashMap<String, Vec<u8>> = conn.hgetall(&key).await?;
        let mut healthy_instances = Vec::new();
        
        for (_instance_id, health_data) in hash {
            if let Ok(health) = self.deserialize::<InstanceHealthData>(health_data).await {
                if health.is_healthy {
                    healthy_instances.push(health.server_instance_id);
                }
            }
        }
        
        Ok(healthy_instances)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionMeta {
    created_at: SystemTime,
    last_activity: SystemTime,
}