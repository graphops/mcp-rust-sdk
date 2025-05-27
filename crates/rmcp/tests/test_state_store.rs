//! Comprehensive tests for state store implementations.

mod tests {
    use std::{collections::HashSet, time::SystemTime};

    use rmcp::{
        model::RequestId,
        state_store::*,
        transport::common::axum::SessionId,
    };

    #[tokio::test]
    async fn test_memory_state_store_session_lifecycle() {
        let store = MemoryStateStore::new();
        let session_id: SessionId = "test-session-123".into();

        // Session should not exist initially
        assert!(!store.session_exists(&session_id).await.unwrap());

        // Create session
        store.create_session(&session_id).await.unwrap();
        assert!(store.session_exists(&session_id).await.unwrap());

        // Update TTL
        store.update_session_ttl(&session_id, 3600).await.unwrap();

        // Delete session
        store.delete_session(&session_id).await.unwrap();
        assert!(!store.session_exists(&session_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_state_store_session_handles() {
        let store = MemoryStateStore::new();
        let session_id: SessionId = "test-session-456".into();
        
        store.create_session(&session_id).await.unwrap();

        let handle_data = SessionHandleData {
            created_at: SystemTime::now(),
            last_activity: SystemTime::now(),
            channel_capacity: 64,
        };

        // Register session handle
        store.register_session_handle(&session_id, &handle_data).await.unwrap();
        
        // Get session handle
        let retrieved = store.get_session_handle(&session_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().channel_capacity, 64);

        // Remove session handle
        store.remove_session_handle(&session_id).await.unwrap();
        let retrieved = store.get_session_handle(&session_id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_memory_state_store_basic_operations() {
        let store = MemoryStateStore::new();
        let session_id: SessionId = "test-session-basic".into();
        
        // Create session
        store.create_session(&session_id).await.unwrap();
        
        // Test session handle operations
        let handle_data = SessionHandleData {
            created_at: SystemTime::now(),
            last_activity: SystemTime::now(),
            channel_capacity: 64,
        };
        store.register_session_handle(&session_id, &handle_data).await.unwrap();
        let retrieved = store.get_session_handle(&session_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().channel_capacity, 64);

        // Test SSE connection operations
        let connection_data = SseConnectionData {
            created_at: SystemTime::now(),
            last_ping: SystemTime::now(),
            ping_interval: std::time::Duration::from_secs(30),
        };
        store.register_sse_connection(&session_id, &connection_data).await.unwrap();
        let retrieved = store.get_sse_connection(&session_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().ping_interval, std::time::Duration::from_secs(30));

        // Test TX route operations
        let http_request_id = 12345u64;
        let route_data = RouteData {
            resources: HashSet::new(),
            capacity: 64,
            created_at: SystemTime::now(),
        };
        store.store_tx_route(&session_id, http_request_id, &route_data).await.unwrap();
        let retrieved = store.get_tx_route(&session_id, http_request_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().capacity, 64);

        // Test service request operations
        let service_id = "test-service";
        let request_id = RequestId::String("req-123".into());
        let responder_data = ResponderData {
            created_at: SystemTime::now(),
            timeout: Some(std::time::Duration::from_secs(30)),
        };
        store.store_request_responder(service_id, &request_id, &responder_data).await.unwrap();
        let retrieved = store.get_request_responder(service_id, &request_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().timeout, Some(std::time::Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn test_state_store_config() {
        // Test default configuration
        let config = StateStoreConfig::default();
        assert!(!config.requires_network());
        assert_eq!(config.name(), "memory");

        // Test builder
        let config = StateStoreConfigBuilder::new()
            .memory()
            .build();
        matches!(config, StateStoreConfig::Memory);

        #[cfg(feature = "state-store-redis")]
        {
            // Test Redis configurations
            let config = StateStoreConfig::redis_default();
            assert!(config.requires_network());
            assert_eq!(config.name(), "redis");

            let config = StateStoreConfig::redis_url("redis://localhost:6379");
            matches!(config, StateStoreConfig::Redis(_));
        }
    }

    #[tokio::test]
    async fn test_memory_state_store_concurrent_access() {
        let store = MemoryStateStore::new();
        let session_id: SessionId = "concurrent-test".into();
        
        store.create_session(&session_id).await.unwrap();

        // Spawn multiple tasks that access the store concurrently
        let handles: Vec<_> = (0..10).map(|i| {
            let store = store.clone();
            let session_id = session_id.clone();
            tokio::spawn(async move {
                let http_request_id = i as u64;
                let route_data = RouteData {
                    resources: HashSet::new(),
                    capacity: 64 + i,
                    created_at: SystemTime::now(),
                };
                
                // Store, retrieve, and remove route
                store.store_tx_route(&session_id, http_request_id, &route_data).await.unwrap();
                let retrieved = store.get_tx_route(&session_id, http_request_id).await.unwrap();
                assert!(retrieved.is_some());
                assert_eq!(retrieved.unwrap().capacity, 64 + i);
                store.remove_tx_route(&session_id, http_request_id).await.unwrap();
            })
        }).collect();

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all routes were cleaned up
        let routes = store.list_tx_routes(&session_id).await.unwrap();
        assert_eq!(routes.len(), 0);
    }

    #[cfg(feature = "transport-sse-server")]
    #[tokio::test]
    async fn test_sse_server_state_store_integration() {
        use rmcp::transport::sse_server::{SseServerConfig, SseServer};
        use std::net::SocketAddr;
        use tokio_util::sync::CancellationToken;
        
        // Create a memory state store
        let state_store = MemoryStateStore::new();
        
        // Create SSE server config with state store
        let config = SseServerConfig {
            bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            sse_path: "/sse".to_string(),
            post_path: "/message".to_string(),
            ct: CancellationToken::new(),
            sse_keep_alive: None,
            state_store: Some(state_store.clone()),
        };
        
        // Create SSE server (just test construction, not actual serving)
        let (server, _router) = SseServer::new(config);
        assert_eq!(server.config.sse_path, "/sse");
        
        // Verify state store is working
        let session_id: SessionId = "test-session".into();
        assert!(!state_store.session_exists(&session_id).await.unwrap());
        
        // Test session creation
        state_store.create_session(&session_id).await.unwrap();
        assert!(state_store.session_exists(&session_id).await.unwrap());
        
        // Test SSE connection registration
        let connection_data = SseConnectionData {
            created_at: std::time::SystemTime::now(),
            last_ping: std::time::SystemTime::now(),
            ping_interval: std::time::Duration::from_secs(30),
        };
        state_store.register_sse_connection(&session_id, &connection_data).await.unwrap();
        
        let retrieved = state_store.get_sse_connection(&session_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().ping_interval, std::time::Duration::from_secs(30));
    }

    #[cfg(feature = "transport-streamable-http-server-session")]
    #[tokio::test]
    async fn test_session_worker_state_store_integration() {
        use rmcp::transport::streamable_http_server::session::{create_session_with_state_store, SessionConfig};
        
        // Create a memory state store
        let state_store = MemoryStateStore::new();
        let session_id: SessionId = "test-session-worker".into();
        
        // Create session with state store
        let config = SessionConfig::default();
        let (_handle, worker) = create_session_with_state_store(session_id.clone(), config, state_store.clone());
        
        // Verify session worker was created correctly
        assert_eq!(worker.id(), &session_id);
        
        // Test state store registration
        worker.register_session_in_state_store().await.unwrap();
        assert!(state_store.session_exists(&session_id).await.unwrap());
        
        let handle_data = state_store.get_session_handle(&session_id).await.unwrap();
        assert!(handle_data.is_some());
        assert_eq!(handle_data.unwrap().channel_capacity, SessionConfig::DEFAULT_CHANNEL_CAPACITY);
    }
}