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
            server_instance_id: "test-server-1".to_string(),
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
            server_instance_id: "test-server-1".to_string(),
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

    #[tokio::test]
    async fn test_service_state_store_integration() {
        // Create a memory state store
        let state_store = MemoryStateStore::new();
        let service_id = "test-service".to_string();
        
        // Test state store operations for service requests
        let request_id = RequestId::String("test-req-123".into());
        
        // Store responder data
        let responder_data = ResponderData {
            created_at: std::time::SystemTime::now(),
            timeout: Some(std::time::Duration::from_secs(30)),
        };
        state_store.store_request_responder(&service_id, &request_id, &responder_data).await.unwrap();
        
        // Verify responder data can be retrieved
        let retrieved = state_store.get_request_responder(&service_id, &request_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().timeout, Some(std::time::Duration::from_secs(30)));
        
        // Store cancellation token data
        let token_data = CancellationTokenData {
            created_at: std::time::SystemTime::now(),
            is_cancelled: false,
            cancel_reason: None,
        };
        state_store.store_cancellation_token(&service_id, &request_id, &token_data).await.unwrap();
        
        // Verify token data can be retrieved
        let retrieved = state_store.get_cancellation_token(&service_id, &request_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().is_cancelled, false);
        
        // Test token cancellation
        let cancelled_token_data = CancellationTokenData {
            created_at: std::time::SystemTime::now(),
            is_cancelled: true,
            cancel_reason: Some("Test cancellation".to_string()),
        };
        state_store.store_cancellation_token(&service_id, &request_id, &cancelled_token_data).await.unwrap();
        
        let retrieved = state_store.get_cancellation_token(&service_id, &request_id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved_data = retrieved.unwrap();
        assert_eq!(retrieved_data.is_cancelled, true);
        assert_eq!(retrieved_data.cancel_reason, Some("Test cancellation".to_string()));
        
        // Clean up
        state_store.remove_request_responder(&service_id, &request_id).await.unwrap();
        state_store.remove_cancellation_token(&service_id, &request_id).await.unwrap();
        
        let retrieved = state_store.get_request_responder(&service_id, &request_id).await.unwrap();
        assert!(retrieved.is_none());
        let retrieved = state_store.get_cancellation_token(&service_id, &request_id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_session_discovery_and_routing() {
        use rmcp::state_store::{MemoryStateStore, StateStore, SseConnectionData};
        
        // Create a memory state store
        let state_store = MemoryStateStore::new();
        
        // Register multiple sessions with different server instances
        let session1: SessionId = "session-1".into();
        let session2: SessionId = "session-2".into();
        let session3: SessionId = "session-3".into();
        
        let connection_data1 = SseConnectionData {
            created_at: std::time::SystemTime::now(),
            last_ping: std::time::SystemTime::now(),
            ping_interval: std::time::Duration::from_secs(30),
            server_instance_id: "server-A".to_string(),
        };
        
        let connection_data2 = SseConnectionData {
            created_at: std::time::SystemTime::now(),
            last_ping: std::time::SystemTime::now(),
            ping_interval: std::time::Duration::from_secs(30),
            server_instance_id: "server-B".to_string(),
        };
        
        let connection_data3 = SseConnectionData {
            created_at: std::time::SystemTime::now(),
            last_ping: std::time::SystemTime::now(),
            ping_interval: std::time::Duration::from_secs(30),
            server_instance_id: "server-A".to_string(),
        };
        
        // Register sessions
        state_store.register_sse_connection(&session1, &connection_data1).await.unwrap();
        state_store.register_sse_connection(&session2, &connection_data2).await.unwrap();
        state_store.register_sse_connection(&session3, &connection_data3).await.unwrap();
        
        // Test find_session_instance
        let instance = state_store.find_session_instance(&session1).await.unwrap();
        assert_eq!(instance, Some("server-A".to_string()));
        
        let instance = state_store.find_session_instance(&session2).await.unwrap();
        assert_eq!(instance, Some("server-B".to_string()));
        
        // Test for non-existent session
        let non_existent: SessionId = "non-existent".into();
        let instance = state_store.find_session_instance(&non_existent).await.unwrap();
        assert_eq!(instance, None);
        
        // Test list_active_sessions
        let all_sessions = state_store.list_active_sessions().await.unwrap();
        assert_eq!(all_sessions.len(), 3);
        
        // Verify all sessions are present
        let session_instances: std::collections::HashMap<_, _> = all_sessions.into_iter().collect();
        assert_eq!(session_instances.get(&session1), Some(&"server-A".to_string()));
        assert_eq!(session_instances.get(&session2), Some(&"server-B".to_string()));
        assert_eq!(session_instances.get(&session3), Some(&"server-A".to_string()));
        
        // Test list_sessions_by_instance
        let server_a_sessions = state_store.list_sessions_by_instance("server-A").await.unwrap();
        assert_eq!(server_a_sessions.len(), 2);
        assert!(server_a_sessions.contains(&session1));
        assert!(server_a_sessions.contains(&session3));
        
        let server_b_sessions = state_store.list_sessions_by_instance("server-B").await.unwrap();
        assert_eq!(server_b_sessions.len(), 1);
        assert!(server_b_sessions.contains(&session2));
        
        // Test for non-existent server instance
        let empty_sessions = state_store.list_sessions_by_instance("server-C").await.unwrap();
        assert_eq!(empty_sessions.len(), 0);
        
        // Clean up
        state_store.remove_sse_connection(&session1).await.unwrap();
        state_store.remove_sse_connection(&session2).await.unwrap();
        state_store.remove_sse_connection(&session3).await.unwrap();
        
        // Verify cleanup
        let all_sessions = state_store.list_active_sessions().await.unwrap();
        assert_eq!(all_sessions.len(), 0);
    }

    #[tokio::test]
    async fn test_connection_health_monitoring() {
        use rmcp::state_store::{MemoryStateStore, StateStore, SseConnectionData};
        use std::time::Duration;
        
        // Create a memory state store
        let state_store = MemoryStateStore::new();
        
        // Register a session
        let session_id: SessionId = "health-test-session".into();
        let connection_data = SseConnectionData {
            created_at: std::time::SystemTime::now(),
            last_ping: std::time::SystemTime::now(),
            ping_interval: std::time::Duration::from_secs(30),
            server_instance_id: "server-health-test".to_string(),
        };
        
        state_store.register_sse_connection(&session_id, &connection_data).await.unwrap();
        
        // Test session is initially healthy
        let is_healthy = state_store.is_session_healthy(&session_id, Duration::from_secs(60)).await.unwrap();
        assert_eq!(is_healthy, true);
        
        // Update heartbeat
        state_store.update_session_heartbeat(&session_id).await.unwrap();
        
        // Still healthy after heartbeat update
        let is_healthy = state_store.is_session_healthy(&session_id, Duration::from_secs(60)).await.unwrap();
        assert_eq!(is_healthy, true);
        
        // Test cleanup with very short timeout (should remove the session)
        let stale_sessions = state_store.cleanup_stale_sessions(Duration::from_millis(1)).await.unwrap();
        
        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Now session should be considered stale and removed
        let stale_sessions = state_store.cleanup_stale_sessions(Duration::from_millis(1)).await.unwrap();
        // Note: cleanup_stale_sessions already removed it, so this should be empty
        
        // Verify session is gone
        let all_sessions = state_store.list_active_sessions().await.unwrap();
        assert_eq!(all_sessions.len(), 0);
        
        // Test instance health tracking
        let instance_id = "test-instance-1";
        
        // Mark instance as unhealthy
        state_store.mark_instance_unhealthy(instance_id).await.unwrap();
        
        // Check healthy instances (should be empty since we marked it unhealthy)
        let healthy_instances = state_store.get_healthy_instances().await.unwrap();
        assert_eq!(healthy_instances.len(), 0);
        
        // Test with non-existent session
        let non_existent: SessionId = "does-not-exist".into();
        let is_healthy = state_store.is_session_healthy(&non_existent, Duration::from_secs(60)).await.unwrap();
        assert_eq!(is_healthy, false);
    }
}