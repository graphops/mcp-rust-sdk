//! Redis integration tests for state store implementations.
//! 
//! These tests require a running Redis instance and are only run when the
//! `test-redis` feature is enabled to avoid CI failures.

#![cfg(all(test, feature = "test-redis"))]

use std::{collections::HashSet, time::{Duration, SystemTime}};
use rmcp::{
    model::RequestId,
    state_store::*,
    transport::common::axum::SessionId,
};
use tokio::time::sleep;

/// Redis connection string for tests
const REDIS_URL: &str = "redis://localhost:6379";

/// Create a Redis state store for testing
async fn create_redis_store() -> RedisStateStore {
    let config = RedisConfig {
        urls: vec![REDIS_URL.to_string()],
        key_prefix: format!("rmcp:test:{}", uuid::Uuid::new_v4()),
        session_ttl: Duration::from_secs(300),
        cache_capacity: 100,
    };
    
    RedisStateStore::new(config).await
        .expect("Failed to create Redis state store - ensure Redis is running")
}

/// Test cleanup helper
async fn cleanup_redis_store(store: &RedisStateStore) {
    // Clean up test data by deleting all keys with our test prefix
    let mut conn = store.connection.clone();
    let pattern = format!("{}:*", store.config.key_prefix);
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(&pattern)
        .query_async(&mut conn)
        .await
        .unwrap_or_default();
    
    if !keys.is_empty() {
        let _: () = redis::cmd("DEL")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
    }
}

#[tokio::test]
async fn test_redis_state_store_session_lifecycle() {
    let store = create_redis_store().await;
    let session_id: SessionId = "redis-test-session-123".into();

    // Session should not exist initially
    assert!(!store.session_exists(&session_id).await.unwrap());

    // Create session
    store.create_session(&session_id).await.unwrap();
    assert!(store.session_exists(&session_id).await.unwrap());

    // Delete session
    store.delete_session(&session_id).await.unwrap();
    assert!(!store.session_exists(&session_id).await.unwrap());

    cleanup_redis_store(&store).await;
}

#[tokio::test]
async fn test_redis_sse_connection_management() {
    let store = create_redis_store().await;
    let session_id: SessionId = "redis-sse-test".into();
    
    let connection_data = SseConnectionData {
        created_at: SystemTime::now(),
        last_ping: SystemTime::now(),
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-test-server".to_string(),
    };

    // Register SSE connection
    store.register_sse_connection(&session_id, &connection_data).await.unwrap();

    // Retrieve connection
    let retrieved = store.get_sse_connection(&session_id).await.unwrap();
    assert!(retrieved.is_some());
    let retrieved_data = retrieved.unwrap();
    assert_eq!(retrieved_data.server_instance_id, "redis-test-server");
    assert_eq!(retrieved_data.ping_interval, Duration::from_secs(30));

    // Remove connection
    store.remove_sse_connection(&session_id).await.unwrap();
    let retrieved = store.get_sse_connection(&session_id).await.unwrap();
    assert!(retrieved.is_none());

    cleanup_redis_store(&store).await;
}

#[tokio::test]
async fn test_redis_session_discovery_and_routing() {
    let store = create_redis_store().await;
    
    // Register multiple sessions with different server instances
    let session1: SessionId = "redis-session-1".into();
    let session2: SessionId = "redis-session-2".into();
    let session3: SessionId = "redis-session-3".into();
    
    let connection_data1 = SseConnectionData {
        created_at: SystemTime::now(),
        last_ping: SystemTime::now(),
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-server-A".to_string(),
    };
    
    let connection_data2 = SseConnectionData {
        created_at: SystemTime::now(),
        last_ping: SystemTime::now(),
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-server-B".to_string(),
    };
    
    let connection_data3 = SseConnectionData {
        created_at: SystemTime::now(),
        last_ping: SystemTime::now(),
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-server-A".to_string(),
    };
    
    // Register sessions
    store.register_sse_connection(&session1, &connection_data1).await.unwrap();
    store.register_sse_connection(&session2, &connection_data2).await.unwrap();
    store.register_sse_connection(&session3, &connection_data3).await.unwrap();
    
    // Test find_session_instance
    let instance = store.find_session_instance(&session1).await.unwrap();
    assert_eq!(instance, Some("redis-server-A".to_string()));
    
    let instance = store.find_session_instance(&session2).await.unwrap();
    assert_eq!(instance, Some("redis-server-B".to_string()));
    
    // Test for non-existent session
    let non_existent: SessionId = "redis-non-existent".into();
    let instance = store.find_session_instance(&non_existent).await.unwrap();
    assert_eq!(instance, None);
    
    // Test list_active_sessions
    let all_sessions = store.list_active_sessions().await.unwrap();
    assert_eq!(all_sessions.len(), 3);
    
    // Verify all sessions are present
    let session_instances: std::collections::HashMap<_, _> = all_sessions.into_iter().collect();
    assert_eq!(session_instances.get(&session1), Some(&"redis-server-A".to_string()));
    assert_eq!(session_instances.get(&session2), Some(&"redis-server-B".to_string()));
    assert_eq!(session_instances.get(&session3), Some(&"redis-server-A".to_string()));
    
    // Test list_sessions_by_instance
    let server_a_sessions = store.list_sessions_by_instance("redis-server-A").await.unwrap();
    assert_eq!(server_a_sessions.len(), 2);
    assert!(server_a_sessions.contains(&session1));
    assert!(server_a_sessions.contains(&session3));
    
    let server_b_sessions = store.list_sessions_by_instance("redis-server-B").await.unwrap();
    assert_eq!(server_b_sessions.len(), 1);
    assert!(server_b_sessions.contains(&session2));
    
    // Test for non-existent server instance
    let empty_sessions = store.list_sessions_by_instance("redis-server-C").await.unwrap();
    assert_eq!(empty_sessions.len(), 0);
    
    cleanup_redis_store(&store).await;
}

#[tokio::test]
async fn test_redis_health_monitoring() {
    let store = create_redis_store().await;
    
    // Register a session
    let session_id: SessionId = "redis-health-test-session".into();
    let connection_data = SseConnectionData {
        created_at: SystemTime::now(),
        last_ping: SystemTime::now(),
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-health-server".to_string(),
    };
    
    store.register_sse_connection(&session_id, &connection_data).await.unwrap();
    
    // Test session is initially healthy
    let is_healthy = store.is_session_healthy(&session_id, Duration::from_secs(60)).await.unwrap();
    assert_eq!(is_healthy, true);
    
    // Update heartbeat
    store.update_session_heartbeat(&session_id).await.unwrap();
    
    // Still healthy after heartbeat update
    let is_healthy = store.is_session_healthy(&session_id, Duration::from_secs(60)).await.unwrap();
    assert_eq!(is_healthy, true);
    
    // Test with very short timeout (session should be considered unhealthy)
    let is_healthy = store.is_session_healthy(&session_id, Duration::from_millis(1)).await.unwrap();
    assert_eq!(is_healthy, false);
    
    // Test instance health tracking
    let instance_id = "redis-test-instance-1";
    
    // Mark instance as unhealthy
    store.mark_instance_unhealthy(instance_id).await.unwrap();
    
    // Check healthy instances (should be empty since we marked it unhealthy)
    let healthy_instances = store.get_healthy_instances().await.unwrap();
    // Note: Our test instance might not appear if no other healthy instances exist
    assert!(!healthy_instances.contains(&instance_id.to_string()));
    
    // Test with non-existent session
    let non_existent: SessionId = "redis-does-not-exist".into();
    let is_healthy = store.is_session_healthy(&non_existent, Duration::from_secs(60)).await.unwrap();
    assert_eq!(is_healthy, false);
    
    cleanup_redis_store(&store).await;
}

#[tokio::test]
async fn test_redis_session_cleanup() {
    let store = create_redis_store().await;
    
    // Register sessions with different timestamps
    let session1: SessionId = "redis-cleanup-session-1".into();
    let session2: SessionId = "redis-cleanup-session-2".into();
    
    let old_time = SystemTime::now() - Duration::from_secs(300); // 5 minutes ago
    let recent_time = SystemTime::now();
    
    let old_connection = SseConnectionData {
        created_at: old_time,
        last_ping: old_time,
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-cleanup-server".to_string(),
    };
    
    let recent_connection = SseConnectionData {
        created_at: recent_time,
        last_ping: recent_time,
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-cleanup-server".to_string(),
    };
    
    store.register_sse_connection(&session1, &old_connection).await.unwrap();
    store.register_sse_connection(&session2, &recent_connection).await.unwrap();
    
    // Cleanup sessions older than 1 minute
    let stale_sessions = store.cleanup_stale_sessions(Duration::from_secs(60)).await.unwrap();
    
    // Should have cleaned up the old session
    assert_eq!(stale_sessions.len(), 1);
    assert!(stale_sessions.contains(&session1));
    
    // Verify old session is gone
    let retrieved = store.get_sse_connection(&session1).await.unwrap();
    assert!(retrieved.is_none());
    
    // Verify recent session still exists
    let retrieved = store.get_sse_connection(&session2).await.unwrap();
    assert!(retrieved.is_some());
    
    cleanup_redis_store(&store).await;
}

#[tokio::test]
async fn test_redis_concurrent_access() {
    let store = create_redis_store().await;
    let session_id: SessionId = "redis-concurrent-test".into();
    
    // Test concurrent session creation/deletion
    let mut handles = vec![];
    
    for i in 0..10 {
        let store_clone = store.clone();
        let session_id_clone = session_id.clone();
        
        let handle = tokio::spawn(async move {
            let test_session: SessionId = format!("redis-concurrent-{}", i).into();
            
            // Create session
            store_clone.create_session(&test_session).await.unwrap();
            assert!(store_clone.session_exists(&test_session).await.unwrap());
            
            // Register SSE connection
            let connection_data = SseConnectionData {
                created_at: SystemTime::now(),
                last_ping: SystemTime::now(),
                ping_interval: Duration::from_secs(30),
                server_instance_id: format!("redis-server-{}", i),
            };
            store_clone.register_sse_connection(&test_session, &connection_data).await.unwrap();
            
            // Update heartbeat
            store_clone.update_session_heartbeat(&test_session).await.unwrap();
            
            // Clean up
            store_clone.remove_sse_connection(&test_session).await.unwrap();
            store_clone.delete_session(&test_session).await.unwrap();
        });
        
        handles.push(handle);
    }
    
    // Wait for all concurrent operations to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    cleanup_redis_store(&store).await;
}

#[tokio::test]
async fn test_redis_persistence_across_connections() {
    let session_id: SessionId = "redis-persistence-test".into();
    let connection_data = SseConnectionData {
        created_at: SystemTime::now(),
        last_ping: SystemTime::now(),
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-persistence-server".to_string(),
    };
    
    // Create first store instance and add data
    {
        let store1 = create_redis_store().await;
        store1.create_session(&session_id).await.unwrap();
        store1.register_sse_connection(&session_id, &connection_data).await.unwrap();
        
        // Verify data exists
        assert!(store1.session_exists(&session_id).await.unwrap());
        let retrieved = store1.get_sse_connection(&session_id).await.unwrap();
        assert!(retrieved.is_some());
    } // store1 goes out of scope, connection dropped
    
    // Create second store instance and verify data persists
    {
        let store2 = create_redis_store().await;
        
        // Data should still exist
        assert!(store2.session_exists(&session_id).await.unwrap());
        let retrieved = store2.get_sse_connection(&session_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().server_instance_id, "redis-persistence-server");
        
        cleanup_redis_store(&store2).await;
    }
}

#[tokio::test]
async fn test_redis_multi_instance_simulation() {
    // Simulate multiple server instances sharing Redis state
    let store1 = create_redis_store().await;
    let store2 = create_redis_store().await;
    
    // Both stores should use same Redis but different connections
    assert_eq!(store1.config.key_prefix, store2.config.key_prefix);
    
    let session_id: SessionId = "redis-multi-instance-test".into();
    
    // Instance 1 creates a session
    store1.create_session(&session_id).await.unwrap();
    
    // Instance 2 should see the session
    assert!(store2.session_exists(&session_id).await.unwrap());
    
    // Instance 1 registers SSE connection
    let connection_data = SseConnectionData {
        created_at: SystemTime::now(),
        last_ping: SystemTime::now(),
        ping_interval: Duration::from_secs(30),
        server_instance_id: "redis-instance-1".to_string(),
    };
    store1.register_sse_connection(&session_id, &connection_data).await.unwrap();
    
    // Instance 2 should see the connection
    let retrieved = store2.get_sse_connection(&session_id).await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().server_instance_id, "redis-instance-1");
    
    // Instance 2 can discover sessions by instance
    let sessions = store2.list_sessions_by_instance("redis-instance-1").await.unwrap();
    assert!(sessions.contains(&session_id));
    
    // Instance 1 updates heartbeat
    store1.update_session_heartbeat(&session_id).await.unwrap();
    
    // Instance 2 should see the updated heartbeat
    let is_healthy = store2.is_session_healthy(&session_id, Duration::from_secs(60)).await.unwrap();
    assert!(is_healthy);
    
    cleanup_redis_store(&store1).await;
}