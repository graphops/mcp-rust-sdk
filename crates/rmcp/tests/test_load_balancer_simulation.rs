//! Load balancer simulation tests for horizontal scaling.
//! 
//! These tests simulate multiple server instances behind a load balancer
//! to validate true horizontal scaling without sticky sessions.

#![cfg(test)]

use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime}};
use rmcp::{
    state_store::*,
    transport::common::axum::SessionId,
};
use tokio::{sync::RwLock, time::sleep};

/// Simulate a server instance with state store
#[derive(Clone)]
struct ServerInstance {
    instance_id: String,
    state_store: MemoryStateStore,
    sessions: Arc<RwLock<HashMap<SessionId, String>>>, // session_id -> client_identifier
}

impl ServerInstance {
    fn new(instance_id: String, shared_state_store: MemoryStateStore) -> Self {
        Self {
            instance_id,
            state_store: shared_state_store,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Simulate client connecting to this server instance
    async fn accept_client(&self, client_id: String) -> Result<SessionId, Box<dyn std::error::Error + Send + Sync>> {
        let session_id: SessionId = format!("session-{}-{}", self.instance_id, fastrand::u64(..)).into();
        
        // Register session locally
        self.sessions.write().await.insert(session_id.clone(), client_id.clone());
        
        // Register in shared state store
        self.state_store.create_session(&session_id).await?;
        
        let connection_data = SseConnectionData {
            created_at: SystemTime::now(),
            last_ping: SystemTime::now(),
            ping_interval: Duration::from_secs(30),
            server_instance_id: self.instance_id.clone(),
        };
        
        self.state_store.register_sse_connection(&session_id, &connection_data).await?;
        
        Ok(session_id)
    }
    
    /// Simulate handling a request that might be for a session on another instance
    async fn handle_request(&self, session_id: &SessionId) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Check if session is local
        if let Some(client_id) = self.sessions.read().await.get(session_id) {
            return Ok(format!("Handled locally on {} for client {}", self.instance_id, client_id));
        }
        
        // Session not local - use state store to find which instance owns it
        if let Some(owner_instance) = self.state_store.find_session_instance(session_id).await? {
            if owner_instance == self.instance_id {
                // Race condition - session was supposed to be ours but we don't have it locally
                return Err("Session ownership mismatch".into());
            } else {
                // Forward to correct instance (simulated)
                return Ok(format!("Forwarded from {} to {} for session {}", 
                                self.instance_id, owner_instance, session_id));
            }
        }
        
        Err("Session not found".into())
    }
    
    /// Simulate server instance going down
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Mark instance as unhealthy
        self.state_store.mark_instance_unhealthy(&self.instance_id).await?;
        
        // Clean up local sessions from shared state
        let local_sessions: Vec<SessionId> = self.sessions.read().await.keys().cloned().collect();
        
        for session_id in local_sessions {
            self.state_store.remove_sse_connection(&session_id).await?;
            self.state_store.delete_session(&session_id).await?;
        }
        
        self.sessions.write().await.clear();
        Ok(())
    }
    
    /// Get session count
    async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

/// Simulate a load balancer that distributes requests
struct LoadBalancer {
    instances: Vec<ServerInstance>,
    current_instance: Arc<RwLock<usize>>,
    shared_state_store: MemoryStateStore,
    failed_instances: Arc<RwLock<std::collections::HashSet<usize>>>,
}

impl LoadBalancer {
    fn new(instance_count: usize) -> Self {
        let shared_state_store = MemoryStateStore::new();
        
        let instances = (0..instance_count)
            .map(|i| ServerInstance::new(format!("server-{}", i), shared_state_store.clone()))
            .collect();
        
        Self {
            instances,
            current_instance: Arc::new(RwLock::new(0)),
            shared_state_store,
            failed_instances: Arc::new(RwLock::new(std::collections::HashSet::new())),
        }
    }
    
    /// Round-robin load balancing for new connections (skips failed instances)
    async fn accept_new_client(&self, client_id: String) -> Result<(usize, SessionId), Box<dyn std::error::Error + Send + Sync>> {
        let failed_instances = self.failed_instances.read().await;
        let mut current = self.current_instance.write().await;
        
        // Find next healthy instance
        let mut attempts = 0;
        let mut instance_index = *current;
        
        while failed_instances.contains(&instance_index) && attempts < self.instances.len() {
            instance_index = (instance_index + 1) % self.instances.len();
            attempts += 1;
        }
        
        if attempts == self.instances.len() {
            return Err("No healthy instances available".into());
        }
        
        *current = (instance_index + 1) % self.instances.len();
        drop(current);
        drop(failed_instances);
        
        let session_id = self.instances[instance_index].accept_client(client_id).await?;
        Ok((instance_index, session_id))
    }
    
    /// Route request to any available instance (simulating no session affinity)
    async fn route_request(&self, session_id: &SessionId) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Try random instance (simulating load balancer not knowing session affinity)
        let random_instance = fastrand::usize(0..self.instances.len());
        self.instances[random_instance].handle_request(session_id).await
    }
    
    /// Get healthy instances
    async fn get_healthy_instances(&self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        self.shared_state_store.get_healthy_instances().await.map_err(|e| e.into())
    }
    
    /// Simulate instance failure
    async fn fail_instance(&self, instance_index: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if instance_index < self.instances.len() {
            // Mark instance as failed
            self.failed_instances.write().await.insert(instance_index);
            
            // Shutdown the instance
            self.instances[instance_index].shutdown().await?;
        }
        Ok(())
    }
    
    /// Get total session count across all instances
    async fn total_session_count(&self) -> usize {
        let mut total = 0;
        for instance in &self.instances {
            total += instance.session_count().await;
        }
        total
    }
    
    /// Clean up stale sessions
    async fn cleanup_stale_sessions(&self) -> Result<Vec<SessionId>, Box<dyn std::error::Error + Send + Sync>> {
        self.shared_state_store.cleanup_stale_sessions(Duration::from_secs(60)).await.map_err(|e| e.into())
    }
}

#[tokio::test]
async fn test_load_balancer_basic_distribution() {
    let lb = LoadBalancer::new(3);
    
    // Connect multiple clients
    let mut client_sessions = Vec::new();
    
    for i in 0..9 {
        let client_id = format!("client-{}", i);
        let (instance_index, session_id) = lb.accept_new_client(client_id).await.unwrap();
        
        // Verify session is registered in shared state
        assert!(lb.shared_state_store.session_exists(&session_id).await.unwrap());
        
        client_sessions.push((instance_index, session_id));
    }
    
    // Verify round-robin distribution (3 clients per 3 instances)
    let mut instance_counts = HashMap::new();
    for (instance_index, _) in &client_sessions {
        *instance_counts.entry(*instance_index).or_insert(0) += 1;
    }
    
    assert_eq!(instance_counts.len(), 3);
    for count in instance_counts.values() {
        assert_eq!(*count, 3); // Even distribution
    }
    
    assert_eq!(lb.total_session_count().await, 9);
}

#[tokio::test]
async fn test_cross_instance_request_routing() {
    let lb = LoadBalancer::new(2);
    
    // Client connects to instance 0
    let (instance_0, session_id) = lb.accept_new_client("test-client".to_string()).await.unwrap();
    assert_eq!(instance_0, 0);
    
    // Simulate load balancer routing request to different instance
    // This should demonstrate session discovery working
    for _ in 0..10 {
        let result = lb.route_request(&session_id).await.unwrap();
        // Should either handle locally or forward correctly
        assert!(result.contains("Handled locally") || result.contains("Forwarded"));
    }
}

#[tokio::test]
async fn test_instance_failure_and_recovery() {
    let lb = LoadBalancer::new(3);
    
    // Connect clients to all instances
    let mut sessions = Vec::new();
    for i in 0..6 {
        let (_, session_id) = lb.accept_new_client(format!("client-{}", i)).await.unwrap();
        sessions.push(session_id);
    }
    
    assert_eq!(lb.total_session_count().await, 6);
    
    // Simulate instance 0 failure
    lb.fail_instance(0).await.unwrap();
    
    // Verify instance is marked unhealthy
    let healthy_instances = lb.get_healthy_instances().await.unwrap();
    assert!(!healthy_instances.contains(&"server-0".to_string()));
    
    // Some sessions should be cleaned up (those from instance 0)
    assert!(lb.total_session_count().await < 6);
    
    // Remaining instances should still be functional
    let new_session = lb.accept_new_client("new-client".to_string()).await.unwrap();
    assert!(new_session.0 != 0); // Should not route to failed instance
}

#[tokio::test]
async fn test_session_discovery_across_instances() {
    let lb = LoadBalancer::new(3);
    
    // Create sessions on different instances
    let (instance_0, session_a) = lb.accept_new_client("client-a".to_string()).await.unwrap();
    let (instance_1, session_b) = lb.accept_new_client("client-b".to_string()).await.unwrap();
    let (instance_2, session_c) = lb.accept_new_client("client-c".to_string()).await.unwrap();
    
    // Verify each instance can discover sessions on other instances
    let instance_0_sessions = lb.shared_state_store
        .list_sessions_by_instance(&format!("server-{}", instance_0))
        .await.unwrap();
    assert!(instance_0_sessions.contains(&session_a));
    
    let instance_1_sessions = lb.shared_state_store
        .list_sessions_by_instance(&format!("server-{}", instance_1))
        .await.unwrap();
    assert!(instance_1_sessions.contains(&session_b));
    
    let instance_2_sessions = lb.shared_state_store
        .list_sessions_by_instance(&format!("server-{}", instance_2))
        .await.unwrap();
    assert!(instance_2_sessions.contains(&session_c));
    
    // Verify all sessions are discoverable
    let all_sessions = lb.shared_state_store.list_active_sessions().await.unwrap();
    assert_eq!(all_sessions.len(), 3);
    
    let session_map: HashMap<SessionId, String> = all_sessions.into_iter().collect();
    assert!(session_map.contains_key(&session_a));
    assert!(session_map.contains_key(&session_b));
    assert!(session_map.contains_key(&session_c));
}

#[tokio::test]
async fn test_concurrent_multi_instance_operations() {
    let lb = Arc::new(LoadBalancer::new(4));
    
    // Simulate high concurrency across multiple instances
    let mut handles = Vec::new();
    
    for i in 0..20 {
        let lb_clone = lb.clone();
        let handle = tokio::spawn(async move {
            let client_id = format!("concurrent-client-{}", i);
            
            // Connect client
            let (_, session_id) = lb_clone.accept_new_client(client_id).await.unwrap();
            
            // Make multiple requests
            for _ in 0..5 {
                let _result = lb_clone.route_request(&session_id).await.unwrap();
                
                // Update heartbeat occasionally
                if fastrand::bool() {
                    lb_clone.shared_state_store.update_session_heartbeat(&session_id).await.unwrap();
                }
                
                sleep(Duration::from_millis(1)).await;
            }
            
            session_id
        });
        
        handles.push(handle);
    }
    
    // Wait for all concurrent operations
    let mut session_ids = Vec::new();
    for handle in handles {
        let session_id = handle.await.unwrap();
        session_ids.push(session_id);
    }
    
    // Verify all sessions are tracked
    assert_eq!(session_ids.len(), 20);
    assert_eq!(lb.total_session_count().await, 20);
    
    // Verify all sessions are in shared state
    let all_sessions = lb.shared_state_store.list_active_sessions().await.unwrap();
    assert_eq!(all_sessions.len(), 20);
}

#[tokio::test]
async fn test_session_cleanup_across_instances() {
    let lb = LoadBalancer::new(2);
    
    // Create sessions with different timestamps
    let (_, session_1) = lb.accept_new_client("client-1".to_string()).await.unwrap();
    
    // Wait a bit then create another session
    sleep(Duration::from_millis(100)).await;
    let (_, session_2) = lb.accept_new_client("client-2".to_string()).await.unwrap();
    
    // Manually manipulate one session to appear stale (for testing)
    // In real scenario, this would happen due to connection timeout
    
    // Run cleanup
    let _stale_sessions = lb.cleanup_stale_sessions().await.unwrap();
    
    // Both sessions should still exist since our cleanup threshold is 60 seconds
    assert!(lb.shared_state_store.session_exists(&session_1).await.unwrap());
    assert!(lb.shared_state_store.session_exists(&session_2).await.unwrap());
}

#[tokio::test]
async fn test_zero_downtime_scaling() {
    // Start with 2 instances
    let lb = LoadBalancer::new(2);
    
    // Connect initial clients
    let mut sessions = Vec::new();
    for i in 0..4 {
        let (_, session_id) = lb.accept_new_client(format!("client-{}", i)).await.unwrap();
        sessions.push(session_id);
    }
    
    assert_eq!(lb.total_session_count().await, 4);
    
    // Simulate adding a new instance (in practice, this would be a new server joining)
    let new_instance = ServerInstance::new("server-new".to_string(), lb.shared_state_store.clone());
    
    // New instance can see existing sessions
    let all_sessions = new_instance.state_store.list_active_sessions().await.unwrap();
    assert_eq!(all_sessions.len(), 4);
    
    // New instance can handle new clients
    let _new_session = new_instance.accept_client("new-client".to_string()).await.unwrap();
    
    // Verify total sessions increased
    let all_sessions = lb.shared_state_store.list_active_sessions().await.unwrap();
    assert_eq!(all_sessions.len(), 5);
    
    // Existing sessions should still be accessible
    for session_id in &sessions {
        assert!(lb.shared_state_store.session_exists(session_id).await.unwrap());
    }
}