//! Configuration system for state stores.

#[cfg(feature = "state-store-redis")]
use super::redis::RedisConfig;

/// State store configuration enum
#[derive(Clone, Debug)]
pub enum StateStoreConfig {
    /// In-memory state store (single instance only)
    Memory,
    /// Redis state store for horizontal scaling
    #[cfg(feature = "state-store-redis")]
    Redis(RedisConfig),
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self::Memory
    }
}

impl StateStoreConfig {
    /// Create a simple Redis configuration with default settings
    #[cfg(feature = "state-store-redis")]
    pub fn redis_default() -> Self {
        Self::Redis(RedisConfig::default())
    }

    /// Create a Redis configuration with custom URL
    #[cfg(feature = "state-store-redis")]
    pub fn redis_url(url: impl Into<String>) -> Self {
        Self::Redis(RedisConfig {
            urls: vec![url.into()],
            ..Default::default()
        })
    }

    /// Create a Redis configuration with multiple URLs for failover
    #[cfg(feature = "state-store-redis")]
    pub fn redis_multi(urls: Vec<String>) -> Self {
        Self::Redis(RedisConfig {
            urls,
            ..Default::default()
        })
    }

    /// Create a Redis configuration with custom TTL
    #[cfg(feature = "state-store-redis")]
    pub fn redis_with_ttl(url: impl Into<String>, ttl: std::time::Duration) -> Self {
        Self::Redis(RedisConfig {
            urls: vec![url.into()],
            session_ttl: ttl,
            ..Default::default()
        })
    }

    /// Check if this configuration requires network connectivity
    pub fn requires_network(&self) -> bool {
        match self {
            Self::Memory => false,
            #[cfg(feature = "state-store-redis")]
            Self::Redis(_) => true,
        }
    }

    /// Get a descriptive name for this configuration
    pub fn name(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            #[cfg(feature = "state-store-redis")]
            Self::Redis(_) => "redis",
        }
    }
}

/// Builder for state store configurations
#[derive(Debug, Default)]
pub struct StateStoreConfigBuilder {
    config: StateStoreConfig,
}

impl StateStoreConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Use in-memory storage (default)
    pub fn memory(mut self) -> Self {
        self.config = StateStoreConfig::Memory;
        self
    }

    /// Use Redis storage
    #[cfg(feature = "state-store-redis")]
    pub fn redis(mut self, config: RedisConfig) -> Self {
        self.config = StateStoreConfig::Redis(config);
        self
    }

    /// Build the configuration
    pub fn build(self) -> StateStoreConfig {
        self.config
    }
}

#[cfg(feature = "state-store-redis")]
impl From<RedisConfig> for StateStoreConfig {
    fn from(config: RedisConfig) -> Self {
        Self::Redis(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StateStoreConfig::default();
        matches!(config, StateStoreConfig::Memory);
        assert!(!config.requires_network());
        assert_eq!(config.name(), "memory");
    }

    #[test]
    fn test_builder() {
        let config = StateStoreConfigBuilder::new()
            .memory()
            .build();
        
        matches!(config, StateStoreConfig::Memory);
    }

    #[cfg(feature = "state-store-redis")]
    #[test]
    fn test_redis_config() {
        let config = StateStoreConfig::redis_default();
        matches!(config, StateStoreConfig::Redis(_));
        assert!(config.requires_network());
        assert_eq!(config.name(), "redis");

        let config = StateStoreConfig::redis_url("redis://localhost:6379");
        matches!(config, StateStoreConfig::Redis(_));

        let config = StateStoreConfig::redis_multi(vec![
            "redis://node1:6379".to_string(),
            "redis://node2:6379".to_string(),
        ]);
        matches!(config, StateStoreConfig::Redis(_));
    }
}