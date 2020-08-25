//! Limitador is a generic rate-limiter.
//!
//! # Basic operation
//!
//! Limitador can store the limits in memory or in Redis. Storing them in memory
//! is faster, but the limits cannot be shared between several instances of
//! Limitador. Storing the limits in Redis is slower, but they can be shared
//! between instances.
//!
//! By default, the rate limiter is configured to store the limits in memory:
//! ```
//! use limitador::RateLimiter;
//! let rate_limiter = RateLimiter::default();
//! ```
//!
//! To use Redis:
//! ```
//! use limitador::RateLimiter;
//! use limitador::storage::redis::RedisStorage;
//!
//! // Default redis URL (redis://localhost:6379).
//! let rate_limiter = RateLimiter::new_with_storage(Box::new(RedisStorage::default()));
//!
//! // Custom redis URL
//! let rate_limiter = RateLimiter::new_with_storage(
//!     Box::new(RedisStorage::new("redis://127.0.0.1:7777"))
//! );
//! ```
//!
//! # Limits
//!
//! The definition of a limit includes:
//! - A namespace that identifies the resource to limit. It could be an API, a
//! Kubernetes service, a proxy ID, etc.
//! - A value.
//! - The length of the period in seconds.
//! - Conditions that define when to apply the limit.
//! - A set of variables. For example, if we need to define the same limit for
//! each "user_id", instead of creating a limit for each hardcoded ID, we just
//! need to define "user_id" as a variable.
//!
//! If we used Limitador in a context where it receives an HTTP request we could
//! define a limit like this to allow 10 requests per minute and per user_id
//! when the HTTP method is "GET".
//!
//! ```
//! use limitador::limit::Limit;
//! let limit = Limit::new(
//!     "my_namespace",
//!      10,
//!      60,
//!      vec!["req.method == GET"],
//!      vec!["user_id"],
//! );
//! ```
//!
//! Notice that the keys and variables are generic, so they do not necessarily
//! have to refer to an HTTP request.
//!
//! # Manage limits
//!
//! ```
//! use limitador::RateLimiter;
//! use limitador::limit::Limit;
//! let limit = Limit::new(
//!     "my_namespace",
//!      10,
//!      60,
//!      vec!["req.method == GET"],
//!      vec!["user_id"],
//! );
//! let mut rate_limiter = RateLimiter::default();
//!
//! // Add a limit
//! rate_limiter.add_limit(&limit);
//!
//! // Delete the limit
//! rate_limiter.delete_limit(&limit);
//!
//! // Get all the limits in a namespace
//! rate_limiter.get_limits("my_namespace");
//!
//! // Delete all the limits in a namespace
//! rate_limiter.delete_limits("my_namespace");
//! ```
//!
//! # Apply limits
//!
//! ```
//! use limitador::RateLimiter;
//! use limitador::limit::Limit;
//! use std::collections::HashMap;
//!
//! let mut rate_limiter = RateLimiter::default();
//!
//! let limit = Limit::new(
//!     "my_namespace",
//!      2,
//!      60,
//!      vec!["req.method == GET"],
//!      vec!["user_id"],
//! );
//! rate_limiter.add_limit(&limit);
//!
//! // We've defined a limit of 2. So we can report 2 times before being
//! // rate-limited
//! let mut values_to_report: HashMap<String, String> = HashMap::new();
//! values_to_report.insert("req.method".to_string(), "GET".to_string());
//! values_to_report.insert("user_id".to_string(), "1".to_string());
//!
//! // Check if we can report
//! assert!(!rate_limiter.is_rate_limited("my_namespace", &values_to_report, 1).unwrap());
//!
//! // Report
//! rate_limiter.update_counters("my_namespace", &values_to_report, 1).unwrap();
//!
//! // Check and report again
//! assert!(!rate_limiter.is_rate_limited("my_namespace", &values_to_report, 1).unwrap());
//! rate_limiter.update_counters("my_namespace", &values_to_report, 1).unwrap();
//!
//! // We've already reported 2, so reporting another one should not be allowed
//! assert!(rate_limiter.is_rate_limited("my_namespace", &values_to_report, 1).unwrap());
//!
//! // You can also check and report if not limited in a single call. It's useful
//! // for example, when calling Limitador from a proxy. Instead of doing 2
//! // separate calls, we can issue just one:
//! rate_limiter.check_rate_limited_and_update("my_namespace", &values_to_report, 1).unwrap();
//! ```

use crate::counter::Counter;
use crate::errors::LimitadorError;
use crate::limit::Limit;
use crate::storage::in_memory::InMemoryStorage;
use crate::storage::Storage;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

pub mod counter;
pub mod errors;
pub mod limit;
pub mod storage;

pub struct RateLimiter {
    storage: Box<dyn Storage>,
}

impl RateLimiter {
    pub fn new() -> RateLimiter {
        RateLimiter {
            storage: Box::new(InMemoryStorage::default()),
        }
    }

    pub fn new_with_storage(storage: Box<dyn Storage>) -> RateLimiter {
        RateLimiter { storage }
    }

    pub fn add_limit(&self, limit: &Limit) -> Result<(), LimitadorError> {
        self.storage.add_limit(limit).map_err(|err| err.into())
    }

    pub fn delete_limit(&self, limit: &Limit) -> Result<(), LimitadorError> {
        self.storage.delete_limit(limit).map_err(|err| err.into())
    }

    pub fn get_limits(&self, namespace: &str) -> Result<HashSet<Limit>, LimitadorError> {
        self.storage.get_limits(namespace).map_err(|err| err.into())
    }

    pub fn delete_limits(&self, namespace: &str) -> Result<(), LimitadorError> {
        self.storage
            .delete_limits(namespace)
            .map_err(|err| err.into())
    }

    pub fn is_rate_limited(
        &self,
        namespace: &str,
        values: &HashMap<String, String>,
        delta: i64,
    ) -> Result<bool, LimitadorError> {
        let counters = self.counters_that_apply(namespace, values)?;

        for counter in counters {
            match self.storage.is_within_limits(&counter, delta) {
                Ok(within_limits) => {
                    if !within_limits {
                        return Ok(true);
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(false)
    }

    pub fn update_counters(
        &self,
        namespace: &str,
        values: &HashMap<String, String>,
        delta: i64,
    ) -> Result<(), LimitadorError> {
        let counters = self.counters_that_apply(namespace, values)?;

        counters
            .iter()
            .try_for_each(|counter| self.storage.update_counter(&counter, delta))
            .map_err(|err| err.into())
    }

    pub fn check_rate_limited_and_update(
        &self,
        namespace: &str,
        values: &HashMap<String, String>,
        delta: i64,
    ) -> Result<bool, LimitadorError> {
        let counters = self.counters_that_apply(namespace, values)?;

        let is_within_limits = self
            .storage
            .check_and_update(&HashSet::from_iter(counters.iter()), delta)?;

        Ok(!is_within_limits)
    }

    pub fn get_counters(&self, namespace: &str) -> Result<HashSet<Counter>, LimitadorError> {
        self.storage
            .get_counters(namespace)
            .map_err(|err| err.into())
    }

    fn counters_that_apply(
        &self,
        namespace: &str,
        values: &HashMap<String, String>,
    ) -> Result<Vec<Counter>, LimitadorError> {
        let limits = self.get_limits(namespace)?;

        let counters = limits
            .iter()
            .filter(|lim| lim.applies(values))
            .map(|lim| Counter::new(lim.clone(), values.clone()))
            .collect();

        Ok(counters)
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}
