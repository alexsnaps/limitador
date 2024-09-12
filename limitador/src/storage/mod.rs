use crate::counter::Counter;
use crate::limit::{Condition, Limit, Namespace};
use crate::storage::atomic_expiring_value::AtomicExpiringValue;
use crate::storage::CounterValueSetKey::{Anonymous, Identity};
use crate::InMemoryStorage;
use async_trait::async_trait;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::SystemTime;
use thiserror::Error;

#[cfg(feature = "disk_storage")]
pub mod disk;
#[cfg(feature = "distributed_storage")]
pub mod distributed;
pub mod in_memory;

#[cfg(feature = "distributed_storage")]
pub use crate::storage::distributed::CrInMemoryStorage as DistributedInMemoryStorage;

#[cfg(feature = "redis_storage")]
pub mod redis;

mod atomic_expiring_value;
#[cfg(any(feature = "disk_storage", feature = "redis_storage"))]
mod keys;

pub enum Authorization {
    Ok,
    Limited(Option<String>), // First counter found over the limits
}

pub struct Storage {
    limits: RwLock<HashMap<Namespace, HashSet<Arc<Limit>>>>,
    counters: Box<dyn CounterStorage>,
}

pub struct AsyncStorage {
    limits: RwLock<HashMap<Namespace, HashSet<Arc<Limit>>>>,
    counters: Box<dyn AsyncCounterStorage>,
}

impl Storage {
    pub fn new(cache_size: u64) -> Self {
        Self {
            limits: RwLock::new(HashMap::new()),
            counters: Box::new(InMemoryStorage::new(cache_size)),
        }
    }

    pub fn with_counter_storage(counters: Box<dyn CounterStorage>) -> Self {
        Self {
            limits: RwLock::new(HashMap::new()),
            counters,
        }
    }

    pub fn get_namespaces(&self) -> HashSet<Namespace> {
        self.limits.read().unwrap().keys().cloned().collect()
    }

    pub fn add_limit(&self, limit: Limit) -> bool {
        let namespace = limit.namespace().clone();
        let mut limits = self.limits.write().unwrap();
        self.counters.add_counter(&limit).unwrap();
        limits.entry(namespace).or_default().insert(Arc::new(limit))
    }

    pub fn update_limit(&self, update: &Limit) -> bool {
        let mut namespaces = self.limits.write().unwrap();
        let limits = namespaces.get_mut(update.namespace());
        if let Some(limits) = limits {
            let req_update = if let Some(limit) = limits.get(update) {
                limit.max_value() != update.max_value() || limit.name() != update.name()
            } else {
                false
            };
            if req_update {
                limits.remove(update);
                limits.insert(Arc::new(update.clone()));
                return true;
            }
        }
        false
    }

    pub fn get_limits(&self, namespace: &Namespace) -> HashSet<Arc<Limit>> {
        match self.limits.read().unwrap().get(namespace) {
            // todo revise typing here?
            Some(limits) => limits.iter().map(Arc::clone).collect(),
            None => HashSet::new(),
        }
    }

    pub fn delete_limit(&self, limit: &Limit) -> Result<(), StorageErr> {
        let arc = match self.limits.read().unwrap().get(limit.namespace()) {
            None => Arc::new(limit.clone()),
            Some(limits) => limits
                .iter()
                .find(|l| ***l == *limit)
                .cloned()
                .unwrap_or_else(|| Arc::new(limit.clone())),
        };
        let mut limits = HashSet::new();
        limits.insert(arc);
        self.counters.delete_counters(&limits)?;

        let mut limits = self.limits.write().unwrap();

        if let Some(limits_for_ns) = limits.get_mut(limit.namespace()) {
            limits_for_ns.remove(limit);

            if limits_for_ns.is_empty() {
                limits.remove(limit.namespace());
            }
        }
        Ok(())
    }

    pub fn delete_limits(&self, namespace: &Namespace) -> Result<(), StorageErr> {
        if let Some(data) = self.limits.write().unwrap().remove(namespace) {
            self.counters.delete_counters(&data)?;
        }
        Ok(())
    }

    pub fn is_within_limits(&self, counter: &Counter, delta: u64) -> Result<bool, StorageErr> {
        self.counters.is_within_limits(counter, delta)
    }

    pub fn update_counter(&self, counter: &Counter, delta: u64) -> Result<(), StorageErr> {
        self.counters.update_counter(counter, delta)
    }

    pub fn check_and_update(
        &self,
        counters: &mut Vec<Counter>,
        delta: u64,
        load_counters: bool,
    ) -> Result<Authorization, StorageErr> {
        self.counters
            .check_and_update(counters, delta, load_counters)
    }

    pub fn get_counters(&self, namespace: &Namespace) -> Result<HashSet<Counter>, StorageErr> {
        match self.limits.read().unwrap().get(namespace) {
            Some(limits) => self.counters.get_counters(limits),
            None => Ok(HashSet::new()),
        }
    }

    pub fn clear(&self) -> Result<(), StorageErr> {
        self.limits.write().unwrap().clear();
        self.counters.clear()
    }
}

impl AsyncStorage {
    pub fn with_counter_storage(counters: Box<dyn AsyncCounterStorage>) -> Self {
        Self {
            limits: RwLock::new(HashMap::new()),
            counters,
        }
    }

    pub fn get_namespaces(&self) -> HashSet<Namespace> {
        self.limits.read().unwrap().keys().cloned().collect()
    }

    pub fn add_limit(&self, limit: Limit) -> bool {
        let namespace = limit.namespace().clone();

        let mut limits_for_namespace = self.limits.write().unwrap();

        match limits_for_namespace.get_mut(&namespace) {
            Some(limits) => limits.insert(Arc::new(limit)),
            None => {
                let mut limits = HashSet::new();
                limits.insert(Arc::new(limit));
                limits_for_namespace.insert(namespace, limits);
                true
            }
        }
    }

    pub fn update_limit(&self, update: &Limit) -> bool {
        let mut namespaces = self.limits.write().unwrap();
        let limits = namespaces.get_mut(update.namespace());
        if let Some(limits) = limits {
            let req_update = if let Some(limit) = limits.get(update) {
                limit.max_value() != update.max_value() || limit.name() != update.name()
            } else {
                false
            };
            if req_update {
                limits.remove(update);
                limits.insert(Arc::new(update.clone()));
                return true;
            }
        }
        false
    }

    pub fn get_limits(&self, namespace: &Namespace) -> HashSet<Arc<Limit>> {
        match self.limits.read().unwrap().get(namespace) {
            Some(limits) => limits.iter().map(Arc::clone).collect(),
            None => HashSet::new(),
        }
    }

    pub async fn delete_limit(&self, limit: &Limit) -> Result<(), StorageErr> {
        let arc = match self.limits.read().unwrap().get(limit.namespace()) {
            None => Arc::new(limit.clone()),
            Some(limits) => limits
                .iter()
                .find(|l| ***l == *limit)
                .cloned()
                .unwrap_or_else(|| Arc::new(limit.clone())),
        };
        let mut limits = HashSet::new();
        limits.insert(arc);
        self.counters.delete_counters(&limits).await?;

        let mut limits_for_namespace = self.limits.write().unwrap();

        if let Some(counters_by_limit) = limits_for_namespace.get_mut(limit.namespace()) {
            counters_by_limit.remove(limit);

            if counters_by_limit.is_empty() {
                limits_for_namespace.remove(limit.namespace());
            }
        }
        Ok(())
    }

    pub async fn delete_limits(&self, namespace: &Namespace) -> Result<(), StorageErr> {
        let option = { self.limits.write().unwrap().remove(namespace) };
        if let Some(data) = option {
            self.counters.delete_counters(&data).await?;
        }
        Ok(())
    }

    pub async fn is_within_limits(
        &self,
        counter: &Counter,
        delta: u64,
    ) -> Result<bool, StorageErr> {
        self.counters.is_within_limits(counter, delta).await
    }

    pub async fn update_counter(&self, counter: &Counter, delta: u64) -> Result<(), StorageErr> {
        self.counters.update_counter(counter, delta).await
    }

    pub async fn check_and_update<'a>(
        &self,
        counters: &mut Vec<Counter>,
        delta: u64,
        load_counters: bool,
    ) -> Result<Authorization, StorageErr> {
        self.counters
            .check_and_update(counters, delta, load_counters)
            .await
    }

    pub async fn get_counters(
        &self,
        namespace: &Namespace,
    ) -> Result<HashSet<Counter>, StorageErr> {
        let limits = self.get_limits(namespace);
        self.counters.get_counters(&limits).await
    }

    pub async fn clear(&self) -> Result<(), StorageErr> {
        self.limits.write().unwrap().clear();
        self.counters.clear().await
    }
}

pub trait CounterStorage: Sync + Send {
    fn is_within_limits(&self, counter: &Counter, delta: u64) -> Result<bool, StorageErr>;
    fn add_counter(&self, limit: &Limit) -> Result<(), StorageErr>;
    fn update_counter(&self, counter: &Counter, delta: u64) -> Result<(), StorageErr>;
    fn check_and_update(
        &self,
        counters: &mut Vec<Counter>,
        delta: u64,
        load_counters: bool,
    ) -> Result<Authorization, StorageErr>;
    fn get_counters(&self, limits: &HashSet<Arc<Limit>>) -> Result<HashSet<Counter>, StorageErr>; // todo revise typing here?
    fn delete_counters(&self, limits: &HashSet<Arc<Limit>>) -> Result<(), StorageErr>; // todo revise typing here?
    fn clear(&self) -> Result<(), StorageErr>;
}

#[async_trait]
pub trait AsyncCounterStorage: Sync + Send {
    async fn is_within_limits(&self, counter: &Counter, delta: u64) -> Result<bool, StorageErr>;
    async fn update_counter(&self, counter: &Counter, delta: u64) -> Result<(), StorageErr>;
    async fn check_and_update<'a>(
        &self,
        counters: &mut Vec<Counter>,
        delta: u64,
        load_counters: bool,
    ) -> Result<Authorization, StorageErr>;
    async fn get_counters(
        &self,
        limits: &HashSet<Arc<Limit>>,
    ) -> Result<HashSet<Counter>, StorageErr>;
    async fn delete_counters(&self, limits: &HashSet<Arc<Limit>>) -> Result<(), StorageErr>;
    async fn clear(&self) -> Result<(), StorageErr>;
}

#[derive(Default)]
pub(crate) struct CounterValueSet {
    values: Vec<CounterValue>,
}

impl CounterValueSet {
    pub fn new(windows: &[Duration]) -> Self {
        Self {
            values: windows
                .iter()
                .map(|d| CounterValue::from_secs(d.as_secs()))
                .collect(),
        }
    }

    pub fn add_window(&mut self, window: Duration) {
        for v in &self.values {
            match v.seconds.cmp(&window.as_secs()) {
                Ordering::Less => {}
                Ordering::Equal => return,
                Ordering::Greater => break,
            }
        }
        self.values.push(CounterValue {
            seconds: window.as_secs(),
            value: Default::default(),
        });
        self.values.sort_by(|a, b| a.seconds.cmp(&b.seconds));
    }

    pub fn value(&self, window: Duration) -> u64 {
        for v in &self.values {
            match v.seconds.cmp(&window.as_secs()) {
                Ordering::Less => {}
                Ordering::Equal => return v.value.value(),
                Ordering::Greater => break,
            }
        }
        0
    }

    pub fn expiring_value_of(&self, window: Duration) -> Option<&AtomicExpiringValue> {
        for v in &self.values {
            match v.seconds.cmp(&window.as_secs()) {
                Ordering::Less => {}
                Ordering::Equal => return Some(&v.value),
                Ordering::Greater => break,
            }
        }
        None
    }

    pub fn update(&self, window: Duration, delta: u64, when: SystemTime) -> Result<u64, ()> {
        for v in &self.values {
            match v.seconds.cmp(&window.as_secs()) {
                Ordering::Less => {}
                Ordering::Equal => return Ok(v.value.update(delta, window, when)),
                Ordering::Greater => break,
            }
        }
        Err(())
    }

    pub fn to_counters(&self, limit: &Arc<Limit>) -> Vec<Counter> {
        self.values
            .iter()
            .map(|v| {
                Counter::with_value(
                    limit.clone(),
                    limit.max_value() - v.value(),
                    v.ttl(),
                    HashMap::default(),
                )
            })
            .collect()
    }
}

pub(crate) struct CounterValue {
    seconds: u64,
    value: AtomicExpiringValue,
}

impl CounterValue {
    pub fn from_secs(window: u64) -> Self {
        Self {
            seconds: window,
            value: Default::default(),
        }
    }

    pub fn value(&self) -> u64 {
        self.value.value()
    }

    pub fn ttl(&self) -> Duration {
        self.value.ttl()
    }
}

#[derive(Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct QualifiedCounterValueSetKey {
    limit_key: CounterValueSetKey,
    qualifiers: BTreeMap<String, String>,
}

impl From<&Counter> for QualifiedCounterValueSetKey {
    fn from(counter: &Counter) -> Self {
        Self {
            limit_key: counter.limit().into(),
            qualifiers: counter.set_variables().clone(),
        }
    }
}

#[derive(Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) enum CounterValueSetKey {
    Identity(String),
    Anonymous(LimitKey),
}

impl CounterValueSetKey {
    pub fn applies_to(&self, limit: &Limit) -> bool {
        match limit.id() {
            None => {
                if let Anonymous(key) = self {
                    key.applies_to(limit)
                } else {
                    false
                }
            }
            Some(id) => {
                if let Identity(our_id) = self {
                    id == our_id
                } else {
                    false
                }
            }
        }
    }
}

impl From<&Limit> for CounterValueSetKey {
    fn from(limit: &Limit) -> Self {
        match limit.id() {
            Some(id) => Identity(id.to_string()),
            None => Anonymous(limit.into()),
        }
    }
}

#[derive(Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct LimitKey {
    namespace: Namespace,
    window: Duration,
    conditions: BTreeSet<Condition>,
    variables: BTreeSet<String>,
}

impl LimitKey {
    pub fn applies_to(&self, limit: &Limit) -> bool {
        limit.namespace() == &self.namespace
            && limit.window() == self.window
            && limit.conditions == self.conditions
            && limit.variables == self.variables
    }
}
impl From<&Limit> for LimitKey {
    fn from(limit: &Limit) -> Self {
        Self {
            namespace: limit.namespace().clone(),
            window: Duration::from_secs(limit.seconds()),
            conditions: limit.conditions.clone(),
            variables: limit.variables.clone(),
        }
    }
}

#[derive(Error, Debug)]
#[error("error while accessing the limits storage: {msg}")]
pub struct StorageErr {
    msg: String,
    transient: bool,
}

impl StorageErr {
    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn is_transient(&self) -> bool {
        self.transient
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::CounterValueSet;
    use std::time::Duration;

    #[test]
    fn test_counter_value_set_is_always_sorted() {
        let mut counters = CounterValueSet::default();
        counters.add_window(Duration::from_secs(20));
        assert_eq!(counters.values[0].seconds, 20);
        counters.add_window(Duration::from_secs(1));
        assert_eq!(counters.values[0].seconds, 1);
        assert_eq!(counters.values[1].seconds, 20);
        counters.add_window(Duration::from_secs(12));
        assert_eq!(counters.values[0].seconds, 1);
        assert_eq!(counters.values[1].seconds, 12);
        assert_eq!(counters.values[2].seconds, 20);
        counters.add_window(Duration::from_secs(30));
        counters.add_window(Duration::from_secs(12));
        assert_eq!(counters.values.len(), 4);
        assert_eq!(counters.values[0].seconds, 1);
        assert_eq!(counters.values[1].seconds, 12);
        assert_eq!(counters.values[2].seconds, 20);
        assert_eq!(counters.values[3].seconds, 30);
    }
}
