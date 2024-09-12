use crate::counter::Counter;
use crate::limit::Limit;
use crate::storage::atomic_expiring_value::AtomicExpiringValue;
use crate::storage::{
    Authorization, CounterStorage, CounterValueSet, CounterValueSetKey,
    QualifiedCounterValueSetKey, StorageErr,
};
use moka::sync::Cache;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

pub struct InMemoryStorage {
    counters: RwLock<BTreeMap<CounterValueSetKey, CounterValueSet>>,
    qualified_counters: Cache<QualifiedCounterValueSetKey, Arc<CounterValueSet>>,
    limit_windows: RwLock<HashMap<CounterValueSetKey, Vec<Duration>>>,
}

impl CounterStorage for InMemoryStorage {
    #[tracing::instrument(skip_all)]
    fn is_within_limits(&self, counter: &Counter, delta: u64) -> Result<bool, StorageErr> {
        let value = if counter.is_qualified() {
            self.qualified_counters
                .get(&counter.into())
                .map(|c| c.value(counter.window()))
                .unwrap_or_default()
        } else {
            let limits = self.counters.read().unwrap();
            limits
                .get(&counter.limit().into())
                .map(|c| c.value(counter.limit().window()))
                .unwrap_or_default()
        };

        Ok(counter.max_value() >= value + delta)
    }

    #[tracing::instrument(skip_all)]
    fn add_counter(&self, limit: &Limit) -> Result<(), StorageErr> {
        if limit.variables().is_empty() {
            let mut limits = self.counters.write().unwrap();
            limits
                .entry(limit.into())
                .or_default()
                .deref_mut()
                .add_window(limit.window());
        } else {
            let mut index = self.limit_windows.write().unwrap();
            let windows = index.entry(limit.into()).or_default();
            windows.push(limit.window());
            windows.dedup();
            windows.sort();
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn update_counter(&self, counter: &Counter, delta: u64) -> Result<(), StorageErr> {
        let now = SystemTime::now();
        if counter.is_qualified() {
            let value = self.qualified_counters.get_with(counter.into(), || {
                let cvs = CounterValueSet::new(
                    self.limit_windows
                        .read()
                        .unwrap()
                        .get(&counter.limit().into())
                        .unwrap(),
                );
                Arc::new(cvs)
            });
            value
                .update(counter.window(), delta, now)
                .expect("There must be a slot for this time window");
        } else {
            let mut counters = self.counters.write().unwrap();
            match counters.entry(counter.limit().into()) {
                Entry::Vacant(_) => {
                    panic!("there must be an entry for this Limit!")
                }
                Entry::Occupied(o) => {
                    o.get()
                        .update(counter.window(), delta, now)
                        .expect("There must be a slot for this time window");
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn check_and_update(
        &self,
        counters: &mut Vec<Counter>,
        delta: u64,
        load_counters: bool,
    ) -> Result<Authorization, StorageErr> {
        let limits = self.counters.read().unwrap();
        let mut first_limited = None;
        let mut counter_values_to_update: Vec<(&AtomicExpiringValue, Duration)> = Vec::new();
        let mut qualified_counter_values_to_updated: Vec<(Arc<CounterValueSet>, Duration)> =
            Vec::new();
        let now = SystemTime::now();

        let mut process_counter =
            |counter: &mut Counter, value: u64, delta: u64| -> Option<Authorization> {
                if load_counters {
                    let remaining = counter.max_value().checked_sub(value + delta);
                    counter.set_remaining(remaining.unwrap_or_default());
                    if first_limited.is_none() && remaining.is_none() {
                        first_limited = Some(Authorization::Limited(
                            counter.limit().name().map(|n| n.to_owned()),
                        ));
                    }
                }
                if !Self::counter_is_within_limits(counter, Some(&value), delta) {
                    return Some(Authorization::Limited(
                        counter.limit().name().map(|n| n.to_owned()),
                    ));
                }
                None
            };

        // Process simple counters
        for counter in counters.iter_mut().filter(|c| !c.is_qualified()) {
            let values = limits.get(&counter.limit().into()).unwrap();

            if let Some(limited) = process_counter(counter, values.value(counter.window()), delta) {
                if !load_counters {
                    return Ok(limited);
                }
            }
            counter_values_to_update.push((
                values
                    .expiring_value_of(counter.window())
                    .expect("There must be an Expiring Value present!"),
                counter.window(),
            ));
        }

        // Process qualified counters
        for counter in counters.iter_mut().filter(|c| c.is_qualified()) {
            let value = self.qualified_counters.get_with((&*counter).into(), || {
                let cvs = CounterValueSet::new(
                    self.limit_windows
                        .read()
                        .unwrap()
                        .get(&counter.limit().into())
                        .unwrap(),
                );
                Arc::new(cvs)
            });

            if let Some(limited) = process_counter(counter, value.value(counter.window()), delta) {
                if !load_counters {
                    return Ok(limited);
                }
            }

            qualified_counter_values_to_updated.push((value, counter.window()));
        }

        if let Some(limited) = first_limited {
            return Ok(limited);
        }

        // Update counters
        counter_values_to_update.iter().for_each(|(v, ttl)| {
            v.update(delta, *ttl, now);
        });
        qualified_counter_values_to_updated
            .iter()
            .for_each(|(v, ttl)| {
                v.update(*ttl, delta, now)
                    .expect("There must be a slot for this time window");
            });

        Ok(Authorization::Ok)
    }

    #[tracing::instrument(skip_all)]
    fn get_counters(&self, limits: &HashSet<Arc<Limit>>) -> Result<HashSet<Counter>, StorageErr> {
        let mut res = HashSet::new();

        for limit in limits.iter().filter(|l| l.variables.is_empty()) {
            if let Some(values) = self.counters.read().unwrap().get(&limit.as_ref().into()) {
                for counter in values.to_counters(limit) {
                    if counter.expires_in().unwrap() > Duration::ZERO {
                        res.insert(counter);
                    }
                }
            }
        }

        for (key, values) in self.qualified_counters.iter() {
            for limit in limits
                .iter()
                .filter(|limit| key.limit_key.applies_to(limit))
            {
                let mut counter_with_val = Counter::new(
                    Arc::clone(limit),
                    key.qualifiers.clone().into_iter().collect(),
                );
                counter_with_val.set_remaining(
                    counter_with_val.max_value() - values.value(counter_with_val.window()),
                );
                counter_with_val.set_expires_in(
                    values
                        .expiring_value_of(counter_with_val.window())
                        .expect("There must be value for this window")
                        .ttl(),
                );
                if counter_with_val.expires_in().unwrap() > Duration::ZERO {
                    res.insert(counter_with_val);
                }
            }
        }

        Ok(res)
    }

    #[tracing::instrument(skip_all)]
    fn delete_counters(&self, limits: &HashSet<Arc<Limit>>) -> Result<(), StorageErr> {
        for limit in limits {
            self.delete_counters_of_limit(limit);
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn clear(&self) -> Result<(), StorageErr> {
        self.counters.write().unwrap().clear();
        self.qualified_counters.invalidate_all();
        Ok(())
    }
}

impl InMemoryStorage {
    pub fn new(cache_size: u64) -> Self {
        Self {
            counters: RwLock::new(BTreeMap::new()),
            qualified_counters: Cache::new(cache_size),
            limit_windows: Default::default(),
        }
    }

    fn delete_counters_of_limit(&self, limit: &Limit) {
        if limit.variables.is_empty() {
            self.counters.write().unwrap().remove(&limit.into());
        } else {
            let key: CounterValueSetKey = limit.into();
            let _ = self
                .qualified_counters
                .invalidate_entries_if(move |k, _value| k.limit_key.eq(&key));
        }
    }

    fn counter_is_within_limits(counter: &Counter, current_val: Option<&u64>, delta: u64) -> bool {
        match current_val {
            Some(current_val) => current_val + delta <= counter.max_value(),
            None => counter.max_value() >= delta,
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {}
