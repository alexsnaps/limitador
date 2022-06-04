use limit::ConcurrentLimit::{QualifiedLimit, SimpleLimit};
use crate::storage::StorageErr;
use crate::{Authorization, Counter, Limit, Namespace, Storage};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use namespace::ConcurrentNamespace;

mod namespace;
mod limit;
mod counter;

pub struct ConcurrentStorage {
    data: RwLock<HashMap<String, ConcurrentNamespace>>,
}

impl Storage for ConcurrentStorage {
    fn get_namespaces(&self) -> Result<HashSet<Namespace>, StorageErr> {
        match self.data.read() {
            Ok(namespaces) => Ok(namespaces
                .iter()
                .map(|(_, ns)| Namespace::from(ns.name().to_owned()))
                .collect()),
            Err(err) => Err(err.to_string().into()),
        }
    }

    // todo should this return a Result<bool, StorageErr> indicating whether it was actually added?
    fn add_limit(&self, limit: &Limit) -> Result<(), StorageErr> {
        let ns = limit.namespace().clone().as_ref().to_string();

        // Could do a quick check under read lock here
        match self.data.write() {
            Ok(mut data) => {
                match data.entry(ns) {
                    Entry::Occupied(e) => {
                        e.get().add_limit(limit.clone().into());
                    }
                    Entry::Vacant(e) => {
                        let ns: ConcurrentNamespace = e.key().clone().into();
                        ns.add_limit(limit.clone().into());
                        e.insert(ns);
                    }
                }
                Ok(())
            }
            Err(e) => Err(e.to_string().into()),
        }
    }

    fn get_limits(&self, namespace: &Namespace) -> Result<HashSet<Limit>, StorageErr> {
        todo!()
    }

    fn delete_limit(&self, limit: &Limit) -> Result<(), StorageErr> {
        todo!()
    }

    fn delete_limits(&self, namespace: &Namespace) -> Result<(), StorageErr> {
        todo!()
    }

    fn is_within_limits(&self, counter: &Counter, delta: i64) -> Result<bool, StorageErr> {
        todo!()
    }

    fn update_counter(&self, counter: &Counter, delta: i64) -> Result<(), StorageErr> {
        todo!()
    }

    fn check_and_update(
        &self,
        counters: HashSet<Counter>,
        delta: i64,
    ) -> Result<Authorization, StorageErr> {
        todo!()
    }

    fn get_counters(&self, namespace: &Namespace) -> Result<HashSet<Counter>, StorageErr> {
        todo!()
    }

    fn clear(&self) -> Result<(), StorageErr> {
        todo!()
    }
}

impl ConcurrentStorage {
    fn add_namespace(&self, namespace: ConcurrentNamespace) -> Result<bool, StorageErr> {
        let fast_exists = match self.data.read() {
            Ok(data) => data.contains_key(namespace.name()),
            Err(e) => return Err(e.to_string().into()),
        };
        if fast_exists {
            Ok(false)
        } else {
            match self.data.write() {
                Ok(mut data) => {
                    if !data.contains_key(namespace.name()) {
                        data.insert(namespace.name().to_string(), namespace);
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
                Err(e) => Err(e.to_string().into()),
            }
        }
    }
}

impl Default for ConcurrentStorage {
    fn default() -> Self {
        Self {
            data: RwLock::new(HashMap::<String, ConcurrentNamespace>::new()),
        }
    }
}

impl From<String> for StorageErr {
    fn from(msg: String) -> Self {
        Self { msg }
    }
}

impl PartialEq for StorageErr {
    fn eq(&self, other: &Self) -> bool {
        self.msg.eq(&other.msg)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::ConcurrentStorage;
    use crate::{Namespace, Storage};

    #[test]
    fn maps_namespaces_correctly() {
        let storage = ConcurrentStorage::default();
        assert_eq!(storage.add_namespace("foo".to_string().into()), Ok(true));
        assert_eq!(storage.add_namespace("foo".to_string().into()), Ok(false));
        assert_eq!(storage.add_namespace("bar".to_string().into()), Ok(true));
        let result = storage.get_namespaces();
        assert!(result.is_ok());
        let namespaces = result.unwrap();
        assert_eq!(namespaces.len(), 2);
        assert!(namespaces.contains(&"foo".to_string().try_into().unwrap()));
        assert!(namespaces.contains(&"bar".to_string().try_into().unwrap()));
    }
}
