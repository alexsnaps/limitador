use crate::storage::concurrent::limit::ConcurrentLimit;

#[derive(Eq, Hash, PartialEq)]
pub struct ConcurrentNamespace {
    name: String,
    // limits: HashSet<ConcurrentLimit>,
}

impl ConcurrentNamespace {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn add_limit(&self, _limit: ConcurrentLimit) {}
}

impl From<String> for ConcurrentNamespace {
    fn from(name: String) -> Self {
        Self {
            name,
            // limits: HashSet::new(),
        }
    }
}
