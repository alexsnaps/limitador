use crate::Limit;

#[derive(Eq, Hash, PartialEq)]
pub enum ConcurrentLimit {
    SimpleLimit,
    QualifiedLimit,
    // SimpleLimit(ConcurrentCounter),
    // QualifiedLimit(HashMap<Vec<String>, ConcurrentCounter>),
}

impl From<Limit> for ConcurrentLimit {
    fn from(limit: Limit) -> Self {
        if limit.variables().is_empty() {
            ConcurrentLimit::SimpleLimit
        } else {
            ConcurrentLimit::QualifiedLimit
        }
    }
}
