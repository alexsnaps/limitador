use std::sync::atomic::{AtomicU64, Ordering};

pub struct ConcurrentCounter {
    value: AtomicU64,
    expires_at: AtomicU64,
    duration: u64,
}

impl ConcurrentCounter {
    pub fn next_at(&self, at: u64, delta: u64) -> (u64, u64) {
        let mut expiry = self.expires_at.load(Ordering::Acquire);
        if at >= expiry {
            match self.expires_at.compare_exchange(
                expiry,
                at + self.duration,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(expiry) => {
                    self.value.store(delta, Ordering::Release);
                    return (delta, expiry);
                }
                Err(updated) => expiry = updated,
            }
        }
        (
            self.value.fetch_add(delta, Ordering::Relaxed) + delta,
            expiry,
        )
    }

    pub fn return_to(&self, at: u64, delta: u64) -> bool {
        let nuclear = Ordering::SeqCst;
        let mut current = self.value.load(nuclear);
        // are we still in time to return?
        while at == self.expires_at.load(nuclear) {
            // we are, can we even subtract that value without rolling over?
            if current > delta {
                // ok, let's try
                match self
                    .value
                    .compare_exchange(current, current - delta, nuclear, nuclear)
                {
                    Ok(_) => return true,
                    Err(updated) => current = updated,
                }
                // otherwise... spin. Tho this could spin until the counter expires!
                // might want to just tableflip after a few tries
            } else {
                // no? either we expired or just wrong delta
                return false;
            }
        }
        // we (finally?) expired
        false
    }
}
