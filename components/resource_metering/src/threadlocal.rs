// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::model::SummaryRecord;
use crate::{utils, ResourceMeteringTag, SharedTagPtr};

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::vec::Drain;

use collections::HashMap;
use lazy_static::lazy_static;

lazy_static! {
    /// `THREAD_REGISTER_BUFFER` is used to store the new thread registrations.
    static ref THREAD_REGISTER_BUFFER: Mutex<Vec<ThreadLocalRef>> = Mutex::new(Vec::new());
}

pub fn take_thread_registrations<F, T>(mut consume: F) -> T
where
    F: FnMut(Drain<ThreadLocalRef>) -> T,
{
    consume(THREAD_REGISTER_BUFFER.lock().unwrap().drain(..))
}

thread_local! {
    /// `LOCAL_DATA` is a thread-localized instance of [ThreadLocalData].
    ///
    /// When a new thread tries to read `LOCAL_DATA`, it will actively store its [ThreadLocalRef]
    /// to [THREAD_REGISTER_BUFFER] during the initialization phase of thread local storage.
    /// The [ThreadLocalRef] contains the thread id and some references to
    /// the thread local fields.
    pub static LOCAL_DATA: ThreadLocalData = {
        let local_data = ThreadLocalData {
            is_set: Cell::new(false),
            shared_ptr: SharedTagPtr::default(),
            summary_cur_record: Arc::new(SummaryRecord::default()),
            summary_records: Arc::new(Mutex::new(HashMap::default())),
            is_thread_down: Arc::new(AtomicBool::new(false)),
        };
        THREAD_REGISTER_BUFFER.lock().unwrap().push(
            ThreadLocalRef {
                id: utils::thread_id(),
                shared_ptr: local_data.shared_ptr.clone(),
                summary_cur_record: local_data.summary_cur_record.clone(),
                summary_records: local_data.summary_records.clone(),
                is_down: local_data.is_thread_down.clone(),
            }
        );
        local_data
    };
}

/// `ThreadLocalData` is a thread-local structure that contains all necessary data of submodules.
///
/// In order to facilitate mutual reference, the thread-local data of all sub-modules
/// need to be stored centrally in `ThreadLocalData`.
#[derive(Debug)]
pub struct ThreadLocalData {
    pub is_set: Cell<bool>,
    pub shared_ptr: SharedTagPtr,
    pub summary_cur_record: Arc<SummaryRecord>,
    pub summary_records: Arc<Mutex<HashMap<ResourceMeteringTag, SummaryRecord>>>,
    pub is_thread_down: Arc<AtomicBool>,
}

impl Drop for ThreadLocalData {
    fn drop(&mut self) {
        self.is_thread_down.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct ThreadLocalRef {
    pub id: usize,
    pub shared_ptr: SharedTagPtr,
    pub summary_cur_record: Arc<SummaryRecord>,
    pub summary_records: Arc<Mutex<HashMap<ResourceMeteringTag, SummaryRecord>>>,
    pub is_down: Arc<AtomicBool>,
}

impl ThreadLocalRef {
    pub fn is_thread_down(&self) -> bool {
        self.is_down.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::sync::WaitGroup;

    #[test]
    fn test_thread_local_registration() {
        let _g = crate::tests::sequential_test();

        let (next_step, stop_t0) = (WaitGroup::new(), WaitGroup::new());
        let (n, s) = (next_step.clone(), stop_t0.clone());
        let t0 = std::thread::spawn(move || {
            LOCAL_DATA.with(|_| {});
            drop(n);
            s.wait();
        });
        next_step.wait();

        let (next_step, stop_t1) = (WaitGroup::new(), WaitGroup::new());
        let (n, s) = (next_step.clone(), stop_t1.clone());
        let t1 = std::thread::spawn(move || {
            LOCAL_DATA.with(|_| {});
            drop(n);
            s.wait();
        });
        next_step.wait();

        let registrations = take_thread_registrations(|t| t.collect::<Vec<_>>());
        assert_eq!(registrations.len(), 2);
        assert!(!registrations[0].is_thread_down());
        assert!(!registrations[1].is_thread_down());

        drop(stop_t0);
        t0.join().unwrap();
        assert!(registrations[0].is_thread_down());

        drop(stop_t1);
        t1.join().unwrap();
        assert!(registrations[1].is_thread_down());
    }
}
