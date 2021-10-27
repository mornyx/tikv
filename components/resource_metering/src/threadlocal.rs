// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::model::SummaryRecord;
use crate::{utils, ResourceMeteringTag, SharedTagPtr};

use std::cell::Cell;
use std::sync::{Arc, Mutex};

use collections::HashMap;
use crossbeam::channel::Sender;
use lazy_static::lazy_static;

lazy_static! {
    /// `THREAD_LOCAL_CHANS` is used to transfer the necessary thread registration events.
    static ref THREAD_LOCAL_CHANS: Mutex<Vec<Sender<ThreadLocalMsg>>> = Mutex::new(Vec::new());
}

thread_local! {
    /// `LOCAL_DATA` is a thread-localized instance of [ThreadLocalData].
    ///
    /// When a new thread tries to read `LOCAL_DATA`, it will actively send a message
    /// to [THREAD_LOCAL_CHANS] during the initialization phase of thread local storage.
    /// The message([ThreadLocalRef]) contains the thread id and some references to
    /// the thread local fields.
    pub static LOCAL_DATA: ThreadLocalData = {
        let local_data = ThreadLocalData {
            is_set: Cell::new(false),
            shared_ptr: SharedTagPtr::default(),
            summary_cur_record: Arc::new(SummaryRecord::default()),
            summary_records: Arc::new(Mutex::new(HashMap::default())),
        };
        THREAD_LOCAL_CHANS.lock().unwrap().iter().for_each(|tx| {
            tx.send(ThreadLocalMsg::Created(ThreadLocalRef{
                id: utils::thread_id(),
                shared_ptr: local_data.shared_ptr.clone(),
                summary_cur_record: local_data.summary_cur_record.clone(),
                summary_records: local_data.summary_records.clone(),
            })).ok();
        });
        local_data
    };
}

/// `ThreadLocalData` is a thread-local structure that contains all necessary data of submodules.
///
/// In order to facilitate mutual reference, the thread-local data of all sub-modules
/// need to be stored centrally in `ThreadLocalData`.
struct ThreadLocalData {
    pub is_set: Cell<bool>,
    pub shared_ptr: SharedTagPtr,
    pub summary_cur_record: Arc<SummaryRecord>,
    pub summary_records: Arc<Mutex<HashMap<ResourceMeteringTag, SummaryRecord>>>,
}

impl Drop for ThreadLocalData {
    fn drop(&mut self) {
        THREAD_LOCAL_CHANS.lock().unwrap().iter().for_each(|tx| {
            tx.send(ThreadLocalMsg::Destroyed(utils::thread_id())).ok();
        });
    }
}

pub struct ThreadLocalRef {
    pub id: usize,
    pub shared_ptr: SharedTagPtr,
    pub summary_cur_record: Arc<SummaryRecord>,
    pub summary_records: Arc<Mutex<HashMap<ResourceMeteringTag, SummaryRecord>>>,
}

/// This enum is transmitted as a event in [THREAD_LOCAL_CHANS].
///
/// See [LOCAL_DATA] for more information.
pub enum ThreadLocalMsg {
    Created(ThreadLocalRef),
    Destroyed(usize),
}

/// Register a channel to notify thread creation & destruction events.
pub fn register_thread_local_chan_tx(tx: Sender<ThreadLocalMsg>) {
    THREAD_LOCAL_CHANS.lock().unwrap().push(tx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;

    #[test]
    fn test_thread_local_chan() {
        let (tx, rx) = unbounded();
        register_thread_local_chan_tx(tx);
        LOCAL_DATA.with(|_| {}); // Just to trigger registration.
        std::thread::spawn(move || {
            LOCAL_DATA.with(|_| {});
        })
        .join()
        .unwrap();
        let mut count = 0;
        while let Ok(msg) = rx.try_recv() {
            assert!(matches!(msg, ThreadLocalMsg::Created(_)));
            match msg {
                ThreadLocalMsg::Created(r) => {
                    assert_ne!(r.id, 0);
                }
                ThreadLocalMsg::Destroyed(id) => {
                    assert_ne!(id, 0);
                }
            }
            count += 1;
        }
        // This value may be greater than 2 if other test threads access `LOCAL_DATA` in parallel.
        assert!(count >= 2);
    }
}
