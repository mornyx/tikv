// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// TODO(mornyx): crate doc.

#![feature(shrink_to)]
#![feature(hash_drain_filter)]

use crate::threadlocal::LOCAL_DATA;

use std::pin::Pin;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll};

mod client;
mod collector;
mod config;
mod model;
mod recorder;
mod reporter;
mod threadlocal;
pub mod utils;

pub use client::{Client, GrpcClient};
pub use collector::Collector;
pub use collector::{register_collector, CollectorHandle, CollectorId};
pub use config::{Config, ConfigManager, GLOBAL_ENABLE};
pub use model::*;
pub use recorder::{
    init_recorder, record_read_keys, record_write_keys, CpuRecorder, RecorderBuilder,
    RecorderHandle, SummaryRecorder,
};
pub use reporter::{Reporter, Task};

pub const TEST_TAG_PREFIX: &[u8] = b"__resource_metering::tests::";

/// This structure is used as a label to distinguish different request contexts.
///
/// In order to associate `ResourceMeteringTag` with a certain piece of code logic,
/// we added a function to [Future] to bind `ResourceMeteringTag` to the specified
/// future context. It is used in the main business logic of TiKV.
///
/// [Future]: futures::Future
#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct ResourceMeteringTag {
    pub infos: Arc<TagInfos>,
}

impl From<Arc<TagInfos>> for ResourceMeteringTag {
    fn from(infos: Arc<TagInfos>) -> Self {
        Self { infos }
    }
}

impl ResourceMeteringTag {
    /// Get data from [Context] and construct [ResourceMeteringTag].
    ///
    /// [Context]: kvproto::kvrpcpb::Context
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        Arc::new(TagInfos::from_rpc_context(context)).into()
    }

    /// This method is used to provide [ResourceMeteringTag] with the ability
    /// to attach to the thread local context.
    ///
    /// When you call this method, the `ResourceMeteringTag` itself will be
    /// attached to [LOCAL_DATA], and a [Guard] used to control the life of the
    /// tag is returned. When the `Guard` is discarded, the tag (and other
    /// fields if necessary) in `LOCAL_DATA` will be cleaned up.
    ///
    /// [LOCAL_DATA]: crate::threadlocal::LOCAL_DATA
    pub fn attach(&self) -> Guard {
        LOCAL_DATA.with(|tld| {
            if tld.is_set.get() {
                panic!("nested attachment is not allowed")
            }
            let prev = tld.shared_ptr.swap(self.clone());
            assert!(prev.is_none());
            tld.is_set.set(true);
            tld.summary_cur_record.reset();
            Guard { tag: self.clone() }
        })
    }
}

/// An RAII implementation of a [ResourceMeteringTag]. When this structure is
/// dropped (falls out of scope), the tag will be removed. You can also clean
/// up other data here if necessary.
///
/// See [ResourceMeteringTag::attach] for more information.
///
/// [ResourceMeteringTag]: crate::ResourceMeteringTag
/// [ResourceMeteringTag::attach]: crate::ResourceMeteringTag::attach
#[derive(Default)]
pub struct Guard {
    tag: ResourceMeteringTag,
}

// Unlike shared_ptr in LOCAL_DATA, summary_records will continue to grow as the
// request arrives. If the recorder thread is not working properly, these maps
// will never be cleaned up, so here we need to make some restrictions.
const MAX_SUMMARY_RECORDS_LEN: usize = 1000;

impl Drop for Guard {
    fn drop(&mut self) {
        LOCAL_DATA.with(|tld| {
            while tld.shared_ptr.take().is_none() {}
            tld.is_set.set(false);
            // Check GLOBAL_ENABLE to avoid unnecessary data accumulation when resource metering is not enabled.
            if !GLOBAL_ENABLE.load(Relaxed) {
                return;
            }
            if self.tag.infos.extra_attachment.is_empty() {
                return;
            }
            let cur = tld.summary_cur_record.take_and_reset();
            if cur.read_keys.load(Relaxed) == 0 && cur.write_keys.load(Relaxed) == 0 {
                return;
            }
            let mut records = tld.summary_records.lock().unwrap();
            match records.get(&self.tag) {
                Some(record) => {
                    record.merge(&cur);
                }
                None => {
                    // See MAX_SUMMARY_RECORDS_LEN.
                    if records.len() < MAX_SUMMARY_RECORDS_LEN {
                        records.insert(self.tag.clone(), cur);
                    }
                }
            }
        })
    }
}

/// This trait extends the standard [Future].
///
/// When the user imports [FutureExt], all futures in its module (such as async block)
/// will additionally support the [FutureExt::in_resource_metering_tag] method. This method
/// can bind a [ResourceMeteringTag] to the scope of this future (actually, it is stored in
/// the local storage of the thread where `Future` is located). During the polling period of
/// the future, we can continue to observe the system resources used by the thread in which
/// it is located, which is associated with `ResourceMeteringTag` and is also stored in thread
/// local storage. There is a background thread that continuously summarizes the storage of
/// each thread and reports it regularly.
///
/// [Future]: futures::Future
pub trait FutureExt: Sized {
    /// Call this method on the async block where you want to observe metrics to
    /// bind the [ResourceMeteringTag] extracted from the request context.
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

impl<T: std::future::Future> FutureExt for T {}

/// See [FutureExt].
pub trait StreamExt: Sized {
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

impl<T: futures::Stream> StreamExt for T {}

/// This structure is the return value of the [FutureExt::in_resource_metering_tag] method,
/// which wraps the original [Future] with a [ResourceMeteringTag].
///
/// see [FutureExt] for more information.
///
/// [Future]: futures::Future
#[pin_project::pin_project]
pub struct InTags<T> {
    #[pin]
    inner: T,
    tag: ResourceMeteringTag,
}

impl<T: std::future::Future> std::future::Future for InTags<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll(cx)
    }
}

impl<T: futures::Stream> futures::Stream for InTags<T> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll_next(cx)
    }
}

/// This structure is the actual internal data of [ResourceMeteringTag], and all
/// internal data comes from the requested [Context].
///
/// [Context]: kvproto::kvrpcpb::Context
#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct TagInfos {
    pub store_id: u64,
    pub region_id: u64,
    pub peer_id: u64,
    pub extra_attachment: Vec<u8>,
}

impl TagInfos {
    fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        let peer = context.get_peer();
        Self {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            extra_attachment: Vec::from(context.get_resource_group_tag()),
        }
    }
}

/// This is a version of [ResourceMeteringTag] that can be shared across threads.
///
/// The typical scenario is that we need to access all threads' tags in the
/// [Recorder] thread for collection purposes, so we need a non-copy way to pass tags.
#[derive(Debug, Default, Clone)]
pub struct SharedTagPtr {
    ptr: Arc<AtomicPtr<TagInfos>>,
}

impl SharedTagPtr {
    /// Gets the tag under the pointer and replace the original value with null.
    pub fn take(&self) -> Option<ResourceMeteringTag> {
        let prev = self.ptr.swap(std::ptr::null_mut(), SeqCst);
        (!prev.is_null()).then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev as _)) })
    }

    /// Gets the tag under the pointer and replace the original value with parameter v.
    pub fn swap(&self, v: ResourceMeteringTag) -> Option<ResourceMeteringTag> {
        let ptr = Arc::into_raw(v.infos);
        let prev = self.ptr.swap(ptr as _, SeqCst);
        (!prev.is_null()).then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev as _)) })
    }

    /// Gets a clone of the tag under the pointer and put it back.
    pub fn take_clone(&self) -> Option<ResourceMeteringTag> {
        self.take().map(|req_tag| {
            let tag = req_tag.clone();
            // Put it back as quickly as possible.
            assert!(self.swap(req_tag).is_none());
            tag
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use std::sync::{Mutex, MutexGuard};

    use crate::threadlocal::take_thread_registrations;
    use lazy_static::lazy_static;

    /// Tests that access [crate::threadlocal::THREAD_REGISTER_BUFFER] or [crate::config::GLOBAL_ENABLE]
    /// need to be run sequentially. A helper function to
    pub fn sequential_test() -> TestGuard {
        TestGuard {
            _guard: SEQ_LOCK.lock().unwrap(),
        }
    }

    lazy_static! {
        static ref SEQ_LOCK: Mutex<()> = Mutex::new(());
    }
    pub struct TestGuard {
        _guard: MutexGuard<'static, ()>,
    }
    impl Drop for TestGuard {
        fn drop(&mut self) {
            take_thread_registrations(|_| {});
        }
    }

    #[test]
    fn test_attach() {
        let _g = crate::tests::sequential_test();

        // Use a thread created by ourself. If we use unit test thread directly,
        // the test results may be affected by parallel testing.
        std::thread::spawn(|| {
            let tag = ResourceMeteringTag {
                infos: Arc::new(TagInfos {
                    store_id: 1,
                    region_id: 2,
                    peer_id: 3,
                    extra_attachment: b"12345".to_vec(),
                }),
            };
            {
                let guard = tag.attach();
                assert_eq!(guard.tag.infos, tag.infos);
                LOCAL_DATA.with(|tld| {
                    let local_tag = tld.shared_ptr.take();
                    assert!(local_tag.is_some());
                    let local_tag = local_tag.unwrap();
                    assert_eq!(local_tag.infos, tag.infos);
                    assert_eq!(local_tag.infos, guard.tag.infos);
                    assert!(tld.shared_ptr.swap(local_tag).is_none());
                });
                // drop here.
            }
            LOCAL_DATA.with(|tld| {
                let local_tag = tld.shared_ptr.take();
                assert!(local_tag.is_none());
            });
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_shared_tag_ptr_take_clone() {
        let info = Arc::new(TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"abc".to_vec(),
        });
        let ptr = SharedTagPtr {
            ptr: Arc::new(AtomicPtr::new(Arc::into_raw(info) as _)),
        };

        assert!(ptr.take_clone().is_some());
        assert!(ptr.take_clone().is_some());
        assert!(ptr.take_clone().is_some());

        assert!(ptr.take().is_some());
        assert!(ptr.take().is_none());

        assert!(ptr.take_clone().is_none());
        assert!(ptr.take_clone().is_none());
        assert!(ptr.take_clone().is_none());
    }
}
