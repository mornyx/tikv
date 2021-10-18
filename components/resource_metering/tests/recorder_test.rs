// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use collections::HashMap;
#[cfg(target_os = "linux")]
use resource_metering::utils;
use resource_metering::{
    init_recorder, record_read_keys, record_write_keys, register_collector, Collector, RawRecord,
    RawRecords, ResourceMeteringTag, TagInfos, GLOBAL_ENABLE, TEST_TAG_PREFIX,
};

use Operation::*;

enum Operation {
    SetContext(&'static str),
    ResetContext,
    #[cfg(target_os = "linux")]
    CpuHeavy(u32),
    #[cfg(target_os = "linux")]
    Sleep(u64),
    ReadKeys(u32),
    WriteKeys(u32),
}

struct Operations {
    ops: Vec<Operation>,
    current_ctx: Option<&'static str>,
    records: HashMap<Vec<u8>, RawRecord>,
}

impl Operations {
    fn begin() -> Self {
        Self {
            ops: Vec::default(),
            current_ctx: None,
            records: HashMap::default(),
        }
    }

    fn then(mut self, op: Operation) -> Self {
        match op {
            SetContext(tag) => {
                assert!(self.current_ctx.is_none(), "cannot set nested contexts");
                self.current_ctx = Some(tag);
                self.ops.push(op);
                self
            }
            ResetContext => {
                assert!(self.current_ctx.is_some(), "context is not set");
                self.current_ctx = None;
                self.ops.push(op);
                self
            }
            #[cfg(target_os = "linux")]
            CpuHeavy(ms) => {
                if let Some(tag) = self.current_ctx {
                    self.records
                        .entry(tag.as_bytes().to_vec())
                        .or_insert_with(RawRecord::default)
                        .cpu_time += ms;
                }
                self.ops.push(op);
                self
            }
            #[cfg(target_os = "linux")]
            Sleep(_) => {
                self.ops.push(op);
                self
            }
            ReadKeys(count) => {
                if let Some(tag) = self.current_ctx {
                    self.records
                        .entry(tag.as_bytes().to_vec())
                        .or_insert_with(RawRecord::default)
                        .read_keys += count;
                }
                self.ops.push(op);
                self
            }
            WriteKeys(count) => {
                if let Some(tag) = self.current_ctx {
                    self.records
                        .entry(tag.as_bytes().to_vec())
                        .or_insert_with(RawRecord::default)
                        .write_keys += count;
                }
                self.ops.push(op);
                self
            }
        }
    }

    fn spawn(self) -> (JoinHandle<()>, HashMap<Vec<u8>, RawRecord>) {
        assert!(
            self.current_ctx.is_none(),
            "should keep context clean finally"
        );

        let Operations { ops, records, .. } = self;

        let handle = std::thread::spawn(|| {
            let mut guard = None;

            for op in ops {
                match op {
                    SetContext(tag) => {
                        let tag = ResourceMeteringTag::from(Arc::new(TagInfos {
                            store_id: 0,
                            region_id: 0,
                            peer_id: 0,
                            extra_attachment: {
                                let mut t = Vec::from(TEST_TAG_PREFIX);
                                t.extend_from_slice(tag.as_bytes());
                                t
                            },
                        }));
                        guard = Some(tag.attach());
                    }
                    ResetContext => {
                        guard.take();
                    }
                    #[cfg(target_os = "linux")]
                    CpuHeavy(ms) => {
                        let begin_stat =
                            utils::stat_task(utils::process_id(), utils::thread_id()).unwrap();
                        let begin_ticks = begin_stat.utime.wrapping_add(begin_stat.stime);
                        loop {
                            Self::heavy_job();
                            let later_stat =
                                utils::stat_task(utils::process_id(), utils::thread_id()).unwrap();
                            let later_ticks = later_stat.utime.wrapping_add(later_stat.stime);
                            let delta_ms =
                                later_ticks.wrapping_sub(begin_ticks) * 1_000 / utils::clock_tick();
                            if delta_ms >= ms as i64 {
                                break;
                            }
                        }
                    }
                    #[cfg(target_os = "linux")]
                    Sleep(ms) => {
                        std::thread::sleep(Duration::from_millis(ms));
                    }
                    ReadKeys(count) => {
                        record_read_keys(count);
                    }
                    WriteKeys(count) => {
                        record_write_keys(count);
                    }
                }
            }
        });

        (handle, records)
    }

    #[cfg(target_os = "linux")]
    fn heavy_job() -> u64 {
        let m: u64 = rand::random();
        let n: u64 = rand::random();
        let m = m ^ n;
        let n = m.wrapping_mul(n);
        let m = m.wrapping_add(n);
        let n = m & n;
        let m = m | n;
        m.wrapping_sub(n)
    }
}

#[derive(Default, Clone)]
struct DummyCollector {
    records: Arc<Mutex<HashMap<Vec<u8>, RawRecord>>>,
}

impl Collector for DummyCollector {
    fn collect(&self, records: Arc<RawRecords>) {
        if let Ok(mut r) = self.records.lock() {
            for (tag, record) in records.records.iter() {
                let (_, k) = tag.infos.extra_attachment.split_at(TEST_TAG_PREFIX.len());
                r.entry(k.to_vec())
                    .or_insert_with(RawRecord::default)
                    .merge(record);
            }
        }
    }
}

impl DummyCollector {
    fn check(&self, mut expected: HashMap<Vec<u8>, RawRecord>) {
        const MAX_DRIFT: u32 = 50;

        // Wait a collect interval to avoid losing records.
        std::thread::sleep(Duration::from_millis(1200));

        let mut records = self.records.lock().unwrap();
        for k in expected.keys() {
            records.entry(k.clone()).or_insert_with(RawRecord::default);
        }
        for k in records.keys() {
            expected.entry(k.clone()).or_insert_with(RawRecord::default);
        }
        for (k, expected_value) in expected {
            let value = records.get(&k).unwrap();
            let l = value.cpu_time.saturating_sub(MAX_DRIFT);
            let r = value.cpu_time.saturating_add(MAX_DRIFT);
            if !(l <= expected_value.cpu_time && expected_value.cpu_time <= r) {
                panic!(
                    "tag {} cpu time expected {} but got {}",
                    String::from_utf8_lossy(&k),
                    expected_value.cpu_time,
                    value.cpu_time
                );
            }
            if value.read_keys != expected_value.read_keys {
                panic!(
                    "tag {} read keys expected {:?} but got {:?}",
                    String::from_utf8_lossy(&k),
                    expected_value,
                    value
                );
            }
            if value.write_keys != expected_value.write_keys {
                panic!(
                    "tag {} write keys expected {:?} but got {:?}",
                    String::from_utf8_lossy(&k),
                    expected_value,
                    value
                );
            }
        }
    }
}

#[test]
#[cfg(target_os = "linux")]
fn test_cpu_recorder() {
    let handle = init_recorder(true, 1000);
    handle.resume();
    fail::cfg("cpu-record-test-filter", "return").unwrap();

    // Heavy CPU only with 1 thread
    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(2000))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);
    }

    // Sleep only with 1 thread
    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(Sleep(2000))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);
    }

    // Hybrid workload with 1 thread
    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(600))
            .then(Sleep(400))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(CpuHeavy(500))
            .then(Sleep(500))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(Sleep(600))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);
    }

    // Heavy CPU with 3 threads
    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle0, expected0) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(1500))
            .then(ResetContext)
            .spawn();
        let (handle1, expected1) = Operations::begin()
            .then(SetContext("ctx-1"))
            .then(CpuHeavy(1500))
            .then(ResetContext)
            .spawn();
        let (handle2, expected2) = Operations::begin()
            .then(SetContext("ctx-2"))
            .then(CpuHeavy(1500))
            .then(ResetContext)
            .spawn();
        handle0.join().unwrap();
        handle1.join().unwrap();
        handle2.join().unwrap();

        collector.check(merge(vec![expected0, expected1, expected2]));
    }

    // Hybrid workload with 3 threads
    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle0, expected0) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(CpuHeavy(200))
            .then(Sleep(300))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(Sleep(200))
            .then(CpuHeavy(600))
            .then(ResetContext)
            .then(CpuHeavy(500))
            .spawn();
        let (handle1, expected1) = Operations::begin()
            .then(SetContext("ctx-1"))
            .then(CpuHeavy(500))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(Sleep(400))
            .then(ResetContext)
            .then(Sleep(300))
            .spawn();
        let (handle2, expected2) = Operations::begin()
            .then(SetContext("ctx-2"))
            .then(CpuHeavy(800))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(Sleep(200))
            .then(ResetContext)
            .then(CpuHeavy(200))
            .spawn();
        handle0.join().unwrap();
        handle1.join().unwrap();
        handle2.join().unwrap();

        collector.check(merge(vec![expected0, expected1, expected2]));
    }
}

#[cfg(target_os = "linux")]
fn merge(
    maps: impl IntoIterator<Item = HashMap<Vec<u8>, RawRecord>>,
) -> HashMap<Vec<u8>, RawRecord> {
    let mut map = HashMap::default();
    for m in maps {
        for (k, v) in m {
            map.entry(k).or_insert_with(RawRecord::default).merge(&v);
        }
    }
    map
}

#[test]
fn test_summary_recorder() {
    // Turn on the switch explicitly.
    GLOBAL_ENABLE.store(true, SeqCst);

    let handle = init_recorder(true, 1000);
    handle.resume();

    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(ReadKeys(101))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        dbg!(&expected);
        collector.check(expected);
    }

    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(ReadKeys(101))
            .then(WriteKeys(102))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);
    }

    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(ReadKeys(101))
            .then(WriteKeys(102))
            .then(ResetContext)
            .then(SetContext("ctx-1"))
            .then(ReadKeys(103))
            .then(WriteKeys(104))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(ReadKeys(105))
            .then(WriteKeys(106))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);
    }

    // Execute `record_xxx` out of context.
    {
        let collector = DummyCollector::default();
        let _handle = register_collector(Box::new(collector.clone()));

        let (handle, expected) = Operations::begin()
            .then(SetContext("ctx-0"))
            .then(ResetContext)
            .then(ReadKeys(101))
            .then(WriteKeys(102))
            .then(SetContext("ctx-1"))
            .then(ReadKeys(103))
            .then(WriteKeys(104))
            .then(ResetContext)
            .then(SetContext("ctx-2"))
            .then(ReadKeys(105))
            .then(WriteKeys(106))
            .then(ResetContext)
            .spawn();
        handle.join().unwrap();

        collector.check(expected);
    }
}
