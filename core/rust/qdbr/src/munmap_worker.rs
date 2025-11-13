/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

/// Ring buffer capacity - must be power of 2
const RING_BUFFER_CAPACITY: usize = 1024;

/// Memory layout of the ring buffer shared between Java (via Unsafe) and Rust
///
/// Layout in native memory:
/// Offset 0:   head (atomic u64) - producer index (updated by Java via CAS)
/// Offset 8:   tail (atomic u64) - consumer index (updated by Rust thread)
/// Offset 16:  capacity (u64) - ring buffer capacity
/// Offset 24:  mask (u64) - capacity - 1, for fast modulo
/// Offset 32:  [padding to 64-byte cache line boundary]
/// Offset 64:  slots[] - array of MunmapSlot entries
///
#[repr(C)]
struct RingBufferHeader {
    head: AtomicU64,
    tail: AtomicU64,
    capacity: u64,
    mask: u64,
    _padding: [u64; 4], // Pad to 64 bytes (cache line)
}

/// Each slot is 16 bytes: [addr: atomic u64][len: u64]
/// addr is atomic and doubles as the readiness flag (0 = not ready/consumed)
#[repr(C)]
struct MunmapSlot {
    addr: AtomicU64,  // 0 = slot not ready or consumed (sentinel)
    len: u64,
}

impl Default for MunmapSlot {
    fn default() -> Self {
        MunmapSlot {
            addr: AtomicU64::new(0),
            len: 0,
        }
    }
}

#[repr(C)]
pub struct RingBuffer {
    header: RingBufferHeader,
    slots: [MunmapSlot; RING_BUFFER_CAPACITY],
}

/// Global ring buffer, lazily initialized on first access
static RING_BUFFER: Lazy<RingBuffer> = Lazy::new(|| {
    RingBuffer {
        header: RingBufferHeader {
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            capacity: RING_BUFFER_CAPACITY as u64,
            mask: (RING_BUFFER_CAPACITY - 1) as u64,
            _padding: [0; 4],
        },
        slots: std::array::from_fn(|_| MunmapSlot::default()),
    }
});

/// Worker thread that polls the ring buffer and executes munmap syscalls
fn munmap_worker_thread() {
    let ring: &RingBuffer = &RING_BUFFER;

    let mut backoff_micros = 1u64;
    const MIN_BACKOFF: u64 = 1;
    const MAX_BACKOFF: u64 = 100;

    loop {
        let tail = ring.header.tail.load(Ordering::Acquire);
        let head = ring.header.head.load(Ordering::Acquire);

        if tail == head {
            thread::sleep(Duration::from_micros(backoff_micros));
            backoff_micros = (backoff_micros * 2).min(MAX_BACKOFF);
            continue;
        }

        let mask = ring.header.mask;
        let mut current_tail = tail;

        while current_tail != head {
            let slot_index = (current_tail & mask) as usize;
            let slot = &ring.slots[slot_index];

            // addr == 0 is our sentinel value meaning "slot not ready or already consumed"
            let addr = loop {
                // note: Java producer uses volatile store for address = it has a Release semantics
                let a = slot.addr.load(Ordering::Acquire);
                if a != 0 {
                    break a;
                }
                std::hint::spin_loop();
            };

            // Read len (safe now, producer finished writing)
            // Note: Java producer writes 'len' before address and address established happens-before edge
            let len = slot.len;

            // Mark slot as consumed by writing sentinel BEFORE we bump tail
            // Ordering::Relaxed is sufficient since producer will NOT access it before we set
            // bump tail with Release semantics
            slot.addr.store(0, Ordering::Relaxed);

            unsafe {
                if libc::munmap(addr as *mut libc::c_void, len as libc::size_t) != 0 {
                    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(-1);
                    eprintln!("[munmap_worker] munmap failed: addr={:#x}, len={}, errno={}",
                              addr, len, errno);
                }
            }
            current_tail = current_tail.wrapping_add(1);
        }

        ring.header.tail.store(current_tail, Ordering::Release);
        backoff_micros = MIN_BACKOFF;
    }
}

static WORKER_INIT: Lazy<Result<(), String>> = Lazy::new(|| {
    thread::Builder::new()
        .name("qdb-munmap-worker".to_string())
        .spawn(munmap_worker_thread)
        .map(|_| ())
        .map_err(|e| format!("Failed to start worker thread: {}", e))
});

/// Returns a pointer to the ring buffer, initializing everything if needed
///
/// Returns: pointer to ring buffer (cast to *mut u8), or null on failure
pub fn get_ring_buffer_ptr() -> *mut u8 {
    // Force initialization of worker thread (which also initializes ring buffer)
    match &*WORKER_INIT {
        Ok(()) => {
            let ring: &RingBuffer = &RING_BUFFER;
            println!("[munmap_worker] Ring buffer ptr: {:p}", ring);
            std::ptr::from_ref(ring) as *mut u8
        }
        Err(e) => {
            eprintln!("[munmap_worker] {}", e);
            std::ptr::null_mut()
        }
    }
}