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

package io.questdb.std;

public final class NativeAsyncMunmapProducer {

    private static final int RING_BUFFER_CAPACITY_OFFSET = 16;
    private static final int RING_BUFFER_HEAD_OFFSET = 0;
    private static final int RING_BUFFER_MASK_OFFSET = 24;
    private static final int RING_BUFFER_SLOTS_OFFSET = 64;
    private static final int RING_BUFFER_SLOT_SIZE = 16; // addr (8) + len (8)
    private static final int RING_BUFFER_TAIL_OFFSET = 8;

    private static long MUNMAP_RING_BUFFER_CAPACITY = 0;
    private static long MUNMAP_RING_BUFFER_MASK = 0;
    // Native munmap ring buffer (allocated in Rust, accessed via Unsafe)
    // Ring buffer memory layout:
    //   Offset 0:  head (atomic u64)
    //   Offset 8:  tail (atomic u64)
    //   Offset 16: capacity (u64)
    //   Offset 24: mask (u64)
    //   Offset 64: slots[] - array of {addr: u64, len: u64}
    //   Each slot occupies 16 bytes (8 bytes addr + 8 bytes len)
    private static long MUNMAP_RING_BUFFER_PTR = 0;

    static native long initMunmapWorker();

    /**
     * Enqueues a munmap request to the native ring buffer using Unsafe.
     * Returns true if successfully enqueued, false if queue is full.
     */
    static boolean munmapEnqueueNative(long address, long len) {
        assert Os.isPosix();

        final sun.misc.Unsafe U = Unsafe.getUnsafe();
        final long ringPtr = MUNMAP_RING_BUFFER_PTR;
        final long capacity = MUNMAP_RING_BUFFER_CAPACITY;
        final long mask = MUNMAP_RING_BUFFER_MASK;

        while (true) {
            // todo: cache tail
            long head = U.getLongVolatile(null, ringPtr + RING_BUFFER_HEAD_OFFSET);
            long tail = U.getLongVolatile(null, ringPtr + RING_BUFFER_TAIL_OFFSET);

            if (head - tail >= capacity) {
                return false; // full
            }

            if (U.compareAndSwapLong(null, ringPtr + RING_BUFFER_HEAD_OFFSET, head, head + 1)) {
                // importing: after winning the CAS we must NOT throw until we write the address
                // otherwise the consumer would spin forever, waiting for the address to be written

                long slotIndex = head & mask;
                long slotPtr = ringPtr + RING_BUFFER_SLOTS_OFFSET + (slotIndex * RING_BUFFER_SLOT_SIZE);

                // invariant: slots available for writing must have addr set to 0
                // the consumer is responsible for zeroing the addr after using it
                assert U.getLong(null, slotPtr) == 0;

                // important: write len first and only THEN the address
                U.putLong(slotPtr + 8, len);

                // write addr LAST with volatile store (release semantics)
                // this signals to the consumer that data is ready (addr != 0)
                U.putLongVolatile(null, slotPtr, address);

                return true;
            }
            Os.pause();
        }
    }

    static {
        if (Os.isPosix()) {
            MUNMAP_RING_BUFFER_PTR = initMunmapWorker();
            if (MUNMAP_RING_BUFFER_PTR == 0) {
                throw new RuntimeException("Failed to initialize munmap worker thread");
            }

            MUNMAP_RING_BUFFER_CAPACITY = Unsafe.getUnsafe().getLong(MUNMAP_RING_BUFFER_PTR + RING_BUFFER_CAPACITY_OFFSET);
            MUNMAP_RING_BUFFER_MASK = Unsafe.getUnsafe().getLong(MUNMAP_RING_BUFFER_PTR + RING_BUFFER_MASK_OFFSET);
        }
    }
}
