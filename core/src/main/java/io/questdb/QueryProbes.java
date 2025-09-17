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

package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;

/**
 * QueryProbes provides JNI bindings to emit USDT probes for per-query profiling.
 * <p>
 * This class enables eBPF-based collection of query performance metrics including:
 * - Wall time
 * - CPU time
 * - Page faults (minor/major)
 * - Thread attribution
 * <p>
 * Probes are only emitted when questdb.usdt system property is enabled and
 * native library is available. Overhead is minimal when disabled.
 */
public final class QueryProbes {
    private static final Log LOG = LogFactory.getLog(QueryProbes.class);
    private static final boolean NATIVE_LOADED;
    private static final boolean USDT_ENABLED;

    /**
     * Emit query_end probe with query ID.
     *
     * @param qid Query ID (must match corresponding start() call)
     */
    public static void end(long qid) {
        if (NATIVE_LOADED) {
            endNative(qid);
        }
    }

    /**
     * Generate a 64-bit hash of SQL text for probe emission.
     * Uses simple hash to avoid dependency on external libraries.
     *
     * @param sql SQL text to hash
     * @return 64-bit hash value
     */
    public static long hashSql(CharSequence sql) {
        if (sql == null) {
            return 0L;
        }

        // Simple FNV-1a hash for SQL text
        long hash = 0xcbf29ce484222325L;
        final long prime = 0x100000001b3L;

        for (int i = 0, n = sql.length(); i < n; i++) {
            hash ^= sql.charAt(i);
            hash *= prime;
        }
        return hash;
    }

    /**
     * Check if USDT profiling is currently enabled.
     *
     * @return true if probes will be emitted, false otherwise
     */
    public static boolean isEnabled() {
        return NATIVE_LOADED;
    }

    /**
     * Emit query_start probe with query ID and SQL hash.
     *
     * @param qid     Query ID (should be unique for query lifetime)
     * @param sqlHash 64-bit hash of the SQL statement
     */
    public static void start(long qid, long sqlHash) {
        if (NATIVE_LOADED) {
            startNative(qid, sqlHash);
        }
    }

    /**
     * Emit query_thread_attach probe when a worker thread starts working on a query.
     *
     * @param qid Query ID
     * @param threadId Thread ID of the worker thread
     */
    public static void threadAttach(long qid, long threadId) {
        if (NATIVE_LOADED) {
            threadAttachNative(qid, threadId);
        }
    }

    /**
     * Emit query_thread_detach probe when a worker thread finishes working on a query.
     *
     * @param qid Query ID
     * @param threadId Thread ID of the worker thread
     */
    public static void threadDetach(long qid, long threadId) {
        if (NATIVE_LOADED) {
            threadDetachNative(qid, threadId);
        }
    }

    private static native void endNative(long qid);

    // Native method declarations
    private static native void startNative(long qid, long sqlHash);

    private static native void threadAttachNative(long qid, long threadId);

    private static native void threadDetachNative(long qid, long threadId);

    // Test method to verify native binding is available
    private static native void testNativeBinding();

    static {
        // Check if USDT profiling is enabled via system property
        USDT_ENABLED = Chars.equalsIgnoreCase("true", System.getProperty("questdb.usdt", "true"));

        boolean nativeLoaded = false;
        if (USDT_ENABLED) {
            try {
                // Native library will be linked into main questdb shared library
                // No separate loading required - just check if symbols are available
                testNativeBinding();
                nativeLoaded = true;
                LOG.info().$("USDT query profiling enabled").$();
            } catch (Throwable e) {
                LOG.error().$("USDT query profiling requested but native library not available: ")
                        .$(e.getMessage()).$();
            }
        }
        NATIVE_LOADED = nativeLoaded;
    }
}