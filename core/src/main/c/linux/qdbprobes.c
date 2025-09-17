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

#define _GNU_SOURCE

#include <jni.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <stdint.h>
#include "qdbprobes.h"

// Architecture-specific syscall numbers
#ifndef __NR_gettid
#if defined(__x86_64__)
#define __NR_gettid 186
#elif defined(__i386__)
#define __NR_gettid 224
#elif defined(__aarch64__)
#define __NR_gettid 178
#elif defined(__arm__)
#define __NR_gettid 224
#else
// Fallback - use getpid() if gettid not available
#define __NR_gettid -1
#endif
#endif

// USDT probe support
#ifdef HAVE_SDT_H
#include <sys/sdt.h>

// Modern systemtap-sdt uses STAP_PROBEV, older versions use DTRACE_PROBE
// We'll use the DTRACE_PROBE interface for compatibility
#ifndef DTRACE_PROBE_ENABLED
// Some versions don't have the _ENABLED check - just assume enabled
#define DTRACE_PROBE_ENABLED(provider, name) (1)
#endif

#ifndef DTRACE_PROBE3
// If DTRACE_PROBE3 doesn't exist, try STAP_PROBE3
#ifdef STAP_PROBE3
#define DTRACE_PROBE3(provider, name, arg0, arg1, arg2) STAP_PROBE3(provider, name, arg0, arg1, arg2)
#else
#define DTRACE_PROBE3(provider, name, arg0, arg1, arg2) do {} while(0)
#endif
#endif

#ifndef DTRACE_PROBE2
#ifdef STAP_PROBE2
#define DTRACE_PROBE2(provider, name, arg0, arg1) STAP_PROBE2(provider, name, arg0, arg1)
#else
#define DTRACE_PROBE2(provider, name, arg0, arg1) do {} while(0)
#endif
#endif

#else
// Fallback macros when sys/sdt.h is not available
#define DTRACE_PROBE_ENABLED(provider, name) (0)
#define DTRACE_PROBE3(provider, name, arg0, arg1, arg2) do {} while(0)
#define DTRACE_PROBE2(provider, name, arg0, arg1) do {} while(0)
#endif

// Helper function to get thread ID
static inline uint32_t get_thread_id(void) {
#if __NR_gettid != -1
    return (uint32_t)syscall(__NR_gettid);
#else
    return (uint32_t)getpid();  // Fallback to PID
#endif
}

/**
 * JNI function to emit query start probe.
 * Maps to questdb:query_start(uint64 qid, uint64 sql_hash, uint32 tid)
 */
JNIEXPORT void JNICALL Java_io_questdb_QueryProbes_startNative
  (JNIEnv* env, jclass cls, jlong qid, jlong sqlHash) {

    // Only do work if USDT probes are enabled to minimize overhead
    if (DTRACE_PROBE_ENABLED(questdb, query_start)) {
        uint32_t tid = get_thread_id();
        DTRACE_PROBE3(questdb, query_start, (uint64_t)qid, (uint64_t)sqlHash, tid);
    }
}

/**
 * JNI function to emit query end probe.
 * Maps to questdb:query_end(uint64 qid, uint32 tid)
 */
JNIEXPORT void JNICALL Java_io_questdb_QueryProbes_endNative
  (JNIEnv* env, jclass cls, jlong qid) {

    // Only do work if USDT probes are enabled to minimize overhead
    if (DTRACE_PROBE_ENABLED(questdb, query_end)) {
        uint32_t tid = get_thread_id();
        DTRACE_PROBE2(questdb, query_end, (uint64_t)qid, tid);
    }
}

/**
 * JNI function to test native binding availability.
 * Used during static initialization to verify library is properly loaded.
 */
JNIEXPORT void JNICALL Java_io_questdb_QueryProbes_testNativeBinding
  (JNIEnv* env, jclass cls) {
    // This function existing and being callable is the test
}