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

#ifndef QDBPROBES_H
#define QDBPROBES_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * JNI function to emit query start probe.
 * Maps to questdb:query_start(uint64 qid, uint64 sql_hash, uint32 tid)
 */
JNIEXPORT void JNICALL Java_io_questdb_QueryProbes_startNative
  (JNIEnv* env, jclass cls, jlong qid, jlong sqlHash);

/**
 * JNI function to emit query end probe.
 * Maps to questdb:query_end(uint64 qid, uint32 tid)
 */
JNIEXPORT void JNICALL Java_io_questdb_QueryProbes_endNative
  (JNIEnv* env, jclass cls, jlong qid);

/**
 * JNI function to test native binding availability.
 * Used during static initialization to verify library is properly loaded.
 */
JNIEXPORT void JNICALL Java_io_questdb_QueryProbes_testNativeBinding
  (JNIEnv* env, jclass cls);

#ifdef __cplusplus
}
#endif

#endif /* QDBPROBES_H */