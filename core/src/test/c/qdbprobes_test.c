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

#include <stdio.h>
#include <stdlib.h>

// Test program to verify USDT probe compilation
// This doesn't test actual probe firing (that requires eBPF),
// but ensures the code compiles and links correctly

// Forward declarations of probe functions (would normally be called via JNI)
extern void Java_io_questdb_QueryProbes_startNative(void* env, void* cls, long qid, long sqlHash);
extern void Java_io_questdb_QueryProbes_endNative(void* env, void* cls, long qid);
extern void Java_io_questdb_QueryProbes_testNativeBinding(void* env, void* cls);

int main() {
    printf("Testing USDT probe compilation...\n");

    // Test that we can call the probe functions without crashing
    // (The actual probes won't fire without proper JNI environment)

    printf("Testing testNativeBinding...\n");
    Java_io_questdb_QueryProbes_testNativeBinding(NULL, NULL);

    printf("Testing startNative...\n");
    Java_io_questdb_QueryProbes_startNative(NULL, NULL, 12345L, 0xabcdef123456L);

    printf("Testing endNative...\n");
    Java_io_questdb_QueryProbes_endNative(NULL, NULL, 12345L);

    printf("All probe functions compiled and linked successfully!\n");

#ifdef HAVE_SDT_H
    printf("USDT support: ENABLED (sys/sdt.h found)\n");
#else
    printf("USDT support: DISABLED (sys/sdt.h not found)\n");
#endif

    return 0;
}