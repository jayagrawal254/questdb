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

package io.questdb.cairo.pool;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;

import java.io.Closeable;

public interface ReaderPool<T extends TableReader> extends ResourcePool<T>, Closeable {

    void attach(TableReader reader);

    void detach(TableReader reader);

    int getBusyCount();

    TableReader getCopyOf(TableReader srcReader);

    int getDetachedRefCount(TableReader srcReader);

    void incDetachedRefCount(TableReader srcReader);

    boolean isDetached(TableReader srcReader);

    boolean lock(TableToken tableToken);

    void refreshAllUnallocatedReaders();

    boolean releaseAll();

    boolean releaseInactive();

    void removeThreadLocalPoolSupervisor();

    void setPoolListener(PoolListener poolListener);

    void unlock(TableToken tableToken);
}
