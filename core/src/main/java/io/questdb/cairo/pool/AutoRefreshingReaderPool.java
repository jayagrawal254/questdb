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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.PartitionOverwriteControl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxnScoreboardPool;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;
import java.util.Map;

public class AutoRefreshingReaderPool extends AbstractPool implements ReaderPool<AutoRefreshingReaderPool.R> {

    public static final int ENTRY_SIZE = 32;
    public final static String NO_LOCK_REASON = "unknown";
    private static final long LOCK_OWNER = Unsafe.getFieldOffset(AbstractMultiTenantPool.Entry.class, "lockOwner");
    private static final int NEXT_ALLOCATED = 1;
    private static final int NEXT_LOCKED = 2;
    private static final int NEXT_OPEN = 0;
    private static final long NEXT_STATUS = Unsafe.getFieldOffset(AbstractMultiTenantPool.Entry.class, "nextStatus");
    private static final long UNLOCKED = -1L;
    private final Log LOG = LogFactory.getLog(this.getClass());
    private final ConcurrentHashMap<AutoRefreshingReaderPool.Entry> entries = new ConcurrentHashMap<>();
    private final int maxEntries;
    private final int maxSegments;
    private final MessageBus messageBus;
    private final PartitionOverwriteControl partitionOverwriteControl;
    private final ThreadLocal<ResourcePoolSupervisor<R>> threadLocalPoolSupervisor;
    private final TxnScoreboardPool txnScoreboardPool;
    private RefreshOnAcquireReaderPool.ReaderListener readerListener;

    public AutoRefreshingReaderPool(CairoConfiguration configuration, TxnScoreboardPool scoreboardPool, MessageBus messageBus, PartitionOverwriteControl partitionOverwriteControl) {
        super(configuration, configuration.getInactiveReaderTTL());
        this.maxSegments = configuration.getReaderPoolMaxSegments();
        this.maxEntries = maxSegments * ENTRY_SIZE;
        threadLocalPoolSupervisor = new ThreadLocal<>();
        this.txnScoreboardPool = scoreboardPool;
        this.messageBus = messageBus;
        this.partitionOverwriteControl = partitionOverwriteControl;
    }

    public AutoRefreshingReaderPool(CairoConfiguration configuration, TxnScoreboardPool scoreboardPool, MessageBus messageBus) {
        this(configuration, scoreboardPool, messageBus, null);
    }

    @Override
    public void attach(TableReader reader) {
        RefreshOnAcquireReaderPool.R rdr = (RefreshOnAcquireReaderPool.R) reader;
        rdr.attach();
    }

    public void configureThreadLocalPoolSupervisor(@NotNull ResourcePoolSupervisor<AutoRefreshingReaderPool.R> poolSupervisor) {
        this.threadLocalPoolSupervisor.set(poolSupervisor);
    }

    @Override
    public void detach(TableReader reader) {
        RefreshOnAcquireReaderPool.R rdr = (RefreshOnAcquireReaderPool.R) reader;
        rdr.detach();
    }

    public Map<CharSequence, AutoRefreshingReaderPool.Entry> entries() {
        return entries;
    }

    @Override
    public R get(TableToken tableToken) {
        return get0(tableToken, null);
    }

    @Override
    public int getBusyCount() {
        int count = 0;
        for (Map.Entry<CharSequence, AutoRefreshingReaderPool.Entry> me : entries.entrySet()) {
            AutoRefreshingReaderPool.Entry e = me.getValue();
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.arrayGetVolatile(e.allocations, i) != UNALLOCATED && e.getTenant(i) != null) {
                        count++;
                    }
                }
                e = e.next;
            } while (e != null);
        }
        return count;
    }

    /**
     * Returns a pooled table reader that is pointed at the same transaction number
     * as the source reader.
     */
    @Override
    public TableReader getCopyOf(TableReader srcTenant) {
        return get0(srcTenant.getTableToken(), (R) srcTenant);
    }

    @Override
    public int getDetachedRefCount(TableReader reader) {
        return ((RefreshOnAcquireReaderPool.R) reader).getDetachedRefCount();
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    @Override
    public void incDetachedRefCount(TableReader reader) {
        ((RefreshOnAcquireReaderPool.R) reader).incrementDetachedRefCount();
    }

    @Override
    public boolean isDetached(TableReader reader) {
        return ((RefreshOnAcquireReaderPool.R) reader).isDetached();
    }

    @Override
    public boolean lock(TableToken tableToken) {
        AutoRefreshingReaderPool.Entry e = getEntry(tableToken);
        final long thread = Thread.currentThread().getId();
        if (Unsafe.cas(e, LOCK_OWNER, UNLOCKED, thread) || e.lockOwner == thread) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                        closeTenant(thread, e, i, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_NAME_LOCK);
                    } else if (Unsafe.arrayGetVolatile(e.allocations, i) == thread) {
                        // same thread, don't need to order reads
                        if (e.getTenant(i) != null) {
                            // this thread has busy reader, it should close first
                            e.lockOwner = -1L;
                            return false;
                        }
                    } else {
                        LOG.info().$("could not lock, busy [table=").$(tableToken)
                                .$(", at=").$(e.index).$(':').$(i)
                                .$(", owner=").$(e.allocations[i])
                                .$(", thread=").$(thread)
                                .I$();
                        e.lockOwner = -1L;
                        return false;
                    }
                }

                // try to prevent new entries from being created
                if (e.next == null) {
                    if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, NEXT_OPEN, NEXT_LOCKED)) {
                        break;
                    } else if (e.nextStatus == NEXT_ALLOCATED) {
                        // now we must wait until another thread that executes a get() call
                        // assigns the newly created next entry
                        while (e.next == null) {
                            Os.pause();
                        }
                    }
                }

                e = e.next;
            } while (e != null);
        } else {
            LOG.error().$("already locked [table=").$(tableToken)
                    .$(", owner=").$(e.lockOwner)
                    .I$();
            notifyListener(thread, tableToken, PoolListener.EV_LOCK_BUSY, -1, -1);
            return false;
        }
        notifyListener(thread, tableToken, PoolListener.EV_LOCK_SUCCESS, -1, -1);
        LOG.debug().$("locked [table=").$(tableToken)
                .$(", thread=").$(thread)
                .I$();
        return true;
    }

    @Override
    public void refreshAllUnallocatedReaders() {
        // todo: start with the most stale reader
        long thread = Thread.currentThread().getId();
        for (AutoRefreshingReaderPool.Entry e : entries.values()) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    R r;
                    if ((r = e.getTenant(i)) != null) {
                        if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                            try {
                                // todo: handle errors
                                r.refresh(null);
                            } finally {
                                Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            }
                        }
                    }
                }
                e = e.next;
            } while (e != null);
        }
    }

    @Override
    public void removeThreadLocalPoolSupervisor() {
        this.threadLocalPoolSupervisor.remove();
    }

    @TestOnly
    public void setTableReaderListener(RefreshOnAcquireReaderPool.ReaderListener readerListener) {
        this.readerListener = readerListener;
    }

    @Override
    public void unlock(TableToken tableToken) {
        AutoRefreshingReaderPool.Entry e = entries.get(tableToken.getDirName());
        long thread = Thread.currentThread().getId();
        if (e == null) {
            LOG.info().$("not found, cannot unlock [table=").$(tableToken).I$();
            notifyListener(thread, tableToken, PoolListener.EV_NOT_LOCKED, -1, -1);
            return;
        }

        if (e.lockOwner == thread) {
            entries.remove(tableToken.getDirName());
            while (e != null) {
                e = e.next;
            }
        } else {
            notifyListener(thread, tableToken, PoolListener.EV_NOT_LOCK_OWNER);
            throw CairoException.nonCritical().put("Not the lock owner of ").put(tableToken.getDirName());
        }

        notifyListener(thread, tableToken, PoolListener.EV_UNLOCKED, -1, -1);
        LOG.debug().$("unlocked [table=").$(tableToken).I$();
    }

    private void checkClosed() {
        if (isClosed()) {
            LOG.info().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    private void closeTenant(long thread, AutoRefreshingReaderPool.Entry entry, int index, short ev, int reason) {
        R tenant = entry.getTenant(index);
        if (tenant != null) {
            tenant.goodbye();
            tenant.close();
            LOG.info().$("closed [table=").$(tenant.getTableToken())
                    .$(", at=").$(entry.index).$(':').$(index)
                    .$(", reason=").$(PoolConstants.closeReasonText(reason))
                    .I$();
            notifyListener(thread, tenant.getTableToken(), ev, entry.index, index);
            entry.assignTenant(index, null);
        }
    }

    private R get0(TableToken tableToken, @Nullable R copyOfTenant) {
        AutoRefreshingReaderPool.Entry e = getEntry(tableToken);

        long lockOwner = e.lockOwner;
        long thread = Thread.currentThread().getId();

        if (lockOwner != UNLOCKED) {
            LOG.info().$("table is locked [table=").$(tableToken)
                    .$(", owner=").$(lockOwner)
                    .I$();
            throw EntryLockedException.instance(NO_LOCK_REASON);
        }

        do {
            for (int i = 0; i < ENTRY_SIZE; i++) {
                if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                    Unsafe.arrayPutOrdered(e.releaseOrAcquireTimes, i, clock.getTicks());
                    // got lock, allocate if needed
                    R tenant = e.getTenant(i);
                    ResourcePoolSupervisor<R> supervisor = threadLocalPoolSupervisor.get();
                    if (tenant == null) {
                        try {
                            LOG.debug()
                                    .$("open [table=").$(tableToken)
                                    .$(", at=").$(e.index).$(':').$(i)
                                    .I$();
                            tenant = copyOfTenant != null
                                    ? newCopyOfTenant(copyOfTenant, e, i, supervisor)
                                    : newTenant(tableToken, e, i, supervisor);
                        } catch (CairoException ex) {
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw ex;
                        }

                        e.assignTenant(i, tenant);
                        notifyListener(thread, tableToken, PoolListener.EV_CREATE, e.index, i);
                    } else {
                        try {
                            if (copyOfTenant != null) {
                                // when a caller is asking for specific txn we force refresh
                                tenant.refreshAt(supervisor, copyOfTenant);
                            } else if (!tenant.isActive()) {
                                // edge-case: readers can be in a pool without being pinned to a transaction.
                                // this can happen after an exception
                                tenant.refresh(supervisor);
                            }
                            // the most common case: a caller did not ask for a specific transaction
                            // -> we rely on the background job to have a reader pinned to a recent-enough transaction
                            // so we don't refresh now. why? refresh can be slow and we don't want to have it on a reading path.
                        } catch (Throwable th) {
                            tenant.goodbye();
                            tenant.close();
                            e.assignTenant(i, null);
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw th;
                        }
                        notifyListener(thread, tableToken, PoolListener.EV_GET, e.index, i);
                    }

                    // todo: explore side-effects of not reloading readers, but updating tokens
                    if (isClosed()) {
                        e.assignTenant(i, null);
                        tenant.goodbye();
                        LOG.info().$("born free [table=").$(tableToken).I$();
                        tenant.updateTableToken(tableToken);
                        if (supervisor != null) {
                            supervisor.onResourceBorrowed(tenant);
                        }
                        return tenant;
                    }
                    LOG.debug().$("assigned [table=").$(tableToken)
                            .$(", at=").$(e.index).$(':').$(i)
                            .$(", thread=").$(thread)
                            .I$();
                    tenant.updateTableToken(tableToken);
                    if (supervisor != null) {
                        supervisor.onResourceBorrowed(tenant);
                    }
                    return tenant;
                }
            }

            LOG.debug().$("Thread ").$(thread).$(" is moving to entry ").$(e.index + 1).$();

            // all allocated, create next entry if possible
            if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, NEXT_OPEN, NEXT_ALLOCATED)) {
                LOG.debug().$("Thread ").$(thread).$(" allocated entry ").$(e.index + 1).$();
                e.next = new AutoRefreshingReaderPool.Entry(e.index + 1, clock.getTicks());
            } else {
                // if the race is lost we need to wait until e.next is set by the winning thread
                while (e.next == null && e.nextStatus == NEXT_ALLOCATED) {
                    Os.pause();
                }
            }
            e = e.next;
        } while (e != null && e.index < maxSegments);

        // max entries exceeded
        notifyListener(thread, tableToken, PoolListener.EV_FULL, -1, -1);
        LOG.info().$("could not get, busy [table=").$(tableToken)
                .$(", thread=").$(thread)
                .$(", retries=").$(this.maxSegments)
                .I$();
        throw EntryUnavailableException.instance(NO_LOCK_REASON);
    }

    private AutoRefreshingReaderPool.Entry getEntry(TableToken token) {
        checkClosed();

        AutoRefreshingReaderPool.Entry e = entries.get(token.getDirName());
        if (e == null) {
            e = new AutoRefreshingReaderPool.Entry(0, clock.getTicks());
            AutoRefreshingReaderPool.Entry other = entries.putIfAbsent(token.getDirName(), e);
            if (other != null) {
                e = other;
            }
        }
        return e;
    }

    private void notifyListener(long thread, TableToken token, short event, int segment, int position) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(getListenerSrc(), thread, token, event, (short) segment, (short) position);
        }
    }

    @Override
    protected void closePool() {
        super.closePool();
        LOG.info().$("closed").$();
    }

    protected byte getListenerSrc() {
        return PoolListener.SRC_READER;
    }

    protected R newCopyOfTenant(R srcReader, AutoRefreshingReaderPool.Entry entry, int index, ResourcePoolSupervisor<R> supervisor) {
        return new R(this, entry, index, srcReader, txnScoreboardPool, messageBus, readerListener, partitionOverwriteControl, supervisor);
    }

    protected R newTenant(TableToken tableToken, AutoRefreshingReaderPool.Entry entry, int index, ResourcePoolSupervisor<R> supervisor) {
        return new R(this, entry, index, tableToken, txnScoreboardPool, messageBus, readerListener, partitionOverwriteControl, supervisor);
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        int casFailures = 0;
        int closeReason = deadline < Long.MAX_VALUE ? PoolConstants.CR_IDLE : PoolConstants.CR_POOL_CLOSE;

        TableToken leftBehind = null;
        for (AutoRefreshingReaderPool.Entry e : entries.values()) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    R r;
                    if (deadline > Unsafe.arrayGetVolatile(e.releaseOrAcquireTimes, i) && (r = e.getTenant(i)) != null) {
                        if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                            // check if deadline violation still holds
                            if (deadline > e.releaseOrAcquireTimes[i]) {
                                removed = true;
                                closeTenant(thread, e, i, PoolListener.EV_EXPIRE, closeReason);
                            }
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                        } else {
                            casFailures++;
                            if (deadline == Long.MAX_VALUE) {
                                r.goodbye();
                                Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                                var rec = LOG.info().$("shutting down, table is left behind [table=").$(r.getTableToken()).$(']');
                                try {
                                    var supervisor = r.getSupervisor();
                                    if (supervisor instanceof TracingResourcePoolSupervisor<R>) {
                                        ((TracingResourcePoolSupervisor<R>) supervisor).printResourceInfo(rec.$(": "), r);
                                    }
                                } finally {
                                    rec.$();
                                }
                                leftBehind = r.getTableToken();
                            }
                        }
                    }
                }
                // this does not release the next
                e = e.next;
            } while (e != null);
        }

        if (leftBehind != null) {
            // This code branch should be in tests only.
            // Release the item, to not block the pool, but throw an exception to fail the test
            throw CairoException.nonCritical()
                    .put("table is left behind on pool shutdown [table=").put(leftBehind).put(']');
        }

        // when we are timing out entries the result is "true" if there was any work done
        // when we're closing pool, the result is true when pool is empty
        if (closeReason == PoolConstants.CR_IDLE) {
            return removed;
        } else {
            return casFailures == 0;
        }
    }

    protected boolean returnToPool(R tenant) {
        final AutoRefreshingReaderPool.Entry e = tenant.getEntry();
        if (e == null) {
            return false;
        }

        final TableToken tableToken = tenant.getTableToken();
        final long thread = Thread.currentThread().getId();
        final int index = tenant.getIndex();
        final long owner = Unsafe.arrayGetVolatile(e.allocations, index);

        if (owner != UNALLOCATED) {
            LOG.debug().$("table is back [table=").$(tableToken)
                    .$(", at=").$(e.index).$(':').$(index)
                    .$(", thread=").$(thread)
                    .I$();
            notifyListener(thread, tableToken, PoolListener.EV_RETURN, e.index, index);

            // release the entry for anyone to pick up
            e.releaseOrAcquireTimes[index] = clock.getTicks();
            Unsafe.arrayPutOrdered(e.allocations, index, UNALLOCATED);
            final boolean closed = isClosed();

            // When pool is closed we will race against release thread
            // to release our entry. No need to bother releasing map entry, pool is going down.
            return !closed || !Unsafe.cas(e.allocations, index, UNALLOCATED, owner);
        }

        if (isClosed()) {
            // Returning to closed pool is ok under race condition
            // We may end up here because our "allocation" has been erased while we
            // still see the reference to the pool. The allocation "erasure"
            // occurs when pool is being closed. The memory writes should be ordered
            // in such a way that when we see "UNALLOCATED" the pool closed flag is already set.
            return false;
        }
        throw CairoException.critical(0).put("double close [table=").put(tableToken)
                .put(", index=").put(index).put(']');
    }

    public static final class Entry {
        private final long[] allocations = new long[ENTRY_SIZE];
        private final int index;
        private final long[] releaseOrAcquireTimes = new long[ENTRY_SIZE];
        @SuppressWarnings("unchecked")
        private final R[] tenants = new R[ENTRY_SIZE];
        int nextStatus = NEXT_OPEN;
        private volatile long lockOwner = -1L;
        private volatile AutoRefreshingReaderPool.Entry next;

        public Entry(int index, long currentMicros) {
            this.index = index;
            Arrays.fill(allocations, UNALLOCATED);
            Arrays.fill(releaseOrAcquireTimes, currentMicros);
        }

        public void assignTenant(int pos, R tenant) {
            tenants[pos] = tenant;
        }

        public int getIndex() {
            return index;
        }

        public AutoRefreshingReaderPool.Entry getNext() {
            return next;
        }

        public long getOwnerVolatile(int pos) {
            return Unsafe.arrayGetVolatile(allocations, pos);
        }

        public long getReleaseOrAcquireTime(int pos) {
            return releaseOrAcquireTimes[pos];
        }

        public AutoRefreshingReaderPool.R getTenant(int pos) {
            return tenants[pos];
        }
    }

    public static class R extends TableReader implements Sinkable {
        private final int index;
        private final RefreshOnAcquireReaderPool.ReaderListener readerListener;
        private boolean detached;
        // Reference counter that may be used to track usage of detached readers.
        // A reader may be obtained from the pool and closed on different threads,
        // but that's fine. In that case, there will be synchronization between the threads,
        // so we don't need to make this field volatile/atomic.
        private int detachedRefCount;
        private Entry entry;
        private AutoRefreshingReaderPool pool;
        private ResourcePoolSupervisor<R> supervisor;

        public R(
                AutoRefreshingReaderPool pool,
                AutoRefreshingReaderPool.Entry entry,
                int index,
                TableToken tableToken,
                TxnScoreboardPool txnScoreboardPool,
                MessageBus messageBus,
                RefreshOnAcquireReaderPool.ReaderListener readerListener,
                PartitionOverwriteControl partitionOverwriteControl,
                ResourcePoolSupervisor<R> supervisor
        ) {
            super(entry.getIndex() * ENTRY_SIZE + index, pool.getConfiguration(), tableToken, txnScoreboardPool, messageBus, partitionOverwriteControl);
            this.pool = pool;
            this.entry = entry;
            this.index = index;
            this.readerListener = readerListener;
            this.supervisor = supervisor;
        }

        public R(
                AutoRefreshingReaderPool pool,
                AutoRefreshingReaderPool.Entry entry,
                int index,
                R srcReader,
                TxnScoreboardPool txnScoreboardPool,
                MessageBus messageBus,
                RefreshOnAcquireReaderPool.ReaderListener readerListener,
                PartitionOverwriteControl partitionOverwriteControl,
                ResourcePoolSupervisor<R> supervisor
        ) {
            super(entry.getIndex() * ENTRY_SIZE + index, pool.getConfiguration(), srcReader, txnScoreboardPool, messageBus, partitionOverwriteControl);
            this.pool = pool;
            this.entry = entry;
            this.index = index;
            this.readerListener = readerListener;
            this.supervisor = supervisor;
        }

        public void attach() {
            assert detachedRefCount == 0;
            detached = false;
        }

        @Override
        public void close() {
            if (isOpen()) {
                if (!detached) {
                    // report reader closure to the supervisor
                    // so that we do not freak out about reader leaks
                    if (supervisor != null) {
                        supervisor.onResourceReturned(this);
                        supervisor = null;
                    }
                    // note: we don't ever go passive when returning to a pool - the reader remains pinned to whatever transaction it was on


                    final AutoRefreshingReaderPool pool = this.pool;
                    if (pool == null || entry == null || !pool.returnToPool(this)) {
                        super.close();
                    }
                } else {
                    detachedRefCount--;
                }
            }
        }

        public void detach() {
            detached = true;
            detachedRefCount = 0;
        }

        public int getDetachedRefCount() {
            return detachedRefCount;
        }

        public AutoRefreshingReaderPool.Entry getEntry() {
            return entry;
        }

        public int getIndex() {
            return index;
        }

        public ResourcePoolSupervisor<R> getSupervisor() {
            return supervisor;
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        public void incrementDetachedRefCount() {
            assert detached;
            detachedRefCount++;
        }

        public boolean isDetached() {
            return detached;
        }

        @Override
        public long openPartition(int partitionIndex) {
            if (readerListener != null) {
                readerListener.onOpenPartition(getTableToken(), partitionIndex);
            }
            return super.openPartition(partitionIndex);
        }

        public void refresh(ResourcePoolSupervisor<R> supervisor) {
            this.supervisor = supervisor;
            try {
                goActive();
            } catch (Throwable ex) {
                close();
                throw ex;
            }
        }

        public void refreshAt(@Nullable ResourcePoolSupervisor<R> supervisor, R srcReader) {
            this.supervisor = supervisor;
            try {
                goActiveAtTxn(srcReader);
            } catch (Throwable ex) {
                close();
                throw ex;
            }
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("ReaderPool.R{index=").put(index).put(", detached=").put(detached).put(", detachedRefCount=").put(detachedRefCount).put('}');
        }
    }
}
