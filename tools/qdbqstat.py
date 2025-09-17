#!/usr/bin/env python3
"""
QuestDB Query Profiler using eBPF + custom USDT probes

Metrics per query:
  - wall_ms       : wall-clock duration from start→end probe (ns clock in kernel)
  - cpu_ms        : on-CPU time accumulated via sched:sched_switch (currently disabled)
  - global_minflt : system-wide minor page faults at query end
  - global_majflt : system-wide major page faults at query end
  - nthreads      : number of threads attributed (v1: 1, the start TID)
  - pid           : process id
  - qid           : 64-bit query id (from your probe)
  - sql_hash      : 64-bit hash of SQL (hex string)

Usage:
  sudo ./qdbqstat.py -p <questdb-pid>

Requirements:
  - python3-bcc
  - your JNI lib with sys/sdt.h probes loaded by QuestDB
  - CAP_PERFMON or root, and permissive perf_event_paranoid

Notes:
  - This v1 attributes to the TID that fired query_start.
  - Page faults are global system counters, not per-query attribution.
  - For multi-threaded queries, add USDTs: query_thread_attach/detach and
    extend active_threads + fault/accounting similarly.
"""

import argparse
import ctypes as ct
import json
import signal
import sys
import time
from collections import namedtuple

try:
    from bcc import BPF, USDT
except ImportError:
    print("ERROR: python3-bcc package is required (apt/yum install bcc/python3-bcc).", file=sys.stderr)
    sys.exit(1)

QueryState = namedtuple("QueryState", ["start_ns", "sql_hash", "threads", "pid"])

BPF_PROGRAM = r"""
#include <uapi/linux/ptrace.h>
#include <linux/sched.h>

BPF_HASH(active_threads, u32, u64);   // tid -> qid
BPF_HASH(oncpu_start,   u32, u64);    // tid -> ts (ns)
BPF_HASH(cpu_time,      u64, u64);    // qid -> cpu ns

struct query_event_t {
    u64 ts;
    u64 qid;
    u64 sql_hash;   // only on start (0 on end)
    u32 tid;
    u32 pid;
    u8  event_type; // 0=start, 1=end
};
BPF_PERF_OUTPUT(events);

// USDT: questdb:query_start(qid, sql_hash, tid)
int query_start(struct pt_regs *ctx) {
    struct query_event_t ev = {};
    u64 qid, sqlh;
    u32 tid;

    // Read USDT arguments
    bpf_usdt_readarg(1, ctx, &qid);    // arg0 in bpftrace
    bpf_usdt_readarg(2, ctx, &sqlh);   // arg1 in bpftrace
    bpf_usdt_readarg(3, ctx, &tid);    // arg2 in bpftrace

    u64 now = bpf_ktime_get_ns();
    ev.ts = now;
    ev.qid = qid;
    ev.sql_hash = sqlh;
    ev.tid = tid;
    ev.pid = (u32)(bpf_get_current_pid_tgid() >> 32);
    ev.event_type = 0;

    active_threads.update(&tid, &qid);
    oncpu_start.update(&tid, &now);

    events.perf_submit(ctx, &ev, sizeof(ev));
    return 0;
}

// USDT: questdb:query_end(qid, tid)
int query_end(struct pt_regs *ctx) {
    struct query_event_t ev = {};
    u64 qid;
    u32 tid;

    bpf_usdt_readarg(1, ctx, &qid);
    bpf_usdt_readarg(2, ctx, &tid);

    u64 now = bpf_ktime_get_ns();
    ev.ts = now;
    ev.qid = qid;
    ev.sql_hash = 0;
    ev.tid = tid;
    ev.pid = (u32)(bpf_get_current_pid_tgid() >> 32);
    ev.event_type = 1;

    // flush final on-CPU slice for this tid if it was running
    u64 *startp = oncpu_start.lookup(&tid);
    if (startp && *startp) {
        u64 delta = now - *startp;
        u64 *cp = cpu_time.lookup(&qid);
        if (cp) {
            *cp += delta;
        } else {
            cpu_time.update(&qid, &delta);
        }
        u64 zero = 0;
        oncpu_start.update(&tid, &zero);
    }

    events.perf_submit(ctx, &ev, sizeof(ev));
    return 0;
}

// CPU time tracking disabled for compatibility
// TODO: Add scheduler tracepoint when BPF issues are resolved
"""


class Profiler:
    def __init__(self, pid: int):
        self.pid = pid
        self.bpf = None
        self.active = {}  # qid -> QueryState
        self.running = True

    @staticmethod
    def _get_global_faults():
        """Get system-wide page fault counters from /proc/vmstat"""
        try:
            with open("/proc/vmstat", "r") as f:
                vmstat = f.read()
                minflt = majflt = 0
                for line in vmstat.splitlines():
                    if line.startswith("pgfault "):
                        minflt = int(line.split()[1])
                    elif line.startswith("pgmajfault "):
                        majflt = int(line.split()[1])
                return {"minflt": minflt, "majflt": majflt}
        except Exception:
            return {"minflt": 0, "majflt": 0}

    def _on_event(self, cpu, data, size):
        ev = self.bpf["events"].event(data)

        if ev.event_type == 0:  # start
            self.active[ev.qid] = QueryState(
                start_ns=ev.ts,
                sql_hash=ev.sql_hash,
                threads={ev.tid},
                pid=ev.pid,
            )
            return

        # end
        st = self.active.get(ev.qid)
        if not st:
            # unmatched end; ignore
            return

        wall_ns = ev.ts - st.start_ns

        # read cpu_time[qid]
        qkey = ct.c_ulonglong(ev.qid)
        try:
            cpu_ns = int(self.bpf["cpu_time"][qkey].value)
        except KeyError:
            cpu_ns = 0

        # Get global page fault counters at query end
        global_faults = self._get_global_faults()

        out = {
            "qid": int(ev.qid),
            "sql_hash": f"0x{int(st.sql_hash):016x}",
            "wall_ms": wall_ns / 1e6,
            "cpu_ms": cpu_ns / 1e6,
            "global_minflt": global_faults["minflt"],
            "global_majflt": global_faults["majflt"],
            "nthreads": len(st.threads),
            "pid": int(st.pid),
            "timestamp": time.time(),
        }
        print(json.dumps(out), flush=True)

        # cleanup BPF maps for this qid/tid
        self._delete_bpf_key("cpu_time", qkey)
        for tid in st.threads:
            self._delete_bpf_key("active_threads", ct.c_uint(tid))
            self._delete_bpf_key("oncpu_start", ct.c_uint(tid))
        # drop memory
        del self.active[ev.qid]

    def _delete_bpf_key(self, table_name, key):
        t = self.bpf[table_name]
        try:
            del t[key]
        except KeyError:
            pass

    def run(self):
        # Find the QuestDB shared library with USDT probes
        import glob
        questdb_libs = glob.glob("/tmp/libquestdb*.so")
        if not questdb_libs:
            print("ERROR: No QuestDB shared library found in /tmp/", file=sys.stderr)
            print("Make sure QuestDB is running with USDT probes enabled", file=sys.stderr)
            return

        lib_path = questdb_libs[0]  # Use the first found library
        print(f"# Using QuestDB library: {lib_path}", file=sys.stderr)

        # Attach USDTs on the target PID
        usdt = USDT(pid=self.pid)
        try:
            usdt.enable_probe(probe="query_start", fn_name="query_start")
            print("# Enabled query_start probe", file=sys.stderr)
        except Exception as e:
            print(f"# Failed to enable query_start probe: {e}", file=sys.stderr)
            return

        try:
            usdt.enable_probe(probe="query_end", fn_name="query_end")
            print("# Enabled query_end probe", file=sys.stderr)
        except Exception as e:
            print(f"# Failed to enable query_end probe: {e}", file=sys.stderr)
            return

        # Load BPF with attached USDT context
        self.bpf = BPF(text=BPF_PROGRAM, usdt_contexts=[usdt])

        # Perf buffer for start/end events
        self.bpf["events"].open_perf_buffer(self._on_event)

        print(f"# Profiling QuestDB PID {self.pid} ...", file=sys.stderr)
        print("# JSON lines will be emitted for each query", file=sys.stderr)

        while self.running:
            try:
                self.bpf.perf_buffer_poll(timeout=1000)
            except KeyboardInterrupt:
                break

    def stop(self):
        self.running = False


def _sigint(_sig, _frm):
    # Let BCC clean up maps/programs gracefully
    sys.exit(0)


def main():
    ap = argparse.ArgumentParser(description="Profile QuestDB queries via eBPF + USDT")
    ap.add_argument("-p", "--pid", type=int, required=True, help="QuestDB process PID")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _sigint)

    p = Profiler(pid=args.pid)
    p.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
