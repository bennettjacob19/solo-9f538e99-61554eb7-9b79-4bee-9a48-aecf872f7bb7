"""清退引擎：按工单推进命名空间清理的状态机。

阶段：VALIDATING -> RETAINING -> DELETING_NAMESPACE -> DONE（异常：FAILED）
阻塞：blocked_reason 字段记录当前阻塞原因，推进时自动清除并重试。

关键不变量：
  * 先留存、验证（在原命名空间之外独立读回校验 sha256），再删除对应源 PVC；
    留存副本未确认与开单 SHA-256 一致前，不开放原链接下载，也不删源 PVC。
  * 任一时刻 原卷字节 + 留存副本字节 <= 开单基线 + 最大选中卷大小。
  * 所有删除按开单时快照的 UID 寻址；同名重建的命名空间 UID 不同，不受影响。
  * 删除操作写入 ops 日志（幂等键）；已确认(confirmed)的删除绝不重发；
    结果未明(unknown)的删除先 GET 核对：目标已消失则标记确认，仍在才重发。

分块短请求（io_limit_bytes 非空的新单）：
  * 每次 advance 的卷正文 I/O（源卷读 + 留存写 + 独立读回 + 恢复核对）不得超过
    开单填写的单次上限（1~8 MiB）；预算用尽本请求即结束，下次凭原单续办。
  * 复制/校验均按 64 KiB 小块推进，每块完成即把偏移（及可续算哈希状态）落 SQLite。
  * 进程在任意时刻退出后重启：复制从已提交偏移续拷（超长部分截断重写），
    校验从已提交偏移与哈希状态续算；最坏只重做最后一个未提交块（64 KiB），
    满足“每个阶段最多重做 256 KiB 原文范围”的合约。
  * 开单只读取元数据快照（sha256 在造数时已计算），不读取任何卷正文。
  * io_limit_bytes 为空的旧单沿用整卷复制/校验路径，照常推进（不受次数限制）。
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import uuid
from pathlib import Path

from .db import DB
from .sha256state import ResumableSHA256
from .simcluster import (ConflictError, DroppedResponseError, NotFoundError,
                         SimCluster)
from .util import now_iso, sha256_file, write_json

MAX_STEPS_PER_ADVANCE = 1000

# 单块正文 I/O 粒度：复制读+写各一块、校验读一块。
COPY_CHUNK_BYTES = 64 * 1024
# 续办重做合约上限（实际最坏仅重做一个未提交块 = COPY_CHUNK_BYTES）。
MAX_RESUME_REWORK_BYTES = 256 * 1024

# 测试钩子：本工单累计正文 I/O 达到该字节数后立即 os._exit（模拟断电/被 kill），
# 落在“实际 I/O 已完成、检查点尚未落盘”的窗口内。仅测试时通过环境变量注入，
# 形如 "ticket_id:bytes"；不带工单号时对所有工单生效。
_CRASH_ENV = "DRILL_CRASH_AFTER_IO_BYTES"


class Engine:
    def __init__(self, data_dir: Path | str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sim = SimCluster(self.data_dir / "cluster")
        self.retained_root = self.data_dir / "retained"
        self.retained_root.mkdir(parents=True, exist_ok=True)
        self.db = DB(self.data_dir / "app.db")
        self._lock = threading.RLock()
        # 当前这次 advance 的正文 I/O 计数（每次 advance 开始时清零）。
        self._cur_read = 0
        self._cur_written = 0
        # 本次 advance 的读/写汇总（跨步骤累计，请求结束写入 last_io_*）。
        self._last_read = 0
        self._last_written = 0
        # 本次 advance 已用预算（flush 累计值不影响它），用于单次 I/O 上限判定。
        self._adv_used = 0
        self._crash_tid: str | None = None

    # ---------------- 正文 I/O 计量 ----------------
    def _charge_io(self, read: int = 0, written: int = 0) -> None:
        self._cur_read += read
        self._cur_written += written
        self._adv_used += read + written
        spec = os.environ.get(_CRASH_ENV)
        if spec:
            target_tid, _, raw = spec.partition(":")
            if not raw:  # 未带工单号 -> 全局生效
                target_tid, raw = None, spec
            if target_tid is None or target_tid == self._crash_tid:
                try:
                    limit = int(raw)
                except ValueError:
                    return
                # 以 SQLite 中已持久化的累计 I/O 为基准，保证“进程中途被 kill 后
                # 重启、计数清零”不会让崩溃阈值重新计数。
                row = self.db.one(
                    "SELECT total_io_read + total_io_written AS done "
                    "FROM tickets WHERE id=?", (self._crash_tid,))
                done = (row["done"] if row else 0) + self._cur_read + self._cur_written
                if limit >= 0 and done >= limit:
                    # 硬退出：不执行 finally/atexit，模拟进程被直接杀死。
                    os._exit(2)
    def _remaining_budget(self, t: dict) -> int | None:
        if t["io_limit_bytes"] is None:
            return None
        return t["io_limit_bytes"] - self._adv_used

    def _flush_io_counters(self, tid: str) -> None:
        """把本进程自上次 flush 以来的正文 I/O 增量并入工单累计值并清零暂存。

        每个状态机步骤后立即调用：即使下一步进程被硬杀，已完成 I/O 的计量也已
        与复制/校验检查点同节奏落盘，重启后的累计读写量仍可对账。
        """
        if self._cur_read == 0 and self._cur_written == 0:
            return
        self._last_read += self._cur_read
        self._last_written += self._cur_written
        self.db.execute(
            "UPDATE tickets SET last_io_read=?, last_io_written=?, "
            "total_io_read=total_io_read+?, total_io_written=total_io_written+? "
            "WHERE id=?",
            (self._cur_read, self._cur_written, self._cur_read, self._cur_written, tid))
        self._cur_read = 0
        self._cur_written = 0

    # ---------------- 开单 ----------------
    def create_ticket(self, namespace: str, retain_pvcs: list[str],
                      io_limit_bytes: int, note: str | None = None) -> dict:
        with self._lock:
            if not retain_pvcs:
                raise ValueError("至少选择一个需保留的 PVC")
            ns = self.sim.get_namespace_by_name(namespace)
            if not ns:
                raise NotFoundError(f"命名空间 {namespace!r} 不存在")
            # 只读取 PVC 元数据（造数时已快照 sha256），不打开任何 data.bin。
            pvcs = self.sim.list_pvcs(ns_uid=ns["uid"])
            by_name = {p["name"]: p for p in pvcs}
            missing = [n for n in retain_pvcs if n not in by_name]
            if missing:
                raise NotFoundError(f"命名空间 {namespace!r} 中不存在 PVC: {missing}")
            tid = uuid.uuid4().hex[:12]
            selected = [by_name[n] for n in retain_pvcs]
            baseline = sum(p["size_bytes"] for p in pvcs)
            max_sel = max(p["size_bytes"] for p in selected)
            self.db.execute(
                "INSERT INTO tickets(id,created_at,namespace_name,namespace_uid,state,"
                "blocked_reason,note,baseline_bytes,max_selected_bytes,peak_bytes,"
                "io_limit_bytes,last_io_read,last_io_written,total_io_read,total_io_written) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,0,0)",
                (tid, now_iso(), ns["name"], ns["uid"], "VALIDATING", None, note,
                 baseline, max_sel, baseline, io_limit_bytes))
            for p in pvcs:
                self.db.execute(
                    "INSERT INTO pvcs(ticket_id,uid,name,pv_name,size_bytes,reclaim_policy,"
                    "selected,sha256,copy_state,delete_state,copy_offset,verify_offset,"
                    "verify_hash_state) VALUES(?,?,?,?,?,?,?,?,?,?,0,0,NULL)",
                    (tid, p["uid"], p["name"], p["pv_name"], p["size_bytes"],
                     p["reclaim_policy"], 1 if p["name"] in retain_pvcs else 0,
                     p["sha256"], "pending", "pending"))
            return self.status(tid)

    # ---------------- 查询 ----------------
    def _ticket(self, tid: str) -> dict:
        t = self.db.one("SELECT * FROM tickets WHERE id=?", (tid,))
        if not t:
            raise NotFoundError(f"工单 {tid} 不存在")
        return t

    def list_tickets(self) -> list[dict]:
        return self.db.query("SELECT * FROM tickets ORDER BY created_at DESC")

    def _current_bytes(self, t: dict) -> int:
        """原卷现存字节 + 本单留存副本字节（含复制中的部分块）。"""
        total = sum(p["size_bytes"] for p in self.sim.list_pvcs(ns_uid=t["namespace_uid"]))
        rdir = self.retained_root / t["id"]
        if rdir.exists():
            total += sum(f.stat().st_size for f in rdir.rglob("data.bin"))
        return total

    def retained_items(self, tid: str) -> list[dict]:
        rdir = self.retained_root / tid
        rows = {r["name"]: r for r in self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1", (tid,))}
        out = []
        if rdir.exists():
            for d in sorted(rdir.iterdir()):
                if not d.is_dir():
                    continue
                data = d / "data.bin"
                meta = d / "meta.json"
                r = rows.get(d.name, {})
                item = {
                    "ticket_id": tid,
                    "pvc_name": d.name,
                    "data_bytes_on_disk": data.stat().st_size if data.exists() else 0,
                    "copy_state": r.get("copy_state"),
                    "verified": r.get("copy_state") == "verified",
                }
                if meta.exists():  # 整卷复制完成后才有 meta.json
                    item.update(json.loads(meta.read_text(encoding="utf-8")))
                out.append(item)
        return out

    def retained_file(self, tid: str, pvc_name: str) -> Path:
        self._ticket(tid)
        row = self.db.one(
            "SELECT * FROM pvcs WHERE ticket_id=? AND name=? AND selected=1", (tid, pvc_name))
        if not row:
            raise NotFoundError(f"工单 {tid} 中不存在选中 PVC {pvc_name!r}")
        if row["copy_state"] != "verified":
            # 独立读回校验未通过前不开放原链接下载
            raise ConflictError(
                f"PVC {pvc_name!r} 尚未完成独立读回校验（当前 {row['copy_state']}），"
                "留存副本与开单 SHA-256 确认一致前不开放下载")
        path = self.retained_root / tid / pvc_name / "data.bin"
        meta = self.retained_root / tid / pvc_name / "meta.json"
        if not path.exists() or not meta.exists():
            raise NotFoundError(f"工单 {tid} 的 PVC {pvc_name!r} 留存副本缺失")
        return path

    def status(self, tid: str) -> dict:
        t = self._ticket(tid)
        pvcs = self.db.query("SELECT * FROM pvcs WHERE ticket_id=? ORDER BY name", (tid,))
        ops = self.db.query("SELECT * FROM ops WHERE ticket_id=? ORDER BY rowid", (tid,))
        current = self._current_bytes(t)
        cap = t["baseline_bytes"] + t["max_selected_bytes"]
        leftover_pvs = []
        for p in pvcs:
            pv = self.sim.get_pv(p["pv_name"])
            if pv:
                leftover_pvs.append({
                    "pv_name": pv["name"], "pvc_name": p["name"], "state": pv["state"],
                    "reclaim_policy": pv["reclaim_policy"], "size_bytes": pv["size_bytes"],
                })
        return {
            "ticket": {**t, "cap_bytes": cap},
            "pvcs": pvcs,
            "ops": ops,
            "io": {
                "limit_bytes": t["io_limit_bytes"],
                "last_advance_read_bytes": t["last_io_read"],
                "last_advance_written_bytes": t["last_io_written"],
                "last_advance_total_bytes": t["last_io_read"] + t["last_io_written"],
                "cumulative_read_bytes": t["total_io_read"],
                "cumulative_written_bytes": t["total_io_written"],
                "cumulative_total_bytes": t["total_io_read"] + t["total_io_written"],
                "chunk_bytes": COPY_CHUNK_BYTES,
                "max_resume_rework_bytes": MAX_RESUME_REWORK_BYTES,
            },
            "storage": {
                "baseline_bytes": t["baseline_bytes"],
                "max_selected_bytes": t["max_selected_bytes"],
                "cap_bytes": cap,
                "current_bytes": current,
                "peak_bytes": t["peak_bytes"],
                "headroom_bytes": cap - current,
            },
            "retained": self.retained_items(tid),
            "leftover_pvs": leftover_pvs,
            "cluster": {
                "namespace_exists": self.sim.get_namespace_by_uid(t["namespace_uid"]) is not None,
                "remaining_pvcs": self.sim.list_pvcs(ns_uid=t["namespace_uid"]),
            },
        }

    # ---------------- 推进 ----------------
    def advance(self, tid: str) -> dict:
        with self._lock:
            t = self._ticket(tid)
            if t["state"] in ("DONE", "FAILED"):
                return self.status(tid)
            self._cur_read = 0
            self._cur_written = 0
            self._last_read = 0
            self._last_written = 0
            self._adv_used = 0
            self._crash_tid = tid
            self._set_blocked(tid, None)
            # 先核对上一回合可能结果未明的删除（仅存在性 GET，不读卷正文，不占 I/O 预算）
            self.reconcile(tid)
            if self.db.one("SELECT 1 AS x FROM ops WHERE ticket_id=? AND state='unknown'", (tid,)):
                self._set_blocked(tid, "存在结果未明的删除，核对后仍未确认（可能再次丢包）；请再次推进")
                self._flush_io_counters(tid)
                return self.status(tid)
            for _ in range(MAX_STEPS_PER_ADVANCE):
                t = self._ticket(tid)
                if t["state"] in ("DONE", "FAILED") or t["blocked_reason"]:
                    break
                if t["state"] == "VALIDATING":
                    progressed = self._validate(t)
                elif t["state"] == "RETAINING":
                    progressed = self._retain_step(t)
                elif t["state"] == "DELETING_NAMESPACE":
                    progressed = self._delete_ns_step(t)
                else:
                    progressed = False
                if not progressed:
                    break
                # 每完成一个步骤立即把 I/O 计量与检查点同节奏落盘（崩溃也不丢计量）
                self._flush_io_counters(tid)
                self._bump_peak(self._ticket(tid))
                # 新单：本次正文 I/O 预算用尽即结束本请求（不是阻塞，下次续办）
                t = self._ticket(tid)
                if t["io_limit_bytes"] is not None and self._adv_used >= t["io_limit_bytes"]:
                    break
            self._flush_io_counters(tid)
            # last_io_* 反映本次请求的实际读写量
            self.db.execute(
                "UPDATE tickets SET last_io_read=?, last_io_written=? WHERE id=?",
                (self._last_read, self._last_written, tid))
            return self.status(tid)

    def _validate(self, t: dict) -> bool:
        tid = t["id"]
        ns = self.sim.get_namespace_by_uid(t["namespace_uid"])
        if not ns:
            self._set_blocked(
                tid,
                f"开单时的命名空间 {t['namespace_name']}(uid={t['namespace_uid']}) 已不存在；"
                "若系同名重建，其 UID 不同，本单不会作用于新命名空间。请人工核查后处理。")
            return False
        missing = [p["name"] for p in self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1", (tid,))
            if self.sim.get_pvc(p["uid"]) is None]
        if missing:
            self._set_blocked(tid, f"选中 PVC 已不存在，无法留存: {missing}")
            return False
        self.db.execute("UPDATE tickets SET state='RETAINING' WHERE id=?", (tid,))
        return True

    def _retain_step(self, t: dict) -> bool:
        tid = t["id"]
        rows = self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1 ORDER BY name", (tid,))
        todo = [r for r in rows
                if not (r["copy_state"] == "verified" and r["delete_state"] == "confirmed")]
        if not todo:
            self.db.execute("UPDATE tickets SET state='DELETING_NAMESPACE' WHERE id=?", (tid,))
            return True
        r = todo[0]
        # 旧单（io_limit_bytes 为空）：沿用整卷复制/校验路径
        if t["io_limit_bytes"] is None:
            if r["copy_state"] in ("pending", "copying"):
                return self._copy_legacy(t, r)
            if r["copy_state"] in ("copied", "verifying"):
                return self._verify_legacy(t, r)
        else:
            if r["copy_state"] in ("pending", "copying"):
                return self._copy_chunk(t, r)
            if r["copy_state"] in ("copied", "verifying"):
                return self._verify_chunk(t, r)
        # 已验证 -> 删除源 PVC（按 PV 回收策略由模拟端处理）
        return self._send_delete(t, "pvc", r["uid"], r["name"])

    def _delete_ns_step(self, t: dict) -> bool:
        tid = t["id"]
        if not self._send_delete(t, "namespace", t["namespace_uid"], t["namespace_name"]):
            return False
        ns = self.sim.get_namespace_by_uid(t["namespace_uid"])
        remaining = self.sim.list_pvcs(ns_uid=t["namespace_uid"])
        if ns is None and not remaining:
            self.db.execute("UPDATE tickets SET state='DONE', done_at=? WHERE id=?",
                            (now_iso(), tid))
            return True
        self._set_blocked(tid, "命名空间或未选卷尚未完全移除，等待核对")
        return False

    # ---------------- 留存：分块复制（新单） ----------------
    def _retained_paths(self, tid: str, name: str) -> tuple[Path, Path]:
        dst_dir = self.retained_root / tid / name
        dst = dst_dir / "data.bin"
        return dst_dir, dst

    def _fail(self, tid: str, reason: str) -> bool:
        self.db.execute(
            "UPDATE tickets SET state='FAILED', blocked_reason=? WHERE id=?", (reason, tid))
        return False

    def _copy_chunk(self, t: dict, r: dict) -> bool:
        tid = t["id"]
        if r["copy_state"] == "pending":
            # 原有存储上限保护不变：拷贝前确认 current + 整卷 <= cap
            current = self._current_bytes(t)
            cap = t["baseline_bytes"] + t["max_selected_bytes"]
            if current + r["size_bytes"] > cap:
                self._set_blocked(
                    tid,
                    f"存储上限保护：当前 {current} + 待拷 {r['size_bytes']} 将超过上限 {cap}"
                    f"（基线 {t['baseline_bytes']} + 最大选中卷 {t['max_selected_bytes']}），暂停拷贝")
                return False
            pvc = self.sim.get_pvc(r["uid"])
            if not pvc:
                self._set_blocked(tid, f"选中 PVC {r['name']}(uid={r['uid']}) 已消失，无法留存")
                return False
            self.db.execute(
                "UPDATE pvcs SET copy_state='copying' WHERE ticket_id=? AND uid=?",
                (tid, r["uid"]))
            r = self.db.one("SELECT * FROM pvcs WHERE ticket_id=? AND uid=?", (tid, r["uid"]))

        budget = self._remaining_budget(t)
        if budget is not None and budget <= 0:
            return False  # 本次预算已尽，下次续办
        pvc = self.sim.get_pvc(r["uid"])
        if not pvc:
            self._set_blocked(tid, f"选中 PVC {r['name']}(uid={r['uid']}) 已消失，无法留存")
            return False
        src = self.sim.pv_data_path(pvc["pv_name"])
        dst_dir, dst = self._retained_paths(tid, r["name"])
        dst_dir.mkdir(parents=True, exist_ok=True)
        off = r["copy_offset"]
        # 续拷：目标长度只可能等于已提交偏移，或多出崩溃前最后一个未提交块 -> 截断重写
        if dst.exists() and dst.stat().st_size > off:
            with open(dst, "r+b") as f:
                f.truncate(off)
        n = min(COPY_CHUNK_BYTES, r["size_bytes"] - off)
        if budget is not None:  # 复制一块同时产生一次读和一次写，各占预算
            n = min(n, budget // 2)
        if n <= 0:
            return False
        with open(src, "rb") as fs, open(dst, "ab") as fd:
            fs.seek(off)
            chunk = fs.read(n)
            if len(chunk) != n:
                return self._fail(tid, f"源卷 {r['name']} 在偏移 {off} 处可读字节不足，"
                                       "与开单快照大小不符，已中止（源 PVC 未删除）")
            self._charge_io(read=n)
            fd.write(chunk)
            fd.flush()
            os.fsync(fd.fileno())
            self._charge_io(written=n)  # 崩溃钩子落在 I/O 之后、检查点之前
        new_off = off + n
        if new_off == r["size_bytes"]:
            write_json(dst_dir / "meta.json", {
                "ticket_id": tid, "pvc_name": r["name"], "pvc_uid": r["uid"],
                "source_namespace": t["namespace_name"],
                "source_namespace_uid": t["namespace_uid"],
                "pv_name": r["pv_name"], "size_bytes": r["size_bytes"],
                "sha256": r["sha256"], "retained_at": now_iso(),
            })
            self.db.execute(
                "UPDATE pvcs SET copy_state='copied', copy_offset=?, verify_offset=0, "
                "verify_hash_state=NULL WHERE ticket_id=? AND uid=?",
                (new_off, tid, r["uid"]))
        else:
            # 每块都提交偏移：崩溃后最坏仅重做这一块（64 KiB <= 256 KiB 合约）
            self.db.execute(
                "UPDATE pvcs SET copy_offset=? WHERE ticket_id=? AND uid=?",
                (new_off, tid, r["uid"]))
        return True

    # ---------------- 留存：分块独立读回校验（新单） ----------------
    def _verify_chunk(self, t: dict, r: dict) -> bool:
        tid = t["id"]
        dst_dir, dst = self._retained_paths(tid, r["name"])
        if not dst.exists():
            return self._fail(tid, f"留存副本缺失: {dst}（源 PVC 未删除）")
        if r["copy_state"] == "copied":
            # 进入校验阶段：初始化可续算哈希并立即落盘
            hasher = ResumableSHA256()
            off = 0
            self.db.execute(
                "UPDATE pvcs SET copy_state='verifying', verify_offset=0, verify_hash_state=? "
                "WHERE ticket_id=? AND uid=?",
                (json.dumps(hasher.export_state()), tid, r["uid"]))
            r = self.db.one("SELECT * FROM pvcs WHERE ticket_id=? AND uid=?", (tid, r["uid"]))
        else:
            if not r["verify_hash_state"]:
                return self._fail(
                    tid, f"留存副本 {r['name']} 处于 verifying 但哈希检查点丢失，"
                         "无法续算（源 PVC 未删除）")
            hasher = ResumableSHA256.from_state(json.loads(r["verify_hash_state"]))
            off = r["verify_offset"]
        budget = self._remaining_budget(t)
        if budget is not None and budget <= 0:
            return False
        n = min(COPY_CHUNK_BYTES, r["size_bytes"] - off)
        if budget is not None:  # 校验只读取留存副本，仅计读
            n = min(n, budget)
        if n <= 0:
            return False
        with open(dst, "rb") as f:
            f.seek(off)
            chunk = f.read(n)
            self._charge_io(read=n)  # 崩溃钩子：I/O 后、检查点前
        if len(chunk) != n:
            return self._fail(tid, f"留存副本 {r['name']} 在偏移 {off} 处可读字节不足，"
                                   "与开单快照大小不符，已中止（源 PVC 未删除）")
        hasher.update(chunk)
        new_off = off + n
        if new_off == r["size_bytes"]:
            actual = hasher.hexdigest()
            if actual != r["sha256"] or dst.stat().st_size != r["size_bytes"]:
                return self._fail(
                    tid,
                    f"留存副本读回校验失败：{r['name']} sha256/大小与开单快照不符，"
                    "已中止（对应源 PVC 未删除）")
            self.db.execute(
                "UPDATE pvcs SET copy_state='verified', verify_offset=?, "
                "verify_hash_state=NULL WHERE ticket_id=? AND uid=?",
                (new_off, tid, r["uid"]))
        else:
            # 每块提交偏移 + 哈希中间状态，重启后零重读续算（崩溃窗口外）
            self.db.execute(
                "UPDATE pvcs SET verify_offset=?, verify_hash_state=? "
                "WHERE ticket_id=? AND uid=?",
                (new_off, json.dumps(hasher.export_state()), tid, r["uid"]))
        return True

    # ---------------- 旧单兼容：整卷路径（无 I/O 上限） ----------------
    def _copy_legacy(self, t: dict, r: dict) -> bool:
        tid = t["id"]
        pvc = self.sim.get_pvc(r["uid"])
        if not pvc:
            self._set_blocked(tid, f"选中 PVC {r['name']}(uid={r['uid']}) 已消失，无法留存")
            return False
        src = self.sim.pv_data_path(pvc["pv_name"])
        dst_dir, dst = self._retained_paths(tid, r["name"])
        dst_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        with open(dst, "rb") as f:
            os.fsync(f.fileno())
        self._charge_io(read=r["size_bytes"], written=r["size_bytes"])
        write_json(dst_dir / "meta.json", {
            "ticket_id": tid, "pvc_name": r["name"], "pvc_uid": r["uid"],
            "source_namespace": t["namespace_name"],
            "source_namespace_uid": t["namespace_uid"],
            "pv_name": r["pv_name"], "size_bytes": r["size_bytes"],
            "sha256": r["sha256"], "retained_at": now_iso(),
        })
        self.db.execute("UPDATE pvcs SET copy_state='copied' WHERE ticket_id=? AND uid=?",
                        (tid, r["uid"]))
        return True

    def _verify_legacy(self, t: dict, r: dict) -> bool:
        """独立读回校验：只读留存目录中的副本，与开单时快照的 sha256 比对。"""
        tid = t["id"]
        _, dst = self._retained_paths(tid, r["name"])
        if not dst.exists():
            return self._fail(tid, f"留存副本缺失: {dst}")
        actual = sha256_file(dst)
        self._charge_io(read=dst.stat().st_size)
        if actual != r["sha256"] or dst.stat().st_size != r["size_bytes"]:
            return self._fail(
                tid,
                f"留存副本读回校验失败：{r['name']} sha256/大小与开单快照不符，"
                "已中止（对应源 PVC 未删除）")
        self.db.execute("UPDATE pvcs SET copy_state='verified' WHERE ticket_id=? AND uid=?",
                        (tid, r["uid"]))
        return True

    # ---------------- 删除（带幂等日志） ----------------
    def _send_delete(self, t: dict, kind: str, target_uid: str, target_name: str) -> bool:
        tid = t["id"]
        key = f"{tid}:{kind}:{target_uid}"
        op = self.db.one("SELECT * FROM ops WHERE idempotency_key=?", (key,))
        if op and op["state"] == "confirmed":
            return True  # 已确认的删除不再发送
        if op is None:
            self.db.execute(
                "INSERT INTO ops(idempotency_key,ticket_id,kind,target_uid,target_name,"
                "state,attempts,last_error,updated_at) VALUES(?,?,?,?,?,?,0,NULL,?)",
                (key, tid, kind, target_uid, target_name, "pending", now_iso()))
        try:
            if kind == "pvc":
                self.sim.delete_pvc(target_uid, key)
            else:
                self.sim.delete_namespace(target_uid, key)
        except DroppedResponseError as e:
            self.db.execute(
                "UPDATE ops SET state='unknown', attempts=attempts+1, last_error=?, "
                "updated_at=? WHERE idempotency_key=?", (str(e), now_iso(), key))
            if kind == "pvc":
                self.db.execute(
                    "UPDATE pvcs SET delete_state='unknown' WHERE ticket_id=? AND uid=?",
                    (tid, target_uid))
            self._set_blocked(
                tid,
                f"删除回包丢失：{kind} {target_name}(uid={target_uid}) 服务端可能已生效，"
                "结果未明；将在核对后续办（已确认的删除不会重发）")
            return False
        except NotFoundError as e:
            # 目标已不存在 -> 删除实际已生效，幂等确认为止
            self.db.execute(
                "UPDATE ops SET state='confirmed', attempts=attempts+1, last_error=?, "
                "updated_at=? WHERE idempotency_key=?",
                (f"target already gone: {e}", now_iso(), key))
            if kind == "pvc":
                self.db.execute(
                    "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                    (tid, target_uid))
            return True
        self.db.execute(
            "UPDATE ops SET state='confirmed', attempts=attempts+1, updated_at=? "
            "WHERE idempotency_key=?", (now_iso(), key))
        if kind == "pvc":
            self.db.execute(
                "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                (tid, target_uid))
        return True

    # ---------------- 核对（启动时与推进前） ----------------
    def reconcile(self, tid: str | None = None) -> int:
        """核对结果未明(unknown)的删除：目标已消失则标记确认（不重发），仍在则重发。

        只做对象存在性 GET，不读取卷正文，因此不计入单次 I/O 预算。
        """
        resolved = 0
        with self._lock:
            if tid:
                rows = self.db.query(
                    "SELECT * FROM ops WHERE state='unknown' AND ticket_id=?", (tid,))
            else:
                rows = self.db.query("SELECT * FROM ops WHERE state='unknown'")
            for op in rows:
                if op["kind"] == "pvc":
                    exists = self.sim.get_pvc(op["target_uid"]) is not None
                else:
                    exists = self.sim.get_namespace_by_uid(op["target_uid"]) is not None
                if not exists:
                    self.db.execute(
                        "UPDATE ops SET state='confirmed', last_error=?, updated_at=? "
                        "WHERE idempotency_key=?",
                        ("reconciled: target already gone, delete took effect; NOT resent",
                         now_iso(), op["idempotency_key"]))
                    if op["kind"] == "pvc":
                        self.db.execute(
                            "UPDATE pvcs SET delete_state='confirmed' "
                            "WHERE ticket_id=? AND uid=?", (op["ticket_id"], op["target_uid"]))
                    resolved += 1
                else:
                    t = self._ticket(op["ticket_id"])
                    if self._send_delete(t, op["kind"], op["target_uid"], op["target_name"]):
                        resolved += 1
        return resolved

    # ---------------- 辅助 ----------------
    def _set_blocked(self, tid: str, reason: str | None) -> None:
        self.db.execute("UPDATE tickets SET blocked_reason=? WHERE id=?", (reason, tid))

    def _bump_peak(self, t: dict) -> None:
        current = self._current_bytes(t)
        self.db.execute("UPDATE tickets SET peak_bytes=MAX(peak_bytes, ?) WHERE id=?",
                        (current, t["id"]))

    def reset(self) -> None:
        with self._lock:
            self.sim.reset()
            shutil.rmtree(self.retained_root, ignore_errors=True)
            self.retained_root.mkdir(parents=True, exist_ok=True)
            for table in ("ops", "pvcs", "tickets"):
                self.db.execute(f"DELETE FROM {table}")
            self._cur_read = 0
            self._cur_written = 0
            self._last_read = 0
            self._last_written = 0
            self._adv_used = 0
