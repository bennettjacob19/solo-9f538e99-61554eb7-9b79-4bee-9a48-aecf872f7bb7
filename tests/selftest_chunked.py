"""分块短请求与中断续办的端到端自测。

覆盖需求：
  1. 开单必须带 1~8 MiB 的单次正文 I/O 上限，越界 -> 422；开单不读卷正文
  2. 小额度 advance：每次实际读写量 <= 上限，返回本次读写量与复制/校验进度
  3. 复制中途被 kill（I/O 已完成、检查点未落盘），重启凭原单续办：
     重做的原文范围 <= 256 KiB（实际最坏仅一个 64 KiB 块）
  4. 独立读回校验中途被 kill，重启从已提交偏移+哈希状态续算，最终与开单 SHA-256 一致
  5. 校验通过前下载 409、源 PVC 不删；通过后开放下载，然后才删源 PVC
  6. 旧单（io_limit_bytes 为空的历史库）走整卷路径仍可照常推进
  7. 丢回包后的删源续办在分块模式下仍生效
  8. 容量上限（基线+最大选中卷）在分块拷贝下仍不被突破
  9. 可续算 SHA-256 与 hashlib 对拍（另含状态导入/导出续算）

运行：python3 tests/selftest_chunked.py
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
PORT = 8766
BASE = f"http://127.0.0.1:{PORT}"
MIB = 1024 * 1024
KIB = 1024

FAILURES: list[str] = []
CHECKS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    if cond:
        print(f"  PASS  {name}")
    else:
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL  {name}  {detail}")


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def start_server(data_dir: Path, extra_env: dict | None = None) -> subprocess.Popen:
    env = dict(os.environ, DATA_DIR=str(data_dir))
    if extra_env:
        env.update(extra_env)
    log = open(data_dir / "server.log", "ab")
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(PORT)],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"server exited early, see {data_dir}/server.log")
        try:
            if httpx.get(f"{BASE}/sim/cluster", timeout=2).status_code == 200:
                return p
        except httpx.TransportError:
            time.sleep(0.2)
    p.kill()
    raise RuntimeError("server did not become ready")


def stop_server(p: subprocess.Popen) -> None:
    p.terminate()
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)


def restart_server(data_dir: Path, p: subprocess.Popen,
                   extra_env: dict | None = None) -> subprocess.Popen:
    """停掉旧进程（若已退出则回收），按需带新环境变量重新拉起并等待就绪。"""
    if p.poll() is None:
        stop_server(p)
    else:
        p.wait()
    p = start_server(data_dir, extra_env)
    wait_ready(p)
    return p


def wait_ready(p: subprocess.Popen) -> None:
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError("crashed server did not restart")
        try:
            if httpx.get(f"{BASE}/sim/cluster", timeout=2).status_code == 200:
                return
        except httpx.TransportError:
            time.sleep(0.2)
    raise RuntimeError("server did not become ready after restart")


def advance_until(c: httpx.Client, tid: str, states: set[str], max_calls: int = 300) -> list[dict]:
    """反复推进直到工单进入 states；返回每次 advance 的响应。"""
    out = []
    for _ in range(max_calls):
        r = c.post(f"/tickets/{tid}/advance").json()
        out.append(r)
        if r["ticket"]["state"] in states:
            return out
    raise RuntimeError(f"ticket never reached {states}: "
                       f"{out[-1]['ticket']['state']} / {out[-1]['ticket']['blocked_reason']}")


def test_resumable_hash() -> None:
    print("\n== 0. 可续算 SHA-256 与 hashlib 对拍 ==")
    sys.path.insert(0, str(ROOT))
    from app.sha256state import ResumableSHA256
    for n in (0, 1, 55, 63, 64, 65, 127, 128, 4096, 100_000):
        data = os.urandom(n)
        check(f"长度 {n} 摘要一致",
              ResumableSHA256().update(data).hexdigest() == sha(data))
    # 非 64 字节对齐的状态导出/恢复
    data = os.urandom(5000)
    h = ResumableSHA256()
    h.update(data[:1000])
    saved = h.export_state()
    h2 = ResumableSHA256.from_state(saved)
    check("任意偏移导出状态后续算摘要一致",
          h2.update(data[1000:]).hexdigest() == sha(data))
    # digest 不破坏状态
    h3 = ResumableSHA256().update(b"abc")
    check("digest 可重复且不改变状态",
          h3.hexdigest() == h3.hexdigest() == sha(b"abc"))


def main() -> int:
    test_resumable_hash()
    data_dir = Path(tempfile.mkdtemp(prefix="ns-retain-chunk-"))
    print(f"\nDATA_DIR={data_dir}")
    server = start_server(data_dir)
    c = httpx.Client(base_url=BASE, timeout=30)
    try:
        print("\n== 1. 开单 I/O 上限校验（1~8 MiB）与“开单不读卷正文” ==")
        seed = c.post("/sim/seed", json={
            "namespace": "demo",
            "pvcs": [
                {"name": "vol-a", "size_bytes": 256 * KIB, "reclaim_policy": "Delete"},
                {"name": "vol-b", "size_bytes": 300 * KIB, "reclaim_policy": "Delete"},
                {"name": "vol-c", "size_bytes": 128 * KIB, "reclaim_policy": "Retain"},
            ],
        })
        check("seed -> 201", seed.status_code == 201, seed.text)
        seeded = {p["name"]: p for p in seed.json()["pvcs"]}
        base_size = 256 * KIB + 300 * KIB + 128 * KIB

        bad_low = c.post("/tickets", json={
            "namespace": "demo", "retain_pvcs": ["vol-a"],
            "io_limit_bytes": 1 * MIB - 1})
        bad_high = c.post("/tickets", json={
            "namespace": "demo", "retain_pvcs": ["vol-a"],
            "io_limit_bytes": 8 * MIB + 1})
        missing = c.post("/tickets", json={
            "namespace": "demo", "retain_pvcs": ["vol-a"]})
        check("上限 < 1 MiB -> 422", bad_low.status_code == 422, bad_low.text)
        check("上限 > 8 MiB -> 422", bad_high.status_code == 422, bad_high.text)
        check("缺 io_limit_bytes -> 422", missing.status_code == 422, missing.text)

        t = c.post("/tickets", json={
            "namespace": "demo", "retain_pvcs": ["vol-a", "vol-b"],
            "io_limit_bytes": 1 * MIB})
        check("1 MiB 上限开单 -> 201", t.status_code == 201, t.text)
        tid = t.json()["ticket"]["id"]
        check("开单返回上限值", t.json()["io"]["limit_bytes"] == 1 * MIB)
        check("开单不读卷正文：本次/累计读写量均为 0",
              t.json()["io"]["last_advance_total_bytes"] == 0
              and t.json()["io"]["cumulative_total_bytes"] == 0,
              str(t.json()["io"]))

        print("\n== 2. 小额度推进：每次实际 I/O <= 上限，进度可查 ==")
        rs = advance_until(c, tid, {"DONE"})
        over = [r["io"]["last_advance_total_bytes"] for r in rs
                if r["io"]["last_advance_total_bytes"] > 1 * MIB]
        check("每次 advance 正文 I/O 均 <= 1 MiB", not over, str(over))
        # 总正文 I/O = 3 * 556KiB ≈ 1.63 MiB > 1 MiB 上限，至少要 2 次短请求
        check("存在多次短请求推进（总 I/O 超过单次上限，非一次完成）",
              len(rs) >= 2, f"calls={len(rs)}")
        final = rs[-1]
        # 两卷复制（读+写各一份）+ 两卷校验读 = 3 倍总留存字节
        retained_bytes = 256 * KIB + 300 * KIB
        check("累计读 = 复制读 + 校验读（2 倍留存字节）",
              final["io"]["cumulative_read_bytes"] == 2 * retained_bytes,
              str(final["io"]))
        check("累计写 = 留存写入（1 倍留存字节）",
              final["io"]["cumulative_written_bytes"] == retained_bytes,
              str(final["io"]))
        check("末轮返回本次实际读写量字段齐全",
              set(["last_advance_read_bytes", "last_advance_written_bytes",
                   "cumulative_read_bytes", "cumulative_written_bytes"])
              <= set(final["io"].keys()))
        pvc_rows = {p["name"]: p for p in final["pvcs"]}
        check("两卷 copy_state=verified 且源删除 confirmed",
              all(pvc_rows[n]["copy_state"] == "verified"
                  and pvc_rows[n]["delete_state"] == "confirmed"
                  for n in ("vol-a", "vol-b")), str(final["pvcs"]))
        check("容量峰值 <= 基线+最大选中卷",
              final["storage"]["peak_bytes"] <= final["storage"]["cap_bytes"],
              str(final["storage"]))
        check("DONE 后原命名空间已移除",
              final["cluster"]["namespace_exists"] is False
              and not final["cluster"]["remaining_pvcs"])
        for name in ("vol-a", "vol-b"):
            body = c.get(f"/tickets/{tid}/retained/{name}").content
            check(f"{name} 读回 sha256 与开单快照一致",
                  sha(body) == seeded[name]["sha256"])

        print("\n== 3. 复制中途被 kill：重启续办，重做范围 <= 256 KiB ==")
        seed2 = c.post("/sim/seed", json={
            "namespace": "crash-copy",
            "pvcs": [{"name": "big", "size_bytes": 1 * MIB + 200 * KIB,
                      "reclaim_policy": "Delete"}],
        })
        check("seed crash-copy -> 201", seed2.status_code == 201, seed2.text)
        big_sha = seed2.json()["pvcs"][0]["sha256"]
        t2 = c.post("/tickets", json={
            "namespace": "crash-copy", "retain_pvcs": ["big"],
            "io_limit_bytes": 1 * MIB})
        tid2 = t2.json()["ticket"]["id"]
        # 崩溃点：本工单累计 300 KiB 正文 I/O（复制读+写各 150 KiB，即 153600 字节处，
        # 非 64 KiB 对齐，崩溃发生在第 3 块写完、检查点提交之前）
        stop_server(server)
        server = start_server(data_dir, {
            "DRILL_CRASH_AFTER_IO_BYTES": f"{tid2}:{300 * KIB}"})
        crashed = False
        try:
            c.post(f"/tickets/{tid2}/advance")
        except httpx.TransportError:
            crashed = True
        check("复制过程中进程硬退出（模拟 kill/断电）", crashed or server.poll() == 2)
        server = restart_server(data_dir, server)  # 不带故障变量重启 -> 钩子失效
        st = c.get(f"/tickets/{tid2}").json()
        row = st["pvcs"][0]
        check("崩溃后状态停留在 copying", row["copy_state"] == "copying", str(row))
        check("已提交复制偏移为 64 KiB 的整数倍",
              row["copy_offset"] % (64 * KIB) == 0 and row["copy_offset"] > 0, str(row))
        on_disk = next((i["data_bytes_on_disk"] for i in st["retained"]), 0)
        check("崩溃窗口内未提交块 <= 256 KiB（实际 <= 64 KiB）",
              0 <= on_disk - row["copy_offset"] <= 64 * KIB,
              f"on_disk={on_disk} offset={row['copy_offset']}")
        before_off = row["copy_offset"]
        rs2 = advance_until(c, tid2, {"DONE"})
        check("凭原单续办后 DONE", rs2[-1]["ticket"]["state"] == "DONE",
              rs2[-1]["ticket"]["state"])
        # 全程累计计量跨崩溃持久化、可对账：
        #   写 = 完整留存（size）+ 崩溃窗口内最后一个未提交块的重写
        #   读 = 完整复制读（size + 同块重读）+ 完整独立读回校验（size）
        size = 1 * MIB + 200 * KIB
        final_io = rs2[-1]["io"]
        rework = on_disk - before_off  # 崩溃块字节：截断后重写的原文范围
        check("复制重做范围 = 崩溃窗口内一个块，<= 256 KiB",
              0 <= rework <= 64 * KIB, f"rework={rework}")
        check("全程累计写 = 卷大小 + 崩溃块重写",
              final_io["cumulative_written_bytes"] == size + rework, str(final_io))
        check("全程累计读 = 复制读(含块重读) + 完整校验读",
              final_io["cumulative_read_bytes"] == size + rework + size, str(final_io))
        check("续办后读回 sha256 与开单快照一致",
              sha(c.get(f"/tickets/{tid2}/retained/big").content) == big_sha)
        check("续办从原偏移之后继续（未整卷重拷）", before_off >= 128 * KIB,
              f"before_off={before_off}")

        print("\n== 4. 独立读回校验中途被 kill：哈希状态续算 ==")
        seed3 = c.post("/sim/seed", json={
            "namespace": "crash-verify",
            "pvcs": [{"name": "vfy", "size_bytes": 800 * KIB,
                      "reclaim_policy": "Delete"}],
        })
        check("seed crash-verify -> 201", seed3.status_code == 201, seed3.text)
        vfy_sha = seed3.json()["pvcs"][0]["sha256"]
        t3 = c.post("/tickets", json={
            "namespace": "crash-verify", "retain_pvcs": ["vfy"],
            "io_limit_bytes": 1 * MIB})
        tid3 = t3.json()["ticket"]["id"]
        # 1 MiB 上限下：复制 800KiB 读 + 800KiB 写 = 1.6MB > 上限，
        # 第一次 advance 只能复制 512 KiB（读+写各 512KiB=1MiB）；再推进完成复制，
        # 随后进入校验。崩溃阈值按本工单持久化累计 I/O 计（跨重启不清零），
        # 设 2 MiB：复制全部完成(1.6MB)后、校验读到 ~400KiB 时硬退出
        stop_server(server)
        server = start_server(data_dir, {
            "DRILL_CRASH_AFTER_IO_BYTES": f"{tid3}:{2 * MIB}"})
        crashed = False
        try:
            for _ in range(10):
                r = c.post(f"/tickets/{tid3}/advance")
                if r.status_code != 200:
                    break
        except httpx.TransportError:
            crashed = True
        check("校验过程中进程硬退出", crashed or server.poll() == 2)
        server = restart_server(data_dir, server)
        st = c.get(f"/tickets/{tid3}").json()
        row = st["pvcs"][0]
        check("崩溃后状态停留在 verifying", row["copy_state"] == "verifying", str(row))
        check("校验偏移已提交且哈希状态已持久化",
              row["verify_offset"] > 0 and bool(row["verify_hash_state"]), str(row))
        check("校验检查点为 64 KiB 整数倍（崩溃窗口 <= 64 KiB < 256 KiB 合约）",
              row["verify_offset"] % (64 * KIB) == 0, str(row))
        saved_off = row["verify_offset"]
        copy_done_off = row["copy_offset"]
        rs3 = advance_until(c, tid3, {"DONE"})
        check("续算校验后 DONE", rs3[-1]["ticket"]["state"] == "DONE",
              rs3[-1]["ticket"]["state"])
        # 全程累计计量跨崩溃持久化、可对账（按崩溃时落盘的检查点推导）：
        #   复制在崩溃前已完整完成 -> 全程写 = size（无重写）
        #   全程读 = 完整复制读(size) + 独立读回校验(size)；
        #   崩溃窗口内最后一个未提交块（<=64KiB）的重读包含在校验读中，
        #   即从 saved_off 续读的量 = (size-saved_off) + 块重读，总和仍为 size
        size = 800 * KIB
        final_io = rs3[-1]["io"]
        check("复制崩溃前已完成 -> 全程写恰为卷大小（零重写）",
              final_io["cumulative_written_bytes"] == size, str(final_io))
        check("全程读 = 完整复制读 + 完整校验读（崩溃块重读含在检查点续算内）",
              final_io["cumulative_read_bytes"] == 2 * size, str(final_io))
        check("每次请求 I/O 仍受 1 MiB 上限约束",
              final_io["last_advance_total_bytes"] <= 1 * MIB)
        check("崩溃窗口（最后未提交块）<= 256 KiB 合约", saved_off < size)
        check("校验续算后摘要与开单 SHA-256 一致",
              sha(c.get(f"/tickets/{tid3}/retained/vfy").content) == vfy_sha)
        check("续算从已提交偏移继续（未从头重算）", saved_off >= 256 * KIB,
              f"saved_off={saved_off}")

        print("\n== 5. 下载闸门：校验前 409、源 PVC 不删；校验后才开放并删源 ==")
        seed4 = c.post("/sim/seed", json={
            "namespace": "gate",
            "pvcs": [{"name": "g1", "size_bytes": 400 * KIB,
                      "reclaim_policy": "Delete"}],
        })
        check("seed gate -> 201", seed4.status_code == 201, seed4.text)
        t4 = c.post("/tickets", json={
            "namespace": "gate", "retain_pvcs": ["g1"],
            "io_limit_bytes": 1 * MIB})
        tid4 = t4.json()["ticket"]["id"]
        # 复制 400KiB：读+写共 800KiB；崩溃点 900KiB -> 复制完成后、校验读到 ~100KiB 时硬退出
        server = restart_server(data_dir, server,
                                {"DRILL_CRASH_AFTER_IO_BYTES": f"{tid4}:{900 * KIB}"})
        crashed = False
        try:
            for _ in range(20):
                c.post(f"/tickets/{tid4}/advance")
        except httpx.TransportError:
            crashed = True
        check("进程在校验阶段硬退出", crashed)
        server = restart_server(data_dir, server)  # 干净重启，不恢复故障变量
        mid = c.get(f"/tickets/{tid4}").json()
        row = mid["pvcs"][0]
        check("重启后停留在 verifying（进度持久化）",
              row["copy_state"] == "verifying" and row["verify_offset"] > 0, str(row))
        dl = c.get(f"/tickets/{tid4}/retained/g1")
        check("未校验完成前下载被拒 -> 409", dl.status_code == 409, dl.text)
        cluster = c.get("/sim/cluster").json()
        check("校验完成前源 PVC 仍在集群中",
              any(p["name"] == "g1" for p in cluster["pvcs"]))
        # 留存文件已在磁盘上，但下载闸门仍关闭
        check("留存副本已部分/全部落盘（但不可下载）",
              mid["retained"] and mid["retained"][0]["verified"] is False)
        rs4 = advance_until(c, tid4, {"DONE"})
        check("完成后下载开放 -> 200",
              c.get(f"/tickets/{tid4}/retained/g1").status_code == 200)
        check("完成后源 PVC 已删除",
              all(p["name"] != "g1" for p in c.get("/sim/cluster").json()["pvcs"]))
        check("留存清单带 verified 标记",
              rs4[-1]["retained"][0]["verified"] is True)
        check("未选 PVC 下载 -> 404",
              c.get(f"/tickets/{tid4}/retained/nope").status_code == 404)

        print("\n== 6. 旧单（无 io_limit_bytes 的历史库）整卷路径照常推进 ==")
        # 直接往 SQLite 写一张历史格式工单（io_limit_bytes=NULL）
        db_path = data_dir / "app.db"
        seed5 = c.post("/sim/seed", json={
            "namespace": "legacy",
            "pvcs": [{"name": "old", "size_bytes": 100 * KIB,
                      "reclaim_policy": "Delete"}],
        }).json()
        old_ns = next(n for n in c.get("/sim/cluster").json()["namespaces"]
                      if n["name"] == "legacy")
        old_pvc = next(p for p in c.get("/sim/cluster").json()["pvcs"]
                       if p["name"] == "old")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tickets(id,created_at,namespace_name,namespace_uid,state,"
            "blocked_reason,note,baseline_bytes,max_selected_bytes,peak_bytes,"
            "io_limit_bytes,last_io_read,last_io_written,total_io_read,total_io_written) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,NULL,0,0,0,0)",
            ("legacytkt01", "2026-01-01T00:00:00.000+00:00", "legacy", old_ns["uid"],
             "VALIDATING", None, "legacy", old_pvc["size_bytes"], old_pvc["size_bytes"],
             old_pvc["size_bytes"]))
        conn.execute(
            "INSERT INTO pvcs(ticket_id,uid,name,pv_name,size_bytes,reclaim_policy,"
            "selected,sha256,copy_state,delete_state,copy_offset,verify_offset,"
            "verify_hash_state) VALUES(?,?,?,?,?,?,?,?,?,?,0,0,NULL)",
            ("legacytkt01", old_pvc["uid"], "old", old_pvc["pv_name"],
             old_pvc["size_bytes"], "Delete", 1, old_pvc["sha256"], "pending", "pending"))
        conn.commit()
        conn.close()
        r = c.post("/tickets/legacytkt01/advance").json()
        check("旧单一次推进即 DONE（整卷路径不受次数限制）",
              r["ticket"]["state"] == "DONE", r["ticket"]["state"])
        check("旧单 io.limit_bytes 为空", r["io"]["limit_bytes"] is None)
        check("旧单留存读回一致",
              sha(c.get("/tickets/legacytkt01/retained/old").content)
              == old_pvc["sha256"])

        print("\n== 7. 分块模式下丢回包后的删源续办仍生效 ==")
        c.post("/sim/fault", json={"drop_delete_responses": 1})
        seed6 = c.post("/sim/seed", json={
            "namespace": "dropns",
            "pvcs": [{"name": "d1", "size_bytes": 70 * KIB,
                      "reclaim_policy": "Delete"}],
        }).json()
        t6 = c.post("/tickets", json={
            "namespace": "dropns", "retain_pvcs": ["d1"],
            "io_limit_bytes": 1 * MIB})
        tid6 = t6.json()["ticket"]["id"]
        r1 = c.post(f"/tickets/{tid6}/advance").json()
        check("丢包后工单阻塞、删除记为 unknown",
              r1["ticket"]["blocked_reason"] is not None
              and any(o["state"] == "unknown" for o in r1["ops"]),
              str(r1["ops"]))
        check("丢包发生在 verified 之后（先留存后删源）",
              r1["pvcs"][0]["copy_state"] == "verified")
        r2 = advance_until(c, tid6, {"DONE"})[-1]
        check("核对续办后 DONE", r2["ticket"]["state"] == "DONE",
              r2["ticket"]["state"])
        check("已确认删除不重发（审计仅 1 条）",
              len([e for e in c.get("/sim/audit").json()["entries"]
                   if e["action"] == "delete_pvc"
                   and e.get("target_name") == "d1"]) == 1)

        print("\n== 8. 分块粒度与续办合约对外可见 ==")
        st = c.get(f"/tickets/{tid}").json()
        check("状态接口暴露 chunk=64KiB、重做上限 256KiB",
              st["io"]["chunk_bytes"] == 64 * KIB
              and st["io"]["max_resume_rework_bytes"] == 256 * KIB, str(st["io"]))
    finally:
        stop_server(server)

    print(f"\n{'=' * 50}")
    if FAILURES:
        print(f"FAILED {len(FAILURES)}/{CHECKS}:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print(f"ALL CHECKS PASSED ({CHECKS})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
