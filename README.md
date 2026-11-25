# K8s 测试命名空间清退留存演练（ns-retain-drill）

值班员清退测试命名空间时，**指定 PVC 的数据要留下**，其余卷随环境回收。本服务用
FastAPI + 文件模拟集群 + SQLite 进度，完整演练这一流程：开单 → 逐卷分块留存并独立
读回校验 → 按 PV 回收策略删除 → 删除命名空间 → 凭单号读回数据。**每次推进是一个
短请求**：开单时填写 1～8 MiB 的单次正文 I/O 上限，源卷读取、留存写入、独立读回
校验、恢复核对的字节全部计入，预算用尽本次即返回，凭原单反复续办；复制或校验中途
进程退出，重启后凭检查点续办，每个阶段最多重做 256 KiB 原文（实际最坏仅一个
64 KiB 块）。支持删除丢包注入、重启核对续办、按开单 UID 清理（同名重建不受影响）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

数据（模拟集群、留存副本、SQLite）持久化在命名卷 `drill-data` 中，容器重启不丢进度。

### 本地运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATA_DIR=./data .venv/bin/uvicorn app.main:app --port 8000
```

### 自测（44 + 70 项断言，含分块额度、崩溃续办、丢包后续办与重启核对）

```bash
.venv/bin/pip install httpx
.venv/bin/python tests/selftest.py           # 原流程回归（44 项）
.venv/bin/python tests/selftest_chunked.py   # 分块短请求/中断续办专项（70 项）
```

## 演练流程（全部可在 /docs 操作）

1. `POST /sim/seed` 造一个命名空间和若干已停写绑定卷（文件保存，可指定 PV 回收策略）。
2. `POST /tickets` 开单：
   `{"namespace": "demo", "retain_pvcs": ["data-a"], "io_limit_bytes": 1048576}`
   （至少一个 PVC；`io_limit_bytes` 取值 1 MiB～8 MiB）。开单只快照命名空间 UID、
   全部 PVC 的 UID/大小/sha256 与基线字节数，**不读取任何卷正文**。
3. 可选：`POST /sim/fault` `{"drop_delete_responses": 1}` 注入删除丢包
   （服务端生效但回包丢失）。
4. 反复 `POST /tickets/{id}/advance` 短请求推进；每次响应的 `io` 段返回
   **本次实际读/写量**与累计读/写量，`pvcs[].copy_state/copy_offset/verify_offset`
   返回复制与独立读回校验的确认进度。单次正文 I/O 不会超过开单上限。
5. 复制或校验中途进程被 kill / 断电：重启后凭原单续办——复制从已提交偏移续拷
   （超长部分截断重写），校验从已提交偏移与可续算 SHA-256 状态续算；最坏只重做
   最后一个未提交的 64 KiB 块（合约上限 256 KiB）。
6. 丢包后工单阻塞并注明“结果未明”；再次 advance 或**重启服务**即自动核对：
   目标已消失则标记确认（**不重发**），仍在才重发。
7. 留存副本**独立读回校验与开单 SHA-256 一致（verified）后**，才开放
   `GET /tickets/{id}/retained/{pvc}` 原链接下载（此前返回 409），并才允许删除源
   PVC；`DONE` 后凭单号读回数据。

## 分块短请求与续办机制

| 机制 | 实现 |
| --- | --- |
| 单次正文 I/O 上限 1～8 MiB | `TicketRequest.io_limit_bytes`（Pydantic 边界校验，越界 422）；每次 advance 按 `_adv_used` 扣预算，读与写各计一次 |
| 计入范围 | 源卷读（复制）、留存写（复制）、独立读回（校验，仅读留存目录）；恢复核对只做对象存在性 GET，不读卷正文故不占预算 |
| 本次/累计实际读写量 | `tickets.last_io_read/written`（本次请求）、`total_io_read/written`（累计，每步骤即时落盘，崩溃也不丢计量）；`GET /tickets/{id}` 的 `io` 段返回 |
| 开单不读卷正文 | 开单仅读 PVC 元数据，sha256 在 `/sim/seed` 造数时已快照；自测断言开单后 I/O 计数为 0 |
| 64 KiB 分块 + 每块检查点 | `COPY_CHUNK_BYTES=64KiB`；每块完成即在 SQLite 提交 `copy_offset` / `verify_offset`（+ 哈希状态） |
| 崩溃后续办，最多重做 256 KiB | 复制：目标超长截断到已提交偏移后续拷；校验：可续算 SHA-256（`app/sha256state.py`，FIPS 180-4，状态可序列化）从检查点续算。最坏重做一个 64 KiB 块 |
| 校验通过前不开放下载、不删源 | `retained_file` 在 `copy_state != verified` 时抛 409；删源 PVC 的状态机步骤排在 verified 之后 |
| 旧单兼容 | `io_limit_bytes` 为空的历史工单走整卷复制/校验路径，行为与 1.0 一致；旧库启动时自动 `ALTER TABLE` 迁移新列 |

## 需求映射

| 要求 | 实现 |
| --- | --- |
| /docs 指定命名空间 + ≥1 个保留 PVC + I/O 上限 | `POST /tickets`，`retain_pvcs` `min_length=1`，`io_limit_bytes` 1～8 MiB |
| 推进清理、查看进度与阻塞原因、实际读写量 | `POST /tickets/{id}/advance`；`GET /tickets/{id}` 返回阶段、`blocked_reason`、`io`、逐步 ops |
| 文件保存已停写绑定卷 | `data/cluster/pvs/<pv>/data.bin` + `meta.json`（`stopped_writing: true`） |
| 按 PV 回收策略处理 PVC 删除 | Delete 删数据 / Recycle 清空转 Available / Retain 保留转 Released（`simcluster._remove_pvc_locked`） |
| 模拟删除生效后丢回包 | 故障注入计数器：删除落盘后抛 `DroppedResponseError`，操作记为 `unknown` |
| 独立读回确认后才删源 PVC、才开放下载 | 留存副本落在集群目录之外，sha256 与**开单快照**比对通过（`verified`）后才发删除；下载接口此前 409 |
| 合计字节 ≤ 开单基线 + 最大选中卷 | 逐卷“分块拷贝→校验→删源”，拷贝前检查 `current+size ≤ cap`，`peak_bytes` 全程记录 |
| 原命名空间与未选卷移除后才完成 | `DONE` 门槛：命名空间不存在且该 UID 下无任何 PVC；Retain 残留的 Released PV 在 `leftover_pvs` 中明示 |
| 凭单号读回所选数据 | `GET /tickets/{id}/retained[/{pvc}]`，verified 后与 DONE 后均可下载 |
| 进度入 SQLite，重启核对未明删除 | `tickets/pvcs/ops` 三表；启动与每次推进前 `reconcile()`：GET 核对，消失即确认、仍在才重发 |
| 已确认的删除不再发送 | ops 幂等键 `ticket:kind:uid`，`confirmed` 直接短路；审计日志可证每目标仅一条删除 |
| 按开单 UID 清理，同名重建免疫 | 所有删除/查询按开单快照 UID 寻址；重建命名空间 UID 不同，旧单触碰不到 |

## 目录结构

```
app/
  main.py          FastAPI 路由（演练面 + 模拟端管理面），启动时核对未明删除
  engine.py        清理状态机：VALIDATING → RETAINING → DELETING_NAMESPACE → DONE/FAILED
                   分块复制/校验、I/O 预算与计量、检查点续办、下载闸门、删除幂等
  sha256state.py   可导出/导入中间状态的纯 Python SHA-256（FIPS 180-4），与 hashlib 对拍
  simcluster.py    文件模拟集群：命名空间/PVC/PV、回收策略、丢包注入、审计日志
  db.py            SQLite（WAL）+ 旧版本库在线迁移
  models.py        请求模型（含 io_limit_bytes 边界）
tests/selftest.py          端到端回归（起停真实服务进程，含重启核对，44 项）
tests/selftest_chunked.py  分块额度/崩溃续办/下载闸门/旧单兼容专项（70 项）
Dockerfile / docker-compose.yml
```

## 验证

- `tests/selftest.py` 44 项：丢包阻塞、**重启核对不重发**、容量峰值恰等于上限边界、
  凭单号读回一致、同名重建免疫、两次丢包续办。
- `tests/selftest_chunked.py` 70 项，要点：
  - 可续算 SHA-256 与 `hashlib` 在 0～10 万字节、随机分块、任意偏移状态导出/恢复下
    摘要完全一致；
  - 每次 advance 正文 I/O ≤ 1 MiB 上限，累计读 = 复制读 + 校验读、累计写 = 留存写；
  - **复制中途 `os._exit` 硬杀**：重启后状态停在 `copying`，按检查点偏移续拷，
    崩溃窗口仅一个 64 KiB 块（≤256 KiB 合约），最终 sha256 与开单快照一致；
  - **独立读回校验中途硬杀**：状态停在 `verifying`，哈希中间状态已持久化，续算后
    摘要一致，全程读/写计量跨崩溃可对账；
  - 校验未完成前下载 409、源 PVC 不删；verified 后下载 200 且源 PVC 已删；
  - 手工构造的 1.0 旧库（无 I/O 字段）自动迁移，旧单整卷路径一次推进 DONE；
  - 分块模式下丢回包后的删源续办仍生效，已确认删除审计仅一条。
- 边界手工验证：`io_limit_bytes` 恰 1 MiB / 8 MiB 合法，1 MiB−1、8 MiB+1、0、负数
  一律 422；8 MiB 上限跑通 5 MiB 卷，累计读 10 MiB、写 5 MiB，符合预期。

## 未完成项 / 限制

- 单进程同步推进；并发推进同一工单靠进程内锁，多副本部署需外部锁/队列。
- 丢包注入计数器在内存中，重启清零（业务进度在 SQLite，不受影响）。
- Recycle 策略以“截零”模拟 scrub；未模拟 PV 动态供给、finalizer 卡顿等更多 K8s 细节。
- 单卷造数上限 64 MiB（`SeedPvc.size_bytes`）；分块机制本身不限卷大小，但未压测超大卷。
- 为保证崩溃后最坏重做量 ≤ 64 KiB，每个 64 KiB 块各写一次 SQLite 检查点；超大卷下
  检查点写放大明显，生产实现可改为更大的检查点间隔（仍 ≤ 256 KiB 合约）或 WAL 批量提交。
- 可续算 SHA-256 为纯 Python 实现（FIPS 180-4，已与 hashlib 充分对拍），吞吐低于
  OpenSSL 原生实现；性能敏感场景可替换为支持状态序列化的原生库。
- 崩溃钩子 `DRILL_CRASH_AFTER_IO_BYTES` 是测试专用环境变量（支持 `工单ID:字节`
  作用域），生产环境不应设置。
- 无认证鉴权；`/sim/*` 是演练管理面，真实环境需隔离。
- 本沙箱无 Docker 守护进程，镜像未实际构建；Dockerfile/compose 已经静态核对，
  本地自测以 uvicorn 直跑验证。
