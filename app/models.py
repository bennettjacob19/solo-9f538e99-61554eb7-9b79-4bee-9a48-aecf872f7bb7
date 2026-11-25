from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# 单次 advance 请求的卷正文 I/O 预算允许区间（字节）：1 MiB ~ 8 MiB。
MIN_IO_LIMIT = 1 * 1024 * 1024
MAX_IO_LIMIT = 8 * 1024 * 1024


class SeedPvc(BaseModel):
    name: str = Field(min_length=1, description="PVC 名称")
    size_bytes: int = Field(gt=0, le=64 * 1024 * 1024, description="卷大小（字节）")
    reclaim_policy: Literal["Delete", "Retain", "Recycle"] = "Delete"


class SeedRequest(BaseModel):
    namespace: str = Field(min_length=1, description="要创建的命名空间名称")
    pvcs: list[SeedPvc] = Field(min_length=1, description="命名空间内的 PVC 列表")


class TicketRequest(BaseModel):
    namespace: str = Field(min_length=1, description="待删除的命名空间")
    retain_pvcs: list[str] = Field(min_length=1, description="至少一个需保留数据的 PVC 名称")
    io_limit_bytes: int = Field(
        ge=MIN_IO_LIMIT, le=MAX_IO_LIMIT,
        description="单次推进(advance)的卷正文 I/O 上限（字节），区间 1~8 MiB；"
                    "源卷读取、留存写入、独立读回校验、恢复核对的字节均计入。开单本身不读卷正文。")
    note: str | None = Field(default=None, description="备注")


class FaultRequest(BaseModel):
    drop_delete_responses: int = Field(
        ge=0, le=100,
        description="接下来 N 次删除：服务端生效但回包丢失（模拟丢包）")
