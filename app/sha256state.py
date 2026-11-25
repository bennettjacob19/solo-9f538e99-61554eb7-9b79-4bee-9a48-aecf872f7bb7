"""可导出/导入内部状态的纯 Python SHA-256（FIPS 180-4）。

标准库 hashlib 的哈希对象不支持序列化中间状态，无法在“分块、可中断、
重启续算”的读回校验中恢复进度。本实现严格按 FIPS 180-4 编写，摘要与
hashlib.sha256 完全一致（tests/selftest_chunked.py 含对拍测试），
额外提供 export_state()/from_state()，使哈希可在任意偏移处续算。

状态仅在 64 字节块边界上的含义最简单，但本实现把不足一块的缓冲一并
序列化，因此任意偏移都可安全导出。
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

_K = (
    0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5, 0x3956C25B, 0x59F111F1,
    0x923F82A4, 0xAB1C5ED5, 0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3,
    0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174, 0xE49B69C1, 0xEFBE4786,
    0x0FC19DC6, 0x240CA1CC, 0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
    0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7, 0xC6E00BF3, 0xD5A79147,
    0x06CA6351, 0x14292967, 0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13,
    0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85, 0xA2BFE8A1, 0xA81A664B,
    0xC24B8B70, 0xC76C51A3, 0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
    0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5, 0x391C0CB3, 0x4ED8AA4A,
    0x5B9CCA4F, 0x682E6FF3, 0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208,
    0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2,
)

_H0 = (0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F,
       0x9B05688C, 0x1F83D9AB, 0x5BE0CD19)

_MASK = 0xFFFFFFFF


def _rotr(x: int, n: int) -> int:
    return ((x >> n) | (x << (32 - n))) & _MASK


@dataclass
class _SavedState:
    h: list[int]
    length: int
    buffer: bytes


class ResumableSHA256:
    digest_size = 32
    block_size = 64
    name = "resumable-sha256"

    def __init__(self, state: _SavedState | None = None):
        if state is None:
            self.h = list(_H0)
            self.length = 0
            self.buffer = b""
        else:
            self.h = list(state.h)
            self.length = state.length
            self.buffer = bytes(state.buffer)

    # ---------------- 压缩函数 ----------------
    def _compress(self, block: bytes) -> None:
        w = list(struct.unpack(">16I", block)) + [0] * 48
        for i in range(16, 64):
            s0 = _rotr(w[i - 15], 7) ^ _rotr(w[i - 15], 18) ^ (w[i - 15] >> 3)
            s1 = _rotr(w[i - 2], 17) ^ _rotr(w[i - 2], 19) ^ (w[i - 2] >> 10)
            w[i] = (w[i - 16] + s0 + w[i - 7] + s1) & _MASK
        a, b, c, d, e, f, g, h = self.h
        for i in range(64):
            s1 = _rotr(e, 6) ^ _rotr(e, 11) ^ _rotr(e, 25)
            ch = (e & f) ^ ((~e) & g)
            t1 = (h + s1 + ch + _K[i] + w[i]) & _MASK
            s0 = _rotr(a, 2) ^ _rotr(a, 13) ^ _rotr(a, 22)
            maj = (a & b) ^ (a & c) ^ (b & c)
            t2 = (s0 + maj) & _MASK
            h, g, f = g, f, e
            e = (d + t1) & _MASK
            d, c, b = c, b, a
            a = (t1 + t2) & _MASK
        self.h = [(x + y) & _MASK for x, y in zip(
            self.h, (a, b, c, d, e, f, g, h))]

    # ---------------- 喂数据 ----------------
    def update(self, data: bytes) -> "ResumableSHA256":
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data must be bytes-like")
        data = bytes(data)
        self.length += len(data)
        self.buffer += data
        while len(self.buffer) >= 64:
            self._compress(self.buffer[:64])
            self.buffer = self.buffer[64:]
        return self

    # ---------------- 状态导入/导出 ----------------
    def export_state(self) -> dict:
        """可 JSON 序列化的中间状态（含当前链值、总长度、尾缓冲）。"""
        return {"h": list(self.h), "length": self.length,
                "buffer": self.buffer.hex()}

    @classmethod
    def from_state(cls, state: dict) -> "ResumableSHA256":
        return cls(_SavedState(h=list(state["h"]), length=int(state["length"]),
                               buffer=bytes.fromhex(state["buffer"])))

    # ---------------- 出摘要（不破坏当前状态） ----------------
    def copy(self) -> "ResumableSHA256":
        return ResumableSHA256(_SavedState(h=list(self.h), length=self.length,
                                           buffer=bytes(self.buffer)))

    def _digest_on_copy(self) -> bytes:
        c = self.copy()
        pad = b"\x80" + b"\x00" * ((56 - (c.length % 64) - 1) % 64)
        pad += struct.pack(">Q", c.length * 8)
        c.buffer += pad
        while c.buffer:
            c._compress(c.buffer[:64])
            c.buffer = c.buffer[64:]
        return struct.pack(">8I", *c.h)

    def digest(self) -> bytes:
        return self._digest_on_copy()

    def hexdigest(self) -> str:
        return self._digest_on_copy().hex()
