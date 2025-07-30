"""
Modbus‑TCP Monitor GUI (Qt for Python) – v0.7.2
================================================
* Excel 매핑/포인트 파일 + **type 열** 지원 (v0.7.1 기반)

### 새 기능
| Config 열 | 의미 | 예시 | 동작 |
|-----------|------|------|------|
| `type`    | 데이터 폭 지정 <br>`16` → 16‑bit word +/‑<br>(미기재) → 32‑bit dword +/‑ | 16 | `s16` 읽기 (1 레지스터) |

* 이전 버전의 **연속 주소 판별 로직**을 제거하고, `type` 열 값으로 디코딩 방식 결정.
* `type` 열이 없거나 빈 셀 → 기본값 **32‑bit signed** (`s32`, 2 레지스터).

Tested on Python 3.11 · pymodbus 3.9.2 · pandas 2.x
"""

from __future__ import annotations
import sys, csv, datetime, asyncio, contextlib
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
try:
    from PySide6.QtWidgets import (
        QApplication, QWidget, QLineEdit, QSpinBox, QDoubleSpinBox, QPushButton,
        QTextEdit, QFormLayout, QHBoxLayout, QMessageBox, QFileDialog
    )
    from PySide6.QtCore import Qt, Signal, QObject
    from PySide6.QtGui import QTextCursor
except ModuleNotFoundError:
    from PyQt6.QtWidgets import (
        QApplication, QWidget, QLineEdit, QSpinBox, QDoubleSpinBox, QPushButton,
        QTextEdit, QFormLayout, QHBoxLayout, QMessageBox, QFileDialog
    )
    from PyQt6.QtCore import Qt, pyqtSignal as Signal, QObject
    from PyQt6.QtGui import QTextCursor

from pymodbus.client import AsyncModbusTcpClient
import qasync

# ──────────────────────────── constants ────────────────────────────────────
MEM_METHOD = {
    "M": "read_coils",            # FC01
    "L": "read_coils",            # FC01
    "X": "read_discrete_inputs",  # FC02
    "D": "read_holding_registers",# FC03
}

# ───────────────────────────── helpers ─────────────────────────────────────
class ModbusPoint:
    __slots__ = ("label", "method", "offset", "size", "fmt", "mem", "plc_addr")
    def __init__(self, *, label: str, method: str, offset: int, size: int, fmt: str, mem: str, plc_addr: int):
        self.label, self.method, self.offset, self.size, self.fmt = label, method, offset, size, fmt
        self.mem, self.plc_addr = mem, plc_addr

def _u16_to_s16(x: int) -> int: return x-65536 if x & 0x8000 else x

def _u32_to_s32(x: int) -> int: return x-4294967296 if x & 0x80000000 else x

def parse_int(val) -> int:
    if isinstance(val, (int, float)): return int(val)
    txt = str(val).strip(); return int(txt, 16) if any(c in txt.upper() for c in "ABCDEF") else int(txt)

def build_mapping(df: pd.DataFrame) -> Dict[str, List[Dict[str,int]]]:
    required = {"mem", "plc_addr", "start_number", "assignment_points"}
    if not required.issubset(df.columns):
        raise ValueError(f"Mapping sheet must contain columns {required}")
    mp: Dict[str, List[Dict[str,int]]] = {}
    for _, r in df.iterrows():
        mem = str(r["mem"]).strip().upper()
        mp.setdefault(mem, []).append({
            "plc_base": parse_int(r["plc_addr"]),
            "mb_base" : parse_int(r["start_number"]),
            "count"   : int(r["assignment_points"])
        })
    for segs in mp.values(): segs.sort(key=lambda s: s["plc_base"])
    return mp

def plc_to_modbus(mem: str, plc_addr: int, mapping: Dict[str,List[Dict[str,int]]]) -> int:
    if mem not in mapping: raise KeyError(f"Memory {mem} missing in mapping file")
    for seg in mapping[mem]:
        if seg["plc_base"] <= plc_addr < seg["plc_base"] + seg["count"]:
            return seg["mb_base"] + (plc_addr - seg["plc_base"])
    raise ValueError(f"{mem}{plc_addr} not covered by mapping")

def load_points(mapping_path: Path, config_path: Path) -> List[ModbusPoint]:
    mp_df  = pd.read_excel(mapping_path)
    cfg_df = pd.read_excel(config_path)
    mapping = build_mapping(mp_df)

    pts: List[ModbusPoint] = []
    for _, r in cfg_df.iterrows():
        mem = str(r["mem"]).strip().upper()
        plc_addr = parse_int(r["plc_addr"])
        label = str(r.get("desc")) if pd.notna(r.get("desc")) else f"{mem}{plc_addr}"
        mb_addr = plc_to_modbus(mem, plc_addr, mapping)
        method = MEM_METHOD.get(mem)
        if method is None: raise ValueError(f"Unsupported memory type {mem}")

        if mem == "D":
            type_val = str(r.get("type")).strip() if pd.notna(r.get("type")) else ""
            if type_val == "16":
                size, fmt = 1, "s16"
            else:
                size, fmt = 2, "s32"
        else:
            size, fmt = 1, ("bit" if mem in ("M","L","X") else "u16")

        pts.append(ModbusPoint(label=label, method=method, offset=mb_addr, size=size, fmt=fmt, mem=mem, plc_addr=plc_addr))
    return pts

# ─────────────────────────── Worker ────────────────────────────────────────
class ModbusWorker(QObject):
    log_ready = Signal(str); finished = Signal()
    def __init__(self, ip: str, port: int, points: List[ModbusPoint], interval: float):
        super().__init__(); self.ip, self.port, self.points, self.interval = ip, port, points, interval
        self._running = True
    def stop(self): self._running = False

    async def run(self):
        client = AsyncModbusTcpClient(self.ip, port=self.port)
        try:
            if not await client.connect(): self.log_ready.emit("❌ connect failed\n"); return
            csv_p = Path(f"modbus_log_{datetime.datetime.now():%Y%m%d_%H%M%S}.csv")
            self.log_ready.emit(f"📂  기록 파일: {csv_p}\n")
            with csv_p.open("w", newline="") as f:
                writer = csv.writer(f); writer.writerow(["timestamp"] + [pt.label for pt in self.points])
                while self._running:
                    vals: List[int] = []
                    for pt in self.points:
                        try:
                            rr = await getattr(client, pt.method)(pt.offset, count=pt.size)
                            if rr.isError():
                                val = -1
                            elif pt.method in ("read_coils","read_discrete_inputs"):
                                val = int(rr.bits[0])
                            else:
                                if pt.size == 1:
                                    raw = rr.registers[0]; val = _u16_to_s16(raw) if pt.fmt=="s16" else raw
                                else:
                                    low, high = rr.registers[0], rr.registers[1]
                                    val = _u32_to_s32(low | (high << 16))
                        except Exception:
                            val = -1
                        vals.append(val)
                    now = datetime.datetime.now(); ts = f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond//100000}"
                    writer.writerow([ts]+vals)
                    self.log_ready.emit(f"{ts}  {vals}\n")
                    await asyncio.sleep(self.interval)
        except Exception as e:
            self.log_ready.emit(f"🚨 {e}\n")
        finally:
            with contextlib.suppress(Exception): client.close(); self.finished.emit()

# ─────────────────────────── UI ────────────────────────────────────────────
class MainWindow(QWidget):
    def __init__(self):
        super().__init__(); self.setWindowTitle("Modbus‑TCP Monitor (Excel v0.7.2)"); self.resize(820,580)
        self.worker: Optional[ModbusWorker] = None; self._build_ui()

    def _build_ui(self):
        form = QFormLayout(self)
        self.ip = QLineEdit("127.0.0.1"); self.port = QSpinBox(); self.port.setRange(1,65535); self.port.setValue(502)
        form.addRow("Server IP", self.ip); form.addRow("Port", self.port)
        self.mapping_path = QLineEdit("modbus_mapping.xlsx"); self.config_path = QLineEdit("modbus_addr_config.xlsx")
        def pick(target):
            fn,_ = QFileDialog.getOpenFileName(self,"Excel","","Excel (*.xlsx *.xls)");
            if fn: target.setText(fn)
        btn_mp, btn_cf = QPushButton("…"), QPushButton("…"); btn_mp.clicked.connect(lambda: pick(self.mapping_path)); btn_cf.clicked.connect(lambda: pick(self.config_path))
        row_mp, row_cf = QHBoxLayout(), QHBoxLayout(); row_mp.addWidget(self.mapping_path); row_mp.addWidget(btn_mp); row_cf.addWidget(self.config_path); row_cf.addWidget(btn_cf)
        form.addRow("Mapping file", row_mp); form.addRow("Config file", row_cf)
        self.interval = QDoubleSpinBox(); self.interval.setRange(0.05,60.0); self.interval.setSingleStep(0.05); self.interval.setValue(1.0); self.interval.setSuffix(" s")
        form.addRow("Poll interval", self.interval)
        row_btn = QHBoxLayout(); self.start = QPushButton("Start"); self.stop = QPushButton("Stop"); self.stop.setEnabled(False); row_btn.addWidget(self.start); row_btn.addWidget(self.stop); form.addRow(row_btn)
        self.log = QTextEdit(); self.log.setReadOnly(True); form.addRow(self.log)
        self.start.clicked.connect(self.start_poll); self.stop.clicked.connect(self.stop_poll)

    def _load_points(self):
        mp_p, cf_p = Path(self.mapping_path.text()).expanduser(), Path(self.config_path.text()).expanduser()
        if not mp_p.exists() or not cf_p.exists(): raise FileNotFound
