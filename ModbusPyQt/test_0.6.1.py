"""
Modbus‑TCP Monitor GUI (Qt for Python) – v0.6.1
===============================================
Real‑time client for any Modbus‑TCP server.

* PySide6 **or** PyQt6 (auto)
* **Auto‑scroll** log
* Adjustable **poll interval** (sec) – min 50 ms (MELSEC Q QJ71MT91 latency spec)
* Holding‑/Input‑Register decode modes:
  • **word 16 +**   (uint16)
  • **word 16 +/‑** (int16)
  • **dword 32 +**  (uint32 = low | high≪16)
  • **dword 32 +/‑** (int32)
* Timestamp log & CSV → **0.1 s** resolution

v0.6.1 – Changes
----------------
* 로그 뷰에 **기록 파일 경로**와 **작업 종료** 메시지 복원.
* 내부 동작에는 영향 없음.

Tested on Python 3.11 · pymodbus 3.9.2
"""

from __future__ import annotations
import sys, csv, datetime, asyncio, contextlib
from pathlib import Path
from typing import Optional, List

# ── Qt autodetect ────────────────────────────────────────────
try:
    from PySide6.QtWidgets import (
        QApplication, QWidget, QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox,
        QPushButton, QTextEdit, QFormLayout, QHBoxLayout, QMessageBox
    )
    from PySide6.QtCore import Qt, Signal, QObject
    from PySide6.QtGui import QTextCursor
except ModuleNotFoundError:
    from PyQt6.QtWidgets import (
        QApplication, QWidget, QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox,
        QPushButton, QTextEdit, QFormLayout, QHBoxLayout, QMessageBox
    )
    from PyQt6.QtCore import Qt, pyqtSignal as Signal, QObject
    from PyQt6.QtGui import QTextCursor

from pymodbus.client import AsyncModbusTcpClient
import qasync

TABLE_MAP = {
    "Coil"           : ("read_coils",             0, 1),
    "Discrete Input" : ("read_discrete_inputs",   0, 1),
    "Holding Register":("read_holding_registers", 100, 1),
    "Input Register" : ("read_input_registers",   100, 1),
}
FMT_LIST = ["word 16 +", "word 16 +/-", "dword 32 +", "dword 32 +/-"]

# ───────────────────────────────── Worker ────────────────────
class ModbusWorker(QObject):
    log_ready = Signal(str); finished = Signal()

    def __init__(self, ip: str, port: int, method: str, addr: int, count: int, fmt: str, interval: float = 1):
        super().__init__(); self.ip, self.port, self.method = ip, port, method
        self.addr, self.count, self.fmt, self.interval = addr, count, fmt, interval
        self._running = True

    def stop(self): self._running = False

    # ------- helpers --------
    @staticmethod
    def _u16_to_s16(x: int) -> int: return x-65536 if x & 0x8000 else x
    @staticmethod
    def _u32_to_s32(x: int) -> int: return x-4294967296 if x & 0x80000000 else x

    def _decode_regs(self, regs: List[int]) -> List[int]:
        if self.fmt.startswith("word"):
            return [self._u16_to_s16(r) if "+/-" in self.fmt else r for r in regs]
        pairs = [(regs[i] | (regs[i+1] << 16)) for i in range(0, len(regs)-1, 2)]
        return [self._u32_to_s32(v) for v in pairs] if "+/-" in self.fmt else pairs

    # -------------------------
    async def run(self):
        client = AsyncModbusTcpClient(self.ip, port=self.port)
        try:
            if not await client.connect():
                self.log_ready.emit("❌ connect failed\n"); return
            csv_p = Path(f"modbus_log_{datetime.datetime.now():%Y%m%d_%H%M%S}.csv")
            self.log_ready.emit(f"📂  기록 파일: {csv_p}\n")  # ← 파일 경로 로그
            with csv_p.open("w", newline="") as f:
                writer = csv.writer(f)
                hdr_cnt = self.count//2 if self.fmt.startswith("dword") else self.count
                writer.writerow(["timestamp"] + [f"val{i}" for i in range(hdr_cnt)])
                while self._running:
                    if self.method in ("read_coils", "read_discrete_inputs"):
                        data = []
                        for off in range(self.count):
                            rr = await getattr(client, self.method)(self.addr+off, count=1)
                            data.append(int(rr.bits[0]) if not rr.isError() else -1)
                    else:
                        rr = await getattr(client, self.method)(self.addr, count=self.count)
                        data = self._decode_regs(rr.registers if not rr.isError() else [])
                    now = datetime.datetime.now()
                    ts  = f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond//100000}"
                    writer.writerow([ts]+data)
                    self.log_ready.emit(f"{ts}  {data}\n")
                    await asyncio.sleep(self.interval)
        except Exception as e:
            self.log_ready.emit(f"🚨 {e}\n")
        finally:
            with contextlib.suppress(Exception): client.close(); self.finished.emit()

# ─────────────────────────────── UI ─────────────────────────
class MainWindow(QWidget):
    def __init__(self):
        super().__init__(); self.setWindowTitle("Modbus‑TCP Monitor"); self.resize(760,520)
        self.worker: Optional[ModbusWorker] = None; self._build_ui()

    def _build_ui(self):
        form = QFormLayout(self)
        self.ip = QLineEdit("127.0.0.1"); self.port = QSpinBox(); self.port.setRange(1,65535); self.port.setValue(502)
        self.table = QComboBox(); self.table.addItems(TABLE_MAP.keys())
        self.addr = QSpinBox(); self.addr.setRange(0,100000)
        self.cnt  = QSpinBox(); self.cnt.setRange(1,125)
        self.fmt  = QComboBox(); self.fmt.addItems(FMT_LIST)
        self.interval = QDoubleSpinBox(); self.interval.setRange(0.05,60.0); self.interval.setSingleStep(0.05); self.interval.setValue(1.0); self.interval.setSuffix(" s")
        self._update_defaults(self.table.currentText()); self.table.currentTextChanged.connect(self._update_defaults)
        form.addRow("Server IP", self.ip); form.addRow("Port", self.port); form.addRow("Table", self.table)
        form.addRow("Start addr", self.addr); form.addRow("Count", self.cnt); form.addRow("Format", self.fmt)
        form.addRow("Poll interval", self.interval)
        row = QHBoxLayout(); self.start = QPushButton("Start"); self.stop = QPushButton("Stop"); self.stop.setEnabled(False)
        row.addWidget(self.start); row.addWidget(self.stop); form.addRow(row)
        self.log = QTextEdit(); self.log.setReadOnly(True); form.addRow(self.log)
        self.start.clicked.connect(self.start_poll); self.stop.clicked.connect(self.stop_poll)

    def _update_defaults(self, name: str):
        _, a, c = TABLE_MAP[name]; self.addr.setValue(a); self.cnt.setValue(c)
        self.fmt.setEnabled("Register" in name)

    @qasync.asyncSlot()
    async def start_poll(self):
        if self.worker: return
        if self.interval.value() < 0.05:
            QMessageBox.warning(self, "Interval too low", "Min interval is 0.05 s (MELSEC Q latency limit).")
            return
        mtd,_a,_c = TABLE_MAP[self.table.currentText()]
        self.worker = ModbusWorker(self.ip.text().strip(), self.port.value(), mtd,
                                   self.addr.value(), self.cnt.value(), self.fmt.currentText(), self.interval.value())
        self.worker.log_ready.connect(self.append_log); self.worker.finished.connect(self.on_finished)
        self.start.setEnabled(False); self.stop.setEnabled(True)
        asyncio.create_task(self.worker.run())

    def stop_poll(self):
        if self.worker: self.worker.stop(); self.stop.setEnabled(False)

    def on_finished(self):
        self.worker = None; self.start.setEnabled(True)
        self.append_log("▶︎  작업 종료\n")  # ← 종료 메시지

    def append_log(self, msg: str):
        self.log.moveCursor(QTextCursor.End); self.log.insertPlainText(msg)
        self.log.moveCursor(QTextCursor.End)

    def closeEvent(self, ev):
        if self.worker: self.worker.stop(); ev.accept()

# ───────────────────────── entry ────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)

    win = MainWindow(); win.show()

    with loop: loop.run_forever()
