"""
Modbus‑TCP Monitor GUI  (Qt for Python)
======================================
*Works with PySide6 **or** PyQt6 automatically.*

A desktop tool to watch any Modbus‑TCP table in real time.

Key changes in **v0.4**
----------------------
* **Coil / Discrete‑Input 테이블**은 *주소마다* 1 bit씩 읽습니다.
  * Modbus 사양상 8·16 개 단위로 묶여 와도, 내부에서 개별 비트를 추출 → CSV / 로그에 `0`·`1` 단위로 저장합니다.
  * Count 필드가 *N*이면 주소 `A … A+N‑1` 을 순서대로 한 번씩 호출하므로 의미가 명확합니다.
* PySide6가 없으면 PyQt6를 자동 사용 (LGPL vs GPL 이슈 해결).

Tested on Python 3.11 / Windows 11 with PyQt6 6.7.0 & PySide6 6.7.1.
"""

import sys, csv, datetime, asyncio, contextlib
from pathlib import Path
from typing import Optional

# ----- Qt autodetect (PySide6 ▸ PyQt6 fallback) ------------------------------
try:
    from PySide6.QtWidgets import (
        QApplication, QWidget, QLineEdit, QSpinBox, QComboBox, QPushButton,
        QTextEdit, QFormLayout, QHBoxLayout
    )
    from PySide6.QtCore import Qt, Signal, QObject
except ModuleNotFoundError:
    from PyQt6.QtWidgets import (
        QApplication, QWidget, QLineEdit, QSpinBox, QComboBox, QPushButton,
        QTextEdit, QFormLayout, QHBoxLayout
    )
    from PyQt6.QtCore import Qt, pyqtSignal as Signal, QObject

from pymodbus.client import AsyncModbusTcpClient
import qasync  # pip install qasync

# ──────────────────────────────────────────────────────────────
# Modbus table presets   name → (method, default addr, default count)
TABLE_MAP: dict[str, tuple[str, int, int]] = {
    "Coil":             ("read_coils",             0,   1),
    "Discrete Input":   ("read_discrete_inputs",   0,   1),
    "Holding Register": ("read_holding_registers", 100, 1),
    "Input Register":   ("read_input_registers",   100, 1),
}

# ──────────────────────────────────────────────────────────────
class ModbusWorker(QObject):
    """Background polling task bridged to Qt via signals."""

    log_ready = Signal(str)
    finished  = Signal()

    def __init__(self, ip: str, port: int, method: str, addr: int, count: int, interval: float = 1.0):
        super().__init__()
        self.ip, self.port = ip, port
        self.method, self.addr, self.count = method, addr, count
        self.interval = interval
        self._running = True

    def stop(self):
        self._running = False

    async def _read_bitwise(self, client, offset: int) -> int:
        """Read a single coil/DI (count=1) and return int 0/1."""
        rr = await getattr(client, self.method)(offset, count=1, slave=1)
        if rr.isError():
            self.log_ready.emit(f"Modbus 오류 @addr {offset}: {rr}\n")
            return -1  # sentinel
        return int(rr.bits[0] if hasattr(rr, "bits") else rr.registers[0])

    async def run(self):
        client = AsyncModbusTcpClient(self.ip, port=self.port)
        try:
            if not await client.connect():
                self.log_ready.emit("❌  서버에 접속할 수 없습니다.\n"); return

            ts_str  = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = Path(f"modbus_log_{ts_str}.csv")
            with csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp"] + [f"val{i}" for i in range(self.count)])
                self.log_ready.emit(f"📂  기록 파일: {csv_path}\n")

                while self._running:
                    if self.method in ("read_coils", "read_discrete_inputs"):
                        # --- bitwise read -------------------------------------------------
                        data_row = [await self._read_bitwise(client, self.addr + off)
                                    for off in range(self.count)]
                    else:
                        rr = await getattr(client, self.method)(self.addr, count=self.count, slave=1)
                        if rr.isError():
                            self.log_ready.emit(f"Modbus 오류: {rr}\n"); data_row = []
                        else:
                            data_row = (getattr(rr, "registers", None) or
                                        getattr(rr, "bits", None) or [])

                    ts = datetime.datetime.now().isoformat(timespec="seconds")
                    writer.writerow([ts] + list(data_row))
                    self.log_ready.emit(f"{ts}  {list(data_row)}\n")
                    await asyncio.sleep(self.interval)
        except Exception as e:
            self.log_ready.emit(f"🚨  예외: {e}\n")
        finally:
            with contextlib.suppress(Exception):
                client.close()   # close() is sync
            self.finished.emit()

# ──────────────────────────────────────────────────────────────
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Modbus‑TCP Monitor")
        self.resize(700, 520)
        self.worker: Optional[ModbusWorker] = None
        self._build_ui()

    def _build_ui(self):
        form = QFormLayout(self)
        self.ip_edit = QLineEdit("127.0.0.1")
        self.port_spin = QSpinBox(); self.port_spin.setRange(1, 65535); self.port_spin.setValue(502)
        self.table_box = QComboBox(); self.table_box.addItems(TABLE_MAP.keys())
        self.addr_spin = QSpinBox(); self.addr_spin.setRange(0, 100000)
        self.count_spin = QSpinBox(); self.count_spin.setRange(1, 125)

        self._update_defaults(self.table_box.currentText())
        self.table_box.currentTextChanged.connect(self._update_defaults)

        form.addRow("Server IP",      self.ip_edit)
        form.addRow("Port",           self.port_spin)
        form.addRow("Table",          self.table_box)
        form.addRow("Start address",  self.addr_spin)
        form.addRow("Count",          self.count_spin)

        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("Start")
        self.stop_btn  = QPushButton("Stop"); self.stop_btn.setEnabled(False)
        btn_row.addWidget(self.start_btn); btn_row.addWidget(self.stop_btn)
        form.addRow(btn_row)

        self.log_view = QTextEdit(); self.log_view.setReadOnly(True); self.log_view.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        form.addRow(self.log_view)

        self.start_btn.clicked.connect(self.start_polling)
        self.stop_btn.clicked.connect(self.stop_polling)

    def _update_defaults(self, table_name: str):
        _, addr, cnt = TABLE_MAP[table_name]
        self.addr_spin.setValue(addr)
        self.count_spin.setValue(cnt)

    @qasync.asyncSlot()
    async def start_polling(self):
        if self.worker:  # already running
            return
        ip, port = self.ip_edit.text().strip(), self.port_spin.value()
        table    = self.table_box.currentText()
        method, _, _ = TABLE_MAP[table]
        addr, count = self.addr_spin.value(), self.count_spin.value()

        self.worker = ModbusWorker(ip, port, method, addr, count)
        self.worker.log_ready.connect(self.log_view.insertPlainText)
        self.worker.finished.connect(self._polling_finished)
        self.start_btn.setEnabled(False); self.stop_btn.setEnabled(True)
        asyncio.create_task(self.worker.run())

    def stop_polling(self):
        if self.worker:
            self.worker.stop(); self.stop_btn.setEnabled(False)

    def _polling_finished(self):
        self.worker = None; self.start_btn.setEnabled(True); self.stop_btn.setEnabled(False)
        self.log_view.insertPlainText("▶︎  작업 종료\n")

    def closeEvent(self, ev):
        if self.worker: self.worker.stop(); ev.accept()

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app); asyncio.set_event_loop(loop)
    win = MainWindow(); win.show()
    with loop: loop.run_forever()
