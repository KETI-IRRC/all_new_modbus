"""
Modbus‑TCP Server GUI (Pyside6 only) – v0.1
==========================================
▶ 즉시 실행 가능한 단일 파일
▶ 포트 502에서 Modbus‑TCP 서버로 동작하며, GUI에서 네 종류 테이블 값을 실시간 편집

기능 요약
---------
* **PySide6 전용** – PyQt 분기 제거.
* 테이블 선택 · Start Address · Count 를 지정 → [Generate] 누르면 위젯을 **새로** 생성.
  * 이전 설정은 제거 → ‘차곡차곡 쌓이는’ 현상 없음.
* [Start Server] 클릭 시 **설정 위젯 잠금** … 값 편집 위젯은 활성.
* 값 수정 → `pymodbus` Datastore 컨텍스트에 즉시 반영.
* [Stop] 후 다시 설정 변경 가능.

의존: `pip install PySide6 pymodbus>=3.9 qasync`
"""

from __future__ import annotations

import sys, asyncio
from functools import partial

from PySide6.QtWidgets import (
    QApplication, QWidget, QLabel, QSpinBox, QComboBox, QPushButton,
    QVBoxLayout, QHBoxLayout, QScrollArea, QCheckBox, QFrame, QMessageBox
)
from PySide6.QtCore import Qt, Signal, QObject

from pymodbus.server import StartAsyncTcpServer
from pymodbus.datastore import ModbusSparseDataBlock, ModbusSlaveContext, ModbusServerContext
import qasync

# ─── Modbus 테이블 정의 ──────────────────────────────────────────
TABLES = {
    "Coil":  dict(fc=1, block="co", dtype="bool"),   # FC1 read / FC5 write
    "Discrete Input":  dict(fc=2, block="di", dtype="bool"),
    "Holding Register":dict(fc=3, block="hr", dtype="int"),
    "Input Register":  dict(fc=4, block="ir", dtype="int"),
}

# ─── 비동기 서버 러너 ────────────────────────────────────────────
class Runner(QObject):
    running_changed = Signal(bool)

    def __init__(self):
        super().__init__()
        self._task: asyncio.Task | None = None
        self.context: ModbusServerContext | None = None

    async def _serve(self):
        self.running_changed.emit(True)
        try:
            await StartAsyncTcpServer(self.context, address=("0.0.0.0", 502))
        except asyncio.CancelledError:
            pass
        finally:
            self.running_changed.emit(False)

    def start(self, context: ModbusServerContext):
        if self._task and not self._task.done():
            return
        self.context = context
        self._task = asyncio.create_task(self._serve())

    def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()

# ─── 메인 윈도우 ────────────────────────────────────────────────
class ServerUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Modbus‑TCP Server GUI")
        self.resize(520, 620)

        # state
        self.runner = Runner(); self.runner.running_changed.connect(self._toggle_lock)
        self.widgets: list[QCheckBox | QSpinBox] = []

        self._build()

    # UI build
    def _build(self):
        layout = QVBoxLayout(self)

        ctrl = QHBoxLayout()
        self.table = QComboBox(); self.table.addItems(TABLES.keys())
        ctrl.addWidget(QLabel("Table")); ctrl.addWidget(self.table)

        self.start_spin = QSpinBox(); self.start_spin.setRange(0, 100000)
        ctrl.addWidget(QLabel("Start")); ctrl.addWidget(self.start_spin)

        self.count_spin = QSpinBox(); self.count_spin.setRange(1, 125); self.count_spin.setValue(8)
        ctrl.addWidget(QLabel("Count")); ctrl.addWidget(self.count_spin)

        self.gen_btn = QPushButton("Generate"); self.gen_btn.clicked.connect(self._generate)
        ctrl.addWidget(self.gen_btn)
        layout.addLayout(ctrl)

        # dynamic area
        self.scroll = QScrollArea(); self.scroll.setWidgetResizable(True)
        self.frame = QFrame(); self.vbox = QVBoxLayout(self.frame)
        self.scroll.setWidget(self.frame)
        layout.addWidget(self.scroll, 1)

        # start/stop
        btnrow = QHBoxLayout()
        self.start_btn = QPushButton("Start Server"); self.start_btn.clicked.connect(self._start)
        self.stop_btn  = QPushButton("Stop"); self.stop_btn.clicked.connect(self.runner.stop); self.stop_btn.setEnabled(False)
        btnrow.addWidget(self.start_btn); btnrow.addWidget(self.stop_btn)
        layout.addLayout(btnrow)

        self._generate()  # initial render

    # build value widgets anew
    def _generate(self):
        for w in self.widgets:
            w.setParent(None); w.deleteLater()
        self.widgets.clear()

        info = TABLES[self.table.currentText()]
        start, count = self.start_spin.value(), self.count_spin.value()

        for i in range(count):
            addr = start + i
            if info["dtype"] == "bool":
                cb = QCheckBox(f"{addr}"); cb.setChecked(False)
                cb.stateChanged.connect(partial(self._bool_changed, i))
                self.vbox.addWidget(cb); self.widgets.append(cb)
            else:
                spin = QSpinBox(); spin.setRange(0, 65535)
                spin.valueChanged.connect(partial(self._int_changed, i))
                row = QHBoxLayout(); row.addWidget(QLabel(f"{addr}")); row.addWidget(spin)
                cont = QFrame(); cont.setLayout(row)
                self.vbox.addWidget(cont); self.widgets.append(spin)

    # value callbacks
    def _bool_changed(self, offset:int, state:int):
        if not self.runner.context: return
        base = self.start_spin.value(); addr = base+offset
        fc = TABLES[self.table.currentText()]["fc"]
        self.runner.context[0].setValues(fc, addr, [1 if state==Qt.Checked else 0])

    def _int_changed(self, offset:int, val:int):
        if not self.runner.context: return
        base = self.start_spin.value(); addr = base+offset
        fc = TABLES[self.table.currentText()]["fc"]
        self.runner.context[0].setValues(fc, addr, [val])

    # start server
    def _start(self):
        info = TABLES[self.table.currentText()]
        start, count = self.start_spin.value(), self.count_spin.value()
        regmap = {start+i: 0 for i in range(count)}
        blocks = {info["block"]: ModbusSparseDataBlock(regmap)}
        ctx = ModbusServerContext(slaves=ModbusSlaveContext(**blocks), single=True)
        self.runner.start(ctx)

    # lock/unlock config widgets
    def _toggle_lock(self, running: bool):
        for w in (self.table, self.start_spin, self.count_spin, self.gen_btn):
            w.setEnabled(not running)
        self.start_btn.setEnabled(not running)
        self.stop_btn.setEnabled(running)
        QMessageBox.information(self, "Server", "Running" if running else "Stopped")

# ─── entry ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app); asyncio.set_event_loop(loop)

    win = ServerUI(); win.show()
    with loop:
        loop.run_forever()
