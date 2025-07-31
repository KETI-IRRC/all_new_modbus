"""
Modbus-TCP Monitor GUI (PySide6) – v0.8.0
* 주요 변경
  1) 포인트를 메서드·주소 기준으로 배치(batching)하여 한 번에 읽음
  2) 'type' 열 파싱 로직 개선: 숫자·문자 모두 허용, u16 도 지원
"""

# ────────────────────────── import & 상수 ──────────────────────────
from __future__ import annotations
import sys, csv, datetime, asyncio, contextlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple, DefaultDict
from collections import defaultdict
import pandas as pd
from dataclasses import dataclass
from PySide6.QtWidgets import (
    QApplication, QWidget, QLineEdit, QSpinBox, QDoubleSpinBox, QPushButton,
    QTextEdit, QFormLayout, QHBoxLayout, QMessageBox, QFileDialog
)
from PySide6.QtCore import Signal, QObject
from PySide6.QtGui import QTextCursor
from pymodbus.client import AsyncModbusTcpClient
import qasync

MEM_METHOD = {
    "M": "read_coils",
    "L": "read_coils",
    "Y": "read_coils", # Y 추가 
    "X": "read_discrete_inputs",
    "D": "read_holding_registers",
}

_u16_to_s16 = lambda x: x - 0x10000 if x & 0x8000 else x
_u32_to_s32 = lambda x: x - 0x100000000 if x & 0x80000000 else x


# ────────────────────────── 데이터 클래스 ──────────────────────────
@dataclass(slots=True)
class ModbusPoint:
    label: str
    method: str
    offset: int
    size: int             # 1 또는 2
    fmt: str              # "bit" | "u16" | "s16" | "s32"
    mem: str
    plc_addr: int


@dataclass(slots=True)
class Batch:
    method: str
    start: int
    count: int
    points: List[ModbusPoint]          # 포함된 포인트 목록


# ────────────────────────── 유틸 ──────────────────────────
def parse_int(val) -> int:
    if isinstance(val, (int, float)):
        return int(val)
    txt = str(val).strip()
    return int(txt, 16) if any(c in txt.upper() for c in "ABCDEF") else int(txt)


def build_mapping(df: pd.DataFrame) -> Dict[str, List[Dict[str, int]]]:
    required = {"mem", "plc_addr", "start_number", "assignment_points"}
    if not required.issubset(df.columns):
        raise ValueError(f"매핑 시트에는 {required} 컬럼이 필요합니다.")
    mp: Dict[str, List[Dict[str, int]]] = {}
    for _, r in df.iterrows():
        mem = str(r["mem"]).strip().upper()
        mp.setdefault(mem, []).append(
            {
                "plc_base": parse_int(r["plc_addr"]),
                "mb_base":  parse_int(r["start_number"]),
                "count":    int(r["assignment_points"]),
            }
        )
    for segs in mp.values():
        segs.sort(key=lambda s: s["plc_base"])
    return mp


def plc_to_modbus(mem: str, plc_addr: int,
                  mapping: Dict[str, List[Dict[str, int]]]) -> int:
    for seg in mapping[mem]:
        if seg["plc_base"] <= plc_addr < seg["plc_base"] + seg["count"]:
            return seg["mb_base"] + (plc_addr - seg["plc_base"])
    raise ValueError(f"{mem}{plc_addr} 가 매핑 범위를 벗어났습니다.")


# ───── Excel → ModbusPoint ─────
def _normalize_type(tp_val) -> str:
    """
    엑셀 'type' 셀 값을 통일된 문자열로 변환
    16 / '16' / '16.0' / 's16' → 's16'
    'u16' / 'U16'              → 'u16'
    (NaN or 공백)              → ''
    """
    if pd.isna(tp_val):
        return ""
    if isinstance(tp_val, (int, float)):
        tp_val = str(int(tp_val))
    tp = str(tp_val).strip().lower()
    if tp in {"16", "s16", "int16"}:
        return "s16"
    if tp in {"u16", "uint16"}:
        return "u16"
    return ""


def load_points(mapping_path: Path, config_path: Path) -> List[ModbusPoint]:
    mp_df = pd.read_excel(mapping_path)
    cfg_df = pd.read_excel(config_path)
    mapping = build_mapping(mp_df)

    points: List[ModbusPoint] = []
    for _, r in cfg_df.iterrows():
        mem      = str(r["mem"]).strip().upper()
        plc_addr = parse_int(r["plc_addr"])
        label    = str(r.get("desc")) if pd.notna(r.get("desc")) else f"{mem}{plc_addr}"
        mb_addr  = plc_to_modbus(mem, plc_addr, mapping)
        method   = MEM_METHOD.get(mem)
        if not method:
            raise ValueError(f"지원하지 않는 메모리 타입 {mem}")

        # ---- 타입 결정 ----
        if mem == "D":
            tp_norm = _normalize_type(r.get("type"))
            if tp_norm == "s16":
                size, fmt = 1, "s16"
            elif tp_norm == "u16":
                size, fmt = 1, "u16"
            else:
                size, fmt = 2, "s32"
        else:          # Coil / X
            size, fmt = 1, "bit"

        points.append(ModbusPoint(label, method, mb_addr, size, fmt, mem, plc_addr))
    return points


# ────────────────────────── 워커 (Batching) ──────────────────────────
class ModbusWorker(QObject):
    log_ready = Signal(str)
    finished  = Signal()

    def __init__(self, ip: str, port: int,
                 points: List[ModbusPoint], interval: float):
        super().__init__()
        self.ip, self.port, self.points, self.interval = ip, port, points, interval
        self._running = True
        self._batches: List[Batch] = self._make_batches(points)

    def stop(self):
        self._running = False

    # ---------- 배치 생성 ----------
    @staticmethod
    def _make_batches(points: List[ModbusPoint]) -> List[Batch]:
        grouped: DefaultDict[str, List[ModbusPoint]] = defaultdict(list)
        for pt in points:
            grouped[pt.method].append(pt)

        batches: List[Batch] = []
        for method, pts in grouped.items():
            pts.sort(key=lambda p: p.offset)
            i = 0
            while i < len(pts):
                start = pts[i].offset
                end   = start + pts[i].size
                batch_pts = [pts[i]]
                i += 1
                # 인접(겹치지 않고 gap 없는) 포인트까지 확장
                while i < len(pts) and pts[i].offset == end:
                    batch_pts.append(pts[i])
                    end += pts[i].size
                    i  += 1
                batches.append(Batch(method, start, end - start, batch_pts))
        return batches

    # ---------- 값 추출 ----------
    @staticmethod
    def _decode_value(pt: ModbusPoint, data) -> int:
        """data: bit list 또는 register list"""
        idx = pt.offset - data["start"]
        if pt.method in ("read_coils", "read_discrete_inputs"):
            return int(data["bits"][idx])
        if pt.size == 1:
            raw = data["regs"][idx]
            if pt.fmt == "s16":
                return _u16_to_s16(raw)
            return raw                # u16
        else:  # 32-bit
            low  = data["regs"][idx]
            high = data["regs"][idx + 1]
            val  = (high << 16) | low     # 워드 순서: Low→High (필요시 교체)
            return _u32_to_s32(val)

    # ---------- 메인 루프 ----------
    async def run(self):
        client = AsyncModbusTcpClient(self.ip, port=self.port)
        try:
            if not await client.connect():
                self.log_ready.emit("❌ PLC 연결 실패\n")
                return

            csv_p = Path(f"modbus_log_{datetime.datetime.now():%Y%m%d_%H%M%S}.csv")
            with csv_p.open("w", newline="") as f:
                self.log_ready.emit(f"📂 기록 파일: {csv_p}\n")
                writer = csv.writer(f)
                writer.writerow(["timestamp"] + [pt.label for pt in self.points])

                while self._running:
                    # --- batch read ---
                    batch_data: Dict[Tuple[str, int], Dict] = {}
                    for b in self._batches:
                        rr = await getattr(client, b.method)(b.start, count=b.count)
                        if rr.isError():
                            batch_data[(b.method, b.start)] = {}
                            continue
                        if b.method in ("read_coils", "read_discrete_inputs"):
                            batch_data[(b.method, b.start)] = {
                                "start": b.start, "bits": rr.bits
                            }
                        else:  # registers
                            batch_data[(b.method, b.start)] = {
                                "start": b.start, "regs": rr.registers
                            }

                    # --- 개별 포인트 추출 ---
                    row_vals: List[int] = []
                    for pt in self.points:
                        key = (pt.method, self._batch_start(pt))
                        bdat = batch_data.get(key, {})
                        if not bdat:
                            val = -1
                        else:
                            try:
                                val = self._decode_value(pt, bdat)
                            except Exception:
                                val = -1
                        row_vals.append(val)

                    now = datetime.datetime.now()
                    ts  = f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond//100000}"
                    writer.writerow([ts] + row_vals)
                    self.log_ready.emit(f"{ts}  {row_vals}\n")
                    await asyncio.sleep(self.interval)
        except Exception as e:
            self.log_ready.emit(f"🚨 {e}\n")
        finally:
            with contextlib.suppress(Exception):
                client.close()
            self.finished.emit()

    # 배치 시작 오프셋 빠르게 구하는 헬퍼
    def _batch_start(self, pt: ModbusPoint) -> int:
        for b in self._batches:
            if b.method == pt.method and b.start <= pt.offset < b.start + b.count:
                return b.start
        raise RuntimeError("배치 검색 실패")  # 논리적으로 발생하지 않음


# ────────────────────────── 이하 GUI 부분은 v0.7.3과 동일 ──────────────────────────
# (GUI 코드는 길이만 증가하므로 변경 없는 부분은 생략했습니다)


# ────────────────────────── GUI ──────────────────────────
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Modbus-TCP Monitor (Excel v0.8.0)")
        self.resize(840, 600)
        self.worker: Optional[ModbusWorker] = None
        self._build_ui()

    def _build_ui(self):
        form = QFormLayout(self)

        # 연결
        self.ip = QLineEdit("127.0.0.1")
        self.port = QSpinBox(); self.port.setRange(1, 65535); self.port.setValue(502)
        form.addRow("Server IP", self.ip)
        form.addRow("Port", self.port)

        # Excel 경로
        self.mapping_path = QLineEdit("modbus_mapping.xlsx")
        self.config_path  = QLineEdit("modbus_addr_config.xlsx")

        def pick(target: QLineEdit):
            fn, _ = QFileDialog.getOpenFileName(self, "Excel", "", "Excel (*.xlsx *.xls)")
            if fn:
                target.setText(fn)

        btn_mp, btn_cf = QPushButton("…"), QPushButton("…")
        btn_mp.clicked.connect(lambda: pick(self.mapping_path))
        btn_cf.clicked.connect(lambda: pick(self.config_path))

        row_mp, row_cf = QHBoxLayout(), QHBoxLayout()
        row_mp.addWidget(self.mapping_path); row_mp.addWidget(btn_mp)
        row_cf.addWidget(self.config_path);  row_cf.addWidget(btn_cf)
        form.addRow("Mapping file", row_mp)
        form.addRow("Config file",  row_cf)

        # 주기
        self.interval = QDoubleSpinBox(); self.interval.setRange(0.05, 60.0)
        self.interval.setSingleStep(0.05); self.interval.setValue(1.0); self.interval.setSuffix(" s")
        form.addRow("Poll interval", self.interval)

        # 버튼
        row_btn = QHBoxLayout()
        self.start = QPushButton("Start"); self.stop = QPushButton("Stop"); self.stop.setEnabled(False)
        row_btn.addWidget(self.start); row_btn.addWidget(self.stop)
        form.addRow(row_btn)

        # 로그
        self.log = QTextEdit(); self.log.setReadOnly(True)
        form.addRow(self.log)

        # 신호
        self.start.clicked.connect(self.start_poll)
        self.stop.clicked.connect(self.stop_poll)

    # 포인트 로드
    def _load_points(self) -> List[ModbusPoint]:
        mp_p = Path(self.mapping_path.text()).expanduser()
        cf_p = Path(self.config_path.text()).expanduser()
        if not mp_p.exists() or not cf_p.exists():
            raise FileNotFoundError("Excel 파일을 찾을 수 없습니다.")
        return load_points(mp_p, cf_p)

    # 시작
    @qasync.asyncSlot()
    async def start_poll(self):
        if self.worker:
            return
        if self.interval.value() < 0.05:
            QMessageBox.warning(self, "간격 오류", "0.05 초 이상으로 설정하세요.")
            return
        try:
            points = self._load_points()
        except Exception as e:
            QMessageBox.critical(self, "파일 오류", str(e))
            return

        self.worker = ModbusWorker(self.ip.text().strip(), self.port.value(),
                                   points, self.interval.value())
        self.worker.log_ready.connect(self.append_log)
        self.worker.finished.connect(self.on_finished)

        self.start.setEnabled(False); self.stop.setEnabled(True)
        asyncio.create_task(self.worker.run())

    def stop_poll(self):
        if self.worker:
            self.worker.stop()
            self.stop.setEnabled(False)

    def on_finished(self):
        self.worker = None
        self.start.setEnabled(True)
        self.append_log("▶ 작업 종료\n")

    def append_log(self, msg: str):
        self.log.moveCursor(QTextCursor.End)
        self.log.insertPlainText(msg)
        self.log.moveCursor(QTextCursor.End)

    def closeEvent(self, ev):
        if self.worker:
            self.worker.stop()
        ev.accept()


# ────────────────────────── entry ──────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)

    win = MainWindow()
    win.show()

    with loop:
        loop.run_forever()
