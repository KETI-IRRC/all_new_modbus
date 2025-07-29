#!/usr/bin/env python3
"""
pymodbus 3.x – Modbus-TCP 서버
10~17번 코일 = 1,0,1,0,1,0,1,0
"""
import logging
from pymodbus.datastore import (
    ModbusServerContext, ModbusSlaveContext, ModbusSequentialDataBlock
)
from pymodbus.server import StartTcpServer

HOST, PORT = "0.0.0.0", 502

# 18개 코일(0~17)을 전부 0으로 만든 뒤 서버 컨텍스트 생성
empty_word_list = [0] * 18          # 단순 자리수용, 실제 값은 아래에서 덮어씀
slave = ModbusSlaveContext(co=ModbusSequentialDataBlock(0, empty_word_list))
context = ModbusServerContext(slaves=slave, single=True)   # 슬레이브 ID = 0

# 원하는 패턴을 코일 비트(FC 1)에 직접 기록
pattern = [1, 0, 1, 0, 1, 0, 1, 0]  # True/False 대신 1/0으로 작성
context[0x00].setValues(1, 10, pattern)  # 1 = FC-1(Coils), addr=10

def log_coils():
    vals = context[0x00].getValues(1, 0, count=18)
    logging.debug(f"★ 현재 Coil 0-17: {vals}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(message)s")
    log_coils()                     # 서버 기동 전 패턴 확인
    StartTcpServer(context, address=(HOST, PORT))
