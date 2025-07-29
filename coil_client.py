#!/usr/bin/env python3
"""
pymodbus 3.x – Modbus-TCP 클라이언트
1초마다 Coil 10~17 읽어서 표시
"""
import time
from pymodbus.client import ModbusTcpClient

SERVER_IP, PORT  = "127.0.0.1", 502
SLAVE_ID         = 0          # 서버가 single=True → ID 0

def main():
    with ModbusTcpClient(SERVER_IP, port=PORT) as client:
        while True:
            rr = client.read_coils(10, count=8, slave=SLAVE_ID)
            if rr.isError():
                print("❌", rr)
            else:
                print("Coils 10-17:", rr.bits)
            time.sleep(1)

if __name__ == "__main__":
    main()
