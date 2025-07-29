"""Asynchronous Modbus‑TCP server that updates a few holding
registers every 0.5 s.  Press `q` then Enter in the console to
shut the server down cleanly.``python"""

from pymodbus.datastore import (
    ModbusSparseDataBlock,
    ModbusSlaveContext,
    ModbusServerContext,
)
from pymodbus.server import StartAsyncTcpServer
import asyncio, random, sys

# -----------------------------------------------------------------
# Register map – address : value (0‑base addressing)
# HR 100 = temperature ×100 (e.g. 2534 → 25.34 °C)
# HR 102 = rpm
# -----------------------------------------------------------------
REGMAP = {0: 0, 1: 0, 100: 2500, 102: 123}

context = ModbusServerContext(
    slaves=ModbusSlaveContext(hr=ModbusSparseDataBlock(REGMAP)),
    single=True,
)

async def updater(stop_event: asyncio.Event):
    """Update HR 100 with a random temperature every 0.5 s."""
    while not stop_event.is_set():
        REGMAP[100] = int(random.uniform(20, 30) * 100)
        context[0x00].setValues(0, 100, [REGMAP[100]])
        context[0x00].setValues(1, 102, [REGMAP[102]])
        await asyncio.sleep(0.5)

async def keyboard_listener(stop_event: asyncio.Event):
    """Wait until the user types "q" then signal other tasks to stop."""
    while True:
        key = await asyncio.to_thread(input, "Press 'q' + Enter to quit › ")
        if key.strip().lower() == "q":
            stop_event.set()
            break

async def main():
    stop_event = asyncio.Event()

    server_task   = asyncio.create_task(StartAsyncTcpServer(context, address=("0.0.0.0", 502)))
    updater_task  = asyncio.create_task(updater(stop_event))
    listener_task = asyncio.create_task(keyboard_listener(stop_event))

    await stop_event.wait()          # block until "q" pressed
    print("\nShutting down …")

    server_task.cancel()             # stop TCP server
    await asyncio.gather(server_task, updater_task, listener_task, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass