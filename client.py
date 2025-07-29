from pymodbus.client import AsyncModbusTcpClient
import asyncio, csv, datetime

SERVER_IP = "127.0.0.1"   # server host
PORT      = 502            # server port
LOGFILE   = "modbus_log.csv"

async def poll_and_log(stop_event: asyncio.Event):
    client = AsyncModbusTcpClient(SERVER_IP, port=PORT)
    if not await client.connect():
        raise ConnectionError("Cannot connect to Modbus server")

    with open(LOGFILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "temp_C", "rpm"])

        while not stop_event.is_set():
            rr = await client.read_coils(102, count=8, slave=1)
            if rr.isError():
                print("Modbus error:", rr)
            else:
                temp = rr.registers[0]
                rpm  = rr.registers[2]
                ts   = datetime.datetime.now().isoformat(timespec="seconds")
                writer.writerow([ts, temp, rpm])
                print(f"{ts} | {temp:} °C | {rpm} RPM")
            await asyncio.sleep(1)

    await client.close()

async def keyboard_listener(stop_event: asyncio.Event):
    while True:
        key = await asyncio.to_thread(input, "Press 'q' + Enter to quit › ")
        if key.strip().lower() == "q":
            stop_event.set()
            break

async def main():
    stop_event = asyncio.Event()
    poll_task  = asyncio.create_task(poll_and_log(stop_event))
    key_task   = asyncio.create_task(keyboard_listener(stop_event))

    await stop_event.wait()
    print("\nClient terminated.")
    await asyncio.gather(poll_task, key_task, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass