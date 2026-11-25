import asyncio
import json
import socket
import time
from multiprocessing.connection import Connection

import pytest
from aioca import caget, caput
from pandablocks.asyncio import AsyncioClient
from softioc import asyncio_dispatcher
from websockets.asyncio.server import ServerConnection, serve

from fixtures.mocked_panda import (
    MULTIPROCESSING_CONTEXT,
    TEST_PREFIX,
    select_and_recv,
)
from pandablocks_ioc import _layout
from pandablocks_ioc._layout import LAYOUT_PATH, MalcolmError, MalcolmLayoutClient
from pandablocks_ioc._pvi import Pvi, PviGroup
from pandablocks_ioc.ioc import IocRecordFactory

# Fast enough for tests, while still letting changes made together be combined
FAST = {"coalesce_time": 0.02, "settle_time": 0.1}


class FakeMalcolm:
    """Minimal Malcolm websocket server serving a PANDA.layout table"""

    def __init__(self):
        self.table = {
            "typeid": "malcolm:core/Table:1.0",
            "name": ["PULSE1", "PCAP"],
            "mri": ["PANDA:PULSE1", "PANDA:PCAP"],
            "x": [10.0, 20.0],
            "y": [30.0, 40.0],
            "visible": [True, False],
        }
        self.puts: list[dict] = []
        self.reject_puts = False
        # Number of upcoming puts to acknowledge but then revert, as the real
        # Malcolm sometimes does
        self.revert_puts = 0
        # Publish the table as it was before each put while handling it, as the
        # real Malcolm does when it reads back the layout it stored on the PandA
        self.stale_updates = False
        # Reset the position of hidden blocks after each put, as the real Malcolm
        # only stores the position of visible blocks
        self.forget_hidden_positions = False
        self.subscribers: list[tuple[ServerConnection, int]] = []

    async def publish(self, table=None):
        for ws, msg_id in self.subscribers:
            await ws.send(
                json.dumps(
                    {
                        "typeid": "malcolm:core/Update:1.0",
                        "id": msg_id,
                        "value": table or self.table,
                    }
                )
            )

    async def handler(self, ws: ServerConnection):
        async for raw in ws:
            message = json.loads(raw)
            assert message["path"] == LAYOUT_PATH
            if message["typeid"] == "malcolm:core/Subscribe:1.0":
                self.subscribers.append((ws, message["id"]))
                await self.publish()
            elif message["typeid"] == "malcolm:core/Put:1.0":
                self.puts.append(message["value"])
                if self.reject_puts:
                    await ws.send(
                        json.dumps(
                            {
                                "typeid": "malcolm:core/Error:1.0",
                                "id": message["id"],
                                "message": "Rejected",
                            }
                        )
                    )
                    continue
                old_table = json.loads(json.dumps(self.table))
                if self.stale_updates:
                    await self.publish(old_table)
                    await asyncio.sleep(0.05)
                value = message["value"]
                for row, name in enumerate(value["name"]):
                    index = self.table["name"].index(name)
                    for column in ("x", "y", "visible"):
                        self.table[column][index] = value[column][row]
                await ws.send(
                    json.dumps(
                        {
                            "typeid": "malcolm:core/Return:1.0",
                            "id": message["id"],
                            "value": None,
                        }
                    )
                )
                await self.publish()
                if self.revert_puts:
                    self.revert_puts -= 1
                    self.table = old_table
                    await self.publish()
                if self.forget_hidden_positions:
                    for index, visible in enumerate(self.table["visible"]):
                        if not visible:
                            self.table["x"][index] = self.table["y"][index] = 0.0
                    await self.publish()


@pytest.fixture
async def fake_malcolm():
    malcolm = FakeMalcolm()
    async with serve(malcolm.handler, "localhost", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        yield malcolm, port


def unused_port() -> int:
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def row(malcolm: FakeMalcolm, name: str) -> tuple:
    index = malcolm.table["name"].index(name)
    return tuple(malcolm.table[c][index] for c in ("x", "y", "visible"))


async def test_layout_client_connect_and_put(fake_malcolm):
    malcolm, port = fake_malcolm
    client = MalcolmLayoutClient("localhost", port, **FAST)
    table = await client.connect()
    assert table["name"] == ["PULSE1", "PCAP"]

    updates = asyncio.Queue()
    client.on_update = updates.put_nowait
    client.start()
    try:
        await client.put("PCAP", x=99.0, visible=True)
        # Only the changed row is sent, with untouched columns from the cache
        assert malcolm.puts == [
            {
                "typeid": "malcolm:core/Table:1.0",
                "name": ["PCAP"],
                "mri": ["PANDA:PCAP"],
                "x": [99.0],
                "y": [40.0],
                "visible": [True],
            }
        ]
        update = await asyncio.wait_for(updates.get(), 1)
        assert update["x"] == [10.0, 99.0]
        assert update["visible"] == [True, True]

        malcolm.reject_puts = True
        with pytest.raises(MalcolmError, match="Rejected"):
            await client.put("PULSE1", y=1.0)
        with pytest.raises(MalcolmError, match="not in the Malcolm layout"):
            await client.put("PULSE9", y=1.0)
    finally:
        await client.close()


async def test_layout_records(fake_malcolm, clear_records):
    malcolm, port = fake_malcolm
    client = MalcolmLayoutClient("localhost", port)
    await client.connect()
    factory = IocRecordFactory(AsyncioClient("123"), TEST_PREFIX, {})

    records = factory.create_layout_records("PULSE1", ("PULSE1",), client)
    assert set(records) == {
        "PULSE1:LAYOUT:X",
        "PULSE1:LAYOUT:Y",
        "PULSE1:LAYOUT:VISIBLE",
    }
    assert records["PULSE1:LAYOUT:X"].record.get() == 10.0
    assert records["PULSE1:LAYOUT:Y"].record.get() == 30.0
    assert records["PULSE1:LAYOUT:VISIBLE"].record.get() == 1
    assert PviGroup.LAYOUT in Pvi.pvi_info_dict["PULSE1"]

    table = dict(client.table, x=[55.0, 20.0], visible=[False, False])
    factory.update_layout_records(table)
    assert records["PULSE1:LAYOUT:X"].record.get() == 55.0
    assert records["PULSE1:LAYOUT:VISIBLE"].record.get() == 0

    await client.close()


async def test_layout_records_matched_to_numbered_layout_name(
    fake_malcolm, clear_records
):
    """A single-instance block such as PCAP may be called PCAP1 by Malcolm"""
    malcolm, port = fake_malcolm
    malcolm.table["name"][1] = "PCAP1"
    client = MalcolmLayoutClient("localhost", port)
    await client.connect()
    factory = IocRecordFactory(AsyncioClient("123"), TEST_PREFIX, {})

    records = factory.create_layout_records("PCAP", ("PCAP", "PCAP1"), client)
    assert records["PCAP:LAYOUT:X"].record.get() == 20.0
    await client.close()


async def test_layout_records_created_without_malcolm(
    fake_malcolm, clear_records, caplog
):
    """If Malcolm can't be reached at startup the records are still created, and
    filled in once it connects"""
    malcolm, port = fake_malcolm
    client = MalcolmLayoutClient("localhost", unused_port())
    with pytest.raises(OSError):
        await client.connect()
    assert not client.connected
    factory = IocRecordFactory(AsyncioClient("123"), TEST_PREFIX, {})

    records = factory.create_layout_records("PULSE1", ("PULSE1",), client)
    assert records["PULSE1:LAYOUT:X"].record.get() == 0

    # Writes while disconnected are logged rather than raised
    await factory.write_layout("PULSE1", ("PULSE1",), client, "x", 5.0)
    assert "Not connected to Malcolm" in caplog.text
    assert malcolm.puts == []

    factory.invalidate_layout_records()
    names = {r.record.name for r in records.values()}
    assert factory._invalid_layout_records == names

    # Straight after a PandA reboot, Malcolm may serve a table before it has any
    # blocks, which must leave the records invalid
    factory.update_layout_records({k: [] for k in ("name", "x", "y", "visible")})
    assert factory._invalid_layout_records == names

    factory.update_layout_records(malcolm.table)
    assert factory._invalid_layout_records == set()
    assert records["PULSE1:LAYOUT:X"].record.get() == 10.0
    assert records["PULSE1:LAYOUT:Y"].record.get() == 30.0
    assert records["PULSE1:LAYOUT:VISIBLE"].record.get() == 1


async def test_layout_client_connects_and_reconnects_in_background(monkeypatch):
    """The client keeps trying to connect, and reconnects after losing the
    connection, reporting each in its callbacks"""
    monkeypatch.setattr(_layout, "_RECONNECT_PERIOD", 0.05)
    port = unused_port()
    client = MalcolmLayoutClient("localhost", port)
    updates: asyncio.Queue = asyncio.Queue()
    disconnects: asyncio.Queue = asyncio.Queue()
    client.on_update = updates.put_nowait
    client.on_disconnect = lambda: disconnects.put_nowait(True)
    client.start()
    try:
        await asyncio.sleep(0.2)
        assert not client.connected

        malcolm = FakeMalcolm()
        async with serve(malcolm.handler, "localhost", port):
            update = await asyncio.wait_for(updates.get(), 2)
            assert update["name"] == ["PULSE1", "PCAP"]
            assert client.connected
        # Server closed, which drops the connection
        await asyncio.wait_for(disconnects.get(), 2)
        assert not client.connected

        async with serve(FakeMalcolm().handler, "localhost", port):
            await asyncio.wait_for(updates.get(), 2)
            assert client.connected
    finally:
        await client.close()


async def test_layout_client_combines_changes_made_together(fake_malcolm):
    """Changes made in quick succession, e.g. caput to X then Y from a script, are
    sent in one Put so that they cannot overwrite each other"""
    malcolm, port = fake_malcolm
    client = MalcolmLayoutClient("localhost", port, **FAST)
    await client.connect()
    client.start()
    try:
        await asyncio.gather(
            client.put("PCAP", x=1.0),
            client.put("PCAP", y=2.0),
            client.put("PCAP", visible=True),
            client.put("PULSE1", x=3.0),
        )
        assert len(malcolm.puts) == 1
        assert malcolm.puts[0]["name"] == ["PCAP", "PULSE1"]
        assert row(malcolm, "PCAP") == (1.0, 2.0, True)
        assert row(malcolm, "PULSE1") == (3.0, 30.0, True)
    finally:
        await client.close()


async def test_layout_client_overlapping_puts_to_same_block(fake_malcolm):
    """Puts that overlap but are not combined must each build on the last, not on
    stale values from the cached table"""
    malcolm, port = fake_malcolm
    malcolm.stale_updates = True
    client = MalcolmLayoutClient("localhost", port, coalesce_time=0, settle_time=0.1)
    await client.connect()
    client.start()
    try:
        first = asyncio.create_task(client.put("PCAP", x=1.0))
        await asyncio.sleep(0.01)
        await asyncio.gather(first, client.put("PCAP", y=2.0))
        assert len(malcolm.puts) == 2
        assert row(malcolm, "PCAP") == (1.0, 2.0, False)
    finally:
        await client.close()


async def test_layout_client_resends_reverted_put(fake_malcolm, caplog):
    """Malcolm sometimes acknowledges a Put and then reverts it. The client should
    notice once Malcolm has settled and send it again"""
    malcolm, port = fake_malcolm
    malcolm.revert_puts = 1
    client = MalcolmLayoutClient("localhost", port, **FAST)
    await client.connect()
    client.start()
    try:
        await client.put("PULSE1", x=5.0, y=6.0)
        assert len(malcolm.puts) == 2
        assert row(malcolm, "PULSE1") == (5.0, 6.0, True)
        assert "Malcolm reverted layout change" in caplog.text

        malcolm.revert_puts = 10
        with pytest.raises(MalcolmError, match="did not keep"):
            await client.put("PULSE1", x=7.0)
    finally:
        await client.close()


async def test_layout_client_accepts_hidden_block_position_reset(fake_malcolm, caplog):
    """Malcolm doesn't keep the position of hidden blocks, which is not a reverted
    change to send again"""
    malcolm, port = fake_malcolm
    malcolm.forget_hidden_positions = True
    client = MalcolmLayoutClient("localhost", port, **FAST)
    await client.connect()
    client.start()
    try:
        await client.put("PCAP", x=5.0)
        assert len(malcolm.puts) == 1
        assert row(malcolm, "PCAP") == (0.0, 0.0, False)
        assert "reverted" not in caplog.text
    finally:
        await client.close()


def _run_layout_ioc(port: int, prefix: str, conn: Connection):
    """Run an IOC with layout records for PULSE1, in its own process as an IOC can
    only be initialised once per process"""
    dispatcher = asyncio_dispatcher.AsyncioDispatcher()

    async def create():
        # No combining or settling, so the puts overlap Malcolm's updates
        client = MalcolmLayoutClient("localhost", port, coalesce_time=0, settle_time=0)
        await client.connect()
        factory = IocRecordFactory(AsyncioClient("123"), prefix, {})
        factory.create_layout_records("PULSE1", ("PULSE1",), client)
        factory.initialise(dispatcher)
        client.on_update = factory.update_layout_records
        client.start()

    asyncio.run_coroutine_threadsafe(create(), dispatcher.loop).result()
    conn.send("R")
    while True:
        time.sleep(1)


async def test_layout_records_do_not_put_back_stale_values(
    fake_malcolm, new_random_test_prefix
):
    """Updates from Malcolm that arrive while a put is in progress, and so show the
    old value, must not be written back to Malcolm after the put completes"""
    malcolm, port = fake_malcolm
    malcolm.stale_updates = True
    parent_conn, child_conn = MULTIPROCESSING_CONTEXT.Pipe()
    process = MULTIPROCESSING_CONTEXT.Process(
        target=_run_layout_ioc, args=(port, new_random_test_prefix, child_conn)
    )
    process.start()
    try:
        # Wait in a thread, so the fake Malcolm can serve the IOC meanwhile
        await asyncio.to_thread(select_and_recv, parent_conn)
        pv = f"{new_random_test_prefix}:PULSE1:LAYOUT"
        # Each put to the fake Malcolm takes 50ms, so these overlap
        await caput(pv + ":X", 11.0)
        await asyncio.sleep(0.02)
        await caput(pv + ":Y", 22.0)
        await asyncio.sleep(0.02)
        await caput(pv + ":VISIBLE", 0)
        await asyncio.sleep(1)

        # None of the puts may send an old value back once the new one was sent.
        # With blocking records, the stale updates caused extra puts that did
        sent = [(p["x"][0], p["y"][0], p["visible"][0]) for p in malcolm.puts]
        assert sent[0] == (11.0, 30.0, True)
        for before, after in zip(sent, sent[1:], strict=False):
            for old, new, original in zip(
                before, after, (10.0, 30.0, True), strict=True
            ):
                assert not (old != original and new == original), sent
        assert len(sent) <= 3
        assert row(malcolm, "PULSE1") == (11.0, 22.0, False)
        assert await caget(pv + ":X") == 11.0
        assert await caget(pv + ":Y") == 22.0
        assert await caget(pv + ":VISIBLE") == 0
    finally:
        process.terminate()
        process.join(timeout=10)
        parent_conn.close()
        child_conn.close()
