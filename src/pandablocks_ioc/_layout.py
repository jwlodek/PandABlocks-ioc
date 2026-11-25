# Reading and writing the block layout stored by the PandA web GUI's Malcolm server
import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

MALCOLM_PORT = 8008
LAYOUT_PATH = ["PANDA", "layout", "value"]
VISIBLE_LABELS = ["Hidden", "Visible"]

_SUBSCRIBE_ID = 0
_UPDATE_TYPEID = "malcolm:core/Update:1.0"
_ERROR_TYPEID = "malcolm:core/Error:1.0"
_RECONNECT_PERIOD = 5.0

# Changes made within this time of each other (e.g. X then Y from a script) are
# sent to Malcolm in a single Put
COALESCE_TIME = 0.1
# Malcolm stores the layout in *METADATA.LAYOUT on the PandA, and re-applies the
# layout whenever it reads that field back. A Put made while it is still reading back
# an earlier one can be reverted, or lost even though Malcolm acknowledged it. So
# give Malcolm this long after each Put before checking it stuck and sending another
SETTLE_TIME = 1.0
_PUT_ATTEMPTS = 3

LayoutTable = dict[str, Any]


class MalcolmError(Exception):
    """Raised when a layout change could not be made in Malcolm"""


class MalcolmLayoutClient:
    """Subscribes to, and writes rows of, the `PANDA.layout` table served by the
    Malcolm instance that backs the PandA web GUI.

    The table has list columns `name`, `mri`, `x`, `y` and `visible`, one row per
    block instance (e.g. "PULSE1")."""

    def __init__(
        self,
        host: str,
        port: int = MALCOLM_PORT,
        timeout: float = 5.0,
        coalesce_time: float = COALESCE_TIME,
        settle_time: float = SETTLE_TIME,
    ):
        self._url = f"ws://{host}:{port}/ws"
        self._timeout = timeout
        self._coalesce_time = coalesce_time
        self._settle_time = settle_time
        self._ws: ClientConnection | None = None
        self._next_id = _SUBSCRIBE_ID + 1
        self._pending: dict[int, asyncio.Future] = {}
        self._task: asyncio.Task | None = None
        # Changes waiting to be sent, by block name then column
        self._queued: dict[str, dict[str, Any]] = {}
        # Puts must not overlap, see SETTLE_TIME
        self._put_lock = asyncio.Lock()
        self.table: LayoutTable = {}
        # Number of subscription updates received
        self._updates = 0
        # Called with the table whenever it changes or the connection is (re)made
        self.on_update: Callable[[LayoutTable], None] | None = None
        # Called when the connection to Malcolm is lost
        self.on_disconnect: Callable[[], None] | None = None

    @property
    def connected(self) -> bool:
        return self._ws is not None

    async def connect(self) -> LayoutTable:
        """Connect, subscribe to the layout table and return its initial value"""
        ws = await connect(self._url, open_timeout=self._timeout)
        try:
            await ws.send(
                json.dumps(
                    {
                        "typeid": "malcolm:core/Subscribe:1.0",
                        "id": _SUBSCRIBE_ID,
                        "path": LAYOUT_PATH,
                        "delta": False,
                    }
                )
            )
            message = json.loads(await asyncio.wait_for(ws.recv(), self._timeout))
            if message.get("typeid") != _UPDATE_TYPEID:
                raise MalcolmError(
                    f"Unexpected response to layout subscribe: {message}"
                )
        except BaseException:
            await ws.close()
            raise

        self._ws = ws
        self.table = message["value"]
        return self.table

    def start(self) -> None:
        """Process subscription updates in the background, connecting first if not
        already connected, and reconnecting whenever the connection is lost"""
        self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
        if self._ws:
            await self._ws.close()

    async def put(self, name: str, **changes: Any) -> None:
        """Change one or more columns (`x`, `y`, `visible`) of the row for block
        instance `name`, leaving every other row untouched.

        Changes made within `coalesce_time` of each other are sent in one Put. Once
        Malcolm has had `settle_time` to settle, the Put is sent again if Malcolm
        has reverted it. Raises `MalcolmError` if the change could not be made."""
        if not self.connected:
            raise MalcolmError("Not connected to Malcolm")
        if name not in self.table["name"]:
            raise MalcolmError(f"{name} is not in the Malcolm layout")

        self._queued.setdefault(name, {}).update(changes)
        async with self._put_lock:
            if not self._queued:
                # Already sent along with an earlier change
                return
            await asyncio.sleep(self._coalesce_time)
            rows, self._queued = self._queued, {}

            for attempt in range(_PUT_ATTEMPTS):
                await self._send_put(rows)
                await asyncio.sleep(self._settle_time)
                rows = self._reverted(rows)
                if not rows:
                    return
                if attempt < _PUT_ATTEMPTS - 1:
                    logging.warning(
                        f"Malcolm reverted layout change {rows}, sending it again"
                    )
            raise MalcolmError(f"Malcolm did not keep layout change {rows}")

    def _reverted(self, rows: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Return the changes in `rows` that the table does not currently show"""
        reverted = {}
        for name, changes in rows.items():
            index = self.table["name"].index(name)
            visible = self.table["visible"][index]
            for column, value in changes.items():
                # Malcolm only stores the position of visible blocks, so resets
                # that of hidden blocks whenever the layout is changed
                if column in ("x", "y") and not visible:
                    continue
                if self.table[column][index] != value:
                    reverted[name] = changes
        return reverted

    async def _send_put(self, rows: dict[str, dict[str, Any]]) -> None:
        """Put the given changes, taking the other columns of each row from the
        cached table"""
        if self._ws is None:
            raise MalcolmError("Not connected to Malcolm")

        value: dict[str, Any] = {
            column: [] if isinstance(values, list) else values
            for column, values in self.table.items()
        }
        for name, changes in rows.items():
            index = self.table["name"].index(name)
            for column, values in self.table.items():
                if isinstance(values, list):
                    value[column].append(changes.get(column, values[index]))

        msg_id = self._next_id
        self._next_id += 1
        future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = future
        try:
            await self._ws.send(
                json.dumps(
                    {
                        "typeid": "malcolm:core/Put:1.0",
                        "id": msg_id,
                        "path": LAYOUT_PATH,
                        "value": value,
                    }
                )
            )
            updates_at_return = await asyncio.wait_for(future, self._timeout)
        finally:
            self._pending.pop(msg_id, None)

        if self._updates != updates_at_return:
            # Malcolm has reported the table since acknowledging the put, which is
            # more up to date than the cache would be with the put applied
            return
        # The subscription update for this put may not have arrived yet, so
        # apply it to the cache now for the next put to build on
        for name, changes in rows.items():
            index = self.table["name"].index(name)
            for column, new_value in changes.items():
                self.table[column][index] = new_value

    def _handle_message(self, message: dict) -> None:
        typeid = message.get("typeid")
        msg_id = message.get("id")
        if msg_id == _SUBSCRIBE_ID:
            if typeid == _UPDATE_TYPEID:
                self._updates += 1
                self.table = message["value"]
                if self.on_update:
                    self.on_update(self.table)
            elif typeid == _ERROR_TYPEID:
                logging.error(f"Malcolm layout subscription error: {message}")
            return

        future = self._pending.get(msg_id)  # type: ignore[arg-type]
        if future is None or future.done():
            return
        if typeid == _ERROR_TYPEID:
            future.set_exception(MalcolmError(message.get("message", str(message))))
        else:
            # Note how many updates had arrived when the put was acknowledged
            future.set_result(self._updates)

    async def _run(self) -> None:
        while True:
            if self._ws is None:
                try:
                    await self.connect()
                except (OSError, TimeoutError, WebSocketException, MalcolmError):
                    await asyncio.sleep(_RECONNECT_PERIOD)
                    continue
                logging.info(f"Connected to Malcolm at {self._url}")
                if self.on_update:
                    self.on_update(self.table)

            assert self._ws
            try:
                async for raw in self._ws:
                    try:
                        self._handle_message(json.loads(raw))
                    except Exception:
                        logging.exception("Error handling Malcolm layout message")
            except ConnectionClosed:
                pass

            logging.warning(
                f"Lost connection to Malcolm at {self._url}, layout records will "
                "be invalid until it is re-established"
            )
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(MalcolmError("Connection to Malcolm lost"))
            self._ws = None
            if self.on_disconnect:
                self.on_disconnect()
            await asyncio.sleep(_RECONNECT_PERIOD)
