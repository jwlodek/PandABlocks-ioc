import asyncio
from typing import Dict, List

import numpy
import numpy.testing
import pytest
from aioca import caget, camonitor, caput
from conftest import TEST_PREFIX, TIMEOUT, DummyServer
from mock import AsyncMock, patch
from mock.mock import MagicMock, PropertyMock, call
from numpy import array, ndarray
from pandablocks.asyncio import AsyncioClient
from pandablocks.commands import GetMultiline, Put
from pandablocks.responses import TableFieldDetails, TableFieldInfo
from softioc import alarm, fields

from pandablocks_ioc._tables import (
    TableFieldRecordContainer,
    TableModeEnum,
    TablePacking,
    TableUpdater,
)
from pandablocks_ioc._types import EpicsName, InErrorException, RecordInfo, RecordValue

PANDA_FORMAT_TABLE_NAME = "SEQ1.TABLE"
EPICS_FORMAT_TABLE_NAME = "SEQ1:TABLE"


@pytest.fixture
def table_data_dict(table_data: List[str]) -> Dict[EpicsName, RecordValue]:
    return {EpicsName(EPICS_FORMAT_TABLE_NAME): table_data}


@pytest.fixture
def table_fields_records(
    table_fields: Dict[str, TableFieldDetails],
    table_unpacked_data: Dict[EpicsName, ndarray],
) -> Dict[str, TableFieldRecordContainer]:
    """A faked list of records containing the table_unpacked_data"""

    data = {}
    for (field_name, field_info), data_array in zip(
        table_fields.items(), table_unpacked_data.values()
    ):
        mocked_record = MagicMock()
        type(mocked_record).name = PropertyMock(
            return_value=EPICS_FORMAT_TABLE_NAME + ":" + field_name
        )
        mocked_record.get = MagicMock(return_value=data_array)
        record_info = RecordInfo(lambda x: None)
        record_info.add_record(mocked_record)
        data[field_name] = TableFieldRecordContainer(field_info, record_info)
    return data


@pytest.fixture
def table_updater(
    table_field_info: TableFieldInfo,
    table_data_dict: Dict[EpicsName, RecordValue],
    clear_records: None,
    table_unpacked_data: Dict[EpicsName, ndarray],
) -> TableUpdater:
    """Provides a TableUpdater with configured records and mocked functionality"""
    client = AsyncioClient("123")
    client.send = AsyncMock()  # type: ignore
    # mypy doesn't play well with mocking so suppress error

    mocked_mode_record = MagicMock()
    # Default mode record to VIEW, as per default construction
    mocked_mode_record.get = MagicMock(return_value=TableModeEnum.VIEW.value)
    mocked_mode_record.set = MagicMock()
    mode_record_info = RecordInfo(
        lambda x: None,
        labels=[
            TableModeEnum.VIEW.name,
            TableModeEnum.EDIT.name,
            TableModeEnum.SUBMIT.name,
            TableModeEnum.DISCARD.name,
        ],
    )
    mode_record_info.add_record(mocked_mode_record)

    updater = TableUpdater(
        client,
        EpicsName(EPICS_FORMAT_TABLE_NAME),
        table_field_info,
        table_data_dict,
    )

    # Put mocks into TableUpdater
    updater.mode_record_info = mode_record_info
    updater.index_record = MagicMock()
    updater.index_record.name = "SEQ1:TABLE:INDEX"
    updater.table_scalar_records[EpicsName("SEQ1:TABLE:POSITION:SCALAR")] = MagicMock()
    for field_name, table_record_container in updater.table_fields_records.items():
        assert table_record_container.record_info
        table_record_container.record_info.record = MagicMock()
        type(table_record_container.record_info.record).name = PropertyMock(
            return_value=EPICS_FORMAT_TABLE_NAME + ":" + field_name
        )
        table_record_container.record_info.record.get = MagicMock(
            return_value=table_unpacked_data[EpicsName(field_name)]
        )

    return updater


@pytest.mark.asyncio
async def test_create_softioc_update_table(
    dummy_server_system: DummyServer,
    subprocess_ioc,
    table_unpacked_data,
):
    """Test that the update mechanism correctly changes table values when PandA
    reports values have changed"""

    # Add more GetChanges data. This adds two new rows and changes row 2 (1-indexed)
    # to all zero values. Include some trailing empty changesets to ensure test code has
    # time to run.
    dummy_server_system.send += [
        "!SEQ1.TABLE<\n.",
        # Deliberate concatenation here
        "!2457862149\n!4294967291\n!100\n!0\n!0\n!0\n!0\n!0\n!4293968720\n!0\n"
        "!9\n!9999\n!2035875928\n!444444\n!5\n!1\n!3464285461\n!4294967197\n!99999\n"
        "!2222\n.",
    ]
    dummy_server_system.send += ["."] * 100

    try:
        # Set up a monitor to wait for the expected change
        capturing_queue: asyncio.Queue = asyncio.Queue()
        monitor = camonitor(TEST_PREFIX + ":SEQ1:TABLE:TIME1", capturing_queue.put)

        curr_val: ndarray = await asyncio.wait_for(capturing_queue.get(), TIMEOUT)
        # First response is the current value
        assert numpy.array_equal(curr_val, table_unpacked_data["TIME1"])

        # Wait for the new value to appear
        curr_val = await asyncio.wait_for(capturing_queue.get(), TIMEOUT)
        assert numpy.array_equal(
            curr_val,
            [100, 0, 9, 5, 99999],
        )

        # And check some other columns too
        curr_val = await caget(TEST_PREFIX + ":SEQ1:TABLE:TRIGGER")
        assert numpy.array_equal(curr_val, [0, 0, 0, 9, 12])

        curr_val = await caget(TEST_PREFIX + ":SEQ1:TABLE:POSITION")
        assert numpy.array_equal(curr_val, [-5, 0, 0, 444444, -99])

        curr_val = await caget(TEST_PREFIX + ":SEQ1:TABLE:OUTD2")
        assert numpy.array_equal(curr_val, [0, 0, 1, 1, 0])

    finally:
        monitor.close()


@pytest.mark.asyncio
async def test_create_softioc_update_index_drvh(
    dummy_server_system: DummyServer,
    subprocess_ioc,
    table_unpacked_data,
):
    """Test that changing the size of the table changes the DRVH value of
    the :INDEX record"""

    # Add more GetChanges data. This adds two new rows and changes row 2 (1-indexed)
    # to all zero values. Include some trailing empty changesets to ensure test code has
    # time to run.
    dummy_server_system.send += [
        "!SEQ1.TABLE<\n.",
        # Deliberate concatenation here
        "!2457862149\n!4294967291\n!100\n!0\n!0\n!0\n!0\n!0\n!4293968720\n!0\n"
        "!9\n!9999\n!2035875928\n!444444\n!5\n!1\n!3464285461\n!4294967197\n!99999\n"
        "!2222\n.",
    ]
    dummy_server_system.send += ["."] * 100

    # All elements in the table_unpacked_data are the same length, so just take the
    # length of the first one
    table_length = len(next(iter(table_unpacked_data.values())))

    try:
        # Set up a monitor to wait for the expected change
        drvh_queue: asyncio.Queue = asyncio.Queue()
        monitor = camonitor(TEST_PREFIX + ":SEQ1:TABLE:INDEX.DRVH", drvh_queue.put)

        curr_val: int = await asyncio.wait_for(drvh_queue.get(), TIMEOUT)
        # First response is the current value (0-indexed hence -1 )
        assert curr_val == table_length - 1

        # Wait for the new value to appear
        curr_val = await asyncio.wait_for(drvh_queue.get(), TIMEOUT)
        assert curr_val == table_length + 2 - 1

    finally:
        monitor.close()


@pytest.mark.asyncio
async def test_create_softioc_table_update_send_to_panda(
    dummy_server_system: DummyServer,
    subprocess_ioc,
):
    """Test that updating a table causes the new value to be sent to PandA"""

    # Set the special response for the server
    dummy_server_system.expected_message_responses.update({"": "OK"})

    # Few more responses to GetChanges to suppress error messages
    dummy_server_system.send += [".", ".", ".", "."]

    await caput(TEST_PREFIX + ":SEQ1:TABLE:MODE", "EDIT")

    await caput(TEST_PREFIX + ":SEQ1:TABLE:REPEATS", [1, 1, 1])

    await caput(TEST_PREFIX + ":SEQ1:TABLE:MODE", "SUBMIT", wait=True, timeout=TIMEOUT)

    # Confirm the server received the expected string
    assert "" not in dummy_server_system.expected_message_responses

    # Check the three numbers that should have updated from the REPEATS column change
    assert "2457862145" in dummy_server_system.received
    assert "269877249" in dummy_server_system.received
    assert "4293918721" in dummy_server_system.received


@pytest.mark.asyncio
async def test_create_softioc_update_table_index(
    dummy_server_system: DummyServer,
    subprocess_ioc,
    table_unpacked_data,
):
    """Test that updating the INDEX updates the SCALAR values"""
    try:
        index_val = 0
        # Set up monitors to wait for the expected changes
        repeats_queue: asyncio.Queue = asyncio.Queue()
        repeats_monitor = camonitor(
            TEST_PREFIX + ":SEQ1:TABLE:REPEATS:SCALAR", repeats_queue.put
        )
        trigger_queue: asyncio.Queue = asyncio.Queue()
        trigger_monitor = camonitor(
            TEST_PREFIX + ":SEQ1:TABLE:TRIGGER:SCALAR", trigger_queue.put
        )

        # Confirm initial values are correct
        curr_val = await asyncio.wait_for(repeats_queue.get(), TIMEOUT)
        assert curr_val == table_unpacked_data["REPEATS"][index_val]
        curr_val = await asyncio.wait_for(trigger_queue.get(), TIMEOUT)
        assert curr_val == table_unpacked_data["TRIGGER"][index_val]

        # Now set a new INDEX
        index_val = 1
        await caput(TEST_PREFIX + ":SEQ1:TABLE:INDEX", index_val)

        # Wait for the new values to appear
        curr_val = await asyncio.wait_for(repeats_queue.get(), TIMEOUT)
        assert curr_val == table_unpacked_data["REPEATS"][index_val]
        curr_val = await asyncio.wait_for(trigger_queue.get(), TIMEOUT)
        assert curr_val == table_unpacked_data["TRIGGER"][index_val]

    finally:
        repeats_monitor.close()
        trigger_monitor.close()


@pytest.mark.asyncio
async def test_create_softioc_update_table_scalars_change(
    dummy_server_system: DummyServer,
    subprocess_ioc,
    table_unpacked_data,
):
    """Test that updating the data in a waveform updates the associated SCALAR value"""
    try:
        index_val = 0
        # Set up monitors to wait for the expected changes
        repeats_queue: asyncio.Queue = asyncio.Queue()
        repeats_monitor = camonitor(
            TEST_PREFIX + ":SEQ1:TABLE:REPEATS:SCALAR", repeats_queue.put
        )

        # Confirm initial values are correct
        curr_val = await asyncio.wait_for(repeats_queue.get(), TIMEOUT)
        assert curr_val == table_unpacked_data["REPEATS"][index_val]

        # Now set a new value
        await caput(TEST_PREFIX + ":SEQ1:TABLE:MODE", "EDIT")
        new_repeats_vals = [9, 99, 999]
        await caput(TEST_PREFIX + ":SEQ1:TABLE:REPEATS", new_repeats_vals)

        # Wait for the new values to appear
        curr_val = await asyncio.wait_for(repeats_queue.get(), TIMEOUT)
        assert curr_val == new_repeats_vals[index_val]

    finally:
        repeats_monitor.close()


def test_table_packing_unpack(
    table_field_info: TableFieldInfo,
    table_fields_records: Dict[str, TableFieldRecordContainer],
    table_data: List[str],
    table_unpacked_data,
):
    """Test table unpacking works as expected"""
    assert table_field_info.row_words
    unpacked = TablePacking.unpack(
        table_field_info.row_words, table_fields_records, table_data
    )

    for actual, expected in zip(unpacked, table_unpacked_data.values()):
        assert numpy.array_equal(actual, expected)


def test_table_packing_pack(
    table_field_info: TableFieldInfo,
    table_fields_records: Dict[str, TableFieldRecordContainer],
    table_data: List[str],
):
    """Test table unpacking works as expected"""
    assert table_field_info.row_words
    unpacked = TablePacking.pack(table_field_info.row_words, table_fields_records)

    for actual, expected in zip(unpacked, table_data):
        assert actual == expected


def test_table_packing_pack_length_mismatched(
    table_field_info: TableFieldInfo,
    table_fields_records: Dict[str, TableFieldRecordContainer],
):
    """Test that mismatching lengths on waveform inputs causes an exception"""
    assert table_field_info.row_words

    # Adjust one of the record lengths so it mismatches
    record_info = table_fields_records[EpicsName("OUTC1")].record_info
    assert record_info
    record_info.record.get = MagicMock(return_value=array([1, 2, 3, 4, 5, 6, 7, 8]))

    with pytest.raises(AssertionError):
        TablePacking.pack(table_field_info.row_words, table_fields_records)


def test_table_packing_roundtrip(
    table_field_info: TableFieldInfo,
    table_fields: Dict[str, TableFieldDetails],
    table_fields_records: Dict[str, TableFieldRecordContainer],
    table_data: List[str],
):
    """Test that calling unpack -> pack yields the same data"""
    assert table_field_info.row_words
    unpacked = TablePacking.unpack(
        table_field_info.row_words, table_fields_records, table_data
    )

    # Put these values into Mocks for the Records
    data: Dict[str, TableFieldRecordContainer] = {}
    for (field_name, field_info), data_array in zip(table_fields.items(), unpacked):
        mocked_record = MagicMock()
        mocked_record.get = MagicMock(return_value=data_array)
        record_info = RecordInfo(lambda x: None)
        record_info.add_record(mocked_record)
        data[field_name] = TableFieldRecordContainer(field_info, record_info)

    packed = TablePacking.pack(table_field_info.row_words, data)

    assert packed == table_data


def test_table_updater_fields_sorted(table_updater: TableUpdater):
    """Test that the field sorting done in init has occurred"""

    # Bits start at 0
    curr_bit = -1
    for field in table_updater.table_fields_records.values():
        field_details = field.field
        assert curr_bit < field_details.bit_low, "Fields are not in bit order"
        assert (
            field_details.bit_low <= field_details.bit_high  # fields may be 1 bit wide
        ), "Field had incorrect bit_low and bit_high order"
        assert (
            curr_bit < field_details.bit_high
        ), "Field had bit_high lower than bit_low"
        curr_bit = field_details.bit_high


def test_table_updater_validate_mode_view(table_updater: TableUpdater):
    """Test the validate method when mode is View"""

    # View is default in table_updater
    record = MagicMock()
    record.name = MagicMock(return_value="NewRecord")
    assert table_updater.validate_waveform(record, "value is irrelevant") is False


def test_table_updater_validate_mode_edit(table_updater: TableUpdater):
    """Test the validate method when mode is Edit"""

    table_updater.mode_record_info.record.get = MagicMock(
        return_value=TableModeEnum.EDIT.value
    )

    record = MagicMock()
    record.name = MagicMock(return_value="NewRecord")
    assert table_updater.validate_waveform(record, "value is irrelevant") is True


def test_table_updater_validate_mode_submit(table_updater: TableUpdater):
    """Test the validate method when mode is Submit"""

    table_updater.mode_record_info.record.get = MagicMock(
        return_value=TableModeEnum.SUBMIT.value
    )

    record = MagicMock()
    record.name = MagicMock(return_value="NewRecord")
    assert table_updater.validate_waveform(record, "value is irrelevant") is False


def test_table_updater_validate_mode_discard(table_updater: TableUpdater):
    """Test the validate method when mode is Discard"""

    table_updater.mode_record_info.record.get = MagicMock(
        return_value=TableModeEnum.DISCARD.value
    )

    record = MagicMock()
    record.name = MagicMock(return_value="NewRecord")
    assert table_updater.validate_waveform(record, "value is irrelevant") is False


def test_table_updater_validate_mode_unknown(table_updater: TableUpdater):
    """Test the validate method when mode is unknown"""

    table_updater.mode_record_info.record.get = MagicMock(return_value="UnknownValue")
    table_updater.mode_record_info.record.set_alarm = MagicMock()

    record = MagicMock()
    record.name = MagicMock(return_value="NewRecord")

    assert table_updater.validate_waveform(record, "value is irrelevant") is False
    table_updater.mode_record_info.record.set_alarm.assert_called_once_with(
        alarm.INVALID_ALARM, alarm.UDF_ALARM
    )


@pytest.mark.asyncio
async def test_table_updater_update_mode_view(table_updater: TableUpdater):
    """Test that update_mode with new value of VIEW takes no action"""
    await table_updater.update_mode(TableModeEnum.VIEW.value)

    assert (
        not table_updater.client.send.called  # type: ignore
    ), "client send method was unexpectedly called"
    assert (
        not table_updater.mode_record_info.record.set.called
    ), "record set method was unexpectedly called"


@pytest.mark.asyncio
async def test_table_updater_update_mode_submit(
    table_updater: TableUpdater, table_data: List[str]
):
    """Test that update_mode with new value of SUBMIT sends data to PandA"""
    await table_updater.update_mode(TableModeEnum.SUBMIT.value)

    assert isinstance(table_updater.client.send, AsyncMock)
    table_updater.client.send.assert_called_once_with(
        Put(PANDA_FORMAT_TABLE_NAME, table_data)
    )

    table_updater.mode_record_info.record.set.assert_called_once_with(
        TableModeEnum.VIEW.value, process=False
    )


@pytest.mark.asyncio
async def test_table_updater_update_mode_submit_exception(
    table_updater: TableUpdater,
    table_data: List[str],
    table_unpacked_data: Dict[EpicsName, ndarray],
):
    """Test that update_mode with new value of SUBMIT handles an exception from Put
    correctly"""

    assert isinstance(table_updater.client.send, AsyncMock)
    table_updater.client.send.side_effect = Exception("Mocked exception")

    await table_updater.update_mode(TableModeEnum.SUBMIT.value)

    table_updater.client.send.assert_called_once_with(
        Put(PANDA_FORMAT_TABLE_NAME, table_data)
    )

    # Confirm each record received the expected data
    for field_name, data in table_unpacked_data.items():
        # Note table_unpacked_data is deliberately in a different order to the sorted
        # data, hence use this lookup mechanism instead
        record_info = table_updater.table_fields_records[field_name].record_info
        assert record_info
        # numpy arrays don't play nice with mock's equality comparisons, do it ourself
        called_args = record_info.record.set.call_args

        numpy.testing.assert_array_equal(data, called_args[0][0])

    table_updater.mode_record_info.record.set.assert_called_once_with(
        TableModeEnum.VIEW.value, process=False
    )


@pytest.mark.asyncio
async def test_table_updater_update_mode_submit_exception_data_error(
    table_updater: TableUpdater, table_data: List[str]
):
    """Test that update_mode with an exception from Put and an InErrorException behaves
    as expected"""
    assert isinstance(table_updater.client.send, AsyncMock)
    table_updater.client.send.side_effect = Exception("Mocked exception")

    table_updater.all_values_dict[
        EpicsName(EPICS_FORMAT_TABLE_NAME)
    ] = InErrorException("Mocked in error exception")

    await table_updater.update_mode(TableModeEnum.SUBMIT.value)

    for field_record in table_updater.table_fields_records.values():
        assert field_record.record_info
        record = field_record.record_info.record
        record.set.assert_not_called()

    table_updater.client.send.assert_called_once_with(
        Put(PANDA_FORMAT_TABLE_NAME, table_data)
    )


@pytest.mark.asyncio
async def test_table_updater_update_mode_discard(
    table_updater: TableUpdater,
    table_data: List[str],
    table_unpacked_data: Dict[EpicsName, ndarray],
):
    """Test that update_mode with new value of DISCARD resets record data"""
    assert isinstance(table_updater.client.send, AsyncMock)
    table_updater.client.send.return_value = table_data

    await table_updater.update_mode(TableModeEnum.DISCARD.value)

    table_updater.client.send.assert_called_once_with(
        GetMultiline(PANDA_FORMAT_TABLE_NAME)
    )

    # Confirm each record received the expected data
    for field_name, data in table_unpacked_data.items():
        # Note table_unpacked_data is deliberately in a different order to the sorted
        # data, hence use this lookup mechanism instead
        record_info = table_updater.table_fields_records[field_name].record_info
        assert record_info
        # numpy arrays don't play nice with mock's equality comparisons, do it ourself
        called_args = record_info.record.set.call_args

        numpy.testing.assert_array_equal(data, called_args[0][0])

    table_updater.mode_record_info.record.set.assert_called_once_with(
        TableModeEnum.VIEW.value, process=False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "enum_val", [TableModeEnum.EDIT.value, TableModeEnum.VIEW.value]
)
async def test_table_updater_update_mode_other(
    table_updater: TableUpdater,
    table_unpacked_data: Dict[EpicsName, ndarray],
    enum_val: int,
):
    """Test that update_mode with non-SUBMIT or DISCARD values takes no action"""

    await table_updater.update_mode(enum_val)

    assert isinstance(table_updater.client.send, AsyncMock)
    table_updater.client.send.assert_not_called()

    # Confirm each record was not called
    for field_name, data in table_unpacked_data.items():
        record_info = table_updater.table_fields_records[field_name].record_info
        assert record_info

        record_info.record.assert_not_called()

    table_updater.mode_record_info.record.set.assert_not_called()


@patch("pandablocks_ioc._tables.db_put_field")
def test_table_updater_update_table(
    db_put_field: MagicMock,
    table_updater: TableUpdater,
    table_data: List[str],
    table_unpacked_data: Dict[EpicsName, ndarray],
):
    """Test that update_table updates records with the new values"""

    # update_scalar is too complex to test as well, so mock it out
    table_updater._update_scalar = MagicMock()  # type: ignore

    table_updater.update_table(table_data)

    table_updater.mode_record_info.record.get.assert_called_once()

    # Confirm each record received the expected data
    for field_name, data in table_unpacked_data.items():
        # Note table_unpacked_data is deliberately in a different order to the sorted
        # data, hence use this lookup mechanism instead
        record_info = table_updater.table_fields_records[field_name].record_info
        assert record_info
        # numpy arrays don't play nice with mock's equality comparisons, do it ourself
        called_args = record_info.record.set.call_args

        numpy.testing.assert_array_equal(data, called_args[0][0])

    table_updater._update_scalar.assert_called()

    db_put_field.assert_called_once()

    # Check the expected arguments are passed to db_put_field.
    # Note we don't check the value of `array.ctypes.data` parameter as it's a pointer
    # to a memory address so will always vary
    put_field_args = db_put_field.call_args.args
    expected_args = ["SEQ1:TABLE:INDEX.DRVH", fields.DBF_LONG, 1]
    for arg in expected_args:
        assert arg in put_field_args
    assert type(put_field_args[2]) == int


def test_table_updater_update_table_not_view(
    table_updater: TableUpdater,
    table_data: List[str],
    table_unpacked_data: Dict[EpicsName, ndarray],
):
    """Test that update_table does nothing when mode is not VIEW"""

    # update_scalar is too complex to test as well, so mock it out
    table_updater._update_scalar = MagicMock()  # type: ignore

    table_updater.mode_record_info.record.get.return_value = TableModeEnum.EDIT

    table_updater.update_table(table_data)

    table_updater.mode_record_info.record.get.assert_called_once()

    # Confirm the records were not called
    for field_name, data in table_unpacked_data.items():
        # Note table_unpacked_data is deliberately in a different order to the sorted
        # data, hence use this lookup mechanism instead
        record_info = table_updater.table_fields_records[field_name].record_info
        assert record_info
        record_info.record.set.assert_not_called()


@pytest.mark.asyncio
async def test_table_updater_update_index(
    table_updater: TableUpdater,
    table_fields: Dict[str, TableFieldDetails],
):
    """Test that update_index passes the full list of records to _update_scalar"""

    # Just need to prove it was called, not that it ran
    table_updater._update_scalar = MagicMock()  # type: ignore

    await table_updater.update_index(None)

    calls = []
    for field in table_fields.keys():
        calls.append(call(EPICS_FORMAT_TABLE_NAME + ":" + field))

    table_updater._update_scalar.assert_has_calls(calls, any_order=True)


def test_table_updater_update_scalar(
    table_updater: TableUpdater,
):
    """Test that update_scalar correctly updates the scalar record for a waveform"""
    scalar_record_name = EpicsName("SEQ1:TABLE:POSITION:SCALAR")
    scalar_record = table_updater.table_scalar_records[scalar_record_name].record

    table_updater.index_record.get.return_value = 1

    table_updater._update_scalar("ABC:SEQ1:TABLE:POSITION")

    scalar_record.set.assert_called_once_with(
        678, severity=alarm.NO_ALARM, alarm=alarm.UDF_ALARM
    )


def test_table_updater_update_scalar_index_out_of_bounds(
    table_updater: TableUpdater,
):
    """Test that update_scalar handles an invalid index"""
    scalar_record_name = EpicsName("SEQ1:TABLE:POSITION:SCALAR")
    scalar_record = table_updater.table_scalar_records[scalar_record_name].record

    table_updater.index_record.get.return_value = 99

    table_updater._update_scalar("ABC:SEQ1:TABLE:POSITION")

    scalar_record.set.assert_called_once_with(
        0, severity=alarm.INVALID_ALARM, alarm=alarm.UDF_ALARM
    )