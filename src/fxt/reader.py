from __future__ import annotations

from dataclasses import dataclass, field
from struct import unpack
from typing import BinaryIO, Protocol

from fxt.models import (
    AsyncBeginEventRecord,
    AsyncEndEventRecord,
    AsyncInstantEventRecord,
    BlobRecord,
    ContextSwitchRecord,
    CounterEventRecord,
    DurationBeginEventRecord,
    DurationCompleteEventRecord,
    DurationEndEventRecord,
    FlowBeginEventRecord,
    FlowEndEventRecord,
    FlowStepEventRecord,
    InstantEventRecord,
    KernelObjectRecord,
    LargeBlobNoMetadataRecord,
    LargeBlobWithMetadataRecord,
    LogRecord,
    ProviderRecordState,
    Record,
    RecordStateByProvider,
    Thread,
    ThreadWakeupRecord,
    UserspaceObjectRecord,
)

from .types import (
    ArgumentType,
    EventType,
    KernelObjectType,
    LargeBlobType,
    LargeRecordType,
    MetadataType,
    ProviderEventType,
    RecordType,
    SchedulingRecordType,
    TraceInfoType,
)

FXT_MAGIC = 0x0016547846040010


class FxtParseError(ValueError):
    pass


class _Readable(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


def _get_field_from_value(begin: int, end: int, value: int) -> int:
    mask = (1 << (end - begin + 1)) - 1
    return (value >> begin) & mask


def _read_exact(reader: _Readable, size: int) -> bytes:
    data = reader.read(size)
    if len(data) != size:
        raise EOFError(f"expected {size} bytes, got {len(data)}")
    return data


def _read_u64(reader: _Readable) -> int:
    return unpack("<Q", _read_exact(reader, 8))[0]


def _read_i64(reader: _Readable) -> int:
    return unpack("<q", _read_exact(reader, 8))[0]


def _read_f64(reader: _Readable) -> float:
    return unpack("<d", _read_exact(reader, 8))[0]


def _read_padded_blob(reader: _Readable, length: int) -> bytes:
    padded_len = (length + 7) & ~7
    data = _read_exact(reader, padded_len)
    return data[:length]


def _read_padded_string(reader: _Readable, length: int) -> str:
    return _read_padded_blob(reader, length).decode("utf-8")


@dataclass
class _OffsetReader:
    reader: _Readable
    current_record_offset: int = 0
    current_offset: int = 0

    def start_record(self) -> None:
        self.current_record_offset = self.current_offset

    def read(self, size: int = -1, /) -> bytes:
        data = self.reader.read(size)
        self.current_offset += len(data)
        return data

    def read_exact(self, size: int) -> bytes:
        data = self.read(size)
        if len(data) != size:
            raise EOFError(f"expected {size} bytes, got {len(data)}")
        return data


@dataclass
class _ReadState:
    num_ticks_per_second: int = 0
    string_table: dict[int, str] = field(default_factory=dict)
    thread_table: dict[int, Thread] = field(default_factory=dict)

    def ticks_to_ns(self, ticks: int) -> int:
        if self.num_ticks_per_second == 0:
            raise FxtParseError("ticks per second not initialized")
        return ticks * 1_000_000_000 // self.num_ticks_per_second

    def parse_string_record(self, header: int, reader: _OffsetReader) -> None:
        str_index = _get_field_from_value(16, 30, header)
        str_len = _get_field_from_value(32, 60, header)
        self.string_table[str_index] = _read_padded_string(reader, str_len)

    def parse_thread_record(self, header: int, reader: _OffsetReader) -> None:
        thread_index = _get_field_from_value(16, 23, header)
        process_id = _read_u64(reader)
        thread_id = _read_u64(reader)
        self.thread_table[thread_index] = Thread(process_id=process_id, thread_id=thread_id)

    def get_or_read_thread(self, thread_ref: int, reader: _OffsetReader) -> Thread:
        if thread_ref == 0:
            return Thread(process_id=_read_u64(reader), thread_id=_read_u64(reader))
        thread = self.thread_table.get(thread_ref)
        if thread is None:
            raise FxtParseError(f"record referenced unknown thread index {thread_ref}")
        return thread

    def get_or_read_string(self, str_ref: int, reader: _OffsetReader) -> str:
        if str_ref == 0:
            return ""
        if (str_ref & 0x8000) == 0x8000:
            return _read_padded_string(reader, str_ref & ~0x8000)
        value = self.string_table.get(str_ref)
        if value is None:
            raise FxtParseError(f"record referenced unknown string index {str_ref}")
        return value

    def parse_argument(self, reader: _OffsetReader) -> tuple[str, object]:
        start_offset = reader.current_offset
        header = _read_u64(reader)

        name_ref = _get_field_from_value(16, 31, header)
        name = self.get_or_read_string(name_ref, reader)

        try:
            arg_type = ArgumentType(_get_field_from_value(0, 3, header))
        except ValueError as exc:
            raise FxtParseError(f"invalid argument type {_get_field_from_value(0, 3, header)}") from exc
        value: object = None
        if arg_type == ArgumentType.NULL:
            value = None
        elif arg_type == ArgumentType.INT32:
            u32 = _get_field_from_value(32, 63, header)
            value = unpack("<i", u32.to_bytes(4, "little", signed=False))[0]
        elif arg_type == ArgumentType.UINT32:
            value = _get_field_from_value(32, 63, header)
        elif arg_type == ArgumentType.INT64:
            value = _read_i64(reader)
        elif arg_type == ArgumentType.UINT64:
            value = _read_u64(reader)
        elif arg_type == ArgumentType.DOUBLE:
            value = _read_f64(reader)
        elif arg_type == ArgumentType.STRING:
            str_ref = _get_field_from_value(32, 47, header)
            value = self.get_or_read_string(str_ref, reader)
        elif arg_type == ArgumentType.POINTER:
            value = _read_u64(reader)
        elif arg_type == ArgumentType.KOID:
            value = _read_u64(reader)
        elif arg_type == ArgumentType.BOOL:
            value = _get_field_from_value(32, 32, header) == 1

        read_size = reader.current_offset - start_offset
        expected_size = _get_field_from_value(4, 15, header) * 8
        if read_size != expected_size:
            raise FxtParseError(
                f"read incorrect number of bytes for argument at offset 0x{start_offset:x} - expected {expected_size}, got {read_size}"
            )
        return name, value

    def parse_event_record(self, header: int, reader: _OffsetReader) -> Record:
        try:
            event_type = EventType(_get_field_from_value(16, 19, header))
        except ValueError as exc:
            raise FxtParseError(f"invalid event type {_get_field_from_value(16, 19, header)}") from exc
        num_args = _get_field_from_value(20, 23, header)
        thread_ref = _get_field_from_value(24, 31, header)
        category_ref = _get_field_from_value(32, 47, header)
        name_ref = _get_field_from_value(48, 63, header)

        timestamp = _read_u64(reader)
        thread = self.get_or_read_thread(thread_ref, reader)
        category = self.get_or_read_string(category_ref, reader)
        name = self.get_or_read_string(name_ref, reader)

        args: dict[str, object] = {}
        for _ in range(num_args):
            arg_name, arg_value = self.parse_argument(reader)
            args[arg_name] = arg_value

        timestamp_ns = self.ticks_to_ns(timestamp)

        if event_type == EventType.INSTANT:
            return InstantEventRecord(timestamp_ns=timestamp_ns, category=category, name=name, thread=thread, args=args)
        if event_type == EventType.COUNTER:
            counter_id = _read_u64(reader)
            return CounterEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                counter_id=counter_id,
            )
        if event_type == EventType.DURATION_BEGIN:
            return DurationBeginEventRecord(
                timestamp_ns=timestamp_ns, category=category, name=name, thread=thread, args=args
            )
        if event_type == EventType.DURATION_END:
            return DurationEndEventRecord(
                timestamp_ns=timestamp_ns, category=category, name=name, thread=thread, args=args
            )
        if event_type == EventType.DURATION_COMPLETE:
            num_ticks = _read_u64(reader)
            return DurationCompleteEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                duration_ns=self.ticks_to_ns(num_ticks),
            )
        if event_type == EventType.ASYNC_BEGIN:
            correlation_id = _read_u64(reader)
            return AsyncBeginEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                correlation_id=correlation_id,
            )
        if event_type == EventType.ASYNC_INSTANT:
            correlation_id = _read_u64(reader)
            return AsyncInstantEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                correlation_id=correlation_id,
            )
        if event_type == EventType.ASYNC_END:
            correlation_id = _read_u64(reader)
            return AsyncEndEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                correlation_id=correlation_id,
            )
        if event_type == EventType.FLOW_BEGIN:
            correlation_id = _read_u64(reader)
            return FlowBeginEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                correlation_id=correlation_id,
            )
        if event_type == EventType.FLOW_STEP:
            correlation_id = _read_u64(reader)
            return FlowStepEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                correlation_id=correlation_id,
            )
        if event_type == EventType.FLOW_END:
            correlation_id = _read_u64(reader)
            return FlowEndEventRecord(
                timestamp_ns=timestamp_ns,
                category=category,
                name=name,
                thread=thread,
                args=args,
                correlation_id=correlation_id,
            )
        raise AssertionError("unreachable")

    def parse_blob_record(self, header: int, reader: _OffsetReader) -> Record:
        name_ref = _get_field_from_value(16, 31, header)
        payload_size = _get_field_from_value(32, 46, header)
        blob_type = _get_field_from_value(48, 55, header)
        name = self.get_or_read_string(name_ref, reader)
        payload = _read_padded_blob(reader, payload_size)
        return BlobRecord(name=name, type=blob_type, payload=payload)

    def parse_userspace_object_record(self, header: int, reader: _OffsetReader) -> Record:
        thread_ref = _get_field_from_value(16, 23, header)
        name_ref = _get_field_from_value(24, 39, header)
        num_args = _get_field_from_value(40, 43, header)

        pointer = _read_u64(reader)
        if thread_ref == 0:
            process_id = _read_u64(reader)
        else:
            thread = self.thread_table.get(thread_ref)
            if thread is None:
                raise FxtParseError(f"record referenced unknown thread index {thread_ref}")
            process_id = thread.process_id

        name = self.get_or_read_string(name_ref, reader)
        args: dict[str, object] = {}
        for _ in range(num_args):
            arg_name, arg_value = self.parse_argument(reader)
            args[arg_name] = arg_value

        return UserspaceObjectRecord(name=name, process_id=process_id, pointer=pointer, args=args)

    def parse_kernel_object_record(self, header: int, reader: _OffsetReader) -> Record:
        try:
            kernel_object_type = KernelObjectType(_get_field_from_value(16, 23, header))
        except ValueError as exc:
            raise FxtParseError(f"invalid kernel object type {_get_field_from_value(16, 23, header)}") from exc
        name_ref = _get_field_from_value(24, 39, header)
        num_args = _get_field_from_value(40, 43, header)

        koid = _read_u64(reader)
        name = self.get_or_read_string(name_ref, reader)

        args: dict[str, object] = {}
        for _ in range(num_args):
            arg_name, arg_value = self.parse_argument(reader)
            args[arg_name] = arg_value

        return KernelObjectRecord(type=kernel_object_type, id=koid, name=name, args=args)

    def parse_scheduling_record(self, header: int, reader: _OffsetReader) -> Record:
        try:
            scheduling_record_type = SchedulingRecordType(_get_field_from_value(60, 63, header))
        except ValueError as exc:
            raise FxtParseError(f"invalid scheduling type {_get_field_from_value(60, 63, header)}") from exc

        if scheduling_record_type == SchedulingRecordType.CONTEXT_SWITCH:
            num_args = _get_field_from_value(16, 19, header)
            cpu_number = _get_field_from_value(20, 35, header)
            outgoing_thread_state = _get_field_from_value(36, 39, header)

            timestamp = _read_u64(reader)
            outgoing_thread_id = _read_u64(reader)
            incoming_thread_id = _read_u64(reader)

            args: dict[str, object] = {}
            for _ in range(num_args):
                arg_name, arg_value = self.parse_argument(reader)
                args[arg_name] = arg_value

            return ContextSwitchRecord(
                timestamp_ns=self.ticks_to_ns(timestamp),
                cpu_id=cpu_number,
                outgoing_thread_id=outgoing_thread_id,
                outgoing_thread_state=outgoing_thread_state,
                incoming_thread_id=incoming_thread_id,
                args=args,
            )

        if scheduling_record_type == SchedulingRecordType.THREAD_WAKEUP:
            num_args = _get_field_from_value(16, 19, header)
            cpu_number = _get_field_from_value(20, 35, header)

            timestamp = _read_u64(reader)
            waking_thread_id = _read_u64(reader)

            wakeup_args: dict[str, object] = {}
            for _ in range(num_args):
                arg_name, arg_value = self.parse_argument(reader)
                wakeup_args[arg_name] = arg_value

            return ThreadWakeupRecord(
                timestamp_ns=self.ticks_to_ns(timestamp),
                cpu_id=cpu_number,
                waking_thread_id=waking_thread_id,
                args=wakeup_args,
            )

        raise AssertionError("unreachable")

    def parse_log_record(self, header: int, reader: _OffsetReader) -> Record:
        log_message_len = _get_field_from_value(16, 30, header)
        thread_ref = _get_field_from_value(32, 39, header)

        timestamp = _read_u64(reader)
        thread = self.get_or_read_thread(thread_ref, reader)
        message = _read_padded_string(reader, log_message_len)
        return LogRecord(timestamp_ns=self.ticks_to_ns(timestamp), thread=thread, message=message)

    def parse_large_blob_record(self, header: int, reader: _OffsetReader) -> Record:
        try:
            large_blob_type = LargeBlobType(_get_field_from_value(40, 43, header))
        except ValueError as exc:
            raise FxtParseError(f"invalid large blob format type {_get_field_from_value(40, 43, header)}") from exc
        format_header = _read_u64(reader)
        category_ref = _get_field_from_value(0, 15, format_header)
        name_ref = _get_field_from_value(16, 31, format_header)

        category = self.get_or_read_string(category_ref, reader)
        name = self.get_or_read_string(name_ref, reader)

        if large_blob_type == LargeBlobType.WITH_METADATA:
            num_args = _get_field_from_value(32, 35, format_header)
            thread_ref = _get_field_from_value(36, 43, format_header)

            timestamp = _read_u64(reader)
            thread = self.get_or_read_thread(thread_ref, reader)
            args: dict[str, object] = {}
            for _ in range(num_args):
                arg_name, arg_value = self.parse_argument(reader)
                args[arg_name] = arg_value

            blob_size = _read_u64(reader)
            payload = _read_padded_blob(reader, blob_size)

            return LargeBlobWithMetadataRecord(
                timestamp_ns=self.ticks_to_ns(timestamp),
                category=category,
                name=name,
                thread=thread,
                args=args,
                payload=payload,
            )

        if large_blob_type == LargeBlobType.NO_METADATA:
            blob_size = _read_u64(reader)
            payload = _read_padded_blob(reader, blob_size)
            return LargeBlobNoMetadataRecord(category=category, name=name, payload=payload)

        raise AssertionError("unreachable")


def parse_records(input: BinaryIO) -> RecordStateByProvider:
    """Parse an FXT stream into records grouped by provider."""
    records_by_provider: RecordStateByProvider = {}
    state_by_provider: dict[int, _ReadState] = {}

    wrapped_reader = _OffsetReader(reader=input)

    current_provider_state: ProviderRecordState | None = None
    current_read_state: _ReadState | None = None

    while True:
        wrapped_reader.start_record()

        maybe_header = wrapped_reader.read(8)
        if len(maybe_header) == 0:
            return records_by_provider
        if len(maybe_header) != 8:
            raise FxtParseError(f"failed to read record header at offset 0x{wrapped_reader.current_record_offset:x}")
        header = unpack("<Q", maybe_header)[0]

        try:
            record_type = RecordType(_get_field_from_value(0, 3, header))
        except ValueError as exc:
            raise FxtParseError(f"invalid record type {_get_field_from_value(0, 3, header)}") from exc
        record: Record | None = None

        try:
            if record_type == RecordType.METADATA:
                try:
                    metadata_type = MetadataType(_get_field_from_value(16, 19, header))
                except ValueError as exc:
                    raise FxtParseError(
                        f"invalid Metadata type {_get_field_from_value(16, 19, header)} at offset 0x{wrapped_reader.current_record_offset:x}"
                    ) from exc

                if metadata_type == MetadataType.PROVIDER_INFO:
                    provider_id = _get_field_from_value(20, 51, header)
                    name_len = _get_field_from_value(52, 59, header)
                    name = _read_padded_string(wrapped_reader, name_len)

                    if provider_id in records_by_provider:
                        raise FxtParseError(
                            f"got multiple ProviderInfo metadata records for provider ID: {provider_id}"
                        )

                    provider_state = ProviderRecordState(name=name)
                    read_state = _ReadState()
                    records_by_provider[provider_id] = provider_state
                    state_by_provider[provider_id] = read_state
                    current_provider_state = provider_state
                    current_read_state = read_state

                elif metadata_type == MetadataType.PROVIDER_SECTION:
                    provider_id = _get_field_from_value(20, 51, header)
                    provider_state = records_by_provider.get(provider_id)
                    read_state = state_by_provider.get(provider_id)
                    if provider_state is None or read_state is None:
                        raise FxtParseError(f"got ProviderSection before ProviderInfo for provider ID: {provider_id}")
                    current_provider_state = provider_state
                    current_read_state = read_state

                elif metadata_type == MetadataType.PROVIDER_EVENT:
                    provider_id = _get_field_from_value(20, 51, header)
                    try:
                        event_type = ProviderEventType(_get_field_from_value(52, 55, header))
                    except ValueError as exc:
                        raise FxtParseError(
                            f"invalid Provider Event Type {_get_field_from_value(52, 55, header)} at offset 0x{wrapped_reader.current_record_offset:x}"
                        ) from exc
                    provider_state = records_by_provider.get(provider_id)
                    if provider_state is None:
                        raise FxtParseError(f"got ProviderEvent before ProviderInfo for provider ID: {provider_id}")
                    provider_state.events.append(event_type)

                elif metadata_type == MetadataType.TRACE_INFO:
                    try:
                        trace_info_type = TraceInfoType(_get_field_from_value(20, 23, header))
                    except ValueError as exc:
                        raise FxtParseError(
                            f"invalid Trace Info Type {_get_field_from_value(20, 23, header)} at offset 0x{wrapped_reader.current_record_offset:x}"
                        ) from exc

                    if trace_info_type != TraceInfoType.MAGIC_NUMBER:
                        raise FxtParseError(
                            f"invalid Trace Info Type {trace_info_type} at offset 0x{wrapped_reader.current_record_offset:x}"
                        )
                    if header != FXT_MAGIC:
                        raise FxtParseError(
                            f"invalid FXT magic number {header:0X} at offset 0x{wrapped_reader.current_record_offset:x}"
                        )
                else:
                    raise AssertionError("unreachable")

            else:
                if current_provider_state is None or current_read_state is None:
                    raise FxtParseError(
                        f"got non-metadata record before any provider section at offset 0x{wrapped_reader.current_record_offset:x}"
                    )

                if record_type == RecordType.INITIALIZATION:
                    current_read_state.num_ticks_per_second = _read_u64(wrapped_reader)
                elif record_type == RecordType.STRING:
                    current_read_state.parse_string_record(header, wrapped_reader)
                elif record_type == RecordType.THREAD:
                    current_read_state.parse_thread_record(header, wrapped_reader)
                elif record_type == RecordType.EVENT:
                    record = current_read_state.parse_event_record(header, wrapped_reader)
                elif record_type == RecordType.BLOB:
                    record = current_read_state.parse_blob_record(header, wrapped_reader)
                elif record_type == RecordType.USERSPACE_OBJECT:
                    record = current_read_state.parse_userspace_object_record(header, wrapped_reader)
                elif record_type == RecordType.KERNEL_OBJECT:
                    record = current_read_state.parse_kernel_object_record(header, wrapped_reader)
                elif record_type == RecordType.SCHEDULING:
                    record = current_read_state.parse_scheduling_record(header, wrapped_reader)
                elif record_type == RecordType.LOG:
                    record = current_read_state.parse_log_record(header, wrapped_reader)
                elif record_type == RecordType.LARGE:
                    try:
                        large_record_type = LargeRecordType(_get_field_from_value(36, 39, header))
                    except ValueError as exc:
                        raise FxtParseError(
                            f"invalid large record type {_get_field_from_value(36, 39, header)}"
                        ) from exc

                    if large_record_type != LargeRecordType.LARGE_BLOB:
                        raise FxtParseError(f"invalid large record type {large_record_type}")
                    record = current_read_state.parse_large_blob_record(header, wrapped_reader)
                else:
                    raise AssertionError("unreachable")

            read_size = wrapped_reader.current_offset - wrapped_reader.current_record_offset
            if record_type == RecordType.LARGE:
                record_size_words = _get_field_from_value(4, 35, header)
            else:
                record_size_words = _get_field_from_value(4, 15, header)
            expected_size = record_size_words * 8

            if read_size < expected_size:
                _ = wrapped_reader.read_exact(expected_size - read_size)
            elif read_size != expected_size:
                raise FxtParseError(
                    f"read incorrect number of bytes for record at offset 0x{wrapped_reader.current_record_offset:x} - expected {expected_size}, got {read_size}"
                )

            if record is not None:
                assert current_provider_state is not None
                current_provider_state.records.append(record)
        except EOFError as exc:
            raise FxtParseError(
                f"unexpected EOF while reading record at offset 0x{wrapped_reader.current_record_offset:x}: {exc}"
            ) from exc
