from __future__ import annotations

from enum import IntEnum


class RecordType(IntEnum):
    METADATA = 0
    INITIALIZATION = 1
    STRING = 2
    THREAD = 3
    EVENT = 4
    BLOB = 5
    USERSPACE_OBJECT = 6
    KERNEL_OBJECT = 7
    SCHEDULING = 8
    LOG = 9
    LARGE = 15


class ArgumentType(IntEnum):
    NULL = 0
    INT32 = 1
    UINT32 = 2
    INT64 = 3
    UINT64 = 4
    DOUBLE = 5
    STRING = 6
    POINTER = 7
    KOID = 8
    BOOL = 9


class MetadataType(IntEnum):
    PROVIDER_INFO = 1
    PROVIDER_SECTION = 2
    PROVIDER_EVENT = 3
    TRACE_INFO = 4


class ProviderEventType(IntEnum):
    BUFFER_FILLED_UP = 0


class TraceInfoType(IntEnum):
    MAGIC_NUMBER = 0


class EventType(IntEnum):
    INSTANT = 0
    COUNTER = 1
    DURATION_BEGIN = 2
    DURATION_END = 3
    DURATION_COMPLETE = 4
    ASYNC_BEGIN = 5
    ASYNC_INSTANT = 6
    ASYNC_END = 7
    FLOW_BEGIN = 8
    FLOW_STEP = 9
    FLOW_END = 10


class KernelObjectType(IntEnum):
    PROCESS = 1
    THREAD = 2


class SchedulingRecordType(IntEnum):
    CONTEXT_SWITCH = 1
    THREAD_WAKEUP = 2


class LargeRecordType(IntEnum):
    LARGE_BLOB = 0


class LargeBlobType(IntEnum):
    WITH_METADATA = 0
    NO_METADATA = 1
