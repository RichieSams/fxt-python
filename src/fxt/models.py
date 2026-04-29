from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeAlias

from fxt.types import ProviderEventType


@dataclass(slots=True)
class Thread:
    process_id: int
    thread_id: int


@dataclass(slots=True)
class InstantEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]


@dataclass(slots=True)
class CounterEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    counter_id: int


@dataclass(slots=True)
class DurationBeginEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]


@dataclass(slots=True)
class DurationEndEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]


@dataclass(slots=True)
class DurationCompleteEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    duration_ns: int


@dataclass(slots=True)
class AsyncBeginEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    correlation_id: int


@dataclass(slots=True)
class AsyncInstantEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    correlation_id: int


@dataclass(slots=True)
class AsyncEndEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    correlation_id: int


@dataclass(slots=True)
class FlowBeginEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    correlation_id: int


@dataclass(slots=True)
class FlowStepEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    correlation_id: int


@dataclass(slots=True)
class FlowEndEventRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    correlation_id: int


@dataclass(slots=True)
class BlobRecord:
    name: str
    type: int
    payload: bytes


@dataclass(slots=True)
class UserspaceObjectRecord:
    name: str
    process_id: int
    pointer: int
    args: dict[str, object]


@dataclass(slots=True)
class KernelObjectRecord:
    type: int
    id: int
    name: str
    args: dict[str, object]


@dataclass(slots=True)
class ContextSwitchRecord:
    timestamp_ns: int
    cpu_id: int
    outgoing_thread_id: int
    outgoing_thread_state: int
    incoming_thread_id: int
    args: dict[str, object]


@dataclass(slots=True)
class ThreadWakeupRecord:
    timestamp_ns: int
    cpu_id: int
    waking_thread_id: int
    args: dict[str, object]


@dataclass(slots=True)
class LogRecord:
    timestamp_ns: int
    thread: Thread
    message: str


@dataclass(slots=True)
class LargeBlobWithMetadataRecord:
    timestamp_ns: int
    category: str
    name: str
    thread: Thread
    args: dict[str, object]
    payload: bytes


@dataclass(slots=True)
class LargeBlobNoMetadataRecord:
    category: str
    name: str
    payload: bytes


Record: TypeAlias = (
    InstantEventRecord
    | CounterEventRecord
    | DurationBeginEventRecord
    | DurationEndEventRecord
    | DurationCompleteEventRecord
    | AsyncBeginEventRecord
    | AsyncInstantEventRecord
    | AsyncEndEventRecord
    | FlowBeginEventRecord
    | FlowStepEventRecord
    | FlowEndEventRecord
    | BlobRecord
    | UserspaceObjectRecord
    | KernelObjectRecord
    | ContextSwitchRecord
    | ThreadWakeupRecord
    | LogRecord
    | LargeBlobWithMetadataRecord
    | LargeBlobNoMetadataRecord
)


@dataclass(slots=True)
class ProviderRecordState:
    name: str
    records: list[Record] = field(default_factory=list)
    events: list[ProviderEventType] = field(default_factory=list)


RecordStateByProvider: TypeAlias = dict[int, ProviderRecordState]


@dataclass(slots=True)
class Span:
    timestamp_ns: int
    category: str
    name: str
    duration_ns: int
    args: dict[str, object]
    parent: Span | None = None
    children: list[Span] = field(default_factory=list)


@dataclass(slots=True)
class ThreadState:
    name: str
    spans: list[Span]


@dataclass(slots=True)
class ProcessState:
    name: str
    threads: dict[int, ThreadState]


SpansByProcess: TypeAlias = dict[int, ProcessState]
