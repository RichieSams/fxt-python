from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from fxt.models import (
    DurationBeginEventRecord,
    DurationCompleteEventRecord,
    DurationEndEventRecord,
    KernelObjectRecord,
    ProcessState,
    Record,
    Span,
    SpansByProcess,
    ThreadState,
)
from fxt.types import KernelObjectType


class FxtTransformError(ValueError):
    pass


@dataclass
class _TransformThreadState:
    span_stack: list[Span] = field(default_factory=list)
    spans: list[Span] = field(default_factory=list)


def transform_records_to_spans(records: Sequence[Record]) -> SpansByProcess:
    """Convert parsed duration records into nested spans grouped by process/thread."""
    process_names: dict[int, str] = {}
    thread_names: dict[int, str] = {}

    filtered_records: list[DurationBeginEventRecord | DurationEndEventRecord | DurationCompleteEventRecord] = []

    for generic_record in records:
        if isinstance(generic_record, KernelObjectRecord):
            if generic_record.type == KernelObjectType.PROCESS:
                process_names[generic_record.id] = generic_record.name
            elif generic_record.type == KernelObjectType.THREAD:
                thread_names[generic_record.id] = generic_record.name
            else:
                raise FxtTransformError(f"unknown Kernel Object Record type: {generic_record.type}")
        elif isinstance(
            generic_record,
            DurationBeginEventRecord | DurationEndEventRecord | DurationCompleteEventRecord,
        ):
            filtered_records.append(generic_record)

    if not filtered_records:
        return {}

    filtered_records.sort(key=lambda record: record.timestamp_ns)
    max_timestamp_ns = filtered_records[-1].timestamp_ns

    processes: dict[int, dict[int, _TransformThreadState]] = {}

    for generic_record in filtered_records:
        if isinstance(generic_record, DurationBeginEventRecord):
            span = Span(
                timestamp_ns=generic_record.timestamp_ns,
                category=generic_record.category,
                name=generic_record.name,
                duration_ns=0,
                args=dict(generic_record.args),
            )

            process_map = processes.setdefault(generic_record.thread.process_id, {})
            thread_state = process_map.setdefault(generic_record.thread.thread_id, _TransformThreadState())

            if not thread_state.span_stack:
                thread_state.span_stack.append(span)
                thread_state.spans.append(span)
            else:
                parent = thread_state.span_stack[-1]
                span.parent = parent
                parent.children.append(span)
                thread_state.span_stack.append(span)

        elif isinstance(generic_record, DurationEndEventRecord):
            process_map = processes.get(generic_record.thread.process_id)
            if process_map is None:
                raise FxtTransformError(
                    f"invalid DurationEndEventRecord {generic_record.category}:{generic_record.name} - ProcessID {generic_record.thread.process_id} is not yet known"
                )

            thread_state = process_map.get(generic_record.thread.thread_id)
            if thread_state is None:
                raise FxtTransformError(
                    f"invalid DurationEndEventRecord {generic_record.category}:{generic_record.name} - ThreadID {generic_record.thread.thread_id} is not yet known"
                )

            if not thread_state.span_stack:
                raise FxtTransformError(
                    f"invalid DurationEndEventRecord {generic_record.category}:{generic_record.name} - no matching DurationBeginEventRecord"
                )

            span = thread_state.span_stack.pop()
            if generic_record.category != span.category or generic_record.name != span.name:
                raise FxtTransformError(
                    f"invalid DurationEndEventRecord {generic_record.category}:{generic_record.name} - no matching DurationBeginEventRecord"
                )

            span.duration_ns = generic_record.timestamp_ns - span.timestamp_ns
            span.args.update(generic_record.args)

        else:
            span = Span(
                timestamp_ns=generic_record.timestamp_ns,
                category=generic_record.category,
                name=generic_record.name,
                duration_ns=generic_record.duration_ns,
                args=dict(generic_record.args),
            )

            process_map = processes.setdefault(generic_record.thread.process_id, {})
            thread_state = process_map.setdefault(generic_record.thread.thread_id, _TransformThreadState())

            if not thread_state.span_stack:
                thread_state.spans.append(span)
            else:
                parent = thread_state.span_stack[-1]
                span.parent = parent
                parent.children.append(span)

    output: SpansByProcess = {}
    for process_id, spans_by_thread in processes.items():
        process_name = process_names.get(process_id, str(process_id))
        process_state = ProcessState(name=process_name, threads={})

        for thread_id, thread_state in spans_by_thread.items():
            while thread_state.span_stack:
                span = thread_state.span_stack.pop()
                span.duration_ns = max_timestamp_ns - span.timestamp_ns

            thread_name = thread_names.get(thread_id, str(thread_id))
            process_state.threads[thread_id] = ThreadState(name=thread_name, spans=thread_state.spans)

        output[process_id] = process_state

    return output
