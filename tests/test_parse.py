from pathlib import Path

from fxt.models import (
    DurationBeginEventRecord,
    DurationCompleteEventRecord,
    DurationEndEventRecord,
    KernelObjectRecord,
    Thread,
)
from fxt.reader import ParseRecords
from fxt.transform import TransformRecordsToSpans


def test_parse_records_on_fixture_trace() -> None:
    trace_file = Path(__file__).resolve().parent.parent / "test_data" / "trace.fxt"
    with trace_file.open("rb") as fxt_file:
        records_by_provider = ParseRecords(fxt_file)

    assert records_by_provider
    assert any(provider_state.records for provider_state in records_by_provider.values())

    for provider_state in records_by_provider.values():
        # Ensure transform can process each provider stream end-to-end.
        TransformRecordsToSpans(provider_state.records)


def test_transform_records_to_spans_nesting_and_close() -> None:
    records = [
        KernelObjectRecord(type=1, id=100, name="proc", args={}),
        KernelObjectRecord(type=2, id=200, name="thread", args={}),
        DurationBeginEventRecord(
            timestamp_ns=100,
            category="cat",
            name="outer",
            thread=Thread(process_id=100, thread_id=200),
            args={"a": 1},
        ),
        DurationCompleteEventRecord(
            timestamp_ns=120,
            category="cat",
            name="inner",
            thread=Thread(process_id=100, thread_id=200),
            args={"b": 2},
            duration_ns=10,
        ),
        DurationEndEventRecord(
            timestamp_ns=150,
            category="cat",
            name="outer",
            thread=Thread(process_id=100, thread_id=200),
            args={"c": 3},
        ),
    ]

    spans_by_process = TransformRecordsToSpans(records)

    assert list(spans_by_process.keys()) == [100]
    process = spans_by_process[100]
    assert process.name == "proc"
    thread = process.threads[200]
    assert thread.name == "thread"

    assert len(thread.spans) == 1
    outer = thread.spans[0]
    assert outer.name == "outer"
    assert outer.args == {"a": 1, "c": 3}
    assert outer.duration_ns == 50

    assert len(outer.children) == 1
    inner = outer.children[0]
    assert inner.name == "inner"
    assert inner.duration_ns == 10
