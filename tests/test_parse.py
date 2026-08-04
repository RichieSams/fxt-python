from io import BytesIO
from pathlib import Path

from fxt.models import (
    DurationBeginEventRecord,
    DurationCompleteEventRecord,
    DurationEndEventRecord,
    KernelObjectRecord,
    Thread,
)
from fxt.reader import parse_records
from fxt.transform import transform_records_to_spans


def test_parse_records_on_fixture_trace() -> None:
    trace_file = Path(__file__).resolve().parent.parent / "test_data" / "trace.fxt"
    with trace_file.open("rb") as fxt_file:
        result = parse_records(fxt_file)

    assert result.had_unexpected_eof is False
    assert result.eof_error is None

    assert result.records_by_provider
    assert any(provider_state.records for provider_state in result.records_by_provider.values())

    for provider_state in result.records_by_provider.values():
        # Ensure transform can process each provider stream end-to-end.
        transform_records_to_spans(provider_state.records)


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

    spans_by_process = transform_records_to_spans(records)

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


def test_parse_records_unexpected_eof_tolerant_mode_returns_partial_records() -> None:
    trace_file = Path(__file__).resolve().parent.parent / "test_data" / "trace.fxt"
    data = trace_file.read_bytes()
    truncated = data[:-17]

    result = parse_records(BytesIO(truncated))
    records_by_provider = result.records_by_provider

    assert result.had_unexpected_eof is True
    assert result.eof_error is not None
    assert records_by_provider
    assert any(provider_state.records for provider_state in records_by_provider.values())


def test_parse_records_unexpected_eof_status_flags_error() -> None:
    trace_file = Path(__file__).resolve().parent.parent / "test_data" / "trace.fxt"
    data = trace_file.read_bytes()
    truncated = data[:-17]

    result = parse_records(BytesIO(truncated))

    assert result.had_unexpected_eof is True
    assert result.eof_error is not None
    assert "unexpected EOF" in str(result.eof_error)
    assert result.records_by_provider
    assert any(provider_state.records for provider_state in result.records_by_provider.values())
