from datetime import datetime, timezone

import pytest

from claude_proxy.task_scheduler import TaskRecord, TaskSchedule, compute_next_run, parse_task_payload


def _task_record(*, time_zone: str, schedule: TaskSchedule, anchor_at: datetime) -> TaskRecord:
    return TaskRecord(
        id="task-1",
        agent_id="agent",
        conversation_id="conversation",
        conversation_group=None,
        cwd="/tmp/project",
        title="",
        prompt="ping",
        enabled=True,
        time_zone=time_zone,
        schedule=schedule,
        anchor_at=anchor_at,
        next_run_at=None,
        last_run_at=None,
        last_error=None,
        request_body={},
        created_at=anchor_at,
        updated_at=anchor_at,
    )


def test_parse_task_payload_accepts_minutely_hourly_and_clamps_interval():
    base_payload = {
        "agent_id": "agent-123",
        "conversation_id": "conv-123",
        "cwd": "/tmp/project",
        "prompt": "scheduled ping",
        "enabled": True,
        "time_zone": "UTC",
    }

    record = parse_task_payload({**base_payload, "schedule": {"frequency": "minutely", "interval": 120}})
    assert record.schedule.frequency == "minutely"
    assert record.schedule.interval == 60

    record = parse_task_payload({**base_payload, "schedule": {"frequency": "hourly", "interval": 999}})
    assert record.schedule.frequency == "hourly"
    assert record.schedule.interval == 24


def test_compute_next_run_minutely_uses_anchor_day_start_for_interval():
    anchor_at = datetime(2026, 1, 30, 13, 6, 30, tzinfo=timezone.utc)
    schedule = TaskSchedule(
        frequency="minutely",
        interval=15,
        weekday_mask=0,
        monthly_mode="day_of_month",
        day_of_month=1,
        weekday_ordinal="first",
        weekday=1,
        month=1,
        time_minutes=0,
    )
    task = _task_record(time_zone="UTC", schedule=schedule, anchor_at=anchor_at)

    after = datetime(2026, 1, 30, 21, 52, 12, tzinfo=timezone.utc)
    next_run = compute_next_run(task, after=after)

    assert next_run == datetime(2026, 1, 30, 22, 0, 0, tzinfo=timezone.utc)


def test_compute_next_run_hourly_keeps_fixed_cadence_when_interval_not_divisor():
    anchor_at = datetime(2026, 1, 30, 10, 0, 0, tzinfo=timezone.utc)
    schedule = TaskSchedule(
        frequency="hourly",
        interval=23,
        weekday_mask=0,
        monthly_mode="day_of_month",
        day_of_month=1,
        weekday_ordinal="first",
        weekday=1,
        month=1,
        time_minutes=0,
    )
    task = _task_record(time_zone="UTC", schedule=schedule, anchor_at=anchor_at)

    after = datetime(2026, 1, 30, 21, 52, 12, tzinfo=timezone.utc)
    first = compute_next_run(task, after=after)
    second = compute_next_run(task, after=first)

    assert first == datetime(2026, 1, 30, 23, 0, 0, tzinfo=timezone.utc)
    assert (second - first).total_seconds() == pytest.approx(23 * 3600)
