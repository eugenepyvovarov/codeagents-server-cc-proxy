import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from claude_proxy.task_scheduler import (
    TaskRecord,
    TaskSchedule,
    TaskScheduler,
    TaskStore,
    PendingRun,
    compute_next_run,
    parse_task_payload,
    serialize_task,
)


class NoopTaskRunner:
    async def start_run(self, *, conversation_id: str, prompt: str, request_body: dict) -> bool:
        _ = conversation_id
        _ = prompt
        _ = request_body
        return True


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


@pytest.mark.asyncio
async def test_create_and_update_return_scheduled_next_run(tmp_path):
    store = TaskStore(tmp_path / "tasks.db")
    scheduler = TaskScheduler(store=store, task_runner=NoopTaskRunner())
    await scheduler.start()
    try:
        record = parse_task_payload(
            {
                "agent_id": "agent-123",
                "conversation_id": "conv-123",
                "cwd": "/tmp/project",
                "prompt": "scheduled ping",
                "enabled": True,
                "time_zone": "UTC",
                "schedule": {"frequency": "hourly", "interval": 1},
            }
        )

        created = await scheduler.create_task(record)

        assert created.next_run_at is not None
        persisted = await store.get_task(created.id)
        assert persisted is not None
        assert persisted.next_run_at == created.next_run_at

        updated_record = replace(
            created,
            schedule=replace(created.schedule, frequency="minutely", interval=30),
            next_run_at=None,
        )
        updated = await scheduler.update_task(created.id, updated_record)

        assert updated.next_run_at is not None
        persisted = await store.get_task(created.id)
        assert persisted is not None
        assert persisted.next_run_at == updated.next_run_at
    finally:
        await scheduler.shutdown()


@pytest.mark.asyncio
async def test_create_is_idempotent_for_client_task_id(tmp_path):
    store = TaskStore(tmp_path / "tasks.db")
    scheduler = TaskScheduler(store=store, task_runner=NoopTaskRunner())
    await scheduler.start()
    try:
        payload = {
            "agent_id": "agent-123",
            "conversation_id": "conv-123",
            "cwd": "/tmp/project",
            "prompt": "scheduled ping",
            "enabled": True,
            "time_zone": "UTC",
            "client_task_id": "local-task-123",
            "schedule": {"frequency": "hourly", "interval": 1},
        }

        first = await scheduler.create_task(parse_task_payload(payload))
        second = await scheduler.create_task(parse_task_payload(payload))

        assert second.id == first.id
        assert len(await store.list_tasks()) == 1
        assert serialize_task(second)["client_task_id"] == "local-task-123"
    finally:
        await scheduler.shutdown()


@pytest.mark.asyncio
async def test_pending_runs_coalesce_and_startup_discards_stale_entries(tmp_path):
    store = TaskStore(tmp_path / "tasks.db")
    now = datetime.now(timezone.utc)
    record = _task_record(
        time_zone="UTC",
        schedule=TaskSchedule(
            frequency="daily",
            interval=1,
            weekday_mask=1,
            monthly_mode="day_of_month",
            day_of_month=1,
            weekday_ordinal="first",
            weekday=1,
            month=1,
            time_minutes=540,
        ),
        anchor_at=now,
    )
    await store.create_task(record)

    def pending(identifier: str, enqueued_at: datetime) -> PendingRun:
        return PendingRun(
            id=identifier,
            task_id=record.id,
            agent_id=record.agent_id,
            conversation_id=record.conversation_id,
            conversation_group=None,
            cwd=record.cwd,
            prompt=record.prompt,
            request_body={},
            scheduled_at=enqueued_at,
            enqueued_at=enqueued_at,
        )

    await store.enqueue_pending(pending("first", now - timedelta(minutes=2)))
    await store.enqueue_pending(pending("second", now - timedelta(minutes=1)))
    assert (await store.pop_pending_for_cwd(record.cwd)).id == "second"

    await store.enqueue_pending(pending("stale", now - timedelta(hours=2)))
    scheduler = TaskScheduler(store=store, task_runner=NoopTaskRunner())
    await scheduler.start()
    try:
        assert await store.pop_pending_for_cwd(record.cwd) is None
    finally:
        await scheduler.shutdown()


@pytest.mark.asyncio
async def test_run_now_clears_error_and_does_not_advance_schedule(tmp_path):
    store = TaskStore(tmp_path / "tasks.db")
    scheduler = TaskScheduler(store=store, task_runner=NoopTaskRunner())
    await scheduler.start()
    try:
        anchor = datetime(2026, 3, 1, 9, 0, tzinfo=timezone.utc)
        next_run = datetime(2026, 3, 2, 9, 0, tzinfo=timezone.utc)
        record = TaskRecord(
            id="task-run-now",
            agent_id="agent",
            conversation_id="conversation",
            conversation_group=None,
            cwd="/tmp/project",
            title="Run me",
            prompt="ping",
            enabled=True,
            time_zone="UTC",
            schedule=TaskSchedule(
                frequency="daily",
                interval=1,
                weekday_mask=0,
                monthly_mode="day_of_month",
                day_of_month=1,
                weekday_ordinal="first",
                weekday=1,
                month=1,
                time_minutes=540,
            ),
            anchor_at=anchor,
            next_run_at=next_run,
            last_run_at=None,
            last_error="previous failure",
            request_body={},
            created_at=anchor,
            updated_at=anchor,
        )
        await store.create_task(record)

        started = await scheduler.run_now(record.id)
        assert started.last_error is None
        assert started.next_run_at == next_run

        # Background job should mark last_run_at without changing next_run.
        for _ in range(20):
            refreshed = await store.get_task(record.id)
            assert refreshed is not None
            assert refreshed.next_run_at == next_run
            if refreshed.last_run_at is not None and refreshed.last_error is None:
                break
            await asyncio.sleep(0.05)
        else:
            refreshed = await store.get_task(record.id)
            assert refreshed is not None
            assert refreshed.last_run_at is not None
            assert refreshed.last_error is None
            assert refreshed.next_run_at == next_run
    finally:
        await scheduler.shutdown()


def test_parse_and_compute_once_uses_next_run_at_as_anchor():
    fire = datetime(2026, 7, 15, 9, 0, tzinfo=timezone.utc)
    record = parse_task_payload(
        {
            "agent_id": "agent-123",
            "conversation_id": "conv-123",
            "cwd": "/tmp/project",
            "prompt": "one shot",
            "enabled": True,
            "time_zone": "UTC",
            "next_run_at": "2026-07-15T09:00:00Z",
            "schedule": {
                "frequency": "once",
                "time_minutes": 540,
                "day_of_month": 15,
                "month": 7,
            },
        }
    )
    assert record.schedule.frequency == "once"
    assert record.anchor_at == fire
    assert record.next_run_at == fire

    before = datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc)
    assert compute_next_run(record, after=before) == fire

    after = datetime(2026, 7, 16, 0, 0, tzinfo=timezone.utc)
    overdue = compute_next_run(record, after=after)
    assert overdue > after
    assert (overdue - after).total_seconds() <= 2


@pytest.mark.asyncio
async def test_once_task_retires_after_successful_run(tmp_path):
    store = TaskStore(tmp_path / "tasks-once.db")
    scheduler = TaskScheduler(store=store, task_runner=NoopTaskRunner())
    await scheduler.start()
    try:
        fire = datetime(2026, 7, 15, 9, 0, tzinfo=timezone.utc)
        record = parse_task_payload(
            {
                "agent_id": "agent-123",
                "conversation_id": "conv-123",
                "cwd": "/tmp/project",
                "prompt": "one shot",
                "enabled": True,
                "time_zone": "UTC",
                "next_run_at": "2026-07-15T09:00:00Z",
                "schedule": {
                    "frequency": "once",
                    "time_minutes": 540,
                    "day_of_month": 15,
                    "month": 7,
                },
            }
        )
        created = await scheduler.create_task(record)
        assert created.next_run_at is not None

        await scheduler._run_task_job(created.id, fire)

        assert await store.get_task(created.id) is None
    finally:
        await scheduler.shutdown()


@pytest.mark.asyncio
async def test_once_task_retries_after_failed_run(tmp_path):
    class FailingRunner:
        async def start_run(self, *, conversation_id: str, prompt: str, request_body: dict) -> bool:
            _ = conversation_id
            _ = prompt
            _ = request_body
            raise RuntimeError("boom")

    store = TaskStore(tmp_path / "tasks-once-fail.db")
    scheduler = TaskScheduler(store=store, task_runner=FailingRunner())
    await scheduler.start()
    try:
        fire = datetime(2026, 7, 15, 9, 0, tzinfo=timezone.utc)
        record = parse_task_payload(
            {
                "agent_id": "agent-123",
                "conversation_id": "conv-123",
                "cwd": "/tmp/project",
                "prompt": "one shot",
                "enabled": True,
                "time_zone": "UTC",
                "next_run_at": "2026-07-15T09:00:00Z",
                "schedule": {
                    "frequency": "once",
                    "time_minutes": 540,
                    "day_of_month": 15,
                    "month": 7,
                },
            }
        )
        created = await scheduler.create_task(record)
        await scheduler._run_task_job(created.id, fire)

        persisted = await store.get_task(created.id)
        assert persisted is not None
        assert persisted.enabled is True
        assert persisted.last_error is not None
        assert "boom" in (persisted.last_error or "")
        assert persisted.next_run_at is not None
        # Retry is scheduled ~5 minutes from failure time (wall clock), not from fire.
        assert persisted.next_run_at > datetime.now(timezone.utc)
    finally:
        await scheduler.shutdown()


@pytest.mark.asyncio
async def test_once_run_now_retires_after_success(tmp_path):
    store = TaskStore(tmp_path / "tasks-once-runnow.db")
    scheduler = TaskScheduler(store=store, task_runner=NoopTaskRunner())
    await scheduler.start()
    try:
        record = parse_task_payload(
            {
                "agent_id": "agent-123",
                "conversation_id": "conv-123",
                "cwd": "/tmp/project",
                "prompt": "one shot now",
                "enabled": True,
                "time_zone": "UTC",
                "next_run_at": "2099-01-01T09:00:00Z",
                "schedule": {
                    "frequency": "once",
                    "time_minutes": 540,
                    "day_of_month": 1,
                    "month": 1,
                },
            }
        )
        created = await scheduler.create_task(record)
        await scheduler.run_now(created.id)
        # run_now fires background task; wait briefly
        for _ in range(50):
            if await store.get_task(created.id) is None:
                break
            await asyncio.sleep(0.05)
        assert await store.get_task(created.id) is None
    finally:
        await scheduler.shutdown()
