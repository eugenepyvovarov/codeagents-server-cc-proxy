from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from claude_proxy.conversation_manager import AgentFolderBusyError, ConversationManager
from claude_proxy.util import sanitize_id

logger = logging.getLogger(__name__)
_MISSING = object()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day


def _weekday_number(value: date) -> int:
    return ((value.weekday() + 1) % 7) + 1


def _combine_local(value: date, time_minutes: int, tz: ZoneInfo) -> datetime:
    hour = max(0, min(time_minutes // 60, 23))
    minute = max(0, min(time_minutes % 60, 59))
    return datetime(value.year, value.month, value.day, hour, minute, tzinfo=tz)


def _month_index(value: date) -> int:
    return value.year * 12 + (value.month - 1)


def _date_from_month_index(index: int) -> date:
    year = index // 12
    month = (index % 12) + 1
    return date(year, month, 1)


def _nth_weekday_in_month(year: int, month: int, weekday: int, ordinal: str) -> date:
    if ordinal == "last":
        current = date(year, month, _last_day_of_month(year, month))
        while _weekday_number(current) != weekday:
            current -= timedelta(days=1)
        return current

    offset_map = {"first": 0, "second": 1, "third": 2, "fourth": 3}
    week_offset = offset_map.get(ordinal, 0)
    current = date(year, month, 1)
    while _weekday_number(current) != weekday:
        current += timedelta(days=1)
    current += timedelta(days=week_offset * 7)
    if current.month != month:
        current -= timedelta(days=7)
    return current


@dataclass(frozen=True)
class TaskSchedule:
    frequency: str
    interval: int
    weekday_mask: int
    monthly_mode: str
    day_of_month: int
    weekday_ordinal: str
    weekday: int
    month: int
    time_minutes: int


@dataclass(frozen=True)
class TaskRecord:
    id: str
    agent_id: str
    conversation_id: str
    conversation_group: str | None
    cwd: str
    title: str
    prompt: str
    enabled: bool
    time_zone: str
    schedule: TaskSchedule
    anchor_at: datetime
    next_run_at: datetime | None
    last_run_at: datetime | None
    last_error: str | None
    request_body: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PendingRun:
    id: str
    task_id: str
    agent_id: str
    conversation_id: str
    conversation_group: str | None
    cwd: str
    prompt: str
    request_body: dict[str, Any]
    scheduled_at: datetime | None
    enqueued_at: datetime


class TaskValidationError(ValueError):
    pass


def _normalize_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _normalize_int(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _sanitize_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TaskValidationError(f"{field_name} must be a string")
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed


def _normalize_time_zone(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        return "UTC"
    zone = value.strip()
    try:
        ZoneInfo(zone)
    except Exception as exc:
        raise TaskValidationError(f"Invalid time_zone: {zone}") from exc
    return zone


def _normalize_schedule(value: Any, *, now: datetime) -> TaskSchedule:
    if not isinstance(value, dict):
        raise TaskValidationError("schedule must be an object")

    frequency = str(value.get("frequency") or "daily").strip().lower()
    if frequency not in {"daily", "weekly", "monthly", "yearly"}:
        raise TaskValidationError("schedule.frequency must be daily, weekly, monthly, or yearly")

    interval = max(1, _normalize_int(value.get("interval"), 1))
    weekday_mask = _normalize_int(value.get("weekday_mask"), 0)
    if weekday_mask <= 0:
        weekday_mask = 1 << (_weekday_number(now.date()) - 1)

    monthly_mode = str(value.get("monthly_mode") or "day_of_month").strip().lower()
    if monthly_mode not in {"day_of_month", "ordinal_weekday"}:
        monthly_mode = "day_of_month"

    day_of_month = _normalize_int(value.get("day_of_month"), now.day)
    day_of_month = min(max(day_of_month, 1), 31)

    weekday_ordinal = str(value.get("weekday_ordinal") or "first").strip().lower()
    if weekday_ordinal not in {"first", "second", "third", "fourth", "last"}:
        weekday_ordinal = "first"

    weekday = _normalize_int(value.get("weekday"), _weekday_number(now.date()))
    if weekday not in range(1, 8):
        weekday = _weekday_number(now.date())

    month = _normalize_int(value.get("month"), now.month)
    month = min(max(month, 1), 12)

    time_minutes = _normalize_int(value.get("time_minutes"), now.hour * 60 + now.minute)
    time_minutes = max(0, min(time_minutes, 1_439))

    return TaskSchedule(
        frequency=frequency,
        interval=interval,
        weekday_mask=weekday_mask,
        monthly_mode=monthly_mode,
        day_of_month=day_of_month,
        weekday_ordinal=weekday_ordinal,
        weekday=weekday,
        month=month,
        time_minutes=time_minutes,
    )


def _build_request_body(payload: dict[str, Any]) -> dict[str, Any]:
    filtered = dict(payload)
    for key in ("schedule", "enabled", "title", "prompt", "text"):
        filtered.pop(key, None)
    filtered.pop("agent_id", None)
    return filtered


def parse_task_payload(payload: dict[str, Any]) -> TaskRecord:
    if not isinstance(payload, dict):
        raise TaskValidationError("Payload must be an object")

    agent_id = _sanitize_string(payload.get("agent_id"), "agent_id")
    if not agent_id:
        raise TaskValidationError("agent_id is required")
    agent_id = sanitize_id(agent_id)

    conversation_id = _sanitize_string(payload.get("conversation_id") or payload.get("session_id"), "conversation_id")
    if not conversation_id:
        raise TaskValidationError("conversation_id is required")
    conversation_id = sanitize_id(conversation_id)

    conversation_group = _sanitize_string(payload.get("conversation_group"), "conversation_group")
    if conversation_group:
        conversation_group = sanitize_id(conversation_group)

    cwd = _sanitize_string(payload.get("cwd"), "cwd")
    if not cwd:
        raise TaskValidationError("cwd is required")

    title = _sanitize_string(payload.get("title"), "title") or ""
    prompt = _sanitize_string(payload.get("prompt") or payload.get("text"), "prompt")
    if not prompt:
        raise TaskValidationError("prompt is required")

    enabled = _normalize_bool(payload.get("enabled"), True)
    time_zone = _normalize_time_zone(payload.get("time_zone"))

    now = _now_utc().astimezone(ZoneInfo(time_zone))
    schedule = _normalize_schedule(payload.get("schedule"), now=now)
    request_body = _build_request_body(payload)

    now_utc = _now_utc()
    return TaskRecord(
        id=uuid.uuid4().hex,
        agent_id=agent_id,
        conversation_id=conversation_id,
        conversation_group=conversation_group,
        cwd=cwd,
        title=title,
        prompt=prompt,
        enabled=enabled,
        time_zone=time_zone,
        schedule=schedule,
        anchor_at=now_utc,
        next_run_at=None,
        last_run_at=None,
        last_error=None,
        request_body=request_body,
        created_at=now_utc,
        updated_at=now_utc,
    )


def update_task_from_payload(existing: TaskRecord, payload: dict[str, Any]) -> TaskRecord:
    if not isinstance(payload, dict):
        raise TaskValidationError("Payload must be an object")

    title = existing.title
    if "title" in payload:
        title = _sanitize_string(payload.get("title"), "title") or ""

    prompt = existing.prompt
    if "prompt" in payload or "text" in payload:
        prompt = _sanitize_string(payload.get("prompt") or payload.get("text"), "prompt")
        if not prompt:
            raise TaskValidationError("prompt is required")

    enabled = existing.enabled
    if "enabled" in payload:
        enabled = _normalize_bool(payload.get("enabled"), existing.enabled)

    time_zone = existing.time_zone
    if "time_zone" in payload:
        time_zone = _normalize_time_zone(payload.get("time_zone"))

    cwd = existing.cwd
    if "cwd" in payload:
        cwd_value = _sanitize_string(payload.get("cwd"), "cwd")
        if not cwd_value:
            raise TaskValidationError("cwd is required")
        cwd = cwd_value

    conversation_group = existing.conversation_group
    if "conversation_group" in payload:
        group_value = _sanitize_string(payload.get("conversation_group"), "conversation_group")
        conversation_group = sanitize_id(group_value) if group_value else None

    schedule = existing.schedule
    if "schedule" in payload:
        now_local = _now_utc().astimezone(ZoneInfo(time_zone))
        schedule = _normalize_schedule(payload.get("schedule"), now=now_local)

    request_body = dict(existing.request_body)
    filtered = _build_request_body(payload)
    if filtered:
        request_body.update(filtered)

    request_body.setdefault("cwd", cwd)
    if conversation_group:
        request_body.setdefault("conversation_group", conversation_group)

    anchor_at = existing.anchor_at
    if schedule != existing.schedule or time_zone != existing.time_zone:
        anchor_at = _now_utc()

    now_utc = _now_utc()
    return TaskRecord(
        id=existing.id,
        agent_id=existing.agent_id,
        conversation_id=existing.conversation_id,
        conversation_group=conversation_group,
        cwd=cwd,
        title=title,
        prompt=prompt,
        enabled=enabled,
        time_zone=time_zone,
        schedule=schedule,
        anchor_at=anchor_at,
        next_run_at=existing.next_run_at,
        last_run_at=existing.last_run_at,
        last_error=existing.last_error,
        request_body=request_body,
        created_at=existing.created_at,
        updated_at=now_utc,
    )


def _schedule_to_dict(schedule: TaskSchedule) -> dict[str, Any]:
    return {
        "frequency": schedule.frequency,
        "interval": schedule.interval,
        "weekday_mask": schedule.weekday_mask,
        "monthly_mode": schedule.monthly_mode,
        "day_of_month": schedule.day_of_month,
        "weekday_ordinal": schedule.weekday_ordinal,
        "weekday": schedule.weekday,
        "month": schedule.month,
        "time_minutes": schedule.time_minutes,
    }


def serialize_task(task: TaskRecord) -> dict[str, Any]:
    return {
        "id": task.id,
        "agent_id": task.agent_id,
        "conversation_id": task.conversation_id,
        "conversation_group": task.conversation_group,
        "cwd": task.cwd,
        "title": task.title,
        "prompt": task.prompt,
        "enabled": task.enabled,
        "time_zone": task.time_zone,
        "schedule": _schedule_to_dict(task.schedule),
        "next_run_at": _iso_utc(task.next_run_at),
        "last_run_at": _iso_utc(task.last_run_at),
        "last_error": task.last_error,
        "created_at": _iso_utc(task.created_at),
        "updated_at": _iso_utc(task.updated_at),
    }


class TaskStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    conversation_group TEXT,
                    cwd TEXT NOT NULL,
                    title TEXT,
                    prompt TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    time_zone TEXT NOT NULL,
                    frequency TEXT NOT NULL,
                    interval INTEGER NOT NULL,
                    weekday_mask INTEGER NOT NULL,
                    monthly_mode TEXT NOT NULL,
                    day_of_month INTEGER NOT NULL,
                    weekday_ordinal TEXT NOT NULL,
                    weekday INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    time_minutes INTEGER NOT NULL,
                    anchor_at TEXT NOT NULL,
                    next_run_at TEXT,
                    last_run_at TEXT,
                    last_error TEXT,
                    request_body_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_agent_id ON tasks(agent_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_conversation_id ON tasks(conversation_id);

                CREATE TABLE IF NOT EXISTS pending_runs (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    conversation_group TEXT,
                    cwd TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    request_body_json TEXT,
                    scheduled_at TEXT,
                    enqueued_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_pending_runs_cwd ON pending_runs(cwd);
                """
            )

    async def list_tasks(
        self,
        *,
        agent_id: str | None = None,
        conversation_id: str | None = None,
        conversation_group: str | None = None,
    ) -> list[TaskRecord]:
        async with self._lock:
            return await asyncio.to_thread(self._list_tasks_sync, agent_id, conversation_id, conversation_group)

    def _list_tasks_sync(
        self,
        agent_id: str | None,
        conversation_id: str | None,
        conversation_group: str | None,
    ) -> list[TaskRecord]:
        conditions = []
        params: list[Any] = []
        if agent_id:
            conditions.append("agent_id = ?")
            params.append(agent_id)
        if conversation_id:
            conditions.append("conversation_id = ?")
            params.append(conversation_id)
        if conversation_group:
            conditions.append("conversation_group = ?")
            params.append(conversation_group)

        query = "SELECT * FROM tasks"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY created_at DESC"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_task(row) for row in rows]

    async def get_task(self, task_id: str) -> TaskRecord | None:
        async with self._lock:
            return await asyncio.to_thread(self._get_task_sync, task_id)

    def _get_task_sync(self, task_id: str) -> TaskRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_task(row)

    async def create_task(self, record: TaskRecord) -> TaskRecord:
        async with self._lock:
            return await asyncio.to_thread(self._create_task_sync, record)

    def _create_task_sync(self, record: TaskRecord) -> TaskRecord:
        payload = self._task_to_row(record)
        columns = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        with self._connect() as conn:
            conn.execute(
                f"INSERT INTO tasks ({columns}) VALUES ({placeholders})",
                list(payload.values()),
            )
        return record

    async def update_task(self, task_id: str, record: TaskRecord) -> TaskRecord:
        async with self._lock:
            return await asyncio.to_thread(self._update_task_sync, task_id, record)

    def _update_task_sync(self, task_id: str, record: TaskRecord) -> TaskRecord:
        payload = self._task_to_row(record)
        assignments = ", ".join([f"{key} = ?" for key in payload.keys()])
        values = list(payload.values())
        values.append(task_id)
        with self._connect() as conn:
            conn.execute(f"UPDATE tasks SET {assignments} WHERE id = ?", values)
        return record

    async def delete_task(self, task_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._delete_task_sync, task_id)

    def _delete_task_sync(self, task_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.execute("DELETE FROM pending_runs WHERE task_id = ?", (task_id,))

    async def update_run_times(
        self,
        task_id: str,
        *,
        next_run_at: datetime | None | object = _MISSING,
        last_run_at: datetime | None | object = _MISSING,
        last_error: str | None | object = _MISSING,
    ) -> TaskRecord | None:
        async with self._lock:
            return await asyncio.to_thread(self._update_run_times_sync, task_id, next_run_at, last_run_at, last_error)

    def _update_run_times_sync(
        self,
        task_id: str,
        next_run_at: datetime | None | object,
        last_run_at: datetime | None | object,
        last_error: str | None | object,
    ) -> TaskRecord | None:
        with self._connect() as conn:
            current = conn.execute(
                "SELECT next_run_at, last_run_at, last_error FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if current is None:
                return None

            next_value = current["next_run_at"] if next_run_at is _MISSING else _iso_utc(next_run_at)
            last_value = current["last_run_at"] if last_run_at is _MISSING else _iso_utc(last_run_at)
            error_value = current["last_error"] if last_error is _MISSING else last_error

            conn.execute(
                """
                UPDATE tasks
                SET next_run_at = ?,
                    last_run_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    next_value,
                    last_value,
                    error_value,
                    _iso_utc(_now_utc()),
                    task_id,
                ),
            )
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return self._row_to_task(row)

    async def enqueue_pending(self, pending: PendingRun) -> None:
        async with self._lock:
            await asyncio.to_thread(self._enqueue_pending_sync, pending)

    def _enqueue_pending_sync(self, pending: PendingRun) -> None:
        payload = {
            "id": pending.id,
            "task_id": pending.task_id,
            "agent_id": pending.agent_id,
            "conversation_id": pending.conversation_id,
            "conversation_group": pending.conversation_group,
            "cwd": pending.cwd,
            "prompt": pending.prompt,
            "request_body_json": json.dumps(pending.request_body, separators=(",", ":")),
            "scheduled_at": _iso_utc(pending.scheduled_at),
            "enqueued_at": _iso_utc(pending.enqueued_at),
        }
        columns = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        with self._connect() as conn:
            conn.execute(
                f"INSERT INTO pending_runs ({columns}) VALUES ({placeholders})",
                list(payload.values()),
            )

    async def pop_pending_for_cwd(self, cwd: str) -> PendingRun | None:
        async with self._lock:
            return await asyncio.to_thread(self._pop_pending_for_cwd_sync, cwd)

    def _pop_pending_for_cwd_sync(self, cwd: str) -> PendingRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM pending_runs
                WHERE cwd = ?
                ORDER BY enqueued_at ASC
                LIMIT 1
                """,
                (cwd,),
            ).fetchone()
            if row is None:
                return None
            conn.execute("DELETE FROM pending_runs WHERE id = ?", (row["id"],))
        return self._row_to_pending(row)

    def _row_to_task(self, row: sqlite3.Row) -> TaskRecord:
        schedule = TaskSchedule(
            frequency=row["frequency"],
            interval=row["interval"],
            weekday_mask=row["weekday_mask"],
            monthly_mode=row["monthly_mode"],
            day_of_month=row["day_of_month"],
            weekday_ordinal=row["weekday_ordinal"],
            weekday=row["weekday"],
            month=row["month"],
            time_minutes=row["time_minutes"],
        )
        request_body: dict[str, Any] = {}
        if row["request_body_json"]:
            try:
                request_body = json.loads(row["request_body_json"])
            except json.JSONDecodeError:
                request_body = {}
        return TaskRecord(
            id=row["id"],
            agent_id=row["agent_id"],
            conversation_id=row["conversation_id"],
            conversation_group=row["conversation_group"],
            cwd=row["cwd"],
            title=row["title"] or "",
            prompt=row["prompt"],
            enabled=bool(row["enabled"]),
            time_zone=row["time_zone"],
            schedule=schedule,
            anchor_at=_parse_iso(row["anchor_at"]) or _now_utc(),
            next_run_at=_parse_iso(row["next_run_at"]),
            last_run_at=_parse_iso(row["last_run_at"]),
            last_error=row["last_error"],
            request_body=request_body,
            created_at=_parse_iso(row["created_at"]) or _now_utc(),
            updated_at=_parse_iso(row["updated_at"]) or _now_utc(),
        )

    def _row_to_pending(self, row: sqlite3.Row) -> PendingRun:
        request_body: dict[str, Any] = {}
        if row["request_body_json"]:
            try:
                request_body = json.loads(row["request_body_json"])
            except json.JSONDecodeError:
                request_body = {}
        return PendingRun(
            id=row["id"],
            task_id=row["task_id"],
            agent_id=row["agent_id"],
            conversation_id=row["conversation_id"],
            conversation_group=row["conversation_group"],
            cwd=row["cwd"],
            prompt=row["prompt"],
            request_body=request_body,
            scheduled_at=_parse_iso(row["scheduled_at"]),
            enqueued_at=_parse_iso(row["enqueued_at"]) or _now_utc(),
        )

    def _task_to_row(self, record: TaskRecord) -> dict[str, Any]:
        return {
            "id": record.id,
            "agent_id": record.agent_id,
            "conversation_id": record.conversation_id,
            "conversation_group": record.conversation_group,
            "cwd": record.cwd,
            "title": record.title,
            "prompt": record.prompt,
            "enabled": 1 if record.enabled else 0,
            "time_zone": record.time_zone,
            "frequency": record.schedule.frequency,
            "interval": record.schedule.interval,
            "weekday_mask": record.schedule.weekday_mask,
            "monthly_mode": record.schedule.monthly_mode,
            "day_of_month": record.schedule.day_of_month,
            "weekday_ordinal": record.schedule.weekday_ordinal,
            "weekday": record.schedule.weekday,
            "month": record.schedule.month,
            "time_minutes": record.schedule.time_minutes,
            "anchor_at": _iso_utc(record.anchor_at),
            "next_run_at": _iso_utc(record.next_run_at),
            "last_run_at": _iso_utc(record.last_run_at),
            "last_error": record.last_error,
            "request_body_json": json.dumps(record.request_body, separators=(",", ":")),
            "created_at": _iso_utc(record.created_at),
            "updated_at": _iso_utc(record.updated_at),
        }


class TaskScheduler:
    def __init__(self, *, store: TaskStore, manager: ConversationManager) -> None:
        self._store = store
        self._manager = manager
        self._scheduler = AsyncIOScheduler(timezone=timezone.utc)
        self._lock = asyncio.Lock()
        self._started = False

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            self._scheduler.start()
            await self.reload()
            self._started = True

    async def shutdown(self) -> None:
        async with self._lock:
            if not self._started:
                return
            self._scheduler.shutdown(wait=False)
            self._started = False

    async def reload(self) -> None:
        tasks = await self._store.list_tasks()
        self._scheduler.remove_all_jobs()
        for task in tasks:
            if task.enabled:
                await self._schedule_task(task, force=True)

    async def create_task(self, record: TaskRecord) -> TaskRecord:
        stored = await self._store.create_task(record)
        if stored.enabled:
            await self._schedule_task(stored, force=True)
        return stored

    async def update_task(self, task_id: str, record: TaskRecord) -> TaskRecord:
        stored = await self._store.update_task(task_id, record)
        if stored.enabled:
            await self._schedule_task(stored, force=True)
        else:
            try:
                self._scheduler.remove_job(stored.id)
            except JobLookupError:
                pass
        return stored

    async def delete_task(self, task_id: str) -> None:
        try:
            self._scheduler.remove_job(task_id)
        except JobLookupError:
            pass
        await self._store.delete_task(task_id)

    async def on_run_finished(self, cwd: str) -> None:
        await self._drain_pending(cwd)

    async def _schedule_task(self, task: TaskRecord, *, force: bool) -> None:
        now = _now_utc()
        next_run = task.next_run_at
        if force or next_run is None or next_run <= now:
            next_run = compute_next_run(task, after=now)
            task = (
                await self._store.update_run_times(
                    task.id,
                    next_run_at=next_run,
                    last_run_at=task.last_run_at,
                    last_error=None,
                )
                or task
            )

        self._scheduler.add_job(
            self._run_task_job,
            trigger=DateTrigger(run_date=next_run),
            args=[task.id, next_run],
            id=task.id,
            replace_existing=True,
            misfire_grace_time=300,
        )

    async def _run_task_job(self, task_id: str, scheduled_at: datetime) -> None:
        task = await self._store.get_task(task_id)
        if task is None or not task.enabled:
            return

        await self._schedule_next(task, scheduled_at)

        try:
            started = await self._start_task_run(task)
        except AgentFolderBusyError:
            await self._enqueue_pending(task, scheduled_at)
            return
        except Exception as exc:
            logger.exception("Task run failed: %s", task_id)
            await self._store.update_run_times(task_id, last_error=str(exc))
            return

        if not started:
            await self._enqueue_pending(task, scheduled_at)
            return

        await self._store.update_run_times(task_id, last_run_at=_now_utc(), last_error=None)

    async def _schedule_next(self, task: TaskRecord, scheduled_at: datetime) -> None:
        next_run = compute_next_run(task, after=scheduled_at)
        await self._store.update_run_times(
            task.id,
            next_run_at=next_run,
            last_run_at=task.last_run_at,
        )
        self._scheduler.add_job(
            self._run_task_job,
            trigger=DateTrigger(run_date=next_run),
            args=[task.id, next_run],
            id=task.id,
            replace_existing=True,
            misfire_grace_time=300,
        )

    async def _start_task_run(self, task: TaskRecord) -> bool:
        request_body = dict(task.request_body)
        request_body.setdefault("cwd", task.cwd)
        if task.conversation_group:
            request_body.setdefault("conversation_group", task.conversation_group)
        return await self._manager.start_run(
            conversation_id=task.conversation_id,
            prompt=task.prompt,
            request_body=request_body,
        )

    async def _enqueue_pending(self, task: TaskRecord, scheduled_at: datetime) -> None:
        pending = PendingRun(
            id=uuid.uuid4().hex,
            task_id=task.id,
            agent_id=task.agent_id,
            conversation_id=task.conversation_id,
            conversation_group=task.conversation_group,
            cwd=task.cwd,
            prompt=task.prompt,
            request_body=task.request_body,
            scheduled_at=scheduled_at,
            enqueued_at=_now_utc(),
        )
        await self._store.enqueue_pending(pending)

    async def _drain_pending(self, cwd: str) -> None:
        while True:
            pending = await self._store.pop_pending_for_cwd(cwd)
            if pending is None:
                return

            task = await self._store.get_task(pending.task_id)
            if task is None or not task.enabled:
                continue

            try:
                started = await self._start_task_run(task)
            except AgentFolderBusyError:
                await self._store.enqueue_pending(pending)
                return
            except Exception as exc:
                logger.exception("Pending run failed: %s", pending.task_id)
                await self._store.update_run_times(pending.task_id, last_error=str(exc))
                continue

            if not started:
                await self._store.enqueue_pending(pending)
                return

            await self._store.update_run_times(task.id, last_run_at=_now_utc(), last_error=None)


def compute_next_run(task: TaskRecord, *, after: datetime) -> datetime:
    tz = ZoneInfo(task.time_zone)
    after_local = after.astimezone(tz)
    anchor_local = task.anchor_at.astimezone(tz)
    schedule = task.schedule
    time_minutes = schedule.time_minutes

    if schedule.frequency == "daily":
        candidate_date = after_local.date()
        candidate_dt = _combine_local(candidate_date, time_minutes, tz)
        if candidate_dt <= after_local:
            candidate_date += timedelta(days=1)
        days_between = (candidate_date - anchor_local.date()).days
        remainder = days_between % schedule.interval
        if remainder != 0:
            candidate_date += timedelta(days=(schedule.interval - remainder))
        return _combine_local(candidate_date, time_minutes, tz).astimezone(timezone.utc)

    if schedule.frequency == "weekly":
        candidate_date = after_local.date()
        candidate_dt = _combine_local(candidate_date, time_minutes, tz)
        if candidate_dt <= after_local:
            candidate_date += timedelta(days=1)

        while True:
            if schedule.weekday_mask & (1 << (_weekday_number(candidate_date) - 1)):
                weeks_between = (candidate_date - anchor_local.date()).days // 7
                if weeks_between % schedule.interval == 0:
                    return _combine_local(candidate_date, time_minutes, tz).astimezone(timezone.utc)
            candidate_date += timedelta(days=1)

    if schedule.frequency == "monthly":
        return _next_monthly_run(schedule, anchor_local, after_local, tz).astimezone(timezone.utc)

    if schedule.frequency == "yearly":
        return _next_yearly_run(schedule, anchor_local, after_local, tz).astimezone(timezone.utc)

    return (after + timedelta(days=1)).astimezone(timezone.utc)


def _next_monthly_run(schedule: TaskSchedule, anchor_local: datetime, after_local: datetime, tz: ZoneInfo) -> datetime:
    anchor_index = _month_index(anchor_local.date())
    candidate_index = _month_index(after_local.date())

    candidate_date = _monthly_date_for_index(schedule, candidate_index)
    candidate_dt = _combine_local(candidate_date, schedule.time_minutes, tz)
    if candidate_dt <= after_local:
        candidate_index += 1

    diff = candidate_index - anchor_index
    remainder = diff % schedule.interval
    if remainder != 0:
        candidate_index += schedule.interval - remainder

    candidate_date = _monthly_date_for_index(schedule, candidate_index)
    candidate_dt = _combine_local(candidate_date, schedule.time_minutes, tz)
    if candidate_dt <= after_local:
        candidate_index += schedule.interval
        candidate_date = _monthly_date_for_index(schedule, candidate_index)
    return _combine_local(candidate_date, schedule.time_minutes, tz)


def _monthly_date_for_index(schedule: TaskSchedule, index: int) -> date:
    base = _date_from_month_index(index)
    if schedule.monthly_mode == "ordinal_weekday":
        return _nth_weekday_in_month(base.year, base.month, schedule.weekday, schedule.weekday_ordinal)
    day = min(schedule.day_of_month, _last_day_of_month(base.year, base.month))
    return date(base.year, base.month, day)


def _next_yearly_run(schedule: TaskSchedule, anchor_local: datetime, after_local: datetime, tz: ZoneInfo) -> datetime:
    anchor_year = anchor_local.year
    candidate_year = after_local.year

    candidate_date = _yearly_date_for_year(schedule, candidate_year)
    candidate_dt = _combine_local(candidate_date, schedule.time_minutes, tz)
    if candidate_dt <= after_local:
        candidate_year += 1

    diff = candidate_year - anchor_year
    remainder = diff % schedule.interval
    if remainder != 0:
        candidate_year += schedule.interval - remainder

    candidate_date = _yearly_date_for_year(schedule, candidate_year)
    candidate_dt = _combine_local(candidate_date, schedule.time_minutes, tz)
    if candidate_dt <= after_local:
        candidate_year += schedule.interval
        candidate_date = _yearly_date_for_year(schedule, candidate_year)
    return _combine_local(candidate_date, schedule.time_minutes, tz)


def _yearly_date_for_year(schedule: TaskSchedule, year: int) -> date:
    if schedule.monthly_mode == "ordinal_weekday":
        return _nth_weekday_in_month(year, schedule.month, schedule.weekday, schedule.weekday_ordinal)
    day = min(schedule.day_of_month, _last_day_of_month(year, schedule.month))
    return date(year, schedule.month, day)
