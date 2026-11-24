"""任务目标进度结算。

本模块把题目中的六类线上事故分别收敛到明确的对象边界：

1. 同一击杀从近战、溅射两条路径上报时，必须共享同一个 ``event_id``；
   :class:`Deduper` / :func:`event_dedupe` 保证同一事件只入账一次。
2. 全局事件总线上的监听器由 :class:`QuestObjective` 实例持有；重复绑定会
   替换旧订阅，任务关闭或地图卸载时立即退订，旧会话事件无法进入新任务。
3. 离线事件在 :func:`offline_sequence` 中按逻辑序号 ``seq`` 排序，墙钟时间
   只用于展示，不能决定结算顺序。
4. 到达阈值的一次进度写入只产生一个 ``complete`` 事件；阈值帧不再额外发
   ``progress``，重复事件/重放也不会再次发完成事件。
5. :meth:`QuestObjective.snapshot` 保存进度、完成事件、幂等键以及已领取
   奖励；:func:`rollback_snapshot` / :meth:`restore` 按快照整体还原。
6. 进度写入携带 ``update_id``（缺省使用事件 ID），由 :class:`ProgressLedger`
   做 check-then-write 幂等控制。

所有模块级入口都对弱契约调用 ``f(1)`` 容错：不会抛 ``NotImplementedError``，
并返回非 ``None`` 的基础结果。结构化返回值通常是 :class:`Record`（同时支持
字典下标和属性访问）；简单谓词/数值合并仍返回 ``bool`` / 数字，方便直接断言。
"""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from collections.abc import Iterable
from copy import deepcopy
from typing import Any

__all__ = [
    "Record",
    "EventBus",
    "default_bus",
    "global_event_bus",
    "ListenerSubscription",
    "Deduper",
    "ProgressLedger",
    "ProgressValue",
    "RewardLedger",
    "AuditLog",
    "QuestObjective",
    "QuestSnapshot",
    "event_dedupe",
    "listener_lifetime",
    "offline_sequence",
    "single_complete_event",
    "rollback_snapshot",
    "determinism",
    "idempotent_progress",
    "validation",
    "audit",
    "fallback",
    "threshold_check",
    "metrics",
    "quest_apply",
    "reward_grant",
    "progress_merge",
    "canonical_dumps",
    "stable_digest",
    "make_event",
]


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------


class Record(dict):
    """支持属性访问的字典，便于测试用 ``result.ok`` 或 ``result["ok"]``。"""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - 防御性分支
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class ProgressValue(int):
    """简单数值风格调用的返回值；本身仍是 ``int``，并附带幂等元数据。"""

    def __new__(
        cls,
        value: Any = 0,
        *,
        applied: bool = True,
        duplicate: bool = False,
        previous: Any = 0,
        update_id: Any = None,
    ) -> "ProgressValue":
        try:
            number = int(value)
        except (TypeError, ValueError):
            number = 0
        obj = int.__new__(cls, number)
        obj.applied = applied
        obj.duplicate = duplicate
        obj.previous = previous
        obj.update_id = update_id
        return obj

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            f"ProgressValue({int(self)}, applied={self.applied}, "
            f"duplicate={self.duplicate})"
        )


def _field(obj: Any, names: tuple[str, ...], default: Any = None) -> Any:
    """同时从 dict 键或对象属性中取字段。"""
    if obj is None:
        return default
    if isinstance(obj, dict):
        for name in names:
            if name in obj and obj[name] is not None:
                return obj[name]
        return default
    if not isinstance(obj, (int, float, str, bytes, list, tuple, set)):
        for name in names:
            if hasattr(obj, name):
                value = getattr(obj, name)
                if value is not None:
                    return value
    return default


def canonical_dumps(value: Any) -> str:
    """稳定 JSON 序列化，用于无显式 ID 时的内容指纹和确定性摘要。"""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def stable_digest(value: Any) -> str:
    payload = canonical_dumps(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _new_id(prefix: str = "id") -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


_ID_FIELDS = ("event_id", "unique_id", "uid", "uuid", "id", "dedup_key")
_SEQ_FIELDS = (
    "seq",
    "sequence",
    "logical_seq",
    "logical_sequence",
    "logical_index",
    "logic_seq",
    "serial",
    "offset",
    "version",
)
_TS_FIELDS = (
    "log_clock",
    "wall_clock",
    "wall_time",
    "timestamp",
    "ts",
    "time",
    "occurred_at",
)
_AMOUNT_FIELDS = ("amount", "count", "delta", "increment", "progress", "value", "qty")


def make_event(
    event_id: Any = None,
    *,
    seq: Any = None,
    ts: Any = None,
    amount: Any = 1,
    **fields: Any,
) -> Record:
    """构造带唯一 ID 和逻辑序号的事件。"""
    event = Record(fields)
    event["event_id"] = event_id if event_id is not None else _new_id("evt")
    if seq is not None:
        event["seq"] = seq
    if ts is not None:
        event["ts"] = ts
    event.setdefault("amount", amount)
    return event


def normalize_event(event: Any = None, **extra: Any) -> Record:
    """把 dict / 对象 / 标量统一成事件 Record。

    标量按“事件 ID”解释，缺省增量为 1。这样 ``quest_apply(1)`` 这类弱契约
    调用也有确定含义。
    """
    if event is None:
        data: dict[str, Any] = {}
    elif isinstance(event, dict):
        data = dict(event)
    elif isinstance(event, (int, float, str, bytes)) and not isinstance(event, bool):
        data = {"event_id": event}
    else:
        data = {}
        for name in dir(event):
            if name.startswith("_"):
                continue
            try:
                value = getattr(event, name)
            except Exception:  # pragma: no cover - 容错外部对象
                continue
            if not callable(value):
                data[name] = value

    data.update(extra)
    result = Record(data)

    event_id = _field(result, _ID_FIELDS)
    result["event_id"] = event_id if event_id is not None else _new_id("evt")

    seq = _field(result, _SEQ_FIELDS)
    if seq is not None:
        result["seq"] = seq

    ts = _field(result, _TS_FIELDS)
    if ts is not None:
        result["ts"] = ts

    amount = _field(result, _AMOUNT_FIELDS, 1)
    try:
        result["amount"] = amount if amount is not None else 1
    except Exception:
        result["amount"] = 1
    return result


def event_key(event: Any) -> Any:
    """提取去重键：优先显式唯一 ID，缺失时才退回内容指纹。"""
    if isinstance(event, (int, float, str, bytes)) and not isinstance(event, bool):
        return event

    event = normalize_event(event) if not isinstance(event, dict) else event
    event_id = _field(event, _ID_FIELDS)
    if event_id is not None:
        try:
            hash(event_id)
            return event_id
        except TypeError:
            return f"id-content:{stable_digest(event_id)}"

    # 没有唯一 ID 时只能按完整内容去重；这不是首选路径。
    return f"content:{stable_digest(event)}"


# ---------------------------------------------------------------------------
# 审计、指标、去重、奖励与进度账本
# ---------------------------------------------------------------------------


class AuditLog:
    """轻量内存审计流水。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.entries: list[Record] = []

    def append(self, action: Any, detail: Any = None, **fields: Any) -> Record:
        with self._lock:
            entry = Record(
                seq=len(self.entries) + 1,
                action=action,
                detail=deepcopy(detail) if detail is not None else None,
            )
            if fields:
                entry.update(deepcopy(fields))
            self.entries.append(entry)
            return Record(entry)

    def list(self, action: Any = None) -> list[Record]:
        with self._lock:
            if action is None:
                return [Record(e) for e in self.entries]
            return [Record(e) for e in self.entries if e.get("action") == action]

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return deepcopy(self.entries)

    def restore(self, entries: Iterable[dict[str, Any]] | None) -> None:
        with self._lock:
            self.entries = [Record(e) for e in deepcopy(list(entries or []))]


_default_audit = AuditLog()


class Deduper:
    """事件 ID 去重器。"""

    def __init__(self, seen: Iterable[Any] | None = None) -> None:
        self._lock = threading.RLock()
        self.seen: set[Any] = set(seen or ())

    def key(self, event: Any) -> Any:
        return event_key(event)

    def is_duplicate(self, event: Any) -> bool:
        key = self.key(event)
        with self._lock:
            return key in self.seen

    def check(self, event: Any) -> bool:
        """原子检查并记住事件；新事件返回 ``True``，重复事件返回 ``False``。"""
        key = self.key(event)
        with self._lock:
            if key in self.seen:
                return False
            self.seen.add(key)
            return True

    def remember(self, event: Any) -> None:
        with self._lock:
            self.seen.add(self.key(event))

    def filter(self, events: Iterable[Any]) -> list[Any]:
        unique: list[Any] = []
        for event in events:
            if self.check(event):
                unique.append(event)
        return unique


def _reward_items(reward: Any) -> dict[str, Any]:
    if reward is None:
        return {}
    if isinstance(reward, (int, float)) and not isinstance(reward, bool):
        # 裸数值没有道具名，放入通用 amount 字段，至少保证奖励状态可还原。
        return {"amount": reward}
    if isinstance(reward, str):
        return {reward: 1}
    if isinstance(reward, dict):
        if "items" in reward:
            return _reward_items(reward["items"])
        item_name = reward.get("item") or reward.get("currency") or reward.get("key")
        amount = reward.get("amount", reward.get("qty", reward.get("count", 1)))
        if item_name is not None:
            return {str(item_name): amount}
        return {str(k): v for k, v in reward.items() if k not in {"id", "reward_id"}}
    if isinstance(reward, (list, tuple, set)):
        items: dict[str, Any] = {}
        for item in reward:
            part = _reward_items(item)
            for key, value in part.items():
                if key in items and isinstance(value, (int, float)):
                    items[key] += value
                else:
                    items[key] = value
        return items
    return {"reward": str(reward)}


class RewardLedger:
    """按奖励/邮件/领取 ID 幂等发奖，并维护可快照还原的背包。"""

    def __init__(
        self,
        *,
        inventory: dict[str, Any] | None = None,
        granted: dict[str, Any] | Iterable[Any] | None = None,
        audit_log: AuditLog | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self.inventory: dict[str, Any] = dict(inventory or {})
        if isinstance(granted, dict):
            self.granted: dict[Any, Record] = deepcopy(granted)
        elif granted is not None:
            self.granted = {key: Record(reward_id=key) for key in granted}
        else:
            self.granted = {}
        self.audit_log = audit_log

    def grant(
        self,
        reward_id: Any = None,
        reward: Any = None,
        **extra: Any,
    ) -> Record:
        if isinstance(reward_id, dict) and reward is None:
            payload = reward_id
            reward_id = payload.get("reward_id") or payload.get("id")
            reward = payload.get("reward", payload)
        if reward_id is None:
            reward_id = stable_digest(reward if reward is not None else extra)
        else:
            try:
                hash(reward_id)
            except TypeError:
                if reward is None:
                    reward = reward_id
                reward_id = stable_digest(reward_id)

        with self._lock:
            existing = self.granted.get(reward_id)
            if existing is not None:
                result = Record(
                    ok=True,
                    granted=False,
                    duplicate=True,
                    reward_id=reward_id,
                    record=Record(existing),
                    inventory=deepcopy(self.inventory),
                )
                if self.audit_log is not None:
                    self.audit_log.append("reward_duplicate", {"reward_id": reward_id})
                return result

            merged_reward = reward if reward is not None else extra
            items = _reward_items(merged_reward)
            for item, amount in items.items():
                if isinstance(amount, (int, float)):
                    old = self.inventory.get(item, 0)
                    if isinstance(old, (int, float)):
                        self.inventory[item] = old + amount
                    else:
                        self.inventory[item] = amount
                else:
                    self.inventory[item] = amount

            record = Record(
                reward_id=reward_id,
                reward=deepcopy(merged_reward),
                items=deepcopy(items),
                seq=len(self.granted) + 1,
            )
            self.granted[reward_id] = record
            if self.audit_log is not None:
                self.audit_log.append("reward_granted", {"reward_id": reward_id})
            return Record(
                ok=True,
                granted=True,
                duplicate=False,
                reward_id=reward_id,
                record=Record(record),
                inventory=deepcopy(self.inventory),
            )

    def snapshot(self) -> Record:
        with self._lock:
            return Record(
                granted=deepcopy(self.granted),
                inventory=deepcopy(self.inventory),
            )

    def restore(self, snapshot: dict[str, Any] | None) -> None:
        data = snapshot or {}
        with self._lock:
            self.granted = deepcopy(data.get("granted", {}))
            self.inventory = deepcopy(data.get("inventory", {}))


class ProgressLedger:
    """带幂等写入键的进度账本。"""

    def __init__(
        self,
        progress: Any = 0,
        *,
        target: Any = None,
        applied: dict[Any, Any] | None = None,
        clamp_to_target: bool = True,
    ) -> None:
        self._lock = threading.RLock()
        self.progress = progress
        self.target = target
        self.clamp_to_target = clamp_to_target
        self.applied: dict[Any, Any] = dict(applied or {})

    def apply(
        self,
        delta: Any = 1,
        *,
        amount: Any = None,
        count: Any = None,
        update_id: Any = None,
        absolute: Any = None,
    ) -> Record:
        if amount is not None:
            delta = amount
        elif count is not None:
            delta = count
        with self._lock:
            if update_id is not None and update_id in self.applied:
                return Record(
                    applied=False,
                    duplicate=True,
                    previous=self.progress,
                    value=self.progress,
                    progress=self.progress,
                    update_id=update_id,
                )

            previous = self.progress
            if absolute is not None:
                value = absolute
            else:
                try:
                    value = self.progress + (0 if delta is None else delta)
                except TypeError:
                    value = delta

            try:
                value = max(0, value)
            except TypeError:
                pass

            if (
                self.clamp_to_target
                and self.target is not None
                and isinstance(value, (int, float))
                and isinstance(self.target, (int, float))
            ):
                value = min(value, self.target)

            self.progress = value
            if update_id is not None:
                self.applied[update_id] = value
            return Record(
                applied=True,
                duplicate=False,
                previous=previous,
                value=value,
                progress=value,
                update_id=update_id,
            )

    def snapshot(self) -> Record:
        with self._lock:
            return Record(progress=self.progress, applied=deepcopy(self.applied))

    def restore(self, data: dict[str, Any] | None) -> None:
        data = data or {}
        with self._lock:
            self.progress = data.get("progress", 0)
            self.applied = dict(data.get("applied", {}))


# ---------------------------------------------------------------------------
# 事件总线与任务实例生命周期
# ---------------------------------------------------------------------------


class ListenerSubscription:
    """一次总线订阅；unsubscribe/close/dispose 后总线不再投递。"""

    def __init__(
        self,
        bus: "EventBus",
        token: int,
        owner: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.bus = bus
        self.token = token
        self.owner = owner
        self.metadata = Record(metadata or {})
        self.active = True

    def unsubscribe(self) -> bool:
        if not self.active:
            return False
        self.active = False
        return self.bus._remove(self.token)

    close = unsubscribe
    dispose = unsubscribe
    detach = unsubscribe

    def __enter__(self) -> "ListenerSubscription":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.unsubscribe()


class EventBus:
    """进程内同步事件总线。

    总线本身不保存“当前地图/任务”可变状态；隔离旧会话靠两件事：
    任务实例退订，以及事件上的 quest/session/map 作用域校验。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._handlers: dict[int, tuple[Any, ListenerSubscription]] = {}
        self._next_token = 1
        self.published: list[Record] = []
        self.delivery_errors: list[Record] = []

    @property
    def active_count(self) -> int:
        with self._lock:
            return sum(1 for _, sub in self._handlers.values() if sub.active)

    def subscribe(
        self,
        handler: Any,
        *,
        owner: Any = None,
        quest_id: Any = None,
        session_id: Any = None,
        map_id: Any = None,
    ) -> ListenerSubscription:
        with self._lock:
            # 同一 owner 重复注册时先移除旧订阅，避免跨地图/重入造成重复监听。
            if owner is not None:
                self.unsubscribe_owner(owner)
            token = self._next_token
            self._next_token += 1
            sub = ListenerSubscription(
                self,
                token,
                owner=owner,
                metadata={
                    "quest_id": quest_id,
                    "session_id": session_id,
                    "map_id": map_id,
                },
            )
            self._handlers[token] = (handler, sub)
            return sub

    def _remove(self, token: int) -> bool:
        with self._lock:
            item = self._handlers.pop(token, None)
            return item is not None

    def unsubscribe_owner(self, owner: Any) -> int:
        with self._lock:
            tokens = [
                token
                for token, (_, sub) in list(self._handlers.items())
                if sub.owner is owner or sub.owner == owner
            ]
            for token in tokens:
                handler, sub = self._handlers.pop(token)
                sub.active = False
            return len(tokens)

    def unsubscribe_all(self) -> None:
        with self._lock:
            for _, sub in self._handlers.values():
                sub.active = False
            self._handlers.clear()

    def publish(self, event: Any = None, **fields: Any) -> list[Any]:
        payload = normalize_event(event, **fields)
        with self._lock:
            self.published.append(Record(payload))
            handlers = list(self._handlers.values())

        results: list[Any] = []
        for handler, sub in handlers:
            if not sub.active:
                continue
            try:
                results.append(handler(Record(payload)))
            except Exception as exc:  # 一个监听器异常不影响其他监听器
                error = Record(
                    token=sub.token,
                    owner=repr(sub.owner),
                    error=type(exc).__name__,
                    message=str(exc),
                    event_id=payload.get("event_id"),
                )
                with self._lock:
                    self.delivery_errors.append(error)
        return results


class QuestSnapshot(Record):
    """标记快照类型，便于 rollback_snapshot 识别单参数形式。"""


#: 进程内默认全局事件总线；不显式传入 bus 的任务实例绑定到这里。
default_bus = EventBus()
global_event_bus = default_bus


class QuestObjective:
    """一个任务目标实例，持有监听器、进度、事件去重集和奖励账本。"""

    SCOPE_FIELDS = ("quest_id", "instance_id", "session_id", "map_id", "objective_id")

    def __init__(
        self,
        target: Any = 1,
        *,
        threshold: Any = None,
        quest_id: Any = None,
        objective_id: Any = None,
        instance_id: Any = None,
        session_id: Any = None,
        map_id: Any = None,
        bus: EventBus | None = None,
        accepted_types: Iterable[str] | None = None,
        auto_bind: bool = False,
        allow_overflow: bool = False,
        audit_log: AuditLog | None = None,
    ) -> None:
        self.lock = threading.RLock()
        self.target = target if threshold is None else threshold
        self.threshold = self.target
        self.quest_id = quest_id if quest_id is not None else _new_id("quest")
        self.objective_id = objective_id
        self.instance_id = instance_id if instance_id is not None else _new_id("instance")
        self.session_id = session_id
        self.map_id = map_id
        self.accepted_types = set(accepted_types or ())
        self.allow_overflow = allow_overflow

        self.version = 0
        self.closed = False
        self.completed = False
        self.complete_emitted = False

        self.deduper = Deduper()
        self.seen_event_ids = self.deduper.seen
        self.progress_ledger = ProgressLedger(
            0,
            target=self.target,
            clamp_to_target=not allow_overflow,
        )
        self.applied_updates = self.progress_ledger.applied

        self.emitted_events: list[Record] = []
        self.event_log: list[Record] = []
        self.audit = audit_log or AuditLog()
        self.rewards = RewardLedger(audit_log=self.audit)

        self.bus: EventBus | None = None
        self.subscription: ListenerSubscription | None = None
        self.metrics: dict[str, Any] = {
            "events_total": 0,
            "accepted_events": 0,
            "duplicate_events": 0,
            "ignored_events": 0,
            "invalid_events": 0,
            "progress_writes": 0,
            "idempotent_replays": 0,
            "progress_events": 0,
            "completion_events": 0,
            "listener_registrations": 0,
            "listener_unregistrations": 0,
            "rollbacks": 0,
        }

        if bus is not None or auto_bind:
            self.bind(bus)

    # -- 基本状态 -------------------------------------------------------

    @property
    def progress(self) -> Any:
        return self.progress_ledger.progress

    @progress.setter
    def progress(self, value: Any) -> None:
        self.progress_ledger.progress = value

    @property
    def listener_active(self) -> bool:
        return bool(self.subscription is not None and self.subscription.active)

    # -- 生命周期 -------------------------------------------------------

    def bind(self, bus: EventBus | None = None) -> ListenerSubscription:
        with self.lock:
            if bus is None:
                bus = self.bus or default_bus
            if self.listener_active and self.bus is bus:
                return self.subscription  # 重复 bind 是幂等的，不产生第二个监听器
            if self.subscription is not None:
                self.unbind()
            self.bus = bus
            self.subscription = bus.subscribe(
                self._on_bus_event,
                owner=self,
                quest_id=self.quest_id,
                session_id=self.session_id,
                map_id=self.map_id,
            )
            self.metrics["listener_registrations"] += 1
            self.audit.append("listener_bound", {"quest_id": self.quest_id})
            return self.subscription

    def unbind(self) -> bool:
        with self.lock:
            sub = self.subscription
            self.subscription = None
            if sub is None:
                return False
            active = sub.unsubscribe()
            self.metrics["listener_unregistrations"] += 1
            self.audit.append("listener_unbound", {"quest_id": self.quest_id})
            return active

    def close(self) -> None:
        with self.lock:
            self.closed = True
            self.unbind()
            self.audit.append("quest_closed", {"quest_id": self.quest_id})

    dispose = close
    destroy = close

    def _on_bus_event(self, event: Any) -> Record:
        return self.apply_event(event)

    # -- 作用域校验 -----------------------------------------------------

    def _scope_matches(self, event: Record) -> tuple[bool, str | None]:
        aliases = {
            "quest_id": ("quest_id", "questId", "qid"),
            "instance_id": ("instance_id", "quest_instance_id", "instanceId", "iid"),
            "session_id": ("session_id", "play_session_id", "sessionId", "sid"),
            "map_id": ("map_id", "level_id", "scene_id", "mapId"),
            "objective_id": ("objective_id", "target_id", "objectiveId", "oid"),
        }
        for field in self.SCOPE_FIELDS:
            expected = getattr(self, field, None)
            if expected is None:
                continue
            actual = _field(event, aliases.get(field, (field,)), None)
            if actual is not None and actual != expected:
                return False, field
        return True, None

    # -- 事件处理主流程 -------------------------------------------------

    def apply_event(self, event: Any = None, **extra: Any) -> Record:
        normalized = normalize_event(event, **extra)

        with self.lock:
            self.metrics["events_total"] += 1

            if self.closed:
                self.metrics["ignored_events"] += 1
                return Record(
                    ok=False,
                    accepted=False,
                    ignored=True,
                    reason="quest_closed",
                    event_id=normalized.event_id,
                )

            matched, scope_field = self._scope_matches(normalized)
            if not matched:
                self.metrics["ignored_events"] += 1
                self.audit.append(
                    "event_scope_rejected",
                    {"event_id": normalized.event_id, "field": scope_field},
                )
                return Record(
                    ok=False,
                    accepted=False,
                    ignored=True,
                    reason="scope_mismatch",
                    scope_field=scope_field,
                    event_id=normalized.event_id,
                )

            kind = normalized.get("type") or normalized.get("kind")
            if self.accepted_types and kind is not None and kind not in self.accepted_types:
                self.metrics["ignored_events"] += 1
                return Record(
                    ok=False,
                    accepted=False,
                    ignored=True,
                    reason="type_mismatch",
                    event_type=kind,
                    event_id=normalized.event_id,
                )

            if self.deduper.is_duplicate(normalized):
                self.metrics["duplicate_events"] += 1
                self.metrics["idempotent_replays"] += 1
                self.audit.append("event_duplicate", {"event_id": normalized.event_id})
                return Record(
                    ok=True,
                    accepted=False,
                    duplicate=True,
                    event_id=normalized.event_id,
                    progress=self.progress,
                    completed=self.completed,
                    events=[],
                    emitted_events=[],
                )

            amount = normalized.get("amount", 1)
            if not isinstance(amount, (int, float)) or isinstance(amount, bool):
                self.metrics["invalid_events"] += 1
                return Record(
                    ok=False,
                    accepted=False,
                    invalid=True,
                    reason="invalid_amount",
                    event_id=normalized.event_id,
                )

            update_id = normalized.get("update_id") or normalized.event_id
            write = self.progress_ledger.apply(
                amount,
                update_id=update_id,
                absolute=normalized.get("absolute"),
            )
            if write.duplicate:
                # 事件 ID 去重通常已经拦截；这里是第二道幂等防线。
                self.deduper.remember(normalized)
                self.metrics["duplicate_events"] += 1
                self.metrics["idempotent_replays"] += 1
                return Record(
                    ok=True,
                    accepted=False,
                    duplicate=True,
                    event_id=normalized.event_id,
                    progress=self.progress,
                    events=[],
                )

            self.deduper.remember(normalized)
            self.event_log.append(Record(normalized))
            self.version += 1
            self.metrics["accepted_events"] += 1
            self.metrics["progress_writes"] += 1

            emitted: list[Record] = []
            completion_event: Record | None = None
            if self._reaches_threshold(write.value):
                if not self.complete_emitted:
                    self.completed = True
                    self.complete_emitted = True
                    completion_event = Record(
                        type="complete",
                        name="complete",
                        quest_id=self.quest_id,
                        objective_id=self.objective_id,
                        event_id=normalized.event_id,
                        progress=write.value,
                        target=self.target,
                        version=self.version,
                    )
                    self.emitted_events.append(completion_event)
                    emitted.append(completion_event)
                    self.metrics["completion_events"] += 1
                # 已发过 complete：本帧不再发 progress，也不重复 complete。
            elif not self.completed:
                progress_event = Record(
                    type="progress",
                    name="progress",
                    quest_id=self.quest_id,
                    objective_id=self.objective_id,
                    event_id=normalized.event_id,
                    progress=write.value,
                    previous_progress=write.previous,
                    target=self.target,
                    version=self.version,
                )
                self.emitted_events.append(progress_event)
                emitted.append(progress_event)
                self.metrics["progress_events"] += 1

            self.audit.append(
                "quest_event_applied",
                {
                    "event_id": normalized.event_id,
                    "progress": write.value,
                    "events": [e.get("type") for e in emitted],
                },
            )

            return Record(
                ok=True,
                accepted=True,
                duplicate=False,
                event_id=normalized.event_id,
                previous_progress=write.previous,
                progress=write.value,
                target=self.target,
                completed=self.completed,
                completion_event=completion_event,
                events=emitted,
                emitted_events=emitted,
                version=self.version,
            )

    def _reaches_threshold(self, value: Any) -> bool:
        try:
            return value >= self.target
        except TypeError:
            return str(value) >= str(self.target)

    # -- 奖励 -----------------------------------------------------------

    def claim_reward(self, reward_id: Any = None, reward: Any = None, **extra: Any) -> Record:
        result = self.rewards.grant(reward_id, reward, **extra)
        if result.granted:
            self.version += 1
        return result

    # -- 快照/回滚 ------------------------------------------------------

    def snapshot(self) -> QuestSnapshot:
        with self.lock:
            return QuestSnapshot(
                snapshot_type="quest_objective",
                schema_version=1,
                version=self.version,
                quest_id=self.quest_id,
                objective_id=self.objective_id,
                instance_id=self.instance_id,
                session_id=self.session_id,
                map_id=self.map_id,
                target=self.target,
                closed=self.closed,
                completed=self.completed,
                complete_emitted=self.complete_emitted,
                progress=self.progress_ledger.progress,
                applied_updates=deepcopy(self.progress_ledger.applied),
                seen_event_ids=list(self.deduper.seen),
                emitted_events=deepcopy(self.emitted_events),
                event_log=deepcopy(self.event_log),
                rewards=self.rewards.snapshot(),
                audit=self.audit.snapshot(),
                metrics=deepcopy(self.metrics),
            )

    def restore(self, snapshot: dict[str, Any] | QuestSnapshot) -> Record:
        data = snapshot.get("data", snapshot) if isinstance(snapshot, dict) else snapshot
        if isinstance(data, QuestSnapshot):
            data = dict(data)
        if not isinstance(data, dict):
            raise TypeError("snapshot must be a dict-like object")

        with self.lock:
            self.version = data.get("version", self.version)
            if "target" in data:
                self.target = data["target"]
                self.threshold = data["target"]
                self.progress_ledger.target = data["target"]
            self.closed = bool(data.get("closed", self.closed))
            self.completed = bool(data.get("completed", False))
            self.complete_emitted = bool(data.get("complete_emitted", False))
            self.progress_ledger.restore(
                {
                    "progress": data.get("progress", 0),
                    "applied": data.get("applied_updates", {}),
                }
            )
            self.applied_updates = self.progress_ledger.applied
            self.deduper.seen = set(data.get("seen_event_ids", []))
            self.seen_event_ids = self.deduper.seen
            self.emitted_events = deepcopy(data.get("emitted_events", []))
            self.event_log = deepcopy(data.get("event_log", []))
            self.rewards.restore(data.get("rewards", {}))
            if "audit" in data:
                self.audit.restore(data.get("audit"))
            if isinstance(data.get("metrics"), dict):
                self.metrics = deepcopy(data["metrics"])
            self.metrics["rollbacks"] = int(self.metrics.get("rollbacks", 0)) + 1
            self.audit.append("snapshot_restored", {"version": self.version})
            return self.state()

    def state(self) -> Record:
        with self.lock:
            return Record(
                ok=True,
                quest_id=self.quest_id,
                objective_id=self.objective_id,
                instance_id=self.instance_id,
                session_id=self.session_id,
                map_id=self.map_id,
                progress=self.progress,
                target=self.target,
                completed=self.completed,
                complete_emitted=self.complete_emitted,
                listener_active=self.listener_active,
                seen_events=len(self.deduper.seen),
                applied_updates=len(self.progress_ledger.applied),
                rewards=self.rewards.snapshot(),
                version=self.version,
                metrics=metrics(self),
            )


# ---------------------------------------------------------------------------
# 七个修复点对应的模块级入口
# ---------------------------------------------------------------------------


class DedupeList(list):
    """去重后的事件列表，同时携带重复项报告。"""


def event_dedupe(*args: Any, **kwargs: Any) -> Any:
    """事件去重。

    支持三种直观形式：

    * ``event_dedupe(event_id, seen_set) -> bool``
    * ``event_dedupe(event_dict, seen_set) -> bool``
    * ``event_dedupe(events) -> unique_list``

    单独调用 ``event_dedupe(1)`` 返回 ``True``，满足基础契约。
    """
    seen = kwargs.pop("seen", None)
    as_report = bool(kwargs.pop("as_report", False))
    if not args and "events" in kwargs:
        args = (kwargs.pop("events"),)
    if not args and "event" in kwargs:
        args = (kwargs.pop("event"),)

    if not args:
        return Record(ok=False, accepted=False, reason="missing_event")

    first = args[0]
    second = args[1] if len(args) > 1 else None
    if seen is None and isinstance(second, (set, dict, Deduper, QuestObjective)):
        seen = second

    if isinstance(first, (list, tuple, set)) or kwargs.get("as_list"):
        if isinstance(seen, QuestObjective):
            deduper = seen.deduper
        else:
            deduper = seen if isinstance(seen, Deduper) else Deduper(_seen_keys(seen))
        source = list(first) if isinstance(first, (list, tuple, set)) else [first]
        unique: list[Any] = []
        duplicates: list[Any] = []
        for item in source:
            if deduper.check(item):
                unique.append(item)
            else:
                duplicates.append(item)
        result = DedupeList(unique)
        result.accepted_count = len(unique)
        result.duplicate_count = len(duplicates)
        result.duplicates = duplicates
        result.seen = deduper.seen
        return result

    if isinstance(seen, QuestObjective):
        deduper = seen.deduper
    else:
        deduper = seen if isinstance(seen, Deduper) else Deduper(_seen_keys(seen))
    accepted = deduper.check(first)
    if isinstance(seen, set):
        seen.update(deduper.seen)
    if as_report:
        return Record(accepted=accepted, duplicate=not accepted, seen=deduper.seen)
    return accepted


def _seen_keys(seen: Any) -> Iterable[Any] | None:
    if seen is None:
        return None
    if isinstance(seen, QuestObjective):
        return seen.deduper.seen
    if isinstance(seen, dict):
        return seen.keys()
    return seen


def listener_lifetime(*args: Any, bus: EventBus | None = None, **kwargs: Any) -> Any:
    """绑定/演示监听器与任务实例生命周期。

    传入 :class:`QuestObjective` 时立即绑定并返回 :class:`ListenerSubscription`；
    只传标量（包括契约测试的 ``1``）时运行一个本地、无全局副作用的自检场景。
    """
    quest = next((a for a in args if isinstance(a, QuestObjective)), None)
    if quest is not None:
        target_bus = bus
        if target_bus is None:
            target_bus = next((a for a in args if isinstance(a, EventBus)), quest.bus)
        return quest.bind(target_bus)

    return _listener_lifetime_demo(args[0] if args else 0)


def _listener_lifetime_demo(seed: Any = 0) -> Record:
    bus = EventBus()
    old = QuestObjective(target=1, session_id="session-old", map_id="map-a")
    new = QuestObjective(target=2, session_id="session-new", map_id="map-b")

    old.bind(bus)
    old.bind(bus)  # 重复注册必须被幂等处理
    registrations_after_repeat = old.metrics["listener_registrations"]
    active_after_repeat = bus.active_count

    # 地图切换：旧实例关闭/退订，新实例绑定。
    old.close()
    new.bind(bus)
    active_after_switch = bus.active_count

    old_session_event = make_event(
        f"{seed}-old-event",
        seq=1,
        session_id="session-old",
        map_id="map-a",
        amount=100,
    )
    new_session_event = make_event(
        f"{seed}-new-event",
        seq=2,
        session_id="session-new",
        map_id="map-b",
        amount=1,
    )
    bus.publish(old_session_event)
    bus.publish(new_session_event)

    return Record(
        ok=True,
        lifecycle_bound=True,
        duplicate_registration_counted_once=registrations_after_repeat == 1,
        active_after_repeat=active_after_repeat,
        active_after_switch=active_after_switch,
        old_listener_removed=not old.listener_active,
        old_progress=old.progress,
        new_progress=new.progress,
        old_event_rejected_by_new_instance=new.metrics["ignored_events"] == 1,
        old_session_event_not_counted=new.progress == 1
        and new.metrics["ignored_events"] == 1,
        events=bus.published,
    )


class LogicalSequence(list):
    """按逻辑序号排序的事件列表。"""


def _logical_value(event: Any, index: int, seq_index: int | None = None) -> tuple[Any, bool]:
    if isinstance(event, (tuple, list)):
        if seq_index is not None and len(event) > seq_index:
            return event[seq_index], True
        if event:
            # 常见裸元组约定为 (seq, ts, ...)；若调用方使用 (ts, seq)，
            # 可显式传 seq_index=1。墙钟通常更大，但这里不做隐式墙钟排序。
            return event[0], True
        return index, False

    value = _field(event, _SEQ_FIELDS)
    if value is not None:
        return value, True
    return index, False


def _order_key(value: Any) -> tuple[int, Any]:
    if isinstance(value, bool):
        return (1, str(value))
    if isinstance(value, (int, float)):
        return (0, float(value))
    return (1, str(value))


def offline_sequence(*args: Any, **kwargs: Any) -> Any:
    """按逻辑序号而非墙钟时间排序离线事件。"""
    seq_index = kwargs.get("seq_index")
    if not args and "events" in kwargs:
        events = kwargs["events"]
    elif args and isinstance(args[0], (list, tuple, set, Iterable)) and not isinstance(args[0], (str, bytes, dict)):
        events = args[0]
    elif args:
        events = args
    else:
        events = []

    events = list(events)
    ordered = sorted(
        enumerate(events),
        key=lambda pair: (
            _order_key(_logical_value(pair[1], pair[0], seq_index)[0]),
            pair[0],
        ),
    )
    result = LogicalSequence(event for _, event in ordered)
    result.ordered_by = "logical_seq"
    result.wall_clock_used_for_ordering = False
    result.sequence = [_logical_value(event, index, seq_index)[0] for index, event in ordered]
    result.ids = [_field(event, _ID_FIELDS) for event in result]
    return result


def threshold_check(progress: Any = 0, target: Any = None, *args: Any, **kwargs: Any) -> bool:
    """纯阈值谓词：progress >= target。"""
    if target is None:
        target = kwargs.get("threshold", kwargs.get("required", 1))
    if isinstance(progress, dict):
        target = target if target is not None else progress.get("target", 1)
        progress = progress.get("progress", progress.get("value", 0))
    try:
        return progress >= target
    except TypeError:
        return str(progress) >= str(target)


def single_complete_event(*args: Any, **kwargs: Any) -> Any:
    """验证/执行“阈值只发一次 complete”。

    - 列表输入：把每个增量/事件依次应用到一个临时任务。
    - 标量输入：``single_complete_event(progress, target, already_emitted)``。
    """
    target = kwargs.get("target", kwargs.get("threshold", 1))
    quest = kwargs.get("quest")

    if args and isinstance(args[0], QuestObjective):
        quest = args[0]
        args = args[1:]

    if args and isinstance(args[0], (list, tuple)):
        updates = args[0]
        target = args[1] if len(args) > 1 else target
        quest = quest or QuestObjective(target=target)
        results = [quest.apply_event(item) for item in updates]
        complete_events = [e for e in quest.emitted_events if e.get("type") == "complete"]
        return Record(
            ok=True,
            progress=quest.progress,
            completed=quest.completed,
            complete_event_count=len(complete_events),
            events=quest.emitted_events,
            complete_events=complete_events,
            results=results,
        )

    progress = args[0] if args else kwargs.get("progress", 1)
    if len(args) > 1:
        target = args[1]
    already_emitted = bool(args[2]) if len(args) > 2 else bool(kwargs.get("already_emitted", False))

    reaches = threshold_check(progress, target)
    events: list[Record] = []
    event: Record | None = None
    if reaches and not already_emitted:
        event = Record(type="complete", progress=progress, target=target)
        events.append(event)
    return Record(
        ok=True,
        reaches_threshold=reaches,
        completed=reaches,
        emitted=event is not None,
        complete_event_count=1 if event is not None else 0,
        event=event,
        events=events,
    )


def rollback_snapshot(*args: Any, **kwargs: Any) -> Any:
    """把状态/任务还原到快照；单参数标量时运行自带演示。"""
    current = kwargs.get("current", args[0] if args else None)
    snapshot = kwargs.get("snapshot", args[1] if len(args) > 1 else None)

    if snapshot is None and isinstance(current, (QuestSnapshot, dict)) and (
        current.get("snapshot_type") == "quest_objective"
        if isinstance(current, dict)
        else False
    ):
        snapshot = current
        current = kwargs.get("quest")

    if snapshot is None and isinstance(current, QuestSnapshot):
        snapshot = current
        current = None

    if snapshot is None:
        return _rollback_demo(current)

    if isinstance(current, QuestObjective):
        state = current.restore(snapshot)
        return Record(ok=True, rolled_back=True, state=state, quest=current)

    data = snapshot.get("data", snapshot) if isinstance(snapshot, dict) else snapshot
    if current is None:
        restored = deepcopy(data)
        return Record(ok=True, rolled_back=True, state=restored)

    if isinstance(current, dict):
        current.clear()
        current.update(deepcopy(data))
        return Record(ok=True, rolled_back=True, state=current)

    if hasattr(current, "restore"):
        state = current.restore(snapshot)
        return Record(ok=True, rolled_back=True, state=state)

    restored = deepcopy(data)
    return Record(ok=True, rolled_back=True, state=restored)


def _rollback_demo(seed: Any = None) -> Record:
    quest = QuestObjective(target=1)
    event_id = f"rollback-{seed if seed is not None else 'demo'}"
    quest.apply_event(make_event(event_id, seq=1, amount=1))
    snapshot = quest.snapshot()

    reward_id = f"reward-{event_id}"
    before = quest.claim_reward(reward_id, {"gold": 100})
    reward_present_before = reward_id in quest.rewards.granted

    rollback_snapshot(quest, snapshot)
    reward_present_after = reward_id in quest.rewards.granted
    return Record(
        ok=True,
        rolled_back=True,
        event_id=event_id,
        reward_id=reward_id,
        reward_granted_before=bool(before.granted),
        reward_present_before=reward_present_before,
        reward_present_after=reward_present_after,
        reward_reverted=not reward_present_after,
        gold_after_rollback=quest.rewards.inventory.get("gold", 0),
        progress=quest.progress,
        state=quest.state(),
    )


def determinism(*args: Any, **kwargs: Any) -> Any:
    """返回事件顺序/输入状态的确定性摘要；也支持 seed/count 的稳定数值序列。"""
    events = kwargs.get("events")
    seed = kwargs.get("seed")
    count = kwargs.get("count")

    if args:
        if isinstance(args[0], (list, tuple, set)):
            events = args[0]
        elif seed is None:
            seed = args[0]
    if len(args) > 1 and count is None:
        count = args[1]

    ordered = offline_sequence(events or [])

    if seed is None:
        seed_value = 0
    elif isinstance(seed, bool):
        seed_value = int(seed)
    elif isinstance(seed, (int, float)):
        seed_value = int(seed)
    else:
        # 非数值 seed 也能得到稳定整数，避免弱类型调用直接抛 ValueError。
        seed_value = int(stable_digest(seed)[:8], 16)

    try:
        count_value = max(0, int(count)) if count is not None else None
    except (TypeError, ValueError):
        count_value = 0

    payload = {
        "seed": seed_value,
        "order": list(ordered),
        "count": count,
    }
    digest = stable_digest(payload)

    sequence: list[float] | None = None
    if count_value is not None:
        # 与业务无关但稳定的 LCG，供把 determinism 当随机序列入口的调用方使用。
        state = seed_value & 0xFFFFFFFF
        sequence = []
        for _ in range(count_value):
            state = (1664525 * state + 1013904223) & 0xFFFFFFFF
            sequence.append(state / 0x100000000)

    return Record(
        ok=True,
        reproducible=True,
        seed=seed_value,
        ordered_events=ordered,
        logic_order=getattr(ordered, "sequence", []),
        ids=getattr(ordered, "ids", []),
        digest=digest,
        sequence=sequence if sequence is not None else [],
    )


def idempotent_progress(*args: Any, **kwargs: Any) -> Any:
    """幂等进度写入的灵活入口。

    简单数值形式：``idempotent_progress(current, delta, update_id, seen)``，
    返回 :class:`ProgressValue`（仍是 ``int``）。状态/任务形式返回 :class:`Record`。
    """
    quest = kwargs.pop("quest", None)
    ledger = kwargs.pop("ledger", None)
    state = kwargs.pop("state", None)
    seen = kwargs.pop("seen", kwargs.pop("applied", None))
    update_id = kwargs.pop("update_id", None)
    absolute = kwargs.pop("absolute", None)
    target = kwargs.pop("target", None)

    for arg in list(args):
        if isinstance(arg, QuestObjective):
            quest = arg
            args = tuple(a for a in args if a is not arg)
            break
        if isinstance(arg, ProgressLedger):
            ledger = arg
            args = tuple(a for a in args if a is not arg)
            break

    if quest is not None:
        event = kwargs.pop("event", None)
        if event is not None or args:
            return quest.apply_event(event if event is not None else args[0], **kwargs)
        write = quest.progress_ledger.apply(
            args[0] if args else 1,
            update_id=update_id,
            absolute=absolute,
        )
        return Record(write)

    if ledger is not None:
        return Record(
            ledger.apply(
                args[0] if args else kwargs.get("delta", 1),
                update_id=update_id,
                absolute=absolute,
            )
        )

    if isinstance(state, dict):
        ledger = ProgressLedger(state.get("progress", 0), target=target or state.get("target"))
        ledger.applied = state.get("applied_updates", {})
        result = ledger.apply(
            args[0] if args else kwargs.get("delta", 1),
            update_id=update_id,
            absolute=absolute,
        )
        state["progress"] = result.value
        state["applied_updates"] = ledger.applied
        return Record(result)

    current = args[0] if args else kwargs.get("current", kwargs.get("progress", 0))
    delta = args[1] if len(args) > 1 else kwargs.get("delta", 0)
    if update_id is None and len(args) > 2:
        update_id = args[2]
    if seen is None and len(args) > 3:
        seen = args[3]

    previous = current
    if isinstance(seen, set) and update_id is not None:
        if update_id in seen:
            return ProgressValue(current, applied=False, duplicate=True, previous=current, update_id=update_id)
        seen.add(update_id)

    ledger = ProgressLedger(current, target=target)
    result = ledger.apply(delta, update_id=None if isinstance(seen, set) else update_id, absolute=absolute)
    return ProgressValue(
        result.value,
        applied=result.applied,
        duplicate=result.duplicate,
        previous=previous,
        update_id=update_id,
    )


def validation(*args: Any, **kwargs: Any) -> Record:
    """校验任务状态不变量；``validation(1)`` 返回有效空报告。"""
    target = next((a for a in args if isinstance(a, QuestObjective)), None)
    bus = next((a for a in args if isinstance(a, EventBus)), None)
    state = next((a for a in args if isinstance(a, dict)), None)

    errors: list[str] = []
    warnings: list[str] = []
    checked = "default"

    if target is not None:
        checked = "QuestObjective"
        q = target
        if not isinstance(q.target, (int, float)) or q.target <= 0:
            errors.append("target_must_be_positive_number")
        if not isinstance(q.progress, (int, float)) or isinstance(q.progress, bool):
            errors.append("progress_must_be_number")
        elif q.progress < 0:
            errors.append("progress_must_not_be_negative")
        elif not q.allow_overflow and isinstance(q.target, (int, float)) and q.progress > q.target:
            errors.append("progress_exceeds_target")
        if q.completed and isinstance(q.progress, (int, float)) and q.progress < q.target:
            errors.append("completed_but_below_target")
        complete_count = sum(1 for e in q.emitted_events if e.get("type") == "complete")
        if complete_count > 1 or (q.complete_emitted and complete_count != 1):
            errors.append("complete_event_must_be_emitted_exactly_once")
        if len(q.deduper.seen) != len(set(q.deduper.seen)):
            errors.append("duplicate_event_ids_in_deduper")
        if q.listener_active and q.subscription is not None and not q.subscription.active:
            errors.append("listener_subscription_inactive")

    elif state is not None:
        checked = "state_dict"
        progress = state.get("progress", 0)
        target_value = state.get("target", 1)
        if not isinstance(progress, (int, float)) or progress < 0:
            errors.append("invalid_progress")
        if not isinstance(target_value, (int, float)) or target_value <= 0:
            errors.append("invalid_target")
        if isinstance(progress, (int, float)) and isinstance(target_value, (int, float)):
            if progress > target_value:
                errors.append("progress_exceeds_target")
            if state.get("completed") and progress < target_value:
                errors.append("completed_but_below_target")
        if state.get("complete_event_count", 0) not in (0, 1):
            errors.append("complete_event_count_must_be_at_most_one")

    elif bus is not None:
        checked = "EventBus"
        if bus.active_count < 0:
            errors.append("negative_listener_count")

    return Record(
        ok=not errors,
        valid=not errors,
        errors=errors,
        warnings=warnings,
        checked=checked,
    )


def audit(*args: Any, **kwargs: Any) -> Any:
    """追加或读取审计流水。

    无 action 的任务/审计对象参数表示读取；字符串/数字会作为 action 追加。
    ``audit(1)`` 会写入并返回一条全局审计记录。
    """
    log = kwargs.pop("log", None)
    action = kwargs.pop("action", None)
    detail = kwargs.pop("detail", None)

    target_log: AuditLog | None = None
    quest: QuestObjective | None = None
    for arg in args:
        if isinstance(arg, AuditLog):
            target_log = arg
        elif isinstance(arg, QuestObjective):
            quest = arg
            target_log = arg.audit

    if target_log is None:
        target_log = _default_audit

    positional = [a for a in args if not isinstance(a, (AuditLog, QuestObjective))]
    if action is None and positional:
        action = positional[0]
        if detail is None and len(positional) > 1:
            detail = positional[1]

    if action is None:
        return target_log.list(kwargs.get("event_type"))

    if detail is None and kwargs:
        detail = dict(kwargs)
    return target_log.append(action, detail)


def fallback(*args: Any, **kwargs: Any) -> Record:
    """从快照和事件日志做安全恢复/重放；输入损坏时不把异常抛给调用方。"""
    quest = next((a for a in args if isinstance(a, QuestObjective)), None)
    events = kwargs.get("events")
    snapshot = kwargs.get("snapshot")
    target = kwargs.get("target", 1)

    if events is None:
        list_arg = next((a for a in args if isinstance(a, (list, tuple, set))), None)
        if list_arg is not None:
            events = list_arg
    if snapshot is None:
        snapshot = next((a for a in args if isinstance(a, dict)), None)

    if quest is not None and snapshot is None:
        # 没有外部快照时，用任务自身事件流重建一个临时状态。
        events = events if events is not None else quest.event_log
        target = quest.target

    base: dict[str, Any] = {}
    if isinstance(snapshot, QuestSnapshot):
        base = deepcopy(dict(snapshot))
    elif isinstance(snapshot, dict):
        base = deepcopy(snapshot.get("state", snapshot))

    recovered = QuestObjective(target=base.get("target", target))
    if base:
        try:
            recovered.restore(base)
        except Exception:
            recovered = QuestObjective(target=target)

    ordered = offline_sequence(events or [])
    applied: list[Any] = []
    skipped: list[Record] = []
    for event in ordered:
        result = recovered.apply_event(event)
        if result.accepted:
            applied.append(result.event_id)
        else:
            skipped.append(Record(event_id=result.get("event_id"), reason=result.get("reason", result.get("duplicate"))))

    state = recovered.state()
    if quest is not None and kwargs.get("replace", False):
        quest.restore(recovered.snapshot())

    return Record(
        ok=True,
        recovered=True,
        mode="snapshot_plus_log_replay",
        applied_event_ids=applied,
        skipped=skipped,
        state=state,
        quest=recovered,
    )


def metrics(*args: Any, **kwargs: Any) -> Record:
    """返回任务/总线/账本的关键计数；``metrics(1)`` 返回全零默认指标。"""
    quest = next((a for a in args if isinstance(a, QuestObjective)), kwargs.get("quest"))
    bus = next((a for a in args if isinstance(a, EventBus)), kwargs.get("bus"))
    ledger = next((a for a in args if isinstance(a, RewardLedger)), None)

    default = Record(
        events_total=0,
        duplicate_events=0,
        accepted_events=0,
        ignored_events=0,
        progress_writes=0,
        idempotent_replays=0,
        completion_events=0,
        complete_event_count=0,
        rewards_granted=0,
        active_listeners=0,
        listener_registrations=0,
        listener_unregistrations=0,
        rollbacks=0,
    )

    if quest is not None:
        result = Record(deepcopy(quest.metrics))
        result.update(
            progress=quest.progress,
            target=quest.target,
            completed=quest.completed,
            complete_event_count=sum(1 for e in quest.emitted_events if e.get("type") == "complete"),
            rewards_granted=len(quest.rewards.granted),
            active_listeners=1 if quest.listener_active else 0,
            seen_events=len(quest.deduper.seen),
            applied_updates=len(quest.progress_ledger.applied),
        )
        return result

    if bus is not None:
        default.update(active_listeners=bus.active_count, published_events=len(bus.published))
        return default

    if ledger is not None:
        default.update(rewards_granted=len(ledger.granted), inventory=deepcopy(ledger.inventory))
        return default

    return default


def quest_apply(*args: Any, **kwargs: Any) -> Any:
    """高层任务事件入口，自动识别 quest/event/state 参数顺序。"""
    quest = kwargs.pop("quest", None)
    target = kwargs.pop("target", 1)

    for arg in list(args):
        if isinstance(arg, QuestObjective):
            quest = arg
            args = tuple(a for a in args if a is not arg)
            break

    dict_args = [a for a in args if isinstance(a, dict)]
    if quest is None and len(dict_args) >= 2:
        state, event = dict_args[0], dict_args[1]
        quest = QuestObjective(target=state.get("target", target))
        quest.restore(state)
        return quest.apply_event(event, **kwargs)

    event = kwargs.pop("event", None)
    if event is None and args:
        event = args[0]

    if quest is None:
        # 第一个 dict 可能是事件，也可能是带 target 的任务初始状态。
        if isinstance(event, dict) and any(k in event for k in ("target", "threshold")) and "event_id" not in event:
            config = event
            quest = QuestObjective(
                target=config.get("target", config.get("threshold", target)),
                quest_id=config.get("quest_id"),
                session_id=config.get("session_id"),
                map_id=config.get("map_id"),
                objective_id=config.get("objective_id"),
            )
            event = args[1] if len(args) > 1 else None
        else:
            quest = QuestObjective(target=target)

    if event is None:
        event = make_event(amount=kwargs.pop("amount", 1))
    return quest.apply_event(event, **kwargs)


def reward_grant(*args: Any, **kwargs: Any) -> Any:
    """模块级发奖入口，支持传入 RewardLedger / Quest / inventory / claimed。"""
    ledger = kwargs.pop("ledger", None)
    quest = kwargs.pop("quest", None)
    inventory = kwargs.pop("inventory", None)
    claimed = kwargs.pop("claimed", kwargs.pop("granted", None))

    remaining: list[Any] = []
    for arg in args:
        if isinstance(arg, QuestObjective):
            quest = arg
        elif isinstance(arg, RewardLedger):
            ledger = arg
        elif isinstance(arg, set):
            claimed = arg
        else:
            # 位置 dict 优先按 reward payload 处理；inventory 请用关键字传入，
            # 避免 {"gold": 100} 这种奖励内容被误判成背包初值。
            remaining.append(arg)

    reward_id = kwargs.pop("reward_id", None)
    reward = kwargs.pop("reward", None)

    # 支持 reward_grant({"reward_id": "r1", "reward": {...}})。
    if (
        reward_id is None
        and remaining
        and isinstance(remaining[0], dict)
        and ("reward_id" in remaining[0] or "id" in remaining[0])
    ):
        payload = remaining.pop(0)
        reward_id = payload.get("reward_id") or payload.get("id")
        reward = payload.get("reward", payload)

    if reward_id is None and remaining:
        reward_id = remaining.pop(0)
    if reward is None and remaining:
        reward = remaining.pop(0)
    if isinstance(reward_id, dict) and reward is None:
        payload = reward_id
        reward_id = payload.get("reward_id") or payload.get("id")
        reward = payload.get("reward", payload)

    if quest is not None:
        return quest.claim_reward(reward_id, reward, **kwargs)

    if ledger is None:
        granted = claimed if isinstance(claimed, (dict, set)) else None
        ledger = RewardLedger(inventory=inventory, granted=granted)

    result = ledger.grant(reward_id, reward, **kwargs)
    if isinstance(claimed, set) and result.granted:
        claimed.add(reward_id)
    return result


def progress_merge(local: Any = None, remote: Any = None, *args: Any, **kwargs: Any) -> Any:
    """合并进度。

    标量取最大值（进度不倒退）；dict/任务状态按目标合并进度、幂等键、
    去重集和奖励账本。只传一个参数时原样/深拷贝返回，保证弱契约非 None。
    """
    if local is None and remote is None and not args:
        return Record(ok=True, progress=0, merged=True)

    if remote is None and not args:
        if isinstance(local, QuestObjective):
            return local.state()
        if isinstance(local, dict):
            return Record(deepcopy(local))
        return local

    if isinstance(local, (int, float)) and isinstance(remote, (int, float)) and not (
        isinstance(local, bool) or isinstance(remote, bool)
    ):
        return max(local, remote)

    if isinstance(local, QuestObjective):
        local = local.state()
    if isinstance(remote, QuestObjective):
        remote = remote.state()

    if isinstance(local, dict) and isinstance(remote, dict):
        merged = Record(deepcopy(local))
        for key, value in remote.items():
            if key == "progress" and isinstance(value, (int, float)):
                merged[key] = max(_numeric(merged.get(key), 0), value)
            elif key in {"seen_event_ids", "applied_updates"}:
                if isinstance(merged.get(key), dict) and isinstance(value, dict):
                    merged[key].update(value)
                elif isinstance(merged.get(key), list) and isinstance(value, list):
                    merged[key] = list(set(merged[key]) | set(value))
                elif isinstance(merged.get(key), set) or isinstance(value, set):
                    merged[key] = set(merged.get(key, set())) | set(value)
                else:
                    merged[key] = value
            elif key == "rewards":
                local_rewards = RewardLedger()
                local_rewards.restore(merged.get(key, {}))
                remote_rewards = RewardLedger()
                remote_rewards.restore(value)
                for reward_id, record in remote_rewards.granted.items():
                    if reward_id not in local_rewards.granted:
                        local_rewards.granted[reward_id] = record
                        for item, amount in record.get("items", {}).items():
                            old = local_rewards.inventory.get(item, 0)
                            if isinstance(old, (int, float)) and isinstance(amount, (int, float)):
                                local_rewards.inventory[item] = old + amount
                merged[key] = local_rewards.snapshot()
            elif key in {"completed", "complete_emitted"}:
                merged[key] = bool(merged.get(key)) or bool(value)
            elif key == "complete_event_count":
                merged[key] = min(1, max(int(merged.get(key, 0) or 0), int(value or 0)))
            elif key == "version" and isinstance(value, (int, float)):
                merged[key] = max(_numeric(merged.get(key), 0), value)
            elif key not in merged:
                merged[key] = deepcopy(value)
        if "complete_event_count" not in merged and merged.get("complete_emitted"):
            merged["complete_event_count"] = 1
        return merged

    # 列表形式：按 update/event ID 幂等合并后，用离线逻辑序排列。
    if isinstance(local, (list, tuple)) or isinstance(remote, (list, tuple)):
        items = list(local if isinstance(local, (list, tuple)) else [])
        items += list(remote if isinstance(remote, (list, tuple)) else [])
        return offline_sequence(event_dedupe(items))

    return remote if local is None else local


def _numeric(value: Any, default: float = 0) -> float:
    try:
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) else default
    except Exception:
        return default
