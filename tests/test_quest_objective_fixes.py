"""针对任务目标结算六类事故的回归测试。

这些测试使用模块自身暴露的稳定对象 API，把中文需求中的每个故障场景
完整跑一遍，而不是只验证 15 个入口非 None。
"""

from concurrent.futures import ThreadPoolExecutor

import gameplay.quest_objective_settle as qm
from gameplay.quest_objective_settle import (
    EventBus,
    QuestObjective,
    event_dedupe,
    idempotent_progress,
    listener_lifetime,
    offline_sequence,
    progress_merge,
    reward_grant,
    rollback_snapshot,
    single_complete_event,
    threshold_check,
    validation,
)


def kill_event(event_id="kill-1", seq=1, ts=1.0, **extra):
    return {
        "event_id": event_id,
        "seq": seq,
        "ts": ts,
        "type": "kill",
        "objective_id": "slime",
        "amount": 1,
        **extra,
    }


# -- 1. 同一击杀的近战/溅射双路径共享事件 ID，只计数一次 ----------------------


def test_melee_and_splash_reports_with_same_event_id_count_once():
    quest = QuestObjective(target=2, objective_id="slime")
    melee = kill_event(source="melee")
    splash = kill_event(source="splash")

    first = quest.apply_event(melee)
    second = quest.apply_event(splash)

    assert first.accepted is True
    assert second.accepted is False
    assert second.duplicate is True
    assert quest.progress == 1
    assert quest.metrics["duplicate_events"] == 1


def test_event_dedupe_filters_duplicate_ids_and_updates_seen_set():
    seen = set()
    assert event_dedupe("e1", seen) is True
    assert event_dedupe({"event_id": "e1", "source": "splash"}, seen) is False
    assert seen == {"e1"}

    unique = event_dedupe(
        [kill_event("a"), kill_event("a"), kill_event("b")]
    )
    assert unique == [kill_event("a"), kill_event("b")]
    assert unique.duplicate_count == 1


def test_concurrent_reports_of_same_event_id_still_count_once():
    quest = QuestObjective(target=1)
    event = kill_event("concurrent-kill")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: quest.apply_event(event), range(32)))

    assert sum(1 for result in results if result.accepted) == 1
    assert quest.progress == 1


# -- 2. 监听器绑定任务实例生命周期，跨地图不重复注册/串会话 -------------------


def test_listener_is_scoped_to_quest_instance_and_map_switch():
    bus = EventBus()
    old = QuestObjective(target=2, session_id="session-a", map_id="map-a")
    old.bind(bus)
    old.bind(bus)  # 重入/重复注册不应产生第二个订阅
    assert bus.active_count == 1

    old.close()
    assert bus.active_count == 0
    assert not old.listener_active

    new = QuestObjective(target=2, session_id="session-b", map_id="map-b")
    new.bind(bus)
    assert bus.active_count == 1

    # 即使旧事件误投到总线，新实例的作用域校验也必须拒绝。
    bus.publish(kill_event("old-kill", session_id="session-a", map_id="map-a", amount=100))
    assert new.progress == 0
    assert new.metrics["ignored_events"] == 1

    bus.publish(kill_event("new-kill", seq=2, session_id="session-b", map_id="map-b"))
    assert new.progress == 1


def test_listener_lifetime_helper_self_check_passes():
    report = listener_lifetime("case")
    assert report.ok is True
    assert report.duplicate_registration_counted_once is True
    assert report.active_after_repeat == 1
    assert report.active_after_switch == 1
    assert report.old_listener_removed is True
    assert report.old_session_event_not_counted is True


# -- 3. 离线补算按逻辑序号，而不是墙钟时间 ------------------------------------


def test_offline_events_sort_by_logical_sequence_not_wall_clock():
    events = [
        kill_event("logical-2", seq=2, ts=100.0),
        kill_event("logical-1", seq=1, ts=999.0),
        kill_event("logical-3", seq=3, ts=1.0),
    ]

    ordered = offline_sequence(events)

    assert [e["event_id"] for e in ordered] == [
        "logical-1",
        "logical-2",
        "logical-3",
    ]
    assert ordered.ordered_by == "logical_seq"
    assert ordered.wall_clock_used_for_ordering is False


def test_offline_replay_settlement_order_follows_sequence():
    settled = []

    class RecordingQuest(QuestObjective):
        def _reaches_threshold(self, value):
            # 测试目标：每个事件代表一个子目标完成，记录结算顺序。
            settled.append(self.objective_id)
            return super()._reaches_threshold(value)

    events = offline_sequence(
        [
            {"event_id": "e2", "seq": 2, "ts": 1, "objective_id": "later-logical"},
            {"event_id": "e1", "seq": 1, "ts": 2_000, "objective_id": "first-logical"},
        ]
    )
    for event in events:
        q = RecordingQuest(target=1, objective_id=event["objective_id"])
        q.apply_event(event)

    assert settled == ["first-logical", "later-logical"]


# -- 4. 阈值帧只发一次 complete，不再同时刷 progress/complete -----------------


def test_threshold_frame_emits_only_one_complete_event():
    quest = QuestObjective(target=3)

    before = quest.apply_event(kill_event("e1", seq=1, amount=1))
    assert [e["type"] for e in before.events] == ["progress"]

    threshold = quest.apply_event(kill_event("e2", seq=2, amount=2))
    assert [e["type"] for e in threshold.events] == ["complete"]
    assert threshold.completion_event["progress"] == 3

    # 同一事件重放被去重，不产生第二个 complete。
    replay = quest.apply_event(kill_event("e2", seq=2, amount=2))
    assert replay.duplicate is True
    assert replay.events == []

    # 完成后的新事件也不能再发 complete。
    after = quest.apply_event(kill_event("e3", seq=3, amount=1))
    assert after.events == []

    complete_events = [e for e in quest.emitted_events if e["type"] == "complete"]
    assert len(complete_events) == 1
    assert quest.metrics["completion_events"] == 1


def test_single_complete_event_helper_is_idempotent():
    first = single_complete_event(3, 3)
    second = single_complete_event(3, 3, already_emitted=True)
    assert first.complete_event_count == 1
    assert second.complete_event_count == 0
    assert threshold_check(3, 3) is True
    assert threshold_check(2, 3) is False


# -- 5. 读档回滚按快照还原奖励、进度与幂等状态 --------------------------------


def test_rollback_restores_rewards_and_progress_from_snapshot():
    quest = QuestObjective(target=1)
    quest.apply_event(kill_event("kill-9", seq=1))
    snapshot = quest.snapshot()

    reward_result = quest.claim_reward("mail-9", {"gold": 100, "gems": 3})
    assert reward_result.granted is True
    assert quest.rewards.inventory == {"gold": 100, "gems": 3}

    duplicate = reward_grant("mail-9", {"gold": 100}, ledger=quest.rewards)
    assert duplicate.duplicate is True
    assert quest.rewards.inventory == {"gold": 100, "gems": 3}

    fresh_ledger = qm.RewardLedger()
    granted = reward_grant("mail-10", {"gold": 25}, ledger=fresh_ledger)
    assert granted.granted is True
    assert fresh_ledger.inventory == {"gold": 25}

    rollback_snapshot(quest, snapshot)

    assert quest.progress == 1
    assert "mail-9" not in quest.rewards.granted
    assert quest.rewards.inventory == {}
    # 回滚后同一奖励可以按新快照后的新领取事务再次发放。
    again = quest.claim_reward("mail-9", {"gold": 100})
    assert again.granted is True
    assert quest.rewards.inventory == {"gold": 100}


def test_rollback_to_pre_progress_snapshot_restores_progress_too():
    quest = QuestObjective(target=3)
    snapshot = quest.snapshot()
    quest.apply_event(kill_event("a", amount=2))
    quest.claim_reward("r", {"gold": 5})

    result = rollback_snapshot(quest, snapshot)

    assert result.rolled_back is True
    assert quest.progress == 0
    assert quest.rewards.inventory == {}
    assert quest.completed is False


# -- 6. 进度写入幂等 -----------------------------------------------------------


def test_idempotent_progress_same_update_id_only_applies_once():
    seen = set()
    first = idempotent_progress(0, 2, "update-1", seen)
    second = idempotent_progress(int(first), 2, "update-1", seen)

    assert first == 2
    assert first.applied is True
    assert second == 2
    assert second.duplicate is True


def test_progress_ledger_replays_same_update_without_double_count():
    quest = QuestObjective(target=10)
    update = {"update_id": "write-1", "amount": 4}

    first = quest.progress_ledger.apply(**update)
    second = quest.progress_ledger.apply(**update)

    assert first.value == 4
    assert first.applied is True
    assert second.value == 4
    assert second.duplicate is True


def test_progress_merge_keeps_max_and_unions_idempotency_state():
    merged = progress_merge(
        {"progress": 3, "seen_event_ids": ["a"], "complete_emitted": False},
        {"progress": 5, "seen_event_ids": ["b"], "complete_emitted": True},
    )

    assert merged["progress"] == 5
    assert set(merged["seen_event_ids"]) == {"a", "b"}
    assert merged["complete_emitted"] is True
    assert progress_merge(3, 5) == 5


# -- 配套校验/指标/恢复能力 ----------------------------------------------------


def test_validation_accepts_healthy_quest_and_detects_double_complete():
    quest = QuestObjective(target=1)
    quest.apply_event(kill_event("ok"))
    assert validation(quest).valid is True

    quest.emitted_events.append({"type": "complete"})
    report = validation(quest)
    assert report.valid is False
    assert "complete_event_must_be_emitted_exactly_once" in report.errors


def test_fallback_replays_deduped_events_in_logical_order():
    result = qm.fallback(
        target=2,
        events=[
            kill_event("dup", seq=2, amount=1),
            kill_event("dup", seq=2, amount=1),
            kill_event("one", seq=1, amount=1),
        ],
    )

    assert result.ok is True
    assert result.state["progress"] == 2
    assert len(result.applied_event_ids) == 2
