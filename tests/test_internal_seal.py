"""SAKUYA_PATCH_INTERNAL_SEAL 测试 (2026-11-09 事故复现).

事故 (2026-11-09 22:53:40-22:53:58):
    1. 主卡 om_x100b6029 走主轮 → 22:53:40 on_completed_wait → 22:53:42.372 封卡完成
       → ``_cleanup_session`` 把单槽位 ``_internal_active`` 置 None。
    2. 22:53:42.377 内部注入轮 (watch-pattern 通知, msg=None) 才建卡
       ``_sakuya_internal:*`` (INTERRUPT 钩子) —— 比主卡 cleanup **晚 5ms**。
    3. 内部轮的封卡钩子 (内层 ``_handle_message_with_agent`` 的 COMPLETE 块)
       传 ``message_id=None`` → controller 走 ``_completion_session(None)`` 兜底
       → 单槽位 ``_internal_active`` 已被清空 → **查不到 session → 静默不封卡**。
    4. 内部卡永远停在 ``state=streaming`` → core 判定"卡片没送达"
       → 22:53:58 同一内容补发一份飞书原生灰底文本 → **主人看到一模两条**。

本文件覆盖:
    A. 内部 session 登记表: 主卡 cleanup 后内部卡仍可被回查 (根因防线)
    B. 内部轮封卡: on_completed_wait(None) 能封住内部卡并返回 True
    C. patch 层封卡钩子: 返回 True → 调用方设 already_sent (core 不再补发)
    D. 回归: 正常轮 (有 om_id) 行为不变
    E. 回归: 无内部 session 时封卡安全 no-op (不会误封主卡)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hermes_lark_streaming.controller import StreamCardController
from hermes_lark_streaming.patch import on_internal_followup_seal
from hermes_lark_streaming.streaming.session import CardSession, SessionState


def _make_ctrl(tmp_path, *, chat_id: str = "oc_test") -> StreamCardController:
    ctrl = StreamCardController(profile_home=tmp_path)
    ctrl._cfg = MagicMock()
    ctrl._cfg.enabled = True
    ctrl._cfg.feishu_app_id = "app-id"
    ctrl._cfg.card_secret = "secret"
    ctrl._cfg.card_duration_sec = 1800
    ctrl._cfg._raw = {
        "streaming": {"enabled": True},
        "feishu": {"app_id": "app", "app_secret": "secret"},
    }
    loop = asyncio.new_event_loop()
    ctrl._loop = loop
    return ctrl


def _new_session(
    ctrl: StreamCardController,
    message_id: str,
    chat_id: str = "oc_test",
    *,
    with_card: bool = False,
) -> CardSession:
    """直接造一个 CardSession 并登记 (绕开建卡的异步任务)."""
    session = CardSession(message_id, chat_id, ctrl._loop)
    session.session_key = f"sk:{message_id}"
    if with_card:
        # 真实场景: 建卡成功后 card_id/card_msg_id 才有值 (has_card 判定依据)
        session.set_card(card_id="card_test", card_msg_id="om_card_test")
    ctrl._sessions[message_id] = session
    ctrl._session_keys[session.session_key] = session
    return session


# --------------------------------------------------------------------------
# A. 内部 session 登记表: 主卡 cleanup 后内部卡仍可回查
# --------------------------------------------------------------------------

def test_internal_session_survives_main_card_cleanup(tmp_path) -> None:
    """主卡收尾 (cleanup) 不得让内部轮刚建的卡查不到。"""
    ctrl = _make_ctrl(tmp_path)

    main_session = _new_session(ctrl, "om_main")
    internal = _new_session(ctrl, "_sakuya_internal:1700000000000")
    ctrl._internal_active = internal
    ctrl._internal_sessions[internal.message_id] = internal

    # 主卡收尾 —— 老代码在这里把 _internal_active 置 None (单槽位)
    ctrl._cleanup_session(main_session)

    # 内部卡必须仍可回查 (登记表兜底)
    found = ctrl._completion_session(None)  # type: ignore[arg-type]
    assert found is not None, "主卡 cleanup 后内部 session 丢失 → 封卡静默失败"
    assert found.message_id == internal.message_id


def test_internal_session_registry_prefers_latest_non_terminal(tmp_path) -> None:
    """多个内部 session 并存时, 回查最新的非终态那个。"""
    ctrl = _make_ctrl(tmp_path)

    old = _new_session(ctrl, "_sakuya_internal:1000")
    new = _new_session(ctrl, "_sakuya_internal:2000")
    ctrl._internal_sessions[old.message_id] = old
    ctrl._internal_sessions[new.message_id] = new

    assert ctrl._internal_session_for() is new


def test_internal_session_registry_skips_terminal(tmp_path) -> None:
    """已终态的内部 session 不该被回查。"""
    ctrl = _make_ctrl(tmp_path)

    done = _new_session(ctrl, "_sakuya_internal:1000")
    done.state = SessionState.COMPLETED
    ctrl._internal_sessions[done.message_id] = done

    assert ctrl._internal_session_for() is None


def test_cleanup_removes_internal_from_registry(tmp_path) -> None:
    """内部卡自身收尾时要从登记表摘除 (不泄漏)。"""
    ctrl = _make_ctrl(tmp_path)

    internal = _new_session(ctrl, "_sakuya_internal:1000")
    ctrl._internal_active = internal
    ctrl._internal_sessions[internal.message_id] = internal

    ctrl._cleanup_session(internal)

    assert internal.message_id not in ctrl._internal_sessions
    assert ctrl._internal_active is None


# --------------------------------------------------------------------------
# B. 内部轮封卡: on_completed_wait(None) 封住内部卡
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_internal_round_seals_its_card(tmp_path) -> None:
    """内部轮封卡: on_completed_wait(message_id=None) 必须封住内部卡并返回 True。"""
    ctrl = _make_ctrl(tmp_path)

    internal = _new_session(ctrl, "_sakuya_internal:1700000000000", with_card=True)
    ctrl._internal_active = internal
    ctrl._internal_sessions[internal.message_id] = internal

    completed = {"called": False}

    async def _fake_complete(session) -> bool:
        completed["called"] = True
        session.state = SessionState.COMPLETED
        return True

    with patch.object(ctrl, "_do_complete_card", side_effect=_fake_complete), patch.object(
        ctrl, "_wait_for_card_creation", new=AsyncMock(return_value=True)
    ):
        sealed = await ctrl.on_completed_wait(message_id=None, answer="内部轮内容")  # type: ignore[arg-type]

    assert sealed is True, "内部卡没封 → core 会补发原生文本 → 一模两条"
    assert completed["called"] is True


@pytest.mark.asyncio
async def test_internal_seal_hook_returns_true(tmp_path) -> None:
    """patch 层钩子: 封卡成功 → 返回 True (调用方据此设 already_sent)。"""
    ctrl = _make_ctrl(tmp_path)

    internal = _new_session(ctrl, "_sakuya_internal:1700000000000", with_card=True)
    ctrl._internal_active = internal
    ctrl._internal_sessions[internal.message_id] = internal

    async def _fake_complete(session) -> bool:
        session.state = SessionState.COMPLETED
        return True

    with patch.object(ctrl, "_do_complete_card", side_effect=_fake_complete), patch.object(
        ctrl, "_wait_for_card_creation", new=AsyncMock(return_value=True)
    ):
        # __wrapped__ 绕过 _safe_hook 装饰器 (它自己会去 get_controller())
        sealed = await on_internal_followup_seal.__wrapped__(
            ctrl=ctrl, message_id=None, answer="内部轮内容"
        )

    assert sealed is True


# --------------------------------------------------------------------------
# D/E. 回归
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_internal_session_is_safe_noop(tmp_path) -> None:
    """没有内部 session 时, 封卡调用安全 no-op (返回 False), 不误伤正常轮。"""
    ctrl = _make_ctrl(tmp_path)

    main_session = _new_session(ctrl, "om_main")

    with patch.object(ctrl, "_complete_session_after_creation", new=AsyncMock(return_value=True)) as m:
        sealed = await ctrl.on_completed_wait(message_id=None, answer="x")  # type: ignore[arg-type]

    assert sealed is False
    assert m.await_count == 0
    assert main_session.state != SessionState.COMPLETED


@pytest.mark.asyncio
async def test_normal_round_with_om_id_still_works(tmp_path) -> None:
    """回归: 正常轮 (有 om_id) 行为不变 —— 按 message_id 精确找到自己的卡。"""
    ctrl = _make_ctrl(tmp_path)

    normal = _new_session(ctrl, "om_normal", with_card=True)
    internal = _new_session(ctrl, "_sakuya_internal:1700000000000", with_card=True)
    ctrl._internal_sessions[internal.message_id] = internal

    async def _fake_complete(session) -> bool:
        session.state = SessionState.COMPLETED
        return True

    with patch.object(ctrl, "_do_complete_card", side_effect=_fake_complete), patch.object(
        ctrl, "_wait_for_card_creation", new=AsyncMock(return_value=True)
    ):
        sealed = await ctrl.on_completed_wait(message_id="om_normal", answer="正常内容")

    assert sealed is True
    assert normal.state == SessionState.COMPLETED
    # 正常轮不该顺带封内部卡
    assert internal.state != SessionState.COMPLETED
