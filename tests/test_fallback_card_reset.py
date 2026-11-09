"""SAKUYA_PATCH_FALLBACK_CARD_RESET 测试 — 流式中途 fallback 激活时的清卡重流。

根因链（2026-10-30 12:23 事故）：MiniMax 流式中途 429 → core 整轮重试回退 GLM →
保留契约不清流面（turn_recovery.settle_delivered_partial）→ 半截文本与完整重答
叠在同一张卡上。修复：try_activate_fallback 成功瞬间由注入钩子调用
StreamCardController.on_fallback_activated，回收半截 answer 文本并替换为提示行。
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from hermes_lark_streaming.controller import StreamCardController
from hermes_lark_streaming.feishu import FeishuClient
from hermes_lark_streaming.streaming.segments import SegmentType
from hermes_lark_streaming.streaming.session import CardSession, SessionState

_NOTICE_MARKER = "主模型限流中断"


def _setup_ctrl() -> StreamCardController:
    """构建启用流式的 controller（内联自 test_controller 的模式，避免跨文件耦合）。"""
    ctrl = StreamCardController()
    ctrl._cfg._raw = {
        "streaming": {"enabled": True},
        "feishu": {"app_id": "app", "app_secret": "secret"},
    }
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ctrl._loop = loop
    ctrl._initialized = True
    client = AsyncMock(spec=FeishuClient)
    client.cardkit_create = AsyncMock(return_value="card_id_abc")
    client.cardkit_batch_update = AsyncMock()
    client.cardkit_stream_element = AsyncMock()
    client.cardkit_close_streaming = AsyncMock()
    client.cardkit_update = AsyncMock()
    ctrl._client = client
    return ctrl


def _make_streaming_session(msg_id: str = "msg_fb_1") -> CardSession:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = CardSession(msg_id, "chat_fb", loop)
    session.state = SessionState.STREAMING
    session.card_id = "card_fb_1"
    return session


def _register(ctrl: StreamCardController, session: CardSession) -> None:
    ctrl._sessions[session.message_id] = session


def test_fallback_reset_clears_partial_and_replaces_with_notice() -> None:
    """场景 1：半截 answer 已流出 → 清空旧段 + 尾段替换为提示行 + 调度 flush。"""
    ctrl = _setup_ctrl()
    session = _make_streaming_session()
    _register(ctrl, session)
    # 模拟 MiniMax 已流出：reasoning 面板 + 半截 answer（已上卡 created=True）
    session.segment_state.on_reasoning_delta("思考中…")
    reason_seg = session.segment_state.segments[-1]
    reason_seg.created = True
    reason_seg.dirty = False  # 模拟已 flush 过的稳定态：reset 不应改动 reasoning 段
    session.segment_state.on_answer_delta("半截 MiniMax 文本")
    ans_seg = session.segment_state.segments[-1]
    ans_seg.created = True

    with patch.object(ctrl, "_schedule_flush") as mock_flush:
        ok = ctrl.on_fallback_activated(
            message_id=session.message_id,
            fallback_model="glm-5.3-flash",
            fallback_provider="zai",
        )

    assert ok is True
    assert _NOTICE_MARKER in ans_seg.text
    assert "zai/glm-5.3-flash" in ans_seg.text
    assert ans_seg.dirty is True
    # 半截文本被回收，不再叠加在卡面缓冲里
    assert "半截 MiniMax 文本" not in ans_seg.text
    # reasoning 面板保留（不在重复症状范围内）
    assert reason_seg.text == "思考中…"
    assert reason_seg.dirty is False
    mock_flush.assert_called_once_with(session)


def test_fallback_reset_then_glm_deltas_append_cleanly() -> None:
    """场景 2：清卡后 GLM 增量从提示行之后干净追加，最终卡面 = 提示行 + GLM 全文。"""
    ctrl = _setup_ctrl()
    session = _make_streaming_session()
    _register(ctrl, session)
    session.segment_state.on_answer_delta("半截 MiniMax 文本")
    session.segment_state.segments[-1].created = True
    ctrl.on_fallback_activated(
        message_id=session.message_id,
        fallback_model="glm-5.3-flash",
        fallback_provider="zai",
    )

    ctrl.on_answer(message_id=session.message_id, text="GLM 的完整回答")

    ans_segs = [s for s in session.segment_state.segments if s.type == SegmentType.ANSWER]
    assert len(ans_segs) == 1
    assert ans_segs[0].text == (
        f"⚠️ {_NOTICE_MARKER}，已切换备用模型（zai/glm-5.3-flash）重新作答：\n\nGLM 的完整回答"
    )
    assert "半截 MiniMax 文本" not in ans_segs[0].text


def test_fallback_reset_noop_before_any_visible_answer() -> None:
    """场景 3：pre-stream fallback（尚无可见 answer 文本）→ 不清卡。"""
    ctrl = _setup_ctrl()
    session = _make_streaming_session()
    _register(ctrl, session)
    # 只有 reasoning，没有 answer —— 用户还没看到任何正文
    session.segment_state.on_reasoning_delta("思考中…")
    session.segment_state.segments[-1].created = True
    with patch.object(ctrl, "_schedule_flush") as mock_flush:
        ok = ctrl.on_fallback_activated(
            message_id=session.message_id,
            fallback_model="glm-5.3-flash",
            fallback_provider="zai",
        )
    assert ok is False
    mock_flush.assert_not_called()
    assert session.segment_state.segments[-1].text == "思考中…"


def test_fallback_reset_noop_without_active_session() -> None:
    """场景 4：session 不存在（内部轮/cron/非 feishu）与终态 session → no-op。"""
    ctrl = _setup_ctrl()

    # 无 session
    assert ctrl.on_fallback_activated(
        message_id="msg_unknown", fallback_model="m", fallback_provider="p"
    ) is False

    # 已终态（completed）
    session = _make_streaming_session()
    session.state = SessionState.COMPLETED
    _register(ctrl, session)
    assert ctrl.on_fallback_activated(
        message_id=session.message_id, fallback_model="m", fallback_provider="p"
    ) is False

    # 插件被禁用
    ctrl._cfg._raw = {}
    session2 = _make_streaming_session()
    session2.segment_state.on_answer_delta("半截")
    session2.segment_state.segments[-1].created = True
    _register(ctrl, session2)
    assert ctrl.on_fallback_activated(
        message_id=session2.message_id, fallback_model="m", fallback_provider="p"
    ) is False


def test_fallback_reset_clears_multiple_answer_segments() -> None:
    """场景 5：reasoning→answer→reasoning→answer 交替（多 answer 段）时全部回收，
    尾段承载提示行。"""
    ctrl = _setup_ctrl()
    session = _make_streaming_session()
    _register(ctrl, session)
    session.segment_state.on_answer_delta("第一段半截")
    session.segment_state.segments[-1].created = True
    session.segment_state.on_reasoning_delta("补充思考")
    session.segment_state.segments[-1].created = True
    session.segment_state.on_answer_delta("第二段半截")
    session.segment_state.segments[-1].created = True

    ok = ctrl.on_fallback_activated(
        message_id=session.message_id,
        fallback_model="glm-5.3-flash",
        fallback_provider="zai",
    )

    assert ok is True
    answers = [s for s in session.segment_state.segments if s.type == SegmentType.ANSWER]
    assert len(answers) == 2
    assert answers[0].text == "" and answers[0].dirty is True
    assert _NOTICE_MARKER in answers[1].text
    assert "第一段半截" not in answers[0].text + answers[1].text
    assert "第二段半截" not in answers[0].text + answers[1].text
