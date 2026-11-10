"""内部轮切卡路径测试 (SAKUYA_PATCH_INTERNAL_SEAL, 2026-11-10 事故复现).

事故链 (真实日志 2026-11-10 10:54:27):
    10:44:24  内部卡 770636816774 建好 (send_card_to_chat)
    ...       内容持续流入, element_count 一路涨到 197
    10:54:27  WARNING CardKit create streaming card failed for msg=_sakuya_inte
              ← 切卡建新卡时拿合成 id 喂 reply_card_by_id → 飞书拒 (99992354)
              → _do_split_card 返回 None → session.split_disabled = True (降级)
              → 切卡再也不会发生 → 元素无限涨
    11:04:58  ERROR CardKit complete failed: code=300305 element exceeds the limit
              ← 封卡 (cardkit_update 全量重建) 也超限 → 卡永远封不上

正常轮 (om_x100b601c) 同期切卡 3 次全部成功 —— 差异就在 message_id 前缀。

本文件覆盖:
    A. 内部轮切卡建新卡必须走 send_card_to_chat (不是 reply_card_by_id)
    B. 正常轮切卡仍然走 reply_card_by_id (不回归)
    C. 内部轮切卡成功后 element_count 重置 (不会无限涨)
    D. 内部轮切卡不再走 split_disabled 降级路径
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from hermes_lark_streaming.controller import StreamCardController
from hermes_lark_streaming.streaming.session import SessionState


def _make_ctrl(tmp_path) -> StreamCardController:
    ctrl = StreamCardController(profile_home=tmp_path)
    ctrl._cfg = MagicMock()
    ctrl._cfg.enabled = True
    ctrl._cfg.feishu_app_id = "app-id"
    ctrl._cfg.feishu_app_secret = "secret"
    ctrl._cfg.env_app_id = ""
    ctrl._cfg.env_app_secret = ""
    ctrl._cfg.card_duration_sec = 1800
    ctrl._cfg.header_enabled = False
    ctrl._cfg.width_mode = "default"
    ctrl._cfg.body_text_size = "normal"
    ctrl._cfg.footer_enabled = False
    ctrl._cfg.footer_fields = []
    ctrl._cfg.footer_show_label = True
    ctrl._cfg.footer_text_size = "normal"
    ctrl._cfg.panel_expanded = True
    ctrl._cfg.show_tool_use = True
    ctrl._cfg._raw = {
        "streaming": {"enabled": True},
        "feishu": {"app_id": "app", "app_secret": "secret"},
    }
    ctrl._loop = asyncio.new_event_loop()
    return ctrl


def _fake_client() -> MagicMock:
    client = MagicMock()
    client.cardkit_create = AsyncMock(return_value="card_new")
    client.cardkit_update = AsyncMock(return_value={})
    client.cardkit_close_streaming = AsyncMock(return_value={})
    client.send_card_to_chat = AsyncMock(return_value="om_new")
    client.reply_card_by_id = AsyncMock(return_value="om_replied")
    return client


@pytest.mark.asyncio
async def test_internal_split_card_uses_send_card_to_chat(tmp_path) -> None:
    """A. 内部轮切卡建新卡必须走 send_card_to_chat, 绝不能喂合成 id 给 reply_card_by_id。"""
    ctrl = _make_ctrl(tmp_path)
    client = _fake_client()
    ctrl._client = client
    ctrl._initialized = True

    # 造一个内部 session (模拟已建卡 + 已有元素)
    ctrl.on_message_started(message_id=None, chat_id="oc_chat")
    key = next(k for k in ctrl._sessions if k.startswith("_sakuya_internal:"))
    session = ctrl._sessions[key]
    session.set_card(card_id="card_old", card_msg_id="om_old")
    session.create_task = None
    session.element_count = 190  # 接近阈值

    new_card_id, new_msg_id = await ctrl._create_streaming_card(session)

    assert client.send_card_to_chat.await_count == 1, "内部轮切卡必须走 send_card_to_chat"
    assert client.reply_card_by_id.await_count == 0, "内部轮绝不能拿合成 id 喂 reply_card_by_id"
    assert new_card_id == "card_new"
    assert new_msg_id == "om_new"


@pytest.mark.asyncio
async def test_normal_split_card_still_uses_reply(tmp_path) -> None:
    """B. 正常轮切卡仍然走 reply_card_by_id (不回归)。"""
    ctrl = _make_ctrl(tmp_path)
    client = _fake_client()
    ctrl._client = client
    ctrl._initialized = True

    ctrl.on_message_started(message_id="om_normal", chat_id="oc_chat")
    session = ctrl._sessions["om_normal"]
    session.set_card(card_id="card_old", card_msg_id="om_old")
    session.create_task = None

    await ctrl._create_streaming_card(session)

    assert client.reply_card_by_id.await_count == 1, "正常轮切卡必须仍走 reply_card_by_id"
    assert client.send_card_to_chat.await_count == 0


@pytest.mark.asyncio
async def test_internal_split_resets_element_count(tmp_path) -> None:
    """C. 内部轮切卡成功后 element_count 重置 —— 这是防止 300305 超限的关键。"""
    ctrl = _make_ctrl(tmp_path)
    client = _fake_client()
    ctrl._client = client
    ctrl._initialized = True

    ctrl.on_message_started(message_id=None, chat_id="oc_chat")
    key = next(k for k in ctrl._sessions if k.startswith("_sakuya_internal:"))
    session = ctrl._sessions[key]
    session.set_card(card_id="card_old", card_msg_id="om_old")
    session.create_task = None
    session.element_count = 190

    # 走完整切卡流程
    with patch_flush(ctrl):
        ok = await ctrl._do_split_card(session, 0, [], set(), {}, [])

    assert ok is True, "内部轮切卡必须成功"
    assert session.element_count == 1, "切卡后 element_count 必须重置 (loading element)"
    assert session.split_disabled is False, "切卡成功后不得留在降级状态"


@pytest.mark.asyncio
async def test_internal_split_does_not_disable_split(tmp_path) -> None:
    """D. 内部轮切卡失败不得静默降级为 split_disabled (那会导致元素无限累积)。"""
    ctrl = _make_ctrl(tmp_path)
    client = _fake_client()
    ctrl._client = client
    ctrl._initialized = True

    ctrl.on_message_started(message_id=None, chat_id="oc_chat")
    key = next(k for k in ctrl._sessions if k.startswith("_sakuya_internal:"))
    session = ctrl._sessions[key]
    session.set_card(card_id="card_old", card_msg_id="om_old")
    session.create_task = None

    with patch_flush(ctrl):
        await ctrl._do_split_card(session, 0, [], set(), {}, [])

    assert session.split_disabled is False, (
        "内部轮切卡若走降级路径 (split_disabled=True), 元素会无限累积直到 300305 超限"
    )


def patch_flush(ctrl):
    """让 flush 不真正打网络 (切卡流程会先 flush pending actions)。"""
    from unittest.mock import patch

    return patch.object(ctrl, "_do_batch_update", new=AsyncMock(return_value=True))
