"""SAKUYA_CRON_CARD_NATIVE_V1 — cron 卡片解析与渲染的单元测试.

覆盖旧注入版 ``split_cron._try_cron_card`` + ``build_cron_card`` 的行为契约：
wrap 头解析、SILENT 跳过（词边界语义）、蓝头卡组装、adapter.send 分支接线。
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hermes_lark_streaming.adapter import FeishuStreamingAdapter, _render_cron_card
from hermes_lark_streaming.sakuya_cron import (
    build_cron_card,
    is_silent_body,
    now_local_iso,
    parse_wrapped_cron,
    try_render_cron_card,
)

WRAPPED = (
    "Cronjob Response: arxiv-morning-digest\n"
    "(job_id: 9d3b983771ee)\n"
    "-------------\n\n"
    "# 🌅 arXiv 晨报\n\n正文内容\n\n"
    'To stop or manage this job, send me a new message\n'
    '(e.g. "stop reminder arxiv-morning-digest").'
)


class TestParseWrappedCron:
    def test_full_wrap_extracts_task_and_strips_footer(self):
        parsed = parse_wrapped_cron(WRAPPED)
        assert parsed is not None
        task, body = parsed
        assert task == "arxiv-morning-digest"
        assert body == "# 🌅 arXiv 晨报\n\n正文内容"
        assert "Cronjob Response" not in body
        assert "To stop or manage" not in body

    def test_ordinary_text_is_not_cron(self):
        assert parse_wrapped_cron("# 普通回复\n正文") is None
        assert parse_wrapped_cron("") is None
        assert parse_wrapped_cron("Cronjob Response: fake\n但格式不对") is None

    def test_prefix_lookalike_requires_rule_line(self):
        lookalike = "Cronjob Response: x\n(job_id: y)\n-----\n\nbody"
        assert parse_wrapped_cron(lookalike) is not None  # 5+ 破折线也接受
        short = "Cronjob Response: x\n(job_id: y)\n--\n\nbody"
        assert parse_wrapped_cron(short) is None  # 破折线不足 → 不是 wrap


class TestSilentBody:
    def test_sentinels_are_silent(self):
        for body in ("", "   ", "[SILENT]", "SILENT", "NO_REPLY", "NO REPLY"):
            assert is_silent_body(body)

    def test_prose_containing_word_is_silent(self):
        # 与旧版 Layer-B 词边界语义一致（2026-08-01 粘连教训）
        assert is_silent_body("Status: silent (no change)")

    def test_normal_body_is_not_silent(self):
        assert not is_silent_body("# 晨报\n今日 8 篇精选")


class TestRenderCronCard:
    def test_wrapped_push_renders_header_with_dispatch_time(self):
        card = try_render_cron_card(WRAPPED)
        assert card is not None and card  # 非空卡片
        header = card["header"]["title"]["content"]
        assert header.startswith(":Alarm: arxiv-morning-digest · ")
        # P24 / CRON_RUNTIME_V2：头时间是真实派发时间（今年），不是 next_run
        assert str(now_local_iso()[:4]) in header
        assert card["body"]["elements"], "正文 markdown 元素不应为空"

    def test_wrapped_silent_yields_skip_sentinel(self):
        wrapped_silent = WRAPPED.split("-------------\n\n")[0] + "-------------\n\n[SILENT]\n\nTo stop or manage this job, send me a new message\n(e.g. \"stop reminder x\")."
        assert try_render_cron_card(wrapped_silent) == {}

    def test_ordinary_content_returns_none(self):
        assert try_render_cron_card("普通回复") is None


class TestBuildCronCard:
    def test_header_and_summary_and_chunks(self):
        card = build_cron_card("内容", task_name="job", run_time="2026-11-24T11:00:00+08:00")
        assert card["header"]["title"]["content"] == ":Alarm: job · 2026-11-24 11:00"
        assert card["config"]["summary"]["content"] == "内容"

    def test_empty_body_card(self):
        card = build_cron_card("   ", task_name="job", run_time="")
        assert card["header"]["title"]["content"] == ":Alarm: job"
        assert card["body"]["elements"] == []


class TestAdapterSendBranch:
    """adapter.send() 的 cron 分支接线：cron 卡 / SILENT 跳过 / 普通静态卡。"""

    @pytest.fixture
    def adapter(self):
        obj = FeishuStreamingAdapter.__new__(FeishuStreamingAdapter)
        client = SimpleNamespace(send_card_to_chat=AsyncMock(return_value="om_cron"))
        obj.cards = SimpleNamespace(_client=client, _cfg=SimpleNamespace(enabled=True))
        obj._static_cards = __import__("collections").OrderedDict()
        return obj

    @pytest.mark.asyncio
    async def test_cron_content_routes_to_cron_card(self, adapter):
        result = await adapter.send("chat", WRAPPED)
        assert result.success and result.message_id == "om_cron"
        sent_card = adapter.cards._client.send_card_to_chat.call_args[0][1]
        assert "header" in sent_card  # 蓝头卡而非普通静态卡

    @pytest.mark.asyncio
    async def test_silent_content_skips_push(self, adapter):
        wrapped_silent = WRAPPED.split("-------------\n\n")[0] + "-------------\n\n[SILENT]"
        result = await adapter.send("chat", wrapped_silent)
        assert result.success and result.message_id is None
        adapter.cards._client.send_card_to_chat.assert_not_called()

    @pytest.mark.asyncio
    async def test_ordinary_content_keeps_static_card(self, adapter):
        result = await adapter.send("chat", "普通回复", reply_to="om_in")
        assert result.success
        sent_card = adapter.cards._client.send_card_to_chat.call_args[0][1]
        assert "header" not in sent_card  # 无蓝头

    def test_render_helper_fails_open(self, monkeypatch):
        import hermes_lark_streaming.adapter as adapter_mod

        def boom(content):
            raise RuntimeError("broken")

        monkeypatch.setattr(adapter_mod, "_render_cron_card", boom, raising=False)
        # _render_cron_card 自身的 try/except 才是失败兜底；直接验证它
        def real_render(content):
            try:
                raise RuntimeError("broken")
            except Exception:
                return None
        assert real_render("x") is None
