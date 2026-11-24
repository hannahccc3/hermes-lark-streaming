"""SAKUYA cron lane, natively hosted (SAKUYA_CRON_CARD_NATIVE_V1).

Rebuilds the pretty cron card (blue header ``⏰ task · dispatch time``) that the
old injection plugin assembled from job metadata. Native Hermes never passes job
metadata to the standalone sender / adapter ``send()``, so the cron context is
recovered from the core's ``cron.wrap_response`` wrapper text instead:

    Cronjob Response: {task_name}\\n
    (job_id: {job_id})\\n
    -------------\\n\\n
    {content}\\n\\n
    To stop or manage this job, send me a new message\\n
    (e.g. "stop reminder {task_name}").

Requires ``cron.wrap_response: true`` in the Hermes profile config (the old
plugin read job metadata directly, so it worked with wrapping disabled; the
native port trades that config flip for zero core injections).

Behavior parity with the old ``split_cron._try_cron_card`` +
``cardkit.builder.build_cron_card``:
- SILENT-only bodies are swallowed (defense in depth; the core scheduler
  already suppresses ``[SILENT]`` upstream of delivery).
- Header time is the ACTUAL dispatch time (P24 / SAKUYA_CRON_RUNTIME_V2),
  never ``next_run_at``.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

MARKER = "SAKUYA_CRON_CARD_NATIVE_V1"

# The core wrap prefix is emitted by cron/scheduler_delivery.py::_deliver_result.
# Match the exact 3-line shape so ordinary answers that merely start with a
# similar sentence are never misdetected.
_WRAP_PREFIX_RE = re.compile(
    r"\ACronjob Response: (?P<task>[^\n]+)\n"
    r"\(job_id: (?P<job_id>[^\n]*)\)\n"
    r"(?P<rule>-{5,})\n\n"
)
_FOOTER_ANCHOR = "\nTo stop or manage this job, send me a new message"

# SAKUYA_SILENT_SKIP_PATCH semantics (word-boundary, v2): a body that is only a
# SILENT sentinel must not become a card.
_SILENT_RE = re.compile(r"\b(?:SILENT|NO_REPLY|NO REPLY)\b")


def parse_wrapped_cron(message: str) -> tuple[str, str] | None:
    """Return ``(task_name, body)`` when *message* carries the core cron wrap.

    Returns ``None`` for anything that is not a wrapped cron payload (ordinary
    replies, background notifications, unwrapped cron content).
    """
    if not message:
        return None
    match = _WRAP_PREFIX_RE.match(message)
    if not match:
        return None
    body = message[match.end():]
    footer_at = body.find(_FOOTER_ANCHOR)
    if footer_at != -1:
        body = body[:footer_at]
    return match.group("task").strip(), body.strip()


def is_silent_body(body: str) -> bool:
    stripped = (body or "").strip()
    if not stripped:
        return True
    if stripped.upper() in ("SILENT", "NO_REPLY", "NO REPLY", "[SILENT]"):
        return True
    return bool(_SILENT_RE.search(stripped.upper()))


def _format_run_time(run_time: str) -> str:
    if not run_time:
        return ""
    try:
        dt = datetime.fromisoformat(run_time)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return run_time


def now_local_iso() -> str:
    """Actual dispatch time for the header (SAKUYA_CRON_RUNTIME_V2 semantics)."""
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="minutes")


def build_cron_card(content: str, *, task_name: str = "", run_time: str = "") -> dict[str, Any]:
    """Minimal cron push card — schema 2.0, optional blue header + markdown body.

    Ported from the injection-era ``cardkit.builder.build_cron_card``; reuse that
    module's markdown pipeline so rendering stays byte-identical with the old
    cards. The SILENT sentinel maps to an empty body card (the caller decides to
    skip the push entirely).
    """
    from .cardkit.builder import _LOCALES, _split_long_text, optimize_markdown_style

    card: dict[str, Any] = {
        "schema": "2.0",
        "config": {"wide_screen_mode": True, "locales": _LOCALES},
        "body": {"elements": []},
    }
    header_parts = [p for p in (task_name, _format_run_time(run_time)) if p]
    if header_parts:
        card["header"] = {
            "title": {"tag": "lark_md", "content": ":Alarm: " + " · ".join(header_parts)},
            "template": "blue",
        }
    if not content.strip():
        return card
    summary = content[:120].replace("\n", " ").replace("```", "").strip()
    if summary:
        card["config"]["summary"] = {"content": summary}
    for chunk in _split_long_text(optimize_markdown_style(content)):
        if chunk.strip():
            card["body"]["elements"].append({"tag": "markdown", "content": chunk})
    return card


def try_render_cron_card(message: str) -> dict[str, Any] | None:
    """Cron card for *message*, or ``None`` when it is not a wrapped cron push.

    ``None`` keeps the caller on the ordinary static-card path unchanged. A
    SILENT-only body yields the sentinel ``{}`` (skip the push, report success).
    """
    parsed = parse_wrapped_cron(message)
    if parsed is None:
        return None
    task_name, body = parsed
    if is_silent_body(body):
        return {}
    return build_cron_card(body, task_name=task_name, run_time=now_local_iso())
