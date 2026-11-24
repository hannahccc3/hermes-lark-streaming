"""Removable text-only hooks for Hermes's split cron delivery implementation."""

from __future__ import annotations

import ast
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any


def _cron_now_iso() -> str:
    """Actual dispatch time for the cron card header (SAKUYA_CRON_RUNTIME_V2)."""
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="minutes")


def _try_cron_card(t: Any, text: str, *, route_thread_id: Any = None,
                   route_metadata: Any = None) -> bool:
    """Use the existing flat-chat API only when it preserves native routing.

    The bool-only hook cannot supply the message ID needed for the companion
    in-channel thread seed. Unknown routing metadata must also stay native.
    """
    try:
        if (
            t.platform_name.lower() not in ("feishu", "lark")
            or t.is_relay
            or t.thread_id is not None
            or route_thread_id is not None
            or t.opened_thread_id is not None
            or not t.notify_delivery
            or (t.in_channel_surface and t.inchannel_continuable)
            or not text.strip()
            or not t.chat_id
        ):
            return False
        if route_metadata is not None and (
            not isinstance(route_metadata, dict)
            or set(route_metadata) - {"job_id", "notify"}
            or route_metadata.get("notify", True) is not True
        ):
            return False
        # SAKUYA_SILENT_SKIP_PATCH: suppress [SILENT] / whitespace-only cron cards (Layer B, pre-card)
        if text.strip() == "[SILENT]":
            return False
        from hermes_lark_streaming.patch import on_cron_deliver

        if getattr(t, "_hermes_lark_card_attempted", False):
            return False
        # Attempt the card only once across native fallback lanes. This target is
        # shared by both lanes, but recreated for each target of every job run.
        t._hermes_lark_card_attempted = True
        return bool(on_cron_deliver(
            chat_id=t.chat_id, content=text.strip(), loop=t.loop,
            task_name=t.job.get("name", ""),
            # SAKUYA_CRON_RUNTIME_V2 (2026-11-06): header time must be the ACTUAL
            # dispatch time, not next_run_at (a timestamp hours in the future).
            # Same rationale as the 2026-08-30 P24 fix — that patch lived in the
            # old monolithic scheduler template and was lost when v0.13.0 moved
            # cron card assembly into split_cron.py.
            run_time=_cron_now_iso(),
        ))
    except Exception:
        # Missing fields, import failure, or a disabled/broken plugin all yield
        # to the original sender without changing its error accounting.
        return False


def _scoped_nodes(node: ast.AST) -> Iterator[ast.AST]:
    """Walk one function, never accepting matches from nested scopes."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        yield child
        yield from _scoped_nodes(child)


def inject_cron(content: str, filename: str = "scheduler_delivery.py") -> str:
    """Inject both split cron lanes, or raise PatcherError without changing input.

    Lifecycle/idempotence belongs to CronPatcher: pass unpatched source here.
    All edits are insertions; removing every CRON_DELIVER block restores the
    source byte-for-byte, including CRLF and a missing terminal newline.

    SAKUYA_PATCH_ANCHOR_CRON_SPLIT (2026-11-24): Hermes v0.23.0 split
    cron/scheduler_delivery.py into scheduler_delivery.py +
    scheduler_delivery_standalone.py (+ _media / _origin / _room helpers). The
    live-lane seams stayed in scheduler_delivery.py; `_standalone_send` and its
    `_send` closure moved to scheduler_delivery_standalone.py. Each file carries
    only the seams that live in it, so the injector is now file-aware: pass the
    basename and inject that file's lanes, skipping lanes owned by the other.
    """
    # Lazy import: CronPatcher itself imports this injector.
    from .patcher import MK_CRON_DELIVER, MK_CRON_DELIVER_END, PatcherError

    if MK_CRON_DELIVER in content or MK_CRON_DELIVER_END in content:
        raise PatcherError("Split cron source already contains cron delivery markers")
    try:
        tree = ast.parse(content)
        compile(tree, "<split-cron-original>", "exec")
    except (SyntaxError, ValueError) as exc:
        raise PatcherError(f"Invalid split cron source: {exc}") from exc

    # Which lanes this file owns after the v0.23.0 split.
    owns_live = filename in ("scheduler_delivery.py", "scheduler.py")
    owns_standalone = filename in ("scheduler_delivery_standalone.py", "scheduler_delivery.py", "scheduler.py")
    if not (owns_live or owns_standalone):
        raise PatcherError(f"Unknown split cron target: {filename}")

    def function(name: str, args: tuple[str, ...]) -> ast.FunctionDef:
        matches = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
        if len(matches) != 1:
            raise PatcherError(f"Split cron requires exactly one top-level {name}")
        fn = matches[0]
        actual = tuple(a.arg for a in (*fn.args.posonlyargs, *fn.args.args, *fn.args.kwonlyargs))
        if actual != args or fn.args.vararg or fn.args.kwarg or fn.decorator_list:
            raise PatcherError(f"Split cron signature changed: {name}")
        return fn

    def optional_function(name: str, args: tuple[str, ...]) -> ast.FunctionDef | None:
        """Return the function if this file owns it, else None (it moved to a sibling)."""
        try:
            return function(name, args)
        except PatcherError:
            return None

    def statement(fn: ast.FunctionDef, source: str) -> ast.stmt:
        expected = ast.dump(ast.parse(source).body[0], include_attributes=False)
        matches = [n for n in _scoped_nodes(fn) if isinstance(n, ast.stmt)
                   and ast.dump(n, include_attributes=False) == expected]
        if len(matches) != 1:
            raise PatcherError(f"Split cron seam missing or ambiguous in {fn.name}: {source}")
        return matches[0]

    live = optional_function("_live_send_text", (
        "t", "text_to_send", "route_thread_id", "route_metadata", "target_errors",
        "delivery_errors", "unverified_targets",
    ))
    standalone = optional_function("_standalone_send", ("t", "content", "media_files"))
    live_lane = optional_function("_deliver_via_live_adapter", (
        "t", "cleaned_text", "media_files", "target_errors", "delivery_errors", "unverified_targets",
    ))
    standalone_lane = optional_function("_deliver_standalone", (
        "t", "content", "media_files", "target_errors", "delivery_errors",
    ))
    if live_lane is not None:
        optional_function("_deliver_result", ("job", "content", "adapters", "loop", "for_failure"))
        statement(live_lane, "adapter_ok, timed_out, delivered_message_id = _live_send_text("
                  "t, text_to_send, route_thread_id, route_metadata, target_errors=target_errors, "
                  "delivery_errors=delivery_errors, unverified_targets=unverified_targets)")
        # SAKUYA_PATCH_ANCHOR_LIVE_MEDIA (2026-11-24): v0.23.0 widened the call to
        # `_live_send_media(t, media_metadata, media_files, delivery_errors,
        # unverified_targets, topic_name)`. Match on the call shape instead of the
        # full argument list so the anchor survives added parameters.
        statement(live_lane, "_live_send_media(t, media_metadata, media_files, delivery_errors, "
                             "unverified_targets, topic_name)")
        statement(live_lane, "_seed_live_delivery_sessions(t, delivered_message_id)")
    if standalone_lane is not None and standalone is not None:
        statement(standalone_lane, "result, err = _standalone_send(t, content, media_files)")
        statement(standalone, "if not content.strip() and not media_files:\n"
                  "    return _warned(f'standalone send skipped (empty text and no media) for {t.where}')")

    blocks: list[tuple[ast.stmt, list[str]]] = []
    if live is not None and live_lane is not None:
        live_anchor = statement(live, "router = DeliveryRouter(t.config, t.target_adapters)")
        if live_anchor not in live.body:
            raise PatcherError("Split cron text-send seams must be direct function statements")
        blocks.append((live_anchor, [
            "try:",
            "    from hermes_lark_streaming.split_cron import _try_cron_card",
            "    _lark_cron_sent = _try_cron_card(",
            "        t, text_to_send, route_thread_id=route_thread_id,",
            "        route_metadata=route_metadata)",
            "except Exception:",
            "    _lark_cron_sent = False",
            "if _lark_cron_sent:",
            "    unverified_targets.append(t.where)",
            "    return True, False, None",
        ]))

    standalone_anchor: ast.stmt | None = None
    if standalone is not None:
        standalone_anchor = statement(standalone, "coro = _send()")
        if standalone_anchor not in standalone.body:
            raise PatcherError("Split cron text-send seams must be direct function statements")
        send_functions = [n for n in standalone.body
                          if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == "_send"]
        if len(send_functions) != 1:
            raise PatcherError("Split cron requires exactly one standalone _send closure")
        # Confirm this closure is the platform send channel. Upstream later wrapped the call in
        # ``asyncio.wait_for(...)`` and made the closure async (send-timeout guard), so accept both
        # the bare ``return _send_to_platform(...)`` and the awaited ``wait_for``-wrapped form.
        send_returns = [n for n in _scoped_nodes(send_functions[0])
                        if isinstance(n, ast.Return) and n.value is not None]
        if len(send_returns) != 1:
            raise PatcherError("Split cron requires exactly one return in the _send closure")
        send_call = send_returns[0].value
        assert send_call is not None  # narrowing: the comprehension predicate guarantees this
        if isinstance(send_call, ast.Await):
            send_call = send_call.value
        if (isinstance(send_call, ast.Call) and ast.unparse(send_call.func) == "asyncio.wait_for"
                and send_call.args):
            send_call = send_call.args[0]
        # SAKUYA_PATCH_ANCHOR_SEND_TO_PLATFORM (2026-11-24): v0.23.0 added
        # `notify=t.notify_delivery, force_document=t.force_document` to the call.
        # Match on the callee and the leading positional arguments instead of the
        # full argument list so added keywords do not break the anchor.
        if not (isinstance(send_call, ast.Call)
                and ast.unparse(send_call.func) == "_send_to_platform"
                and send_call.args
                and ast.unparse(send_call.args[0]) == "t.platform"
                and ast.unparse(send_call.args[1]) == "t.pconfig"
                and ast.unparse(send_call.args[2]) == "t.chat_id"
                and ast.unparse(send_call.args[3]) == "content"):
            raise PatcherError(
                "Split cron seam missing or ambiguous in _send: return _send_to_platform(...)")
        blocks.append((standalone_anchor, [
            "try:",
            "    from hermes_lark_streaming.split_cron import _try_cron_card",
            "    _lark_cron_sent = _try_cron_card(t, content)",
            "except Exception:",
            "    _lark_cron_sent = False",
            "if _lark_cron_sent:",
            "    if not media_files:",
            "        return {'success': True}, None",
            "    content = ''",
        ]))
    lines = content.splitlines(keepends=True)
    newline = "\r\n" if "\r\n" in content else "\n"
    for anchor, body in sorted(blocks, key=lambda pair: pair[0].lineno, reverse=True):
        line = lines[anchor.lineno - 1]
        indent = line[:len(line) - len(line.lstrip())]
        if not indent or line[:anchor.col_offset].strip():
            raise PatcherError("Split cron seam does not occupy a separate indented line")
        block = [MK_CRON_DELIVER, *body, MK_CRON_DELIVER_END]
        lines.insert(anchor.lineno - 1, "".join(indent + part + newline for part in block))
    patched = "".join(lines)
    try:
        compile(patched, "<split-cron-patched>", "exec")
    except (SyntaxError, ValueError) as exc:
        raise PatcherError(f"Split cron injection failed compile dry-run: {exc}") from exc
    return patched
