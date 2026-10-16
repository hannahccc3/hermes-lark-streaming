__version__ = "0.13.0"

# SAKUYA_REGISTER_BRIDGE_V1 (2026-10-16): no-op bridge for Hermes 0.20+ plugin discovery.
# This plugin operates via AST-injected hooks (`python -m hermes_lark_streaming install`
# into gateway/run_*.py + cron/scheduler_delivery.py), NOT via PluginContext registration.
# The loader calls register(ctx) on every discover tick and warns "no register() function"
# (~2 lines / 5 min in errors.log) when it is absent. This bridge registers nothing by
# design; it only satisfies discovery so the warning stops. See
# software-development/plugin-patch-defense "register(ctx) bridge" section.
def register(_ctx: object) -> None:  # noqa: ARG001 - ctx intentionally unused
    """Declare this AST-hook plugin loadable to Hermes discovery (no-op)."""
    return None

