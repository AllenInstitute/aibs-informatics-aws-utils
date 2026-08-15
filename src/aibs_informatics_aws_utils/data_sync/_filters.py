"""Internal helpers for unpacking :class:`DataSyncFilterConfig` at call sites.

Lives in its own module rather than in :mod:`.file_system` or :mod:`.operations`
so both can use it: ``data_sync/__init__`` imports from both of those, so a
helper hosted in either would have to be imported across them.
"""

from __future__ import annotations

from re import Pattern

from aibs_informatics_core.models.data_sync import DataSyncFilterConfig

__all__ = ["extract_filter_patterns"]


def extract_filter_patterns(
    filter_config: DataSyncFilterConfig | None,
) -> tuple[list[Pattern] | None, list[Pattern] | None]:
    """Unpack a (possibly absent) filter config into include/exclude patterns.

    Every filtering call site needs the same ``None``-guarded unpack, and the
    filters contract treats an absent config and a config with no patterns
    identically. Keeping the unpack in one place means a change to how patterns
    are read off the config is a change to one line rather than to every caller.

    Args:
        filter_config: The filter config to unpack, or None if unfiltered.

    Returns:
        An ``(include, exclude)`` tuple, both None when ``filter_config`` is None.
    """
    if filter_config is None:
        return None, None
    return filter_config.include_patterns, filter_config.exclude_patterns
