"""Runtime configuration, overridable via environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_MAX_FILE_BYTES = 32 * 1024 * 1024  # 32 MiB
DEFAULT_QUORUM = 2  # distinct build machines that must agree before a hit


@dataclass(frozen=True)
class Settings:
    data_dir: str
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES
    quorum: int = DEFAULT_QUORUM


def load_settings() -> Settings:
    return Settings(
        data_dir=os.environ.get("CACHE_DATA_DIR", "./data"),
        max_file_bytes=int(
            os.environ.get("CACHE_MAX_FILE_BYTES", str(DEFAULT_MAX_FILE_BYTES))
        ),
        quorum=int(os.environ.get("CACHE_QUORUM", str(DEFAULT_QUORUM))),
    )
