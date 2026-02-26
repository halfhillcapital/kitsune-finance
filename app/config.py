from __future__ import annotations

import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://kitsune:kitsune@localhost:5432/kitsune"
)
