"""Shared bounded retry delays; jitter avoids synchronized workers after an outage."""

import random


def retry_delay(base_seconds: float, attempt: int) -> float:
    ceiling = float(min(base_seconds * 2 ** min(max(attempt - 1, 0), 20), 600))
    return ceiling * random.uniform(0.8, 1.0)
