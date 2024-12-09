"""Utility functions to aid the scorers."""
import time

from threading import Semaphore
from typing import Any
import logging


class VertexAPIException(Exception):
    """Vertex API Exception Class"""


def rate_limited_execute(
    prompt: str,
    generation_config,
    execution_method,
    execs_per_minute: int,
    semaphore: Semaphore,
    max_attempts: int,
) -> Any:
    """Rate limit the generate_content method"""
    semaphore.acquire()
    attempt = 1
    while attempt <= max_attempts:
        try:
            result = execution_method(prompt, generation_config=generation_config)
            break
        except VertexAPIException as e:
            # exponentially backoff starting at 5 seconds
            time.sleep(5 * (2 ** (attempt)))
            attempt += 1
            logging.debug('Vertex API Exception: %s', e)

    time.sleep(60 / execs_per_minute)
    semaphore.release()
    return result


def make_hashable(value):
    if isinstance(value, list):
        return tuple(value)
    elif isinstance(value, dict):
        return frozenset((k, make_hashable(v)) for k, v in value.items())
    return value
