import asyncio
import pprint
import time
from datetime import datetime
from functools import partial, wraps
from typing import Any


def to_async(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        func_partial = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, func_partial)

    return run


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        pprint.pprint(
            f"Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds"
        )
        return result

    return timeit_wrapper


def safe_get(dct: dict, *keys) -> Any:
    """Safely retrieve a value from a nested dictionary.

    Parameters
    ----------
    dct : dict
        A dictionary containing nested keys and values.
    *keys : Any
        One or more keys to use when traversing the dictionary.

    Returns
    -------
    Any
        The value at the end of the key traversal, or None if any of the keys are missing.
    """
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


def datetime_from_iso_str(date: str) -> datetime | None:
    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z") if date else None
