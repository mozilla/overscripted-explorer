""" Put processes in a seperate thread

This beauty is used to be able to decorate functions with pyspark work, so that
the work can be done in another thread and we can update the user on the pyspark progress.
"""
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from bokeh.io import curdoc

# It's important to define these out here
DOC = curdoc()
EXECUTOR = ThreadPoolExecutor(max_workers=4)


def unblock_with_finish(to_finish):
    def unblock(to_pool):
        def wrapper(*args, **kwargs):
            EXECUTOR.submit(
                partial(to_pool, *args, **kwargs)
            ).add_done_callback(
                lambda x: DOC.add_next_tick_callback(to_finish)
            )
        return wrapper
    return unblock
