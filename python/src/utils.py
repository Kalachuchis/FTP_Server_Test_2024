import time
from enum import Enum
from functools import wraps


def timerdecorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        print(
            bcolors.OKGREEN
            + f"{func.__name__} took {toc-tic:0.4f} seconds to execute"
            + bcolors.ENDC,
        )
        return value

    return wrapper


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
