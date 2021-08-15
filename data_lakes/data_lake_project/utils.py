import time

from functools import wraps

def timer(func):
    '''
    Calculate the executing time of the function.
    '''
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print('%s totally elapsed time: %.2f seconds' % (func.__name__, elapsed_time))
        return result
    return wrapper_func