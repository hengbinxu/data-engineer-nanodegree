import time
from functools import wraps

def timer(func):
    """
    Calculate the executing of the function. 
    """
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print('%s totally elapsed time: %.1f seconds\n' % (func.__name__, elapsed_time))
        return result
    return wrapper_func
