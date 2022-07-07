import functools


def exception(logger, err_ret_val=None):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except Exception as e:
                logger.exception(f'Function: {function.__name__}')
                return err_ret_val
        return wrapper
    return decorator
