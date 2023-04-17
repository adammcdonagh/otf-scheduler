import time


def func(kwargs=None):
    # Get the sleep time from the kwargs
    # If sleep exists
    if kwargs and "sleep" in kwargs:
        sleep = kwargs.get("sleep", 0)
        # Sleep for the given time
        time.sleep(sleep)
    print("Hello World")
