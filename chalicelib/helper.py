import functools,datetime,time
from multiprocessing import Process, Pipe
from multiprocessing.connection import wait

class Pool():
    """Naive implementation of a process pool with mp.Pool API.
    This is useful since multiprocessing.Pool uses a Queue in /dev/shm, which
    is not mounted in an AWS Lambda environment.
    """

    def __init__(self, process_count=1):
        assert process_count >= 1
        self.process_count = process_count

    @staticmethod
    def wrap_pipe(pipe, index, func):
        def wrapper(args):
            try:
                result = func(args)
            except Exception as exc:  # pylint: disable=broad-except
                result = exc
            pipe.send((index, result))
        return wrapper

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def map(self, function, arguments):
        pending = list(enumerate(arguments))
        running = []
        finished = [None] * len(pending)
        while pending or running:
            # Fill the running queue with new jobs
            while len(running) < self.process_count:
                if not pending:
                    break
                index, args = pending.pop(0)
                pipe_parent, pipe_child = Pipe(False)
                process = Process(
                    target=Pool.wrap_pipe(pipe_child, index, function),
                    args=(args, ))
                process.start()
                running.append((index, process, pipe_parent))
            # Wait for jobs to finish
            for pipe in wait(list(map(lambda t: t[2], running))):
                index, result = pipe.recv()
                # Remove the finished job from the running list
                running = list(filter(lambda x: x[0] != index, running))
                # Add the result to the finished list
                finished[index] = result

        return finished

def repeatEveryDay(hour=0, minutes=0, seconds=0):
    """
    Decorator that will run the decorated function everyday at that hour, minutes and seconds.
    :param hour: 0-24
    :param minutes: 0-60 (Optional)
    :param seconds: 0-60 (Optional)
    """
    def decoratorRepeat(func):

        @functools.wraps(func)
        def wrapperRepeat(*args, **kwargs):

            def getLocalTime():
                return datetime.datetime.fromtimestamp(time.mktime(time.localtime()))

            # Get the datetime of the first function call
            td = datetime.timedelta(seconds=15)
            if wrapperRepeat.nextSent == None:
                now = getLocalTime()
                wrapperRepeat.nextSent = datetime.datetime(now.year, now.month, now.day, hour, minutes, seconds)
                if wrapperRepeat.nextSent < now:
                    wrapperRepeat.nextSent += td

            # Waiting till next day
            while getLocalTime() < wrapperRepeat.nextSent:
                time.sleep(1)

            # Call the function
            func(*args, **kwargs)

            # Get the datetime of the next function call
            wrapperRepeat.nextSent += td
            wrapperRepeat(*args, **kwargs)

        wrapperRepeat.nextSent = None
        return wrapperRepeat

    return decoratorRepeat