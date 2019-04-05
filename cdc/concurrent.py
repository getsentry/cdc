from concurrent.futures import Future
from threading import Thread


def execute(function, daemon=True):
    future = Future()

    def run():
        if not future.set_running_or_notify_cancel():
            return

        try:
            result = function()
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(result)

    t = Thread(target=run)
    t.daemon = daemon
    t.start()

    return future
