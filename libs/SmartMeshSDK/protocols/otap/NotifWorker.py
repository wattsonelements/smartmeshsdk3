import logging
import threading
import traceback
from queue import Queue


log = logging.getLogger('otap_communicator')


class NotifWorker(threading.Thread):
    '''
    NotifWorker is a simple worker thread for handling notifications.

    It can be subclassed or call out to external function tasks as appropriate.
    '''
    def __init__(self, is_daemon = True):
        threading.Thread.__init__(self)
        self.name = "NotifWorker"
        self.daemon = is_daemon
        self.tasks = Queue()
        self.start()
        
    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))
            
    def run(self):
        # wait for a task and process it
        while True:
            func, args, kargs = self.tasks.get()
            # TODO: need a sentinel task to indicate shutdown
            try:
                func(*args, **kargs)
            except Exception as e:
                # TODO: log this somewhere
                print("NotifWorker task raised Exception:")
                print("Error in %s: %s" % (func.__qualname__, e))
                print(traceback.format_exc())
                traceback.print_exc()
                log.exception("Error in %s: %s", func.__qualname__, e, exc_info=True, stack_info=True)
            self.tasks.task_done()

