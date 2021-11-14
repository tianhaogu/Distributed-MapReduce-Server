"""Store helper classes and functions in this file."""
import time
from enum import Enum


class WorkerState(Enum):
    """Enumerate class for the worker state."""

    READY = 1
    BUSY = 2
    DEAD = 3


class WorkerInDict:
    """Class for storing workers in the manager's workers dictionary."""

    def __init__(self, pid, worker_host, worker_port):
        """Construct member variables."""
        self.pid = pid
        self.worker_host = worker_host
        self.worker_port = worker_port
        self.state = WorkerState.READY
        self.last_hb_time = time.time()
        self.curr_task = None

    def access_curr_task(self):
        """Access the current task."""
        return self.curr_task

    def modify_curr_task(self, filelist):
        """Modify the current task."""
        self.curr_task = filelist


class Job:
    """Class for storing job in the manager's jobQueue."""

    def __init__(self, job_count, message_dict):
        """Construct member variables."""
        self.jid = job_count
        self.message_dict = message_dict
