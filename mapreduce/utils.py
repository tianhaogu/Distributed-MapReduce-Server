"""Utils file.

This file is to house code common between the Manager and the Worker

"""

import socket
import json
import time
from pathlib import Path
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

    def get_message_dict(self):
        """Access the message dictionary."""
        return self.message_dict

    def set_job_counter(self, jid):
        """Modify the job id."""
        self.jid = jid


def forward_ack_registration(worker_host, worker_port, worker_pid):
    """Forward the ack message to the corresponding worker."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((worker_host, worker_port))
        ack_message = json.dumps({
            "message_type": "register_ack",
            "worker_host": worker_host,
            "worker_port": worker_port,
            "worker_pid": worker_pid
        })
        sock.sendall(ack_message.encode('utf-8'))


def mapping_partition(msg_dict):
    """Return a list of list of files according to number of mappers."""
    input_filelist = []
    for file in Path(msg_dict["input_directory"]).glob('*'):
        input_filelist.append(str(file))
    input_filelist.sort()
    partitioned_filelist = []
    for _ in range(msg_dict["num_mappers"]):
        partitioned_filelist.append([])
    for index, file in enumerate(input_filelist):
        partitioned_index = index % msg_dict["num_mappers"]
        partitioned_filelist[partitioned_index].append(file)
    return partitioned_filelist
