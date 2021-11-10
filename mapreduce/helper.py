import os
import threading
import socket
import logging
import json
import time
import click
from enum import Enum
import mapreduce.utils


logging.basicConfig(level=logging.DEBUG)


class WorkerState(Enum):
    READY = 1
    BUSY = 2
    DEAD = 3


class WorkerInDict:
    def __init__(self, pid, worker_host, worker_port):
        self.pid = pid
        self.worker_host = worker_host
        self.worker_port = worker_port
        self.state = WorkerState.READY


class Job:
    def __init__(self, message_dict):
        self.jid = job_count
        self.message_dict = message_dict
