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
    def __init__(self, job_count, input_directory, output_directory,
            mapper_executable, reducer_executable,
            num_mappers, num_reducers):
        self.jid = job_count
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.mapper_executable = mapper_executable
        self.reducer_executable = reducer_executable
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
