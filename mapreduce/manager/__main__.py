"""Map-reducer manager.

The purpose of this class is run a manager to recieve message from users
and communicate with workers to assign the tasks and coordinate task progress.
Author: Chenkai, Ori, Tianhao
Email: gtianhao@umich.edu
"""
import os
import threading
import socket
import logging
import json
import time
import shutil
import heapq
from pathlib import Path
from queue import Queue
from contextlib import ExitStack
import click
from mapreduce.utils import WorkerState, WorkerInDict, Job, \
    mapping_partition, forward_ack_registration

logging.basicConfig(level=logging.DEBUG)


class Manager:
    """Implement all operations of the Manager Module."""

    def __init__(self, port, hb_port):
        """Construct member variables, functions and threads."""
        logging.info("Starting manager:%s", port)
        logging.info("Manager:%s PWD %s", port, os.getcwd())

        self.one = {
            "port": port,
            "hb_port": hb_port,
            "tmp": Path("tmp"),
            "server_state": 'READY',
            "task_state": 'FREE',
            "shutdown": False
        }

        self.two = {
            "readyed_workers": Queue(),
            "filelist_remaining": Queue(),
            "num_list_remaining": 0
        }

        self.thr = {
            "job_queue": Queue(),
            "job_counter": 0
        }

        self.workers = {}
        self.message_dict = {"message_type": ''}

        self._create_folder()
        self.udp_hb_thread = threading.Thread(
            target=self.listen_hb_message
        )
        self.udp_hb_thread.start()
        self.fault_tol_thread = threading.Thread(
            target=self.fault_tolerance
        )
        self.fault_tol_thread.start()
        self.listen_incoming_message()

        self._forward_shutdown()
        self.fault_tol_thread.join()
        self.udp_hb_thread.join()

    def _create_folder(self):
        """Create the tmp folder when the class is constructed."""
        tmp = self.one["tmp"]
        tmp.mkdir(exist_ok=True)
        for old_job_folder in tmp.glob('job-*'):
            shutil.rmtree(old_job_folder)

    def listen_hb_message(self):
        """Listen heartbeat message sent from workers."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.one["hb_port"]))
            sock.settimeout(1)
            while not self.one["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    msg_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                logging.debug(msg_dict)
                pid = msg_dict["worker_pid"]
                if pid in self.workers:
                    self.workers[pid].last_hb_time = time.time()
                print("Receive heartbeat message from Worker[%s]", pid)

    def fault_tolerance(self):
        """Handle fault tolerance."""
        while not self.one["shutdown"]:
            for pid, worker in self.workers.items():
                if worker.state != WorkerState.DEAD:
                    if (time.time() - worker.last_hb_time) > 10:
                        prev_state = worker.state
                        self.workers[worker.pid].state = WorkerState.DEAD
                        # worker.state = WorkerState.DEAD
                        print("Worker[%s] is dead!", pid)
                        if prev_state == WorkerState.BUSY:
                            self._fault_busy_handle(worker)
            # time.sleep(0.1)

    def listen_incoming_message(self):
        """Listen to incoming messages from workers and the command line."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.one["port"]))
            sock.listen()
            sock.settimeout(1)

            while not self.one["shutdown"]:
                self._check_taskjob_at_beginning()
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    msg_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                logging.debug(msg_dict)
                self._handle_incoming_message(msg_dict)

    def _check_taskjob_at_beginning(self):
        """Check and assign in-executing task and in-queue job at beginning."""
        if not self.two["filelist_remaining"].empty():
            if not self.two["readyed_workers"].empty():
                curr_filelist = self.two["filelist_remaining"].get()
                first_worker = self.two["readyed_workers"].get()
                job_id = self.message_dict["job_id"]
                if self.one["task_state"] in (
                        'MAPPING', 'MAPPING_END', 'REDUCING', 'REDUCING_END'):
                    output_directory = \
                        self.one["tmp"] / f"job-{job_id}" / "mapper-output" \
                        if self.one["task_state"] in \
                        ('MAPPING', 'MAPPING_END') \
                        else \
                        self.one["tmp"] / f"job-{job_id}" / "reducer-output"
                    executable = self.message_dict["mapper_executable"] \
                        if self.one["task_state"] in \
                        ('MAPPING', 'MAPPING_END') \
                        else self.message_dict["reducer_executable"]
                    self._send_mapping_task(
                        curr_filelist, executable,
                        output_directory, first_worker.pid
                    )
                if self.one["task_state"] == 'GROUPING_ONE':
                    self._send_grouping_task(
                        curr_filelist[0], curr_filelist[1], first_worker.pid
                    )
                self.workers[first_worker.pid].state = WorkerState.BUSY
                self.workers[first_worker.pid].modify_curr_task(curr_filelist)
                if self.two["filelist_remaining"].empty():
                    if self.one["task_state"] == 'MAPPING':
                        self.one["task_state"] = 'MAPPING_END'
                    if self.one["task_state"] == 'REDUCING':
                        self.one["task_state"] = 'REDUCING_END'
        elif self.two["filelist_remaining"].empty() and \
                self.one["task_state"] != 'FREE':
            pass
        elif not self.thr["job_queue"].empty():
            if self.one["server_state"] == 'READY':
                self._get_readyed_workers()
                if not self.two["readyed_workers"].empty():
                    self.one["server_state"] = 'EXECUTING'
                    curr_job = self.thr["job_queue"].get()
                    self.message_dict = curr_job.message_dict
                    self._job_execution(curr_job.message_dict, "mapping")
        else:
            pass

    def _fault_busy_handle(self, worker):
        """Handle next step when finding the dead worker was busy before."""
        curr_task = worker.access_curr_task()
        self.two["filelist_remaining"].put(curr_task)
        whether_find = False
        while not whether_find:
            self._get_readyed_workers()
            if self.two["readyed_workers"].qsize() > 0:
                whether_find = True
                self._check_taskjob_at_beginning()

    def _handle_incoming_message(self, msg_dict):
        """Handle any message received in the TCP socket."""
        if msg_dict["message_type"] == "shutdown":
            self._handle_shutdown()
        elif msg_dict["message_type"] == "register":
            self._handle_register(msg_dict)
            self._check_taskjob_for_worker(msg_dict["worker_pid"])
        elif msg_dict["message_type"] == "new_manager_job":
            self._handle_new_manager_job(msg_dict)
        elif msg_dict["message_type"] == "status":
            self._handle_status(msg_dict)
        else:
            pass

    def _handle_shutdown(self):
        """Shutdown the manager and consequently shutdown all workers."""
        self.one["shutdown"] = True
        # self._forward_shutdown()

    def _handle_register(self, msg_dict):
        """Register the worker, send ack message and put it into the Dict."""
        worker_host = msg_dict["worker_host"]
        worker_port = msg_dict["worker_port"]
        worker_pid = msg_dict["worker_pid"]
        forward_ack_registration(
            worker_host, worker_port, worker_pid
        )
        self.workers[worker_pid] = WorkerInDict(
            worker_pid, worker_host, worker_port
        )

    def _handle_new_manager_job(self, msg_dict):
        """Handle the new coming job, execute it or put it in the job_queue."""
        job_id = self.thr["job_counter"]
        msg_dict["job_id"] = job_id
        self._create_directories(msg_dict)
        whether_ready = self._check_worker_server()
        if whether_ready:
            self._get_readyed_workers()
            self.message_dict = msg_dict
            self.thr["job_counter"] += 1
            self.one["server_state"] = "EXECUTING"
            self._job_execution(msg_dict, "mapping")  # start of job, only map
        else:
            self.thr["job_queue"].put(Job(self.thr["job_counter"], msg_dict))
            self.thr["job_counter"] += 1

    def _handle_status(self, msg_dict):
        """Handle the worker status message, set the task and worker Queue."""
        pid = msg_dict["worker_pid"]
        self.workers[pid].state = WorkerState.READY
        if self.one["task_state"] == 'MAPPING':
            self.two["num_list_remaining"] -= 1
            if not self.two["filelist_remaining"].empty():
                self.two["readyed_workers"].put(self.workers[pid])
        elif self.one["task_state"] == 'MAPPING_END':
            self.two["num_list_remaining"] -= 1
            if self.two["num_list_remaining"] == 0:
                logging.info("Manager:%s end map stage", self.one["port"])
                self._get_readyed_workers()
                self._job_execution(self.message_dict, "grouping_one")
        elif self.one["task_state"] == 'GROUPING_ONE':
            self.two["num_list_remaining"] -= 1
            if self.two["num_list_remaining"] == 0:
                self._get_readyed_workers()
                self._job_execution(self.message_dict, "grouping_two")
                logging.info("Manager:%s end group stage", self.one["port"])
                self._job_execution(self.message_dict, "reducing")
        elif self.one["task_state"] == 'REDUCING':
            self.two["num_list_remaining"] -= 1
            if not self.two["filelist_remaining"].empty():
                self.two["readyed_workers"].put(self.workers[pid])
        elif self.one["task_state"] == 'REDUCING_END':
            self.two["num_list_remaining"] -= 1
            if self.two["num_list_remaining"] == 0:
                logging.info("Manager:%s end reduce stage", self.one["port"])
                self.two["readyed_workers"].queue.clear()
                self._job_execution(self.message_dict, "wrapping")
        else:
            logging.info("ERROR! Unknown executing job state!")

    def _forward_shutdown(self):
        """Forward the shutdown message to all the workers in the Dict."""
        for worker in self.workers.values():
            if worker.state != WorkerState.DEAD:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((worker.worker_host, worker.worker_port))
                    shutdown_message = json.dumps({"message_type": "shutdown"})
                    sock.sendall(shutdown_message.encode('utf-8'))
        self.one["shutdown"] = True

    def _get_readyed_workers(self):
        """Get non-busy or dead workers at the beginning of all 3 stages."""
        self.two["readyed_workers"].queue.clear()
        for worker in self.workers.values():
            if worker.state not in (WorkerState.BUSY, WorkerState.DEAD):
                self.two["readyed_workers"].put(worker)

    def _create_directories(self, msg_dict):
        """Create the directory for all input & output when new job comes."""
        job_id = msg_dict["job_id"]
        first_layer = self.one["tmp"] / f"job-{job_id}"
        Path.mkdir(first_layer)
        second_layer_mapper = first_layer / 'mapper-output'
        second_layer_grouper = first_layer / 'grouper-output'
        second_layer_reducer = first_layer / 'reducer-output'
        output_layer_final = Path(msg_dict["output_directory"])
        Path.mkdir(second_layer_mapper)
        Path.mkdir(second_layer_grouper)
        Path.mkdir(second_layer_reducer)
        Path.mkdir(output_layer_final)

    def _check_worker_server(self):
        """Check avaiable workers and whether server is ready for execution."""
        whether_all_busy = True
        for worker in self.workers.values():
            if worker.state not in (WorkerState.BUSY, WorkerState.DEAD):
                whether_all_busy = False
                break
        if whether_all_busy or self.one["server_state"] != 'READY':
            return False
        return True

    def _check_taskjob_for_worker(self, pid):
        """Check currently executing task or pending task/job for worker."""
        if self.one["server_state"] == "EXECUTING" and \
                not self.two["filelist_remaining"].empty():
            self.two["readyed_workers"].put(self.workers[pid])

    def _job_execution(self, msg_dict, task_signal):
        """Start to execute the mapping task, note only mapping currently."""
        if task_signal == "mapping":
            partitioned_filelist = mapping_partition(msg_dict)
            self._execute_map(msg_dict, partitioned_filelist)
        elif task_signal == "grouping_one":
            partitioned_filelist = self._grouping_partition(msg_dict)
            self._execute_group(msg_dict, partitioned_filelist)
        elif task_signal == "grouping_two":
            self._execute_merge(msg_dict)
        elif task_signal == "reducing":
            partitioned_filelist = self._reducing_partition(msg_dict)
            self._execute_reduce(msg_dict, partitioned_filelist)
        elif task_signal == "wrapping":
            self._execute_wrap(msg_dict)
        else:
            pass

    def _grouping_partition(self, msg_dict):
        """Perform partition on the matching of files and readyed workers."""
        input_files = []
        job_id = msg_dict["job_id"]
        input_directory = self.one["tmp"] / f"job-{job_id}" / "mapper-output"
        for file in input_directory.glob('*'):
            input_files.append(str(file))
        input_files.sort()
        partitioned_filelist = []
        min_file_worker = min(
            len(input_files), self.two["readyed_workers"].qsize()
        )
        for _ in range(min_file_worker):
            partitioned_filelist.append([])
        for index, input_file in enumerate(input_files):
            partitioned_index = index % min_file_worker
            partitioned_filelist[partitioned_index].append(input_file)
        return partitioned_filelist

    def _reducing_partition(self, msg_dict):
        """Return a list of list of files according to number of reducers."""
        input_files = []
        job_id = msg_dict["job_id"]
        input_directory = self.one["tmp"] / f"job-{job_id}" / "grouper-output"
        for file in input_directory.glob('reduce*'):
            input_files.append(str(file))
        input_files.sort()
        partitioned_filelist = []
        for _ in range(msg_dict["num_reducers"]):
            partitioned_filelist.append([])
        for index, file in enumerate(input_files):
            partitioned_index = index % msg_dict["num_reducers"]
            partitioned_filelist[partitioned_index].append(file)
        return partitioned_filelist

    def _execute_map(self, msg_dict, partitioned_filelist):
        """Execute mapping stage, assign mapping tasks to readyed workers."""
        job_id = msg_dict["job_id"]
        output_directory = self.one["tmp"] / f"job-{job_id}" / "mapper-output"
        for filelist in partitioned_filelist:
            self.two["filelist_remaining"].put(filelist)
        logging.info("Manager:%s begin map stage", self.one["port"])
        self.one["task_state"] = 'MAPPING'
        self.two["num_list_remaining"] = len(partitioned_filelist)
        num_of_original_workers = self.two["readyed_workers"].qsize()
        while num_of_original_workers > 0:
            if not self.two["filelist_remaining"].empty():
                curr_filelist = self.two["filelist_remaining"].get()
                top_worker = self.two["readyed_workers"].get()
                self._send_mapping_task(
                    curr_filelist, msg_dict["mapper_executable"],
                    output_directory, top_worker.pid
                )
                self.workers[top_worker.pid].state = WorkerState.BUSY
                self.workers[top_worker.pid].modify_curr_task(curr_filelist)
                num_of_original_workers -= 1
            else:
                break
        if self.two["filelist_remaining"].empty():
            self.one["task_state"] = 'MAPPING_END'

    def _execute_group(self, msg_dict, partitioned_filelist):
        """Execute grouping stage, assign sorting tasks to readyed workers."""
        logging.info("Manager:%s begin group stage", self.one["port"])
        self.one["task_state"] = 'GROUPING_ONE'
        self.two["num_list_remaining"] = len(partitioned_filelist)
        for index, filelist in enumerate(partitioned_filelist):
            curr_worker = self.two["readyed_workers"].get()
            job_id = msg_dict["job_id"]
            sortfile_index = str(index + 1).zfill(2)
            output_file = self.one["tmp"] / f"job-{job_id}" /\
                "grouper-output" / f"sorted{sortfile_index}"
            self._send_grouping_task(
                filelist, output_file, curr_worker.pid
            )
            self.workers[curr_worker.pid].state = WorkerState.BUSY
            self.workers[curr_worker.pid].modify_curr_task(
                (filelist, output_file)
            )

    def _execute_merge(self, msg_dict):
        """Merge sort all lines in all files, then merge lines to reduces."""
        self.one["task_state"] = 'GROUPING_TWO'
        job_id = msg_dict["job_id"]
        inout_directory = self.one["tmp"] / f"job-{job_id}" / "grouper-output"
        input_filelist = \
            [str(file) for file in inout_directory.glob('sorted*')]
        output_filelist = \
            [str(inout_directory) + '/reduce' + str(i + 1).zfill(2)
             for i in range(msg_dict["num_reducers"])]
        key_counter = 0
        last_key = ''
        with ExitStack() as stack:
            inlines = [stack.enter_context(open(fname, 'r', encoding='utf-8'))
                       for fname in input_filelist]
            outlines = [stack.enter_context(open(fname, 'w', encoding='utf-8'))
                        for fname in output_filelist]
            for index, line in enumerate(heapq.merge(*inlines)):
                key = line.split('\t')[0]
                if index == 0:
                    last_key = key
                if last_key != key:
                    key_counter += 1
                reducefile_index = key_counter % msg_dict["num_reducers"]
                outlines[reducefile_index].write(line)
                last_key = key

    def _execute_reduce(self, msg_dict, partitioned_filelist):
        """Execute reducing stage, assign reducing tasks to readyed workers."""
        job_id = msg_dict["job_id"]
        output_directory = self.one["tmp"] / f"job-{job_id}" / "reducer-output"
        for filelist in partitioned_filelist:
            self.two["filelist_remaining"].put(filelist)
        logging.info("Manager:%s begin reduce stage", self.one["port"])
        self.one["task_state"] = 'REDUCING'
        self.two["num_list_remaining"] = len(partitioned_filelist)
        num_of_original_workers = self.two["readyed_workers"].qsize()
        while num_of_original_workers > 0:
            if not self.two["filelist_remaining"].empty():
                curr_filelist = self.two["filelist_remaining"].get()
                curr_worker = self.two["readyed_workers"].get()
                self._send_mapping_task(
                    curr_filelist, msg_dict["reducer_executable"],
                    output_directory, curr_worker.pid
                )
                self.workers[curr_worker.pid].state = WorkerState.BUSY
                self.workers[curr_worker.pid].modify_curr_task(curr_filelist)
                num_of_original_workers -= 1
            else:
                break
        if self.two["filelist_remaining"].empty():
            self.one["task_state"] = 'REDUCING_END'

    def _execute_wrap(self, msg_dict):
        """Execute wrapping up stage of manager, move and rename files."""
        self.one["task_state"] = 'WRAPPING'
        job_id = msg_dict["job_id"]
        input_directory = self.one["tmp"] / f"job-{job_id}" / "reducer-output"
        output_directory = Path(msg_dict["output_directory"])
        input_filelist = [str(file) for file in input_directory.glob('*')]
        output_filelist = \
            [output_directory / ("outputfile" + str(i + 1).zfill(2))
             for i in range(len(input_filelist))]
        for index, output_file in enumerate(output_filelist):
            if not os.path.exists(output_file.parent):
                os.mkdir(output_file.parent)
            shutil.move(input_filelist[index], output_file)
        self.one["task_state"] = 'FREE'
        self.one["server_state"] = 'READY'

    def _send_mapping_task(self, filelist, executable, output_directory, pid):
        """Send the mapping task message to the corresponding worker."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(
                (self.workers[pid].worker_host, self.workers[pid].worker_port)
            )
            mapping_message = json.dumps({
                "message_type": "new_worker_task",
                "input_files": filelist,
                "executable": str(executable),
                "output_directory": str(output_directory),
                "worker_pid": pid
            })
            sock.sendall(mapping_message.encode('utf-8'))
            logging.debug(mapping_message)

    def _send_grouping_task(self, filelist, output_file, pid):
        """Send the grouping(sorting) task to the corresponding worker."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(
                (self.workers[pid].worker_host, self.workers[pid].worker_port)
            )
            grouping_message = json.dumps({
                "message_type": "new_sort_task",
                "input_files": filelist,
                "output_file": str(output_file),
                "worker_pid": pid
            })
            sock.sendall(grouping_message.encode('utf-8'))
            logging.debug(grouping_message)


@click.command()
@click.argument("port", nargs=1, type=int)
@click.argument("hb_port", nargs=1, type=int)
def main(port, hb_port):
    """Begin the Manager Module."""
    Manager(port, hb_port)


if __name__ == '__main__':
    main()
