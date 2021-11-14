"""Implement the Manager class."""
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
import mapreduce.utils
from mapreduce.helper import WorkerState, WorkerInDict, Job

logging.basicConfig(level=logging.DEBUG)


class Manager:
    """Implement all operations of the Manager Module."""

    def __init__(self, port, hb_port):
        """Construct member variables, functions and threads."""
        logging.info("Starting manager:%s", port)
        logging.info("Manager:%s PWD %s", port, os.getcwd())

        self.port = port
        self.hb_port = hb_port

        self.workers = {}
        self.jobQueue = Queue()
        self.jobCounter = 0
        self.serverState = 'READY'
        self.exeJobState = 'FREE'
        self.readyed_workers = Queue()
        # self.dead_worker_busy = []
        self.filelist_remaining = Queue()
        self.num_list_remaining = 0

        self.shutdown = False
        self.message_dict = {"message_type": ''}

        self.tmp = Path('tmp')
        self.createFolder()
        self.udpHBThread = threading.Thread(
            target=self.listenHBMessage
        )
        self.udpHBThread.start()
        self.faultTolThread = threading.Thread(
            target=self.faultTolerance
        )
        self.faultTolThread.start()
        self.listenIncomingMessage()

        self.forwardShutdown()
        self.faultTolThread.join()
        self.udpHBThread.join()

    def createFolder(self):
        """Create the tmp folder when the class is constructed."""
        tmp = self.tmp
        tmp.mkdir(exist_ok=True)
        for oldJobFolder in tmp.glob('job-*'):
            shutil.rmtree(oldJobFolder)

    def listenHBMessage(self):
        """Listen heartbeat message sent from workers."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.hb_port))
            sock.settimeout(1)
            while not self.shutdown:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    msg_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(msg_dict)
                pid = msg_dict["worker_pid"]
                if pid in self.workers:
                    self.workers[pid].last_hb_time = time.time()
                print("Receive heartbeat message from Worker[%s]", pid)

    def faultTolerance(self):
        """Handle fault tolerance."""
        while not self.shutdown:
            for pid, worker in self.workers.items():
                if self.workers[pid].state != WorkerState.DEAD:
                    if (time.time() - worker.last_hb_time) > 10:
                        prev_state = self.workers[pid].state
                        self.workers[pid].state = WorkerState.DEAD
                        print("Worker[%s] is dead!", pid)
                        if prev_state == WorkerState.BUSY:
                            curr_task = worker.access_curr_task()
                            self.filelist_remaining.put(curr_task)
                            # self.dead_worker_busy.append(curr_task)
                            whether_find = False
                            while not whether_find:
                                self.getReadyedWorkers()
                                if self.readyed_workers.qsize() > 0:
                                    whether_find = True
                                    self.checkTaskJobAtBeginning()
            # time.sleep(0.1)

    def listenIncomingMessage(self):
        """Listen to incoming messages from workers and the command line.
        Seems as the main thread of this program."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.port))
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown:
                self.checkTaskJobAtBeginning()
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
                            # if len(self.dead_worker_busy) != 0:
                            #     print("Detect dead worker!!!!!!!!!!!!!!!!!!!!")
                            #     self.getReadyedWorkers()
                            #     self.checkTaskJobAtBeginning()
                            #     self.dead_worker_busy.pop(0)
                            #     continue
                            break
                        message_chunks.append(data)
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    msg_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(msg_dict)
                self.handleIncomingMessage(msg_dict)

    def checkTaskJobAtBeginning(self):
        """Check in-executing tasks and in-queue job at the beginning and
        assign corresponding task/job to the worker if necessary."""
        if not self.filelist_remaining.empty():
            if not self.readyed_workers.empty():
                curr_filelist = self.filelist_remaining.get()
                first_worker = self.readyed_workers.get()
                job_id = self.message_dict["job_id"]
                if self.exeJobState in (
                        'MAPPING', 'MAPPING_END', 'REDUCING', 'REDUCING_END'):
                    output_directory = \
                        self.tmp / 'job-{}'.format(job_id) / "mapper-output" \
                        if self.exeJobState in ('MAPPING', 'MAPPING_END') else \
                        self.tmp / 'job-{}'.format(job_id) / "reducer-output"
                    executable = self.message_dict["mapper_executable"] \
                        if self.exeJobState in ('MAPPING', 'MAPPING_END') else \
                        self.message_dict["reducer_executable"]
                    self.sendMappingTask(
                        curr_filelist, executable,
                        output_directory, first_worker.pid
                    )
                if self.exeJobState == 'GROUPING_ONE':
                    self.sendGroupingTask(
                        curr_filelist[0], curr_filelist[1], first_worker.pid
                    )
                self.workers[first_worker.pid].state = WorkerState.BUSY
                self.workers[first_worker.pid].modify_curr_task(curr_filelist)
                if self.filelist_remaining.empty():
                    if self.exeJobState == 'MAPPING':
                        self.exeJobState = 'MAPPING_END'
                    if self.exeJobState == 'REDUCING':
                        self.exeJobState = 'REDUCING_END'
        elif self.filelist_remaining.empty() and self.exeJobState != 'FREE':
            pass
        elif not self.jobQueue.empty():
            if self.serverState == 'READY':
                self.readyed_workers.queue.clear()
                self.getReadyedWorkers()
                if not self.readyed_workers.empty():
                    self.serverState = 'EXECUTING'
                    curr_job = self.jobQueue.get()
                    self.message_dict = curr_job.message_dict
                    # self.dead_worker_busy.clear()
                    self.jobExecution(curr_job.message_dict, "mapping")
        else:
            pass

    def handleIncomingMessage(self, msg_dict):
        """Handle any message received in the TCP socket."""
        if msg_dict["message_type"] == "shutdown":
            self.handleShutdown()
        elif msg_dict["message_type"] == "register":
            self.handleRegister(msg_dict)
            self.checkTaskJobForWorker(msg_dict["worker_pid"])
        elif msg_dict["message_type"] == "new_manager_job":
            self.handleNewManagerJob(msg_dict)
        elif msg_dict["message_type"] == "status":
            self.handleStatus(msg_dict)
        else:
            pass

    def handleShutdown(self):
        """Shutdown the manager and consequently shutdown all workers."""
        self.shutdown = True
        # self.forwardShutdown()

    def handleRegister(self, msg_dict):
        """Register the worker, send ack message and put it into the Dict."""
        worker_host = msg_dict["worker_host"]
        worker_port = msg_dict["worker_port"]
        worker_pid = msg_dict["worker_pid"]
        self.forwardAckRegistration(
            worker_host, worker_port, worker_pid
        )
        self.workers[worker_pid] = WorkerInDict(
            worker_pid, worker_host, worker_port
        )

    def handleNewManagerJob(self, msg_dict):
        """Handle the new coming job, execute it or put it in the jobQueue
        depending on the the result of checkWorkerServer function."""
        job_id = self.jobCounter
        msg_dict["job_id"] = job_id
        self.createDirectories(msg_dict)
        whether_ready = self.checkWorkerServer()
        if whether_ready:
            self.getReadyedWorkers()
            self.message_dict = msg_dict
            self.jobCounter += 1
            self.serverState = "EXECUTING"
            # self.dead_worker_busy.clear()
            self.jobExecution(msg_dict, "mapping")  # start of job, only map
        else:
            self.jobQueue.put(Job(self.jobCounter, msg_dict))
            self.jobCounter += 1

    def handleStatus(self, msg_dict):
        """Handle the worker status message, set the task and worker Queue."""
        pid = msg_dict["worker_pid"]
        self.workers[pid].state = WorkerState.READY
        if self.exeJobState == 'MAPPING':
            self.num_list_remaining -= 1
            if not self.filelist_remaining.empty():
                self.readyed_workers.put(self.workers[pid])
        elif self.exeJobState == 'MAPPING_END':
            self.num_list_remaining -= 1
            if self.num_list_remaining == 0:
                logging.info("Manager:%s end map stage", self.port)
                self.readyed_workers.queue.clear()
                self.getReadyedWorkers()
                # self.dead_worker_busy.clear()
                self.jobExecution(self.message_dict, "grouping_one")
            else:
                pass  # all tasks assigned to workers, but not get back all.
        elif self.exeJobState == 'GROUPING_ONE':
            self.num_list_remaining -= 1
            if self.num_list_remaining == 0:
                self.readyed_workers.queue.clear()
                self.getReadyedWorkers()
                # self.dead_worker_busy.clear()
                self.jobExecution(self.message_dict, "grouping_two")
                logging.info("Manager:%s end group stage", self.port)
                # self.dead_worker_busy.clear()
                self.jobExecution(self.message_dict, "reducing")
            else:
                pass  # need to wait for all workers to return sorting messages
        elif self.exeJobState == 'REDUCING':
            self.num_list_remaining -= 1
            if not self.filelist_remaining.empty():
                self.readyed_workers.put(self.workers[pid])
        elif self.exeJobState == 'REDUCING_END':
            self.num_list_remaining -= 1
            if self.num_list_remaining == 0:
                logging.info("Manager:%s end reduce stage", self.port)
                self.readyed_workers.queue.clear()
                # self.dead_worker_busy.clear()
                self.jobExecution(self.message_dict, "wrapping")
            else:
                pass  # all tasks assigned to workers, but not get back all.
        else:
            logging.info("ERROR! Unknown executing job state!")

    def forwardShutdown(self):
        """Forward the shutdown message to all the workers in the Dict."""
        for pid, worker in self.workers.items():
            if worker.state != WorkerState.DEAD:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((worker.worker_host, worker.worker_port))
                    shutdown_message = json.dumps({"message_type": "shutdown"})
                    sock.sendall(shutdown_message.encode('utf-8'))
        self.shutdown = True

    def forwardAckRegistration(self, worker_host, worker_port, worker_pid):
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

    def getReadyedWorkers(self):
        """Get non-busy or dead workers at the beginning of all 3 stages."""
        for pid, worker in self.workers.items():
            if not (worker.state == WorkerState.BUSY or
                    worker.state == WorkerState.DEAD):
                self.readyed_workers.put(worker)

    def createDirectories(self, msg_dict):
        """Create the directory for all input & output when new job comes."""
        first_layer = self.tmp / 'job-{}'.format(msg_dict["job_id"])
        Path.mkdir(first_layer)
        second_layer_mapper = first_layer / 'mapper-output'
        second_layer_grouper = first_layer / 'grouper-output'
        second_layer_reducer = first_layer / 'reducer-output'
        output_layer_final = Path(msg_dict["output_directory"])
        Path.mkdir(second_layer_mapper)
        Path.mkdir(second_layer_grouper)
        Path.mkdir(second_layer_reducer)
        Path.mkdir(output_layer_final)

    def checkWorkerServer(self):
        """When new job comes, the manager checks whether there's any avaiable
        workers, and whether it's ready for execution or it's busy."""
        whetherAllBusy = True
        for pid, worker in self.workers.items():
            if not (worker.state == WorkerState.BUSY or
                    worker.state == WorkerState.DEAD):
                whetherAllBusy = False
                break
        if whetherAllBusy or self.serverState != 'READY':
            return False
        else:
            return True

    def checkTaskJobForWorker(self, pid):
        """Check whether there's currently executing task or pending task/job
        when a new worker registers, if so, put it into the readyed queue."""
        if self.serverState == "EXECUTING" and \
                not self.filelist_remaining.empty():
            self.readyed_workers.put(self.workers[pid])

    def jobExecution(self, msg_dict, task_signal):
        """Start to execute the mapping task, note only mapping currently."""
        if task_signal == "mapping":
            partitioned_filelist = self.mappingPartition(msg_dict)
            self.executeMap(msg_dict, partitioned_filelist)
        elif task_signal == "grouping_one":
            partitioned_filelist = self.groupingPartition(msg_dict)
            self.executeGroup(msg_dict, partitioned_filelist)
        elif task_signal == "grouping_two":
            self.executeMerge(msg_dict)
        elif task_signal == "reducing":
            partitioned_filelist = self.reducingPartition(msg_dict)
            self.executeReduce(msg_dict, partitioned_filelist)
        elif task_signal == "wrapping":
            self.executeWrap(msg_dict)
        else:
            pass

    def mappingPartition(self, msg_dict):
        """Perform the partition on the mapping files using round robin.
        Return a list of list of files according to number of mappers."""
        input_filelist = []
        for file in Path(msg_dict["input_directory"]).glob('*'):
            input_filelist.append(str(file))
        input_filelist.sort()
        partitioned_filelist = []
        for i in range(msg_dict["num_mappers"]):
            partitioned_filelist.append([])
        for index, file in enumerate(input_filelist):
            partitioned_index = index % msg_dict["num_mappers"]
            partitioned_filelist[partitioned_index].append(file)
        return partitioned_filelist

    def groupingPartition(self, msg_dict):
        """Perform the partition on the grouping of matching files and
        currently readyed workers using roung robin."""
        input_files = []
        input_directory = \
            self.tmp / 'job-{}'.format(msg_dict["job_id"]) / "mapper-output"
        for file in input_directory.glob('*'):
            input_files.append(str(file))
        input_files.sort()
        partitioned_filelist = []
        min_file_worker = min(len(input_files), self.readyed_workers.qsize())
        for i in range(min_file_worker):
            partitioned_filelist.append([])
        for index in range(len(input_files)):
            partitioned_index = index % min_file_worker
            partitioned_filelist[partitioned_index].append(input_files[index])
        return partitioned_filelist

    def reducingPartition(self, msg_dict):
        """Perform the partition on the reducing files using round robin.
        Return a list of list of files according to number of reducers."""
        input_files = []
        input_directory = \
            self.tmp / 'job-{}'.format(msg_dict["job_id"]) / "grouper-output"
        for file in input_directory.glob('reduce*'):
            input_files.append(str(file))
        input_files.sort()
        partitioned_filelist = []
        for i in range(msg_dict["num_reducers"]):
            partitioned_filelist.append([])
        for index, file in enumerate(input_files):
            partitioned_index = index % msg_dict["num_reducers"]
            partitioned_filelist[partitioned_index].append(file)
        return partitioned_filelist

    def executeMap(self, msg_dict, partitioned_filelist):
        """Execute the mapping stage, assign mapping tasks to readyed workers
        in registered order. Note the # workers < # tasks case."""
        job_id = msg_dict["job_id"]
        output_directory = self.tmp / f"job-{job_id}" / "mapper-output"
        for filelist in partitioned_filelist:
            self.filelist_remaining.put(filelist)
        logging.info("Manager:%s begin map stage", self.port)
        self.exeJobState = 'MAPPING'
        self.num_list_remaining = len(partitioned_filelist)
        num_of_original_workers = self.readyed_workers.qsize()
        while num_of_original_workers > 0:
            if not self.filelist_remaining.empty():
                curr_filelist = self.filelist_remaining.get()
                firstWorker = self.readyed_workers.get()
                self.sendMappingTask(
                    curr_filelist, msg_dict["mapper_executable"],
                    output_directory, firstWorker.pid
                )
                self.workers[firstWorker.pid].state = WorkerState.BUSY
                self.workers[firstWorker.pid].modify_curr_task(curr_filelist)
                num_of_original_workers -= 1
            else:
                break
        if self.filelist_remaining.empty():
            self.exeJobState = 'MAPPING_END'

    def executeGroup(self, msg_dict, partitioned_filelist):
        """Execute the grouping stage, assign all grouping(sorting) tasks to
        readyed workers in registeration order in one time."""
        logging.info("Manager:%s begin group stage", self.port)
        self.exeJobState = 'GROUPING_ONE'
        self.num_list_remaining = len(partitioned_filelist)
        for index, filelist in enumerate(partitioned_filelist):
            curr_worker = self.readyed_workers.get()
            job_id = msg_dict["job_id"]
            sortfile_index = str(index + 1).zfill(2)
            output_file = self.tmp / 'job-{}'.format(job_id) /\
                "grouper-output" / 'sorted{}'.format(sortfile_index)
            self.sendGroupingTask(
                filelist, output_file, curr_worker.pid
            )
            self.workers[curr_worker.pid].state = WorkerState.BUSY
            self.workers[curr_worker.pid].modify_curr_task(
                (filelist, output_file)
            )

    def executeMerge(self, msg_dict):
        """Use heapq to merge sort all lines in all files, then perform robin
        round to merge lines to reduces, with the same key in the same file."""
        self.exeJobState = 'GROUPING_TWO'
        num_reducers = msg_dict["num_reducers"]
        job_id = msg_dict["job_id"]
        inout_directory = self.tmp / 'job-{}'.format(job_id) / "grouper-output"
        input_filelist = \
            [str(file) for file in inout_directory.glob('sorted*')]
        output_filelist = \
            [str(inout_directory) + '/reduce' + str(i + 1).zfill(2)
             for i in range(num_reducers)]
        key_counter = 0
        last_key = ''
        with ExitStack() as stack:
            inlines = [stack.enter_context(open(fname, 'r'))
                       for fname in input_filelist]
            outlines = [stack.enter_context(open(fname, 'w'))
                        for fname in output_filelist]
            for index, line in enumerate(heapq.merge(*inlines)):
                key = line.split('\t')[0]
                if index == 0:
                    last_key = key
                if last_key != key:
                    key_counter += 1
                reducefile_index = key_counter % num_reducers
                outlines[reducefile_index].write(line)
                last_key = key

    def executeReduce(self, msg_dict, partitioned_filelist):
        """Execute the reducing stage, assign reducing tasks to readyed workers
        in registered order. Note the # workers < # tasks case."""
        job_id = msg_dict["job_id"]
        output_directory = self.tmp / f"job-{job_id}" / "reducer-output"
        for filelist in partitioned_filelist:
            self.filelist_remaining.put(filelist)
        logging.info("Manager:%s begin reduce stage", self.port)
        self.exeJobState = 'REDUCING'
        self.num_list_remaining = len(partitioned_filelist)
        num_of_original_workers = self.readyed_workers.qsize()
        while num_of_original_workers > 0:
            if not self.filelist_remaining.empty():
                curr_filelist = self.filelist_remaining.get()
                curr_worker = self.readyed_workers.get()
                self.sendMappingTask(
                    curr_filelist, msg_dict["reducer_executable"],
                    output_directory, curr_worker.pid
                )
                self.workers[curr_worker.pid].state = WorkerState.BUSY
                self.workers[curr_worker.pid].modify_curr_task(curr_filelist)
                num_of_original_workers -= 1
            else:
                break
        if self.filelist_remaining.empty():
            self.exeJobState = 'REDUCING_END'

    def executeWrap(self, msg_dict):
        """Executing the wrapping stage of manager, loop over reducer-output
        and rename the files."""
        self.exeJobState = 'WRAPPING'
        job_id = msg_dict["job_id"]
        input_directory = self.tmp / 'job-{}'.format(job_id) / "reducer-output"
        output_directory = Path(msg_dict["output_directory"])
        input_filelist = [str(file) for file in input_directory.glob('*')]
        output_filelist = \
            [output_directory / ("outputfile" + str(i + 1).zfill(2))
             for i in range(len(input_filelist))]
        for i in range(len(output_filelist)):
            if not os.path.exists(output_filelist[i].parent):
                os.mkdir(output_filelist[i].parent)
            shutil.move(input_filelist[i], output_filelist[i])
        self.exeJobState = 'FREE'
        self.serverState = 'READY'

    def sendMappingTask(self, filelist, executable, output_directory, pid):
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
            print(mapping_message)

    def sendGroupingTask(self, filelist, output_file, pid):
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
            print(grouping_message)


@click.command()
@click.argument("port", nargs=1, type=int)
@click.argument("hb_port", nargs=1, type=int)
def main(port, hb_port):
    """Begin the Manager Module."""
    Manager(port, hb_port)


if __name__ == '__main__':
    main()
