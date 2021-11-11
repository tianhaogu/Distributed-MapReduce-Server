import os
import threading
import socket
import logging
import json
import time
import click
import shutil
from pathlib import Path
from queue import Queue
import mapreduce.utils
from mapreduce.helper import WorkerState, WorkerInDict, Job


logging.basicConfig(level=logging.DEBUG)


class Manager:
    def __init__(self, port, hb_port):
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
        self.listenIncomingMsg()

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

    def faultTolerance(self):
        return

    def listenIncomingMsg(self):
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
                            break
                        message_chunks.append(data)
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    msg_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    print("Error occurs here!!!")
                    continue
                print(msg_dict)
                if msg_dict["message_type"] == "shutdown":
                    self.handleShutdown()
                elif msg_dict["message_type"] == "register":
                    self.handleRegister(msg_dict)
                    self.checkTaskJobForWorker(msg_dict["worker_pid"])
                elif msg_dict["message_type"] == "new_manager_job":
                    self.message_dict = msg_dict
                    self.handleNewManagerJob(msg_dict)
                elif msg_dict["message_type"] == "status":
                    print("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
                    self.handleStatus(msg_dict)
                else:
                    pass

    def checkTaskJobAtBeginning(self):
        """Check in-executing tasks and in-queue job at the beginning,
        assign corresponding task/job to the worker if necessary."""
        if not self.filelist_remaining.empty():
            print("A")
            if not self.readyed_workers.empty():
                print("AA")
                curr_filelist = self.filelist_remaining.get()
                first_worker = self.readyed_workers.get()
                sendMappingTask(
                    curr_filelist, self.message_dict["mapper_executable"],
                    self.message_dict["output_directory"], first_worker.pid
                )
                self.workers[first_worker.pid].state = WorkerState.BUSY
                if self.filelist_remaining.empty():
                    print("AAA")
                    self.exeJobState = 'MAPPING_END'
        elif self.filelist_remaining.empty() and self.exeJobState != 'FREE':
            print("B")
            pass
        elif not self.jobQueue.empty():
            print("C")
            if self.serverState == 'READY':
                while not self.readyed_workers.empty():
                    self.readyed_workers.get()
                self.getReadyedWorkers()
                if not self.readyed_workers.empty():
                    self.serverState = 'EXECUTING'
                    curr_job = self.jobQueue.get()
                    self.jobExecution(curr_job.message_dict, "mapping")
        else:
            print("D")
            pass

    def handleShutdown(self):
        """Shutdown the manager and consequently shutdown all workers."""
        self.shutdown = True
        #self.forwardShutdown()

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
        """Handle the new coming job, execute it or put it in the jobQueue,
        depending on the the result of checkWorkerServer function."""
        self.createDirectories()
        whether_ready = self.checkWorkerServer()
        self.jobCounter += 1
        if whether_ready:
            self.getReadyedWorkers()
            self.serverState = "EXECUTING"
            print("THis is the get_readyed_workers: ", self.readyed_workers)
            self.jobExecution(msg_dict, "mapping")
        else:
            self.jobQueue.put(Job(self.jobCounter, msg_dict))
    
    def handleStatus(self, msg_dict):
        """Handle the worker status message, set the task and worker Queue."""
        pid = msg_dict["worker_pid"]
        self.workers[pid].state = WorkerState.READY
        print("Status comes!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        if self.exeJobState == 'MAPPING':
            self.num_list_remaining -= 1
            if not self.filelist_remaining.empty():
                self.readyed_workers.put(self.workers[pid])
        elif self.exeJobState == 'MAPPING_END':
            self.num_list_remaining -= 1
            if self.num_list_remaining == 0:
                logging.info("Manager:%s end map stage", self.port)
                while not self.readyed_workers.empty():
                    useless_worker = self.readyed_workers.get()
                self.getReadyedWorkers()
                # TODO execute the grouping stage next and change state
                # self.jobExecution(self.message_dict, "grouping")
            else:
                pass
        else:
            pass
            # TODO following stages

    def forwardShutdown(self):
        """Forward the shutdown message to all the workers in the Dict."""
        for pid, worker in self.workers.items():
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
                "worker_pid" : worker_pid
            })
            sock.sendall(ack_message.encode('utf-8'))
    
    def getReadyedWorkers(self):
        """Get non-busy or dead workers at the beginning of all 3 stages."""
        for pid, worker in self.workers.items():
            if not (worker.state == WorkerState.BUSY or \
                    worker.state == WorkerState.DEAD):
                self.readyed_workers.put(worker)
                print("This is the worker: ", self.workers)

    def createDirectories(self):
        """Create the directory for all input & output when new job comes."""
        first_layer = self.tmp / 'job-{}'.format(self.jobCounter)
        Path.mkdir(first_layer)
        second_layer_mapper = first_layer / 'mapper-output'
        second_layer_grouper = first_layer / 'grouper-output'
        second_layer_reducer = first_layer / 'reducer-output'
        Path.mkdir(second_layer_mapper)
        Path.mkdir(second_layer_grouper)
        Path.mkdir(second_layer_reducer)

    def checkWorkerServer(self):
        """When new job comes, the manager checks whether there's any 
        avaiable workers, and whether it's ready for execution or it's busy."""
        whetherAllBusy = True
        for pid, worker in self.workers.items():
            if not (worker.state == WorkerState.BUSY or \
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
            partitioned_filelist = self.inputPartition(msg_dict)
            print(partitioned_filelist)
            self.executeMap(msg_dict, partitioned_filelist)
        elif task_signal == "grouping":
            pass
            # TODO need to be implemented later for the grouping stage
        elif task_signal == "reducing":
            pass
            # TODO need to be implemented later for the grouping stage
        else:
            pass

    def inputPartition(self, msg_dict):
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

    def executeMap(self, msg_dict, partitioned_filelist):
        """Execute the mapping stage, assign mapping tasks to readyed workers
        in registered order. Note that # workers < # tasks."""
        for filelist in partitioned_filelist:
            self.filelist_remaining.put(filelist)
        logging.info("Manager:%s begin map stage", self.port)
        self.exeJobState = 'MAPPING'
        self.num_list_remaining = len(partitioned_filelist)
        num_of_original_workers = self.readyed_workers.qsize()
        print("readyed_workers.qsize(): ", num_of_original_workers)
        while num_of_original_workers > 0:
            curr_filelist = self.filelist_remaining.get()
            firstWorker = self.readyed_workers.get()
            print("curr_filelist: ", curr_filelist)
            self.sendMappingTask(
                curr_filelist, msg_dict["mapper_executable"],
                msg_dict["output_directory"], firstWorker.pid
            )
            self.workers[firstWorker.pid].state = WorkerState.BUSY
            num_of_original_workers -= 1
            print("remaining filelist length: ", self.filelist_remaining.qsize())
            print("num_of_original_workers: ", num_of_original_workers)
        if self.filelist_remaining.empty():
            self.exeJobState = 'MAPPING_END'

    def sendMappingTask(self, filelist, executable, output_directory, pid):
        """Send the mapping task message to the corresponding worker."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(
                (self.workers[pid].worker_host, self.workers[pid].worker_port)
            )
            print("workers[pid].worker_host: ", self.workers[pid].worker_host)
            print("workers[pid].worker_port: ", self.workers[pid].worker_port)
            mapping_message = json.dumps({
                "message_type": "new_worker_task",
                "input_files": filelist,
                "executable": executable,
                "output_directory": str(output_directory),
                "worker_pid": pid
            })
            sock.sendall(mapping_message.encode('utf-8'))
            print("Send Successfully")


@click.command()
@click.argument("port", nargs=1, type=int)
@click.argument("hb_port", nargs=1, type=int)
def main(port, hb_port):
    Manager(port, hb_port)


if __name__ == '__main__':
    main()
