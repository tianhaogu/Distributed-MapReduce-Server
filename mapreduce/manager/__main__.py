import os
import threading
import socket
import logging
import json
import time
import click
import shutil
from pathlib import Path
import mapreduce.utils
from mapreduce.helper import WorkerState, WorkerInDict, Job


logging.basicConfig(level=logging.DEBUG)


class Manager:
    def __init__(self, port, hb_port):
        logging.info("Starting manager:%s", port)
        logging.info("Manager:%s PWD %s", port, os.getcwd())

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        #     "worker_pid": 77811
        # }
        # logging.debug("Manager:%s received\n%s",
        #     port,
        #     json.dumps(message_dict, indent=2),
        # )

        self.port = port
        self.hb_port = hb_port
        self.workers = {}
        self.jobQueue = []
        self.jobCounter = 0
        self.serverState = 'READY'
        self.shutdown = False
        self.message_dict = {"message_type": ''}
        self.tmp = self.createFolder()
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
        tmp = Path('tmp')
        tmp.mkdir(exist_ok=True)
        for oldJobFolder in tmp.glob('job-*'):
            shutil.rmtree(oldJobFolder)
        return tmp
    
    def listenHBMessage(self):
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.port))
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown:
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
                print(msg_dict)
                self.message_dict = msg_dict
                if self.message_dict["message_type"] == "shutdown":
                    self.shutdown = True
                    #self.forwardShutdown()
                elif self.message_dict["message_type"] == "register":
                    worker_host = self.message_dict["worker_host"]
                    worker_port = self.message_dict["worker_port"]
                    worker_pid = self.message_dict["worker_pid"]
                    self.forwardAckRegistration(
                        worker_host, worker_port, worker_pid
                    )
                    self.workers[worker_pid] = WorkerInDict(
                        worker_pid, worker_host, worker_port
                    )
                elif self.message_dict["message_type"] == "new_manager_job":
                    self.createDirectories()
                    readyed_workers = self.checkWorkerServer(self.message_dict)
                    if len(readyed_workers) > 0:
                        self.jobExecution(message_dict, readyed_workers)
                else:
                    pass
    
    def forwardShutdown(self):
        for pid, worker in self.workers.items():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((worker.worker_host, worker.worker_port))
                shutdown_message = json.dumps({"message_type": "shutdown"})
                sock.sendall(shutdown_message.encode('utf-8'))
        self.shutdown = True
    
    def forwardAckRegistration(self, worker_host, worker_port, worker_pid):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker_host, worker_port))
            ack_message = json.dumps({
                "message_type": "register_ack",
                "worker_host": worker_host,
                "worker_port": worker_port,
                "worker_pid" : worker_pid
            })
            sock.sendall(ack_message.encode('utf-8'))
        #self.workers.append(ack_message)
    
    def createDirectories(self):
        first_layer = self.tmp / 'job-{}'.format(self.jobCounter)
        Path.mkdir(first_layer)
        second_layer_mapper = first_layer / 'mapper-output'
        second_layer_grouper = first_layer / 'grouper-output'
        second_layer_reducer = first_layer / 'reducer-output'
        Path.mkdir(second_layer_mapper)
        Path.mkdir(second_layer_grouper)
        Path.mkdir(second_layer_reducer)
    
    def checkWorkerServer(self, message_dict):
        readyed_workers = {}
        whetherAllBusy = True
        for pid, worker in self.workers.items():
            if not (worker.state == WorkerState.BUSY or \
                    worker.state == WorkerState.DEAD):
                whetherAllBusy = False
                readyed_workers[pid] = worker
        if whetherAllBusy or self.serverState != "READY":
            self.jobQueue.append(
                Job(
                    self.jobCounter,
                    message_dict["input_directory"],
                    message_dict["output_directory"],
                    message_dict["mapper_executable"],
                    message_dict["reducer_executable"],
                    message_dict["num_mappers"],
                    message_dict["num_reducers"]
                )
            )
            self.jobCounter += 1
        return readyed_workers
    
    def jobExecution(self, message_dict, readyed_workers):
        self.serverState = "EXECUTING"
        partitioned_filelist = self.inputPartition(message_dict)
        self.executeMap(message_dict, partitioned_filelist, readyed_workers)
    
    def inputPartition(self, message_dict):
        input_filelist = []
        for file in Path(message_dict["input_directory"]).glob('*'):
            original_filelist.append(str(file))
        input_filelist.sort()
        partitioned_filelist = []
        for i in range(message_dict["num_mappers"]):
            partitioned_filelist.append([])
        for index, file in enumerate(input_filelist):
            partitioned_index = index % message_dict["num_mappers"]
            partitioned_filelist[partitioned_index].append(file)
        return partitioned_filelist
    
    def executeMap(self, message_dict, partitioned_filelist, readyed_workers):
        logging.info("Manager:%s begin map stage", self.port)
        # TODO need to be implemented
        logging.info("Manager:%s end map stage", port_num)


@click.command()
@click.argument("port", nargs=1, type=int)
@click.argument("hb_port", nargs=1, type=int)
def main(port, hb_port):
    Manager(port, hb_port)


if __name__ == '__main__':
    main()
