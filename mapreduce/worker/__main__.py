import os
import threading
import socket
import logging
import json
import time
import click
from pathlib import Path
import subprocess
import mapreduce.utils
from mapreduce.helper import WorkerState, WorkerInDict, Job


logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Implement all operations of the Worker Module."""

    def __init__(self, manager_port, manager_hb_port, worker_port):
        """Constructor for member variables, functions and threads."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        self.manager_port = manager_port
        self.manager_hb_port = manager_hb_port
        self.worker_port = worker_port
        self.shutdown = False
        self.state = ''
        self.registered = False
        self.message_dict = {"message_type": ''}
        self.pid = os.getpid()
        self.udpHBThread = threading.Thread(
            target=self.sendHBMessage
        )
        self.listenIncomingMsg()

        #self.udpHBThread.join()
    
    def sendHBMessage(self):
        """Send heartbeat message back to the manager after it registers."""
        while not self.shutdown:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect(("localhost", self.manager_hb_port))
                message = json.dumps(
                    {"message_type": "heartbeat", "worker_pid": self.pid}
                )
                sock.sendall(message.encode('utf-8'))
            time.sleep(2)
    
    def listenIncomingMsg(self):
        """Listen to incoming messages such as ack, task from the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.worker_port))
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown:
                if not self.registered:
                    self.sendRegistration()
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
                elif self.message_dict["message_type"] == "register_ack":
                    self.udpHBThread.start()
                    self.registered = True
                    self.state = WorkerState.READY
                elif self.message_dict["message_type"] == "new_worker_task":
                    if self.registered and self.state == WorkerState.READY:
                        self.performMapping(self.message_dict)
                    else:
                        logging.debug(
                            "ERROR! Should not assign task to a non-ready worker!"
                        )
                elif self.message_dict["message_type"] == "new_sort_task":
                    if self.registered and self.state == WorkerState.READY:
                        self.performSorting(self.message_dict)
                    else:
                        logging.debug(
                            "ERROR! Should not assign task to a non-ready worker!"
                        )
                else:
                    pass
            if self.udpHBThread.is_alive():
                self.udpHBThread.join()
    
    def sendRegistration(self):
        """Send the registration message to the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("localhost", self.manager_port))
            rgst_message = json.dumps({
                "message_type": "register",
                "worker_host": "localhost",
                "worker_port": self.worker_port,
                "worker_pid": self.pid
            })
            sock.sendall(rgst_message.encode('utf-8'))
    
    def performMapping(self, message_dict):
        """Perform the real mapping, pipe to the output via executable cmd."""
        self.state = WorkerState.BUSY
        input_files = message_dict["input_files"] # a list of strings
        executable = message_dict["executable"]
        output_directory = message_dict["output_directory"]
        output_files = []
        for input_directory in input_files:
            input_filename = Path(input_directory).name
            output_directory = Path(message_dict["output_directory"]) / input_filename
            with open(input_directory, 'r') as infile:
                outfile = open(str(output_directory), 'w')
                subprocess.run(
                    executable, stdin=infile, stdout=outfile, check=True
                )
                outfile.close()
            output_files.append(str(output_directory))
        self.sendStatusMessage(output_files, "output_files")
        self.state = WorkerState.READY
    
    def performSorting(self, message_dict):
        """Perform the real sorting, pipe to the output via executable cmd."""
        self.state = WorkerState.BUSY
        input_file_list = message_dict["input_files"]
        output_file = message_dict["output_file"]
        data = []
        for input_file in input_file_list:
            with open(input_file, 'r') as infile:
                for line in infile:
                    data.append(line)
        data.sort()
        data = "".join(data)
        with open(output_file, 'w') as outfile:
            outfile.write(data)
        self.sendStatusMessage(output_file, "output_file")
        self.state = WorkerState.READY
    
    def sendStatusMessage(self, output_files, output_file_key):
        """Send the status messages to the manager, which means it finishes
        the current task, and ready for the next if there's one."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("localhost", self.manager_port))
            status_message = json.dumps({
                "message_type": "status",
                output_file_key: output_files,
                "status": "finished",
                "worker_pid": self.pid
            })
            sock.sendall(status_message.encode('utf-8'))


@click.command()
@click.argument("manager_port", nargs=1, type=int)
@click.argument("manager_hb_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_port, manager_hb_port, worker_port):
    Worker(manager_port, manager_hb_port, worker_port)


if __name__ == '__main__':
    main()
