"""Map-reducer worker.

The purpose of this class is to run a worker to recieve message from managers
and doing the work assigned by the manager. After finishing the work, the
workers will send message to notify the manager.
Author: Chenkai, Ori, Tianhao
Email: gtianhao@umich.edu
"""
import os
import threading
import socket
import time
import logging
import json
from pathlib import Path
import subprocess
import click
from mapreduce.utils import WorkerState

logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Implement all operations of the Worker Module."""

    def __init__(self, manager_port, manager_hb_port, worker_port):
        """Construct member variables, functions and threads."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        self.manager_ports = {
            "manager_port": manager_port,
            "manager_hb_port": manager_hb_port
        }
        self.worker_port = worker_port
        self.shutdown = False
        self.state = ''
        self.registered = False
        self.pid = os.getpid()
        self.udp_hb_thread = threading.Thread(
            target=self.send_hb_message
        )
        self.listen_incoming_msg()

        # self.udp_hb_thread.join()

    def send_hb_message(self):
        """Send heartbeat message back to the manager after it registers."""
        while not self.shutdown:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((
                    "localhost",
                    self.manager_ports["manager_hb_port"]
                ))
                message = json.dumps(
                    {"message_type": "heartbeat", "worker_pid": self.pid}
                )
                sock.sendall(message.encode('utf-8'))
            time.sleep(2)

    def listen_incoming_msg(self):
        """Listen to incoming messages such as ack, task from the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.worker_port))
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown:
                if not self.registered:
                    self.send_registration()
                try:
                    _clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                with _clientsocket:
                    _message_chunks = []
                    while True:
                        try:
                            _data = _clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not _data:
                            break
                        _message_chunks.append(_data)
                _message_bytes = b''.join(_message_chunks)
                _message_str = _message_bytes.decode("utf-8")

                try:
                    msg_dict = json.loads(_message_str)
                except json.JSONDecodeError:
                    continue
                logging.debug(msg_dict)
                self.handle_incoming_message(msg_dict)

            if self.udp_hb_thread.is_alive():
                self.udp_hb_thread.join()

    def send_registration(self):
        """Send the registration message to the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("localhost", self.manager_ports["manager_port"]))
            rgst_message = json.dumps({
                "message_type": "register",
                "worker_host": "localhost",
                "worker_port": self.worker_port,
                "worker_pid": self.pid
            })
            sock.sendall(rgst_message.encode('utf-8'))

    def handle_incoming_message(self, msg_dict):
        """Handle any message received in the TCP socket from the manager."""
        if msg_dict["message_type"] == "shutdown":
            self.shutdown = True
        elif msg_dict["message_type"] == "register_ack":
            self.udp_hb_thread.start()
            self.registered = True
            self.state = WorkerState.READY
        elif msg_dict["message_type"] == "new_worker_task":
            if self.registered and self.state == WorkerState.READY:
                self.perform_mapping(msg_dict)
            else:
                logging.debug(
                    "ERROR! Do Not assign task to a non-ready worker!"
                )
        elif msg_dict["message_type"] == "new_sort_task":
            if self.registered and self.state == WorkerState.READY:
                self.perform_sorting(msg_dict)
            else:
                logging.debug(
                    "ERROR! Do not assign task to a non-ready worker!"
                )
        else:
            pass

    def perform_mapping(self, message_dict):
        """Perform the real mapping, pipe to the output via executable cmd."""
        self.state = WorkerState.BUSY
        input_files = message_dict["input_files"]  # a list of strings
        executable = message_dict["executable"]
        output_directory = message_dict["output_directory"]
        output_files = []
        for input_directory in input_files:
            input_filename = Path(input_directory).name
            output_directory = \
                Path(message_dict["output_directory"]) / input_filename
            with open(input_directory, 'r', encoding='utf-8') as infile:
                with open(output_directory, 'w', encoding='utf-8') as outfile:
                    subprocess.run(
                        executable, stdin=infile, stdout=outfile, check=True
                    )
            output_files.append(str(output_directory))
        self.send_status_message(output_files, "output_files")
        self.state = WorkerState.READY

    def perform_sorting(self, message_dict):
        """Perform the real sorting, pipe to the output via executable cmd."""
        self.state = WorkerState.BUSY
        input_file_list = message_dict["input_files"]
        output_file = message_dict["output_file"]
        data = []
        for input_file in input_file_list:
            with open(input_file, 'r', encoding='utf-8') as infile:
                for line in infile:
                    data.append(line)
        data.sort()
        data = "".join(data)
        with open(output_file, 'w', encoding='utf-8') as outfile:
            outfile.write(data)
        self.send_status_message(output_file, "output_file")
        self.state = WorkerState.READY

    def send_status_message(self, output_files, output_file_key):
        """Send the status messages to the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("localhost", self.manager_ports["manager_port"]))
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
    """Begin the Worker Module."""
    Worker(manager_port, manager_hb_port, worker_port)


if __name__ == '__main__':
    main()
