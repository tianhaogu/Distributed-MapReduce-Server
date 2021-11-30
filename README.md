# Multi-Process-Multi-Thread-MapReduce-Server

Build a multi-process & multi-threaded MapReduce server that executes user-submitted MapReduce jobs, and simulates communication over Manager and Workers by TCP/UDP Socket in Python.

Construct the Manager module that (1) listens for MapReduce jobs & new-coming workers & status messages through the main TCP listening thread, handles these messages and determines the whole process; (2) distributes mapping/grouping/sorting/reducing tasks among Workers by using round robin partition, etc; (3) tracks aliveness of workers via the UDP heartbeat thread; (4) redistributes tasks of dead workers to alive workers via the fault tolerance thread.

Construct the Worker module that registers with the manager, awaits commands, performs real mapping, sorting and reducing tasks given by Manager, and sends status messages to the Manager.

Use Mock in Python to test the overall framework, or each module individually.
