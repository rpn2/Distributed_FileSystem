# Simple Distributed File System (SDFS)

SDFS is a simplified version of HDFS (Hadoop Distributed File System).
SDFS is intended to be scalable as the number of servers increases. Data
stored in SDFS is tolerant to two machine failures at a time. After
failure(s), you must ensure that data is re-replicated quickly so that
another (set of) failures that happens soon after is tolerated. SDFS
files are immutable - once inserted, they are never changed, although
they may be deleted. SDFS is a flat file system, i.e., it has no concept
of directories, although filenames are allowed to contain slashes.

## Requirements

python2.7

## Logs

Generated log files include:
* clientdebug.log: Generated from client.py
* intro.log: Generated from intro.py
* serverdebug.log: Generated from server.py
* node.log: Generated from node.py
* faildetector.log: Generated from failure_detector.py
* master.log: Generated from master.py
* node_fail.log: Generated from node_replicate.py

## File List
### Distributed Grep
* client.py: Client code
* server-names.txt: File input with the hostnames of servers
* server.py: Server code

### SimpleDFS
* console.py: Code for each node
* failure_detector.py: Failure Detection code for each node
* intro.py: Code for introducer
* memlist: Membership List
* node.py: Code for node to handle commandline commands
* util.py: Has basic broadcast and unicast functionality
* failure_detector.py: Initialize failure detector class
* master.py: Master Node
* sdfs_utils.py

## SimpleDFS Commands

* put <localfilename> <sdfsfilename> - Upload file from local dir
* get <sdfsfilename> ./<localfilename> - Fetches files to local dir
* delete <sdfsfilename - Delete file from all machines
* ls <sdfsfilename> - List all machine addresses where file is stored
* store <sdfsfilename> - List all files stored at this machine
* leader - List current leader

## SimpleDFS Code Example

### Start Introducer

python intro.py - Machine: fa16-cs425-g01-01

join

### Start SDFS Nodes

python console.py

join

### Set Current Master

elect_leader








