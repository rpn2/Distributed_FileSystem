#!/usr/bin/env python

from node import *
import os
from os import walk
import subprocess
from sdfs_utils import *
from master import*
import getpass
import Queue
from node_replicate import *
import socket
import threading
import time

class console_client(threading.Thread):
    
    def __init__(self, mlist, host, port,fail_queue,elect_queue,init_queue, introducer=False):
        super(console_client, self).__init__()
        self.mlist = mlist
        self.host = host
        self.port = port
        self.intro = introducer
        self.user = getpass.getuser()
        self.block_size = 1024 # value in MB
        self.rep_factor = 3         
        self.master_id = None
        self.master = None
        self.sdfs_utils = sdfs_utils()
        #Communication from fail detector
        self.fail_queue = fail_queue       
        #Communication from leader elector
        self.elect_queue = elect_queue
        #Communication from introducer on initial leader
        self.init_queue = init_queue
        #Port listening for leader election
        self.lport = 10040
        #Port for Master node communication
        self.master_port = 10020
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        except (socket.error, socket.gaierror) as err_msg:
            logging.exception(err_msg)
            self.sock.close()

    # Contact the Introducer and Join the Group
    # Get the current leader from the introducer
    def join_group(self):
        
        #Clean the SDFS storage area when a node joins
        CMD="rm /usr/local/sdfs_mp3/*"       
        # Remove file from this location
        ret = subprocess.call(["ssh", str(self.user) + "@" + str(socket.gethostname()), CMD], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
        # Send Join Message
        msg = {
            'cmd':'join',
            'host': self.host,
            'port': self.port,
            'time': time.time()
        }
        self.mlist.time = msg['time']
        snd_msg = pickle.dumps(msg)
        
        # Send Message
        self.sock.sendto(snd_msg, (self.mlist.ihost,self.mlist.iport))
        logging.info("Node Command: " + msg['cmd'])
        
        
        # Get the current leader from the introducer
        
        #Wait until membership is obtained from introducer
        while (len(self.mlist.lst) <=0):
            pass
            
        #Check the init_leader queue, assign master for the new node
        if not self.init_queue.empty():
            ip_add = self.init_queue.get()
            if ip_add is not None:
                self.master_id = (ip_add,self.master_port)
        
        # print "Current Master is " + str(self.master_id)
        
        #Start leader wait thread after process joins
        leader_wait = threading.Thread(target=self.wait_for_leader)
        leader_wait.daemon = True
        leader_wait.start()
        
        ## Start node_replicate thread as soon as node joins
        replica_thread = node_replicate()
        replica_thread.daemon = True
        replica_thread.start()
         
    # Contact the Introducer and Leave the Group
    def leave_group(self):
        # Send Leave Message
        msg = {
            'cmd': 'leave',
            'host': self.host,
            'port': self.port
        }
        self.mlist.leave()
        logging.info("Node Command: " + msg['cmd'])
    
 
    #Start leader election: 
    def initial_leader_election(self,election_id):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_id = ('', int(self.lport))
            sock.connect(node_id)
            send_pkt = pickle.dumps({
                      'cmd':'initiate',
                      'id' : election_id
                      }) 
            send_msg = (send_pkt)
            sock.send(send_msg)
            ret = ''
            try:
                data = sock.recv(8192)
                ret = ret + data
                # print "Leader election has been " + ret
            except socket.timeout:
                logging.info("Socket Timeout in Client")
                # print "Socket TimeOut"
            sock.close()
        except (socket.error,socket.gaierror) as err_msg:
            logging.info(str(err_msg))  
            print "socket connection error"  
            # print err_msg
        

                
    def wait_for_leader(self):
        while True:
            if self.master_id is None:        
                #Wait for election to complete
                master_ip = self.elect_queue.get()
                #Set master id for SDFS operations            
                self.master_id = (master_ip,int(self.master_port))
                # print master_ip
                current_ip =socket.gethostbyname(socket.gethostname())
                #If current node is Master, start the Master thread
                if (current_ip == master_ip):
                    # print "Current node is Master"
                    self.master = master_node(self.mlist,self.fail_queue)
                    self.master.run()
                
                #Send the SDFS information to Master to populate metadata
                file_list = self.store()
                #Call Master after a delay, similar to exponential back-off
                
                
                ip_split = current_ip.split('.');
                node_id= ip_split[len(ip_split)-1];
                time.sleep((int(node_id)/10.0));
                
                self.sdfs_utils.node_info_handler(file_list,self.master_id)
                  
            else:
                #If current node is not master, look for master failure
                if (socket.gethostbyname(socket.gethostname()) != self.master_id[0]):
                    
                    #Wait for fail information 
                    fail_ip = self.fail_queue.get() 
                    queue_info = (fail_ip).split('/')
                    #If detected fail is a Master:
                    if (queue_info[0] == self.master_id[0]):
                        #Sleep for 20s to eliminate false fail
                        time.sleep(20)
                        lst = self.mlist.lst
                        #Check if node is removed from mlist
                        for i in range(len(lst)):
                            host = lst[i]['host']
                            if (host == queue_info[0]):
                                false_fail = 1
                                break
                            else:
                                false_fail = 0
                        if (false_fail == 0):
                            #Set master to none
                            self.master_id = None
                            #Initiate new election
                            #print "master fail detected " + str(queue_info[0]) + ",  New Election Initiated"
                            self.initial_leader_election(queue_info[0])
    
    # Contact the Master and Place the File
    def put(self, lfle, sfle):
        if self.master_id is not None:
            logging.info("SDFS Command: Put")
            # Use Helper Function to handle put
            self.sdfs_utils.put_handler(lfle,sfle,self.block_size,self.rep_factor,self.master_id,self.user)
            logging.info("Put: " + str(lfle))
        else:
            print "Master not elected, hold SDFS operations"
    # Contact the Master and Get the File
    def get(self, sfle, lfle):
        if self.master_id is not None: 
            logging.info("SDFS Command: Get")
            # Contact Master, Determine Best Node
            item = max(self.mlist.lst)     
            self.sdfs_utils.get_handler(lfle,sfle,self.master_id,self.user) 
            logging.info("Get: " + str(sfle))
        else:
           print "Master not elected, hold SDFS operations"

    # Contact the Master, Get metadata of file and delete file from SDFS
    def delete(self, sfle):
        if self.master_id is not None: 
            logging.info("SDFS Command: Delete")
            # Use Helper Function to Handle Delete
            self.sdfs_utils.delete_handler(sfle,self.master_id,self.user)
        else:
           print "Master not elected, hold SDFS operations"

    # Get list of files from this machine
    def store(self):
        logging.info("SDFS Command: Store")
        f = []
        # Go through set directory and get all the files
        for (dirpath, dirnames, filenames) in walk('/usr/local/sdfs_mp3/'):
            f.extend(filenames)
        # Print files on machines
        return f

    # Handle list command
    def lstaddrs(self, sdfsfn):
        if self.master_id is not None: 
            logging.info("SDFS Command: List VM Addresses")
            # Get metadata of sdfs from Master
            file_mdata = self.sdfs_utils.get_metadata(sdfsfn,self.master_id)
            # Display metadata to user
            # print "File is split into blocks : " + str(file_mdata.keys())
            for key,val in file_mdata.items():
                print key, val
        else:
           print "Master not elected, hold SDFS operations"
        
        
    # Prompt user of questions
    def run(self):
        
        prompt = '()==[:::::::::::::> '
        if self.intro:
            prompt = '[intro] ' + prompt
        while True:
            cmds = raw_input(prompt)
            cmd = cmds.split(" ")
            if cmd[0] == 'put':
                self.put(cmd[1], cmd[2])
            elif cmd[0] == 'get':
                self.get(cmd[1], cmd[2])
            elif cmd[0] == 'store':                
                file_list = self.store()
                print file_list
            elif cmd[0] == 'delete':
                self.delete(cmd[1])
            elif cmd[0] == 'ls':
                self.lstaddrs(cmd[1])
            elif cmd[0] == 'join':
                self.join_group()
            elif cmd[0] == 'lm':
                print self.mlist
            elif cmd[0] == 'li':
                print socket.gethostbyname(socket.gethostname())
            elif cmd[0] == 'user':
                self.user = cmd[1]
            elif cmd[0] == 'leader':
                print str(self.master_id)
            elif cmd[0] == 'block_size':
                self.block_size = int(cmd[1])
            elif cmd[0] == 'rep_factor':
                self.rep_factor = int(cmd[1])
            elif cmd[0] == 'set_master':
                self.set_as_master(cmd[1])
            elif cmd[0] == 'elect_leader':
                self.initial_leader_election('000.000.000.000')
            elif cmd[0] == 'exit':
                import os
                os._exit(0)
            else:
                print 'invalid command!'


if __name__ == '__main__':
    # Start logging
    logging.basicConfig(filename="node.log", level=logging.INFO, filemode="w")
    # Set host and port
    host = socket.gethostbyname(socket.gethostname())
    port = 10013
    # Communicate between FailDetector and other threads is via Queue
    fail_queue = Queue.Queue()
    # Communicate elected Master between threads
    elect_queue = Queue.Queue()
    # Communicate between console and drone on initial master
    init_queue = Queue.Queue()
    # Initialize membership list
    mlist = member_list()
    # Initilaize Console
    cc = console_client(mlist, host, port,fail_queue,elect_queue,init_queue)
    cc.start()
    # Initialize Drone
    drn = drone(mlist, host, port,fail_queue,elect_queue,init_queue)
    drn.start()
