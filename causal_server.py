import socket
from threading import Thread
import threading
import csv
import time
import yaml
import zmq
import sys

# SERVER_1

'''

Set command needs a lock on the file. 
Reader writer lock, as soon as set command initiated by any client, Writer lock
'''


class Store:
    def __init__(self, id, pubSock, subSock, STORE_PATH):
        self.id = id
        self.pubSock = pubSock
        self.subSock = subSock
        self.fileLock = ReadWriteLock()
        self.STORE = STORE_PATH
        # dictionary for key version
        self.key_version = {}
        # start a thread to listen on subscribe socket

        Thread(target=self.recv_multicast).start()

    '''
    We have to make the read wait, the version from client has 
    to be <= version of key in store to process read.
    '''

    def get_command(self, key, version, sock):

        # pending write request is still in process,
        # when this key gets updated the control moves forward to process the read.
        while int(version) > self.key_version[key]:
            print("read version - %s is waiting on server %s that has version %s" %
                  (version, self.id, self.key_version[key]))
            time.sleep(2)

        # read from store
        self.fileLock.acquire_read()
        with open(self.STORE, 'r') as csv_store:
            reader = csv.reader(csv_store)
            store_data = {}
            for line in reader:
                store_data[line[0]] = line[1]
        self.fileLock.release_read()

        if key not in store_data:
            # attach version 0 if key is not in store
            return_message = 'Key not in-store--0'
        else:
            value = store_data[key]
            return_message = "KEY-%s VALUE-%s--%s" % (
                key, value, self.key_version[key])

        sock.sendall(bytes(return_message, 'utf-8'))

    '''
    set command when initiated by - 
        async server message - just store the new key
        client request - store the key and reply back to client
        version for the key is updated in the server on set command and sent back to the client, it is also attached to the broadcast message.
    '''

    def set_command(self, key, value, version, sock=None):
        # Acquire write lock
        self.fileLock.acquire_write()

        # Get dictionary values from file
        store_data = {}
        with open(self.STORE, 'r') as csv_store:
            # read values
            reader = csv.reader(csv_store)
            for line in reader:
                if line is []:
                    continue
                store_data[line[0]] = line[1]

        # make change in dictionary
        store_data[key] = value
        return_message = ''
        with open(self.STORE, 'w', newline='') as csv_store:
            try:
                writer = csv.writer(csv_store)
                for k, v in store_data.items():
                    # write key, flag, exp_time, value
                    writer.writerow([k, v])
                    return_message = 'STORED'
            except Exception as e:
                return_message = 'NOT STORED'

        self.fileLock.release_write()

        # write delay
        time.sleep(5)

        print("update version in server %s" % (self.id))
        # Update version of key
        if key in self.key_version:
            self.key_version[key] += 1
        else:
            self.key_version[key] = 1

        if sock is not None:
            # send version for the key to the client
            sock.sendall(bytes(return_message+"--" +
                         str(self.key_version[key]), 'utf-8'))
            self.broadcast(key, value, version)

    # thread to handle recieved broadcast
    def recv_thread(self, data):
        '''
        set command format - 'set--{id}--{key}--{value}--{version}'
        '''
        id, key, value, version = data.split('--')[1:]
        if key not in self.key_version:
            self.key_version[key] = 0
        id = int(id)
        print("Async set command recieved on ", self.id, " from server ", id)
        self.set_command(key, value, version)

    # RECV broadcast request
    def recv_multicast(self):
        # Receive messages sent to the multicast group(only set commands)
        while True:
            data = self.subSock.recv_string()
            Thread(target=self.recv_thread, args=[data]).start()

    # broadcast async
    def broadcast(self, key, value, version):
        '''
        message format- set--{id}--{key}--{value}--{version}
        '''
        message = "set--%s--%s--%s--%s" % (self.id, key, value, version)

        self.pubSock.send_string(message)
        print("server ", self.id, " is broadcasting")

    # Function to handle client requests, input- socket, address
    def handle_clients(self, sock, addr):
        print("server %s is handling client %s:%s" %
              (self.id, addr[0], addr[1]))
        while True:
            request = sock.recv(1024).decode()
            if request == 'exit':
                sock.close()
                break
            request = request.split('--')
            if request[1] not in self.key_version:
                self.key_version[request[1]] = 0
            if request[0] == 'set':
                Thread(target=self.set_command, args=[
                       request[1], request[2], request[3], sock]).start()
            else:
                Thread(target=self.get_command, args=[
                       request[1], request[2], sock]).start()

# class for readerwriterlock


class ReadWriteLock:
    """ A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release()


arg = sys.argv
hostname = socket.gethostname()
server_IP = socket.gethostbyname(hostname)
server_id, STORE_PATH = int(arg[1]),  arg[2]
serverKey = 'server%s' % (server_id)

# config file read
with open('config.yaml', 'r') as yml:
    cfg = yaml.load(yml, Loader=yaml.FullLoader)

cfg['SERVER']["IP"][serverKey] = server_IP

# write server_IP to config
with open('config.yaml', 'w') as yml:
    yaml.dump(cfg, yml)

server_PORT = cfg["SERVER"]["PORT"][serverKey]

context = zmq.Context()

pubSock = context.socket(zmq.PUB)
subSock = context.socket(zmq.SUB)

# BIND pub socket
pubSock.bind("tcp://%s:%s" % (server_IP, server_PORT))

# connect to other server sockets
for i in range(1, 4):
    if i == server_id:
        continue
    tmp_key = 'server%s' % (i)

    tmp_IP = cfg["SERVER"]["IP"][tmp_key]
    tmp_PORT = cfg["SERVER"]["PORT"][tmp_key]
    subSock.connect("tcp://%s:%s" % (tmp_IP, tmp_PORT))

# Subscribe to messages on subscriber socket
subSock.subscribe("")

# initialize the store instance
storeObject = Store(server_id, pubSock, subSock, STORE_PATH)

# TCP socket to interact with client
clientPORT = cfg["CLIENT"]["PORT"]["server%s" % (server_id)]
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('', clientPORT))

print("server ", server_id, " is listening")


def exit():
    while True:
        a = input()
        if a is not None:
            server_socket.close()
            pubSock.close()
            subSock.close()
            context.term()
            break


Thread(target=exit).start()

'''
client request - 
get--{key}--{version} -> version is 0 for first request made by the client
set--{key}--{value}--{version}

reply to client also has min acceptable version for the key.
'''
while True:
    server_socket.listen(5)
    client_socket, client_addr = server_socket.accept()
    Thread(target=storeObject.handle_clients, args=[
           client_socket, client_addr]).start()
