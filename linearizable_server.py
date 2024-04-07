import socket
from threading import Thread
import threading
import csv
import time
import yaml
import zmq
import sys
import heapq
import random

# SERVER_1

'''
Make a message QUEUE, reply dictionary, lamport clock for set request.
Lamport clock - increment with set_request.   
'''

'''
in messageQueue we need it sorted by timestamp, so we need ([k,v], ts)
'''


class Store:
    def __init__(self, id, pubSock, subSock, STORE_PATH):
        self.id = id
        self.pubSock = pubSock
        self.subSock = subSock
        self.fileLock = ReadWriteLock()
        self.clock = LamportClock()
        self.STORE = STORE_PATH
        self.messageQueue = []
        self.reply_store = {}
        self.client_info = {}
        # start a thread to listen on subscribe socket
        Thread(target=self.recv_multicast).start()

        # start a thread to process messages
        Thread(target=self.process_message_from_queue).start()

    # READ IS ALSO BROADCASTED
    def get_command(self, key):
        # read from store
        self.fileLock.acquire_read()
        with open(self.STORE, 'r') as csv_store:
            reader = csv.reader(csv_store)
            store_data = {}
            for line in reader:
                store_data[line[0]] = line[1]
        self.fileLock.release_read()

        if key not in store_data:
            return_message = 'Key not in-store'
        else:
            value = store_data[key]
            return_message = "KEY-%s VALUE-%s" % (key, value)

        return return_message

    '''
    set command - store key,  value and return string
    '''

    def set_command(self, key, value):

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

        return return_message

    '''
    We need a function to broadcast ack for top element in message q
    messageq storing format ([key, value, socket],timestamp)
    socket - None for broadcast message | client socket for client request. 
    2types of requests in message q - get--key--ts and set--key--value--ts
    message Q tuple is - (ts, [key]) for get and (ts, [key, value])
    '''

    def process_get(self, key, item):
        heapq.heappop(self.messageQueue)
        print("Server ", self.id, " Processing message ", item)
        self.reply_store.pop(item)
        message = self.get_command(key)
        if item in self.client_info:
            # Send reply back to client
            sk = self.client_info[item]
            sk.sendall(bytes(message, 'utf-8'))
            self.client_info.pop(item)

    def process_set(self, key, value, item):
        heapq.heappop(self.messageQueue)
        print("Server ", self.id, " Processing message ", item)

        self.reply_store.pop(item)
        message = self.set_command(key, value)

        if item in self.client_info:
            # Send reply back to client
            sk = self.client_info[item]
            sk.sendall(bytes(message, 'utf-8'))
            self.client_info.pop(item)

    def process_message_from_queue(self):
        while True:
            print("message queue on server", str(
                self.id), " is %s" % (self.messageQueue))
            if len(self.messageQueue) > 0:

                # 2 conditions to process - front of queue and all other acknowledged.
                ts, info = self.messageQueue[0]
                if len(info) == 1:
                    key = info[0]
                    item = "%s--%s" % (key, ts)
                    if len(self.reply_store[item]) == 2:
                        # get request
                        Thread(target=self.process_get,
                               args=[key, item]).start()

                else:
                    key, value = info
                    # set request
                    item = "%s--%s--%s" % (key, value, ts)
                    if len(self.reply_store[item]) == 2:
                        # process condition met.
                        Thread(target=self.process_set, args=[
                               key, value, item]).start()
            time.sleep(random.randint(2, 3))

    def recv_ack(self, data):
        request_type = data.split('--')[1]
        # print("Printing the data on recieving acknowledgment ", data)
        if request_type == 'W':
            _, request_type, id, key, val, ts = data.split('--')
            item = "%s--%s--%s" % (key, val, ts)
        else:
            _, request_type, id, key, ts = data.split('--')
            item = "%s--%s" % (key, ts)
        print(request_type, " ack recieved on ", self.id, " from server ", id)
        self.clock.recieve_event(int(ts))
        if item not in self.reply_store:

            self.reply_store[item] = [id]
        elif id not in self.reply_store[item]:
            self.reply_store[item].append(id)

    # thread to handle recieved broadcast
    def recv_thread(self, data):
        '''
        set command format - 'set--{id}--{ts}--{key}--{value}'
        get command format - 'get--{id}--{ts}--{key}'
        ack format for set- 1>'ack--W--id--key--value--ts' 
        => ([key,value], ts) is the unique identifier
        for get  2>'ack--R--id--key--ts' => 
        ([key], ts) is the unique identifier
        all requests are broadcasted
        '''

        # IF ACK, call recv_ack
        if data.startswith('ack'):
            self.recv_ack(data)
        # ELSE IT IS BROADCAST MESSAGE
        else:
            if data.startswith('set'):
                _, id, ts, key, value = data.split('--')
                ts = int(ts)

                # element for q and item for set
                q_element = (ts, [key, value])
                item = "%s--%s--%s" % (key, value, ts)

                # ack for set
                ack_message = "ack--W--%s--%s--%s--%s" % (
                    self.id, key, value, ts)
                print("sync set command recieved on ",
                      self.id, " from server ", id)
            elif data.startswith('get'):
                # get
                _, id, ts, key = data.split('--')
                ts = int(ts)

                # element for q and item for set
                q_element = (ts, [key])
                item = "%s--%s" % (key, ts)

                # acknowledgement message for get
                ack_message = "ack--R--%s--%s--%s" % (self.id, key, ts)
                print("sync get command recieved on ",
                      self.id, " from server ", id)

            self.clock.recieve_event(ts)
            # instead of directly calling set_command or get command just add request to message queue
            heapq.heappush(self.messageQueue, q_element)

            if item not in self.reply_store:
                self.reply_store[item] = [id]
            elif id not in self.reply_store[item]:
                self.reply_store[item].append(id)

            # braodcast acknowledgement
            print('sending ack message - ', ack_message)
            self.pubSock.send_string(ack_message)

    # RECV broadcast request

    def recv_multicast(self):
        # Receive messages sent to the multicast group(only set commands)
        while True:
            data = self.subSock.recv_string()
            Thread(target=self.recv_thread, args=[data]).start()

    # broadcast async
    def broadcast(self, key, value=None):
        '''
        message format- set--{id}--{ts}--{key}--{value}
                if value is None - get--{id}--{ts}--{key}
        '''
        if value is not None:
            message = "set--%s--%s--%s--%s" % (self.id,
                                               self.clock.timestamp, key, value)

        else:
            message = "get--%s--%s--%s" % (self.id, self.clock.timestamp, key)
        # Introducing Broadcast delay of 3 seconds
        time.sleep(random.randint(1, 3))

        self.pubSock.send_string(message)

        print("server ", self.id, " is broadcasting message - ", message)
        time.sleep(1)

    # Function to handle client requests, input- socket, address
    def handle_clients(self, sock, addr):
        print("server %s is handling client %s:%s" %
              (self.id, addr[0], addr[1]))
        while True:
            '''
            message from client, format > set--{key}--{value} ; 
                                          get--{key}

            '''
            request = sock.recv(1024).decode()
            if request == 'exit':
                sock.close()
                break
            request = request.split('--')

            self.clock.recieve_event()
            if request[0] == 'set':
                # Write request - recieve event
                key, value, sk = request[1], request[2], sock
                q_element = (self.clock.timestamp, [key, value])
                item = "%s--%s--%s" % (key, value, self.clock.timestamp)

            elif request[0] == 'get':
                # Read request - recieve event
                key, sk = request[1], sock
                item = "%s--%s" % (key, self.clock.timestamp)
                q_element = (self.clock.timestamp, [key])
                value = None
            heapq.heappush(self.messageQueue, q_element)

            self.reply_store[item] = []
            self.client_info[item] = sk
            self.broadcast(key, value)


class LamportClock:
    def __init__(self) -> None:
        self.timestamp = 0

    def send_event(self):
        self.timestamp += 1

    def recieve_event(self, ts=0):
        self.timestamp = 1 + max(self.timestamp, ts)


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
ackSubSock = context.socket(zmq.SUB)

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


while True:
    server_socket.listen(5)
    client_socket, client_addr = server_socket.accept()
    Thread(target=storeObject.handle_clients, args=[
           client_socket, client_addr]).start()
