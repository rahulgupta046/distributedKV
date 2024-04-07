import socket
from threading import Thread
import threading
import csv 
import time 
import yaml
import zmq
import sys
import heapq

#SERVER_1

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
        #start a thread to listen on subscribe socket
        Thread(target= self.recv_multicast).start()

        #start a thread to process messages
        Thread(target=self.process_message_from_queue).start()

    #LOCAL INSTANT READ
    def get_command(self, key, sock):
        #read from store
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
            return_message = "KEY-%s VALUE-%s" %(key,value)
        
        sock.sendall(bytes(return_message, 'utf-8'))

    '''
    set command - store key,  value and return string
    '''
    def set_command(self, key, value):

        #Acquire write lock
        self.fileLock.acquire_write()

        #Get dictionary values from file
        store_data = {}
        with open(self.STORE, 'r') as csv_store:
            #read values
            reader = csv.reader(csv_store)
            for line in reader:
                if line is []:
                    continue
                store_data[line[0]] = line[1]
        
        #make change in dictionary
        store_data[key] =  value
        return_message = ''
        with open(self.STORE, 'w', newline='') as csv_store:
            try:
                writer = csv.writer(csv_store)
                for k,v in store_data.items():
                    #write key, flag, exp_time, value
                    writer.writerow([k,v])
                    return_message = 'STORED'
            except Exception as e:
                return_message = 'NOT STORED'
            
        self.fileLock.release_write()

        return return_message
            


    '''
    We need a function to broadcast ack for top element in message q
    messageq storing format ([key, value, socket],timestamp)
    socket - None for broadcast message | client socket for client request. 
    Another sub socket to listen to acknowledgements, acksubSock
    '''
    
    def process_message_from_queue(self):
        while True:
            if len(self.messageQueue) > 0:
                #2 conditions to process - front of queue and all other acknowledged.
                front_message = self.messageQueue[0]
                ts, [key, value]= front_message
                item = "%s--%s--%s"%(key,value,ts)
                if len(self.reply_store[item]) == 2:
                    #process condition met. 
                    heapq.heappop(self.messageQueue) 
                    print("Server ",self.id, " Processing message ", item)
                    
                    self.reply_store.pop(item)
                    message = self.set_command(key, value)

                    if item in self.client_info:
                        #Send reply back to client
                        sk = self.client_info[item]
                        sk.sendall(bytes(message, 'utf-8'))
                        self.client_info.pop(item)
            time.sleep(3)
                
    def recv_ack(self, data):
        id, key, val, ts = data.split('--')[1:]
        print("ack recieved on ",self.id," from server ", id)
        item = "%s--%s--%s"%(key,val,ts)
        if item not in self.reply_store:
            self.reply_store[item]= [id]
        elif id not in self.reply_store[item]:
            self.reply_store[item].append(id)

    #thread to handle recieved broadcast
    def recv_thread(self, data):
        '''
        set command format - 'set--{id}--{ts}--{key}--{value}'
        ack format - 'ack--id--key--value--ts' => ([key,value], ts) is the unique identifier
        only writes broadcasted => recieve event
        '''
        #IF ACK, call recv_ack
        if data.startswith('ack'):
            self.recv_ack(data)
        #ELSE IT IS BROADCAST MESSAGE
        else:    
            id, ts, key, value = data.split('--')[1:]

            ts = int(ts)
            self.clock.recieve_event(ts)
            print("sync set command recieved on ",self.id," from server ", id)

            
            #instead of directly calling set_command just add request to message queue
            heapq.heappush(self.messageQueue, (ts, [key,value]))

            item = "%s--%s--%s"%(key,value,ts)
            if item not in self.reply_store:
                self.reply_store[item]= [id]
            elif id not in self.reply_store[item]:
                self.reply_store[item].append(id)
            
            #braodcast acknowledgement
            ack_message = "ack--%s--%s--%s--%s"%(self.id, key, value, ts)
            self.pubSock.send_string(ack_message)
            # self.set_command(key, value)
 
    #RECV broadcast request
    def recv_multicast(self):
        # Receive messages sent to the multicast group(only set commands)
        while True:
            data = self.subSock.recv_string()
            Thread(target = self.recv_thread, args = [data]).start()

    #broadcast async     
    def broadcast(self, key, value):
        '''
        message format- set--{id}--{ts}--{key}--{value}
        '''
        message = "set--%s--%s--%s--%s"%(self.id, self.clock.timestamp ,key, value)
        
        #Introducing Broadcast delay of 5 seconds
        time.sleep(3)

        self.pubSock.send_string(message)
        
        print("server ", self.id, " is broadcasting")
        time.sleep(1)
    
    #Function to handle client requests, input- socket, address
    def handle_clients(self, sock, addr):
        print("server %s is handling client %s:%s"%(self.id, addr[0], addr[1]))
        while True:
            '''
            message from client, format > set--{key}--{value} ; get--{key}
            to add in queue > ([key, value, sock], ts)
            '''
            request = sock.recv(1024).decode()
            if request == 'exit':
                sock.close()
                break
            request = request.split('--')
            if request[0] == 'set':
                #Write request - recieve event
                key,value,sk = request[1], request[2], sock
                self.clock.recieve_event()
                heapq.heappush(self.messageQueue,(self.clock.timestamp,[key, value]))
                item = "%s--%s--%s"%(key, value, self.clock.timestamp)
                self.reply_store[item] = []
                self.client_info[item] = sk
                self.broadcast(key, value)
                #Thread(target = self.set_command, args = [request[1], request[2], sock]).start()
            else:
                Thread(target = self.get_command, args = [request[1], sock]).start()


class LamportClock:
    def __init__(self) -> None:
        self.timestamp = 0
    
    def send_event(self):
        self.timestamp += 1

    def recieve_event(self, ts = 0):
        self.timestamp = 1 + max(self.timestamp, ts)


#class for readerwriterlock
class ReadWriteLock:
    """ A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock(  ))
        self._readers = 0

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire(  )
        try:
            self._readers += 1
        finally:
            self._read_ready.release(  )

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire(  )
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll(  )
        finally:
            self._read_ready.release(  )

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire(  )
        while self._readers > 0:
            self._read_ready.wait(  )

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release(  )

arg = sys.argv
hostname = socket.gethostname()
server_IP = socket.gethostbyname(hostname)
server_id, STORE_PATH = int(arg[1]),  arg[2]
serverKey = 'server%s'%(server_id)
                
#config file read
with open('config.yaml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

cfg['SERVER']["IP"][serverKey] = server_IP

#write server_IP to config
with open('config.yaml', 'w') as yml:
    yaml.dump(cfg, yml)

server_PORT = cfg["SERVER"]["PORT"][serverKey]

context = zmq.Context()

pubSock = context.socket(zmq.PUB)
subSock = context.socket(zmq.SUB)
ackSubSock = context.socket(zmq.SUB)

#BIND pub socket
pubSock.bind("tcp://%s:%s"%(server_IP,server_PORT))

#connect to other server sockets
for i in range(1, 4):
    if i == server_id:
        continue
    tmp_key = 'server%s'%(i)

    tmp_IP = cfg["SERVER"]["IP"][tmp_key]
    tmp_PORT = cfg["SERVER"]["PORT"][tmp_key]
    subSock.connect("tcp://%s:%s"%(tmp_IP, tmp_PORT))
    
#Subscribe to messages on subscriber socket
subSock.subscribe("")

#initialize the store instance
storeObject = Store(server_id, pubSock, subSock, STORE_PATH)

#TCP socket to interact with client
clientPORT = cfg["CLIENT"]["PORT"]["server%s"%(server_id)]
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('', clientPORT))

print("server ",server_id," is listening")

def exit():
    while True:
        a = input()
        if a is not None:
            server_socket.close()
            pubSock.close()
            subSock.close()
            context.term()
            break

Thread(target = exit).start()


while True:
    server_socket.listen(5)
    client_socket, client_addr = server_socket.accept()
    Thread(target=storeObject.handle_clients, args = [client_socket, client_addr]).start()