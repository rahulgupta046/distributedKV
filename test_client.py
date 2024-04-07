import yaml
import socket
import random
import sys
import time
import random

args = sys.argv
client_id = args[1]
# config file read
with open('config.yaml', 'r') as yml:
    cfg = yaml.load(yml, Loader=yaml.FullLoader)

server_ips = [(k, v) for k, v in cfg['SERVER']['IP'].items()]

index = random.randint(0, 2)
IP = server_ips[index][1]
port_key = server_ips[index][0]
PORT = int(cfg['CLIENT']['PORT'][port_key])

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((IP, PORT))


def send_message(sock, message):
    message = message.replace(' ', '--')
    sock.sendall(bytes(message, 'utf-8'))


ip_address = sock.getsockname()[0]
port = sock.getsockname()[1]

print("Client information %s : %s" % (ip_address, port))


# SET AND GET COMMANDS
send_message(sock, 'set 1 100'+client_id)
print(sock.recv(1024).decode(), "   on client "+client_id)
time.sleep(random.randint(2, 5))

send_message(sock, 'get 1')
print(sock.recv(1024).decode(), "   on client "+client_id)
time.sleep(random.randint(2, 5))

send_message(sock, 'set 2 200'+client_id)
print(sock.recv(1024).decode(), "   on client "+client_id)
time.sleep(random.randint(2, 5))

send_message(sock, 'set 3 300'+client_id)
print(sock.recv(1024).decode(), "   on client "+client_id)
time.sleep(random.randint(2, 5))

send_message(sock, 'get 3')
print(sock.recv(1024).decode(), "   on client "+client_id)
time.sleep(random.randint(2, 5))

send_message(sock, 'exit')
