import matplotlib.pyplot as plt
import yaml
import socket
import random
import time

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
req_times = []
print("set {key} {value} ; get {key} ; exit -> for operations")

while True:
    req = input()
    st = time.time()
    if req == 'exit':
        sock.sendall(bytes("exit", 'utf-8'))
        sock.close()
        break
    req = req.replace(' ', '--')
    sock.sendall(bytes(req, 'utf-8'))
    ip_address = sock.getsockname()[0]
    port = sock.getsockname()[1]

    print("Client information %s : %s" % (ip_address, port))

    data = sock.recv(1024)
    print(data.decode())
    en = time.time()
    req_times.append((en - st)*1000)

# # plot req_times

# plt.plot([i for i in range(1, len(req_times) + 1)], req_times)
# plt.xlabel('instances')
# plt.ylabel('time  *10^-3 seconds')

# plt.savefig('performance_eval.png')
