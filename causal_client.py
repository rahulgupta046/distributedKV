import yaml
import socket
import random

# config file read
with open('config.yaml', 'r') as yml:
    cfg = yaml.load(yml, Loader=yaml.FullLoader)

server_ips = [(k, v) for k, v in cfg['SERVER']['IP'].items()]

print("set {key} {value} ; get {key} ; exit -> for operations")

key_version = {}
while True:
    # for the test cases, each request of client weill be a new connection.
    index = random.randint(0, 2)
    IP = server_ips[index][1]
    port_key = server_ips[index][0]
    PORT = int(cfg['CLIENT']['PORT'][port_key])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((IP, PORT))

    req = input()
    if req == 'exit':
        sock.sendall(bytes("exit", 'utf-8'))
        sock.close()
        break
    req = req.replace(' ', '--')
    # Before sending we have to put the key in clients version vector

    if req.startswith("set"):
        _, key, val = req.split('--')
        # increment min acceptable version(with write request)
        if key not in key_version:
            key_version[key] = 1
        else:
            key_version[key] += 1
    else:
        _, key = req.split('--')
        if key not in key_version:
            key_version[key] = 0

    # add min acceptable version of key at the end
    req = req+'--'+str(key_version[key])
    print("sending request to server with acceptable key version %s" %
          (key_version[key]))
    sock.sendall(bytes(req, 'utf-8'))
    ip_address = sock.getsockname()[0]
    port = sock.getsockname()[1]

    print("Client information %s : %s" % (ip_address, port))

    data = sock.recv(1024).decode()
    print(data)

    # update key_version
    new_version = int(data.split('--')[-1])
    key_version[key] = max(new_version, key_version[key])

    print(key+' version is %s' % (key_version[key]))

    # terminate the client request after executing command.
    sock.sendall(bytes("exit", 'utf-8'))
    sock.close()
