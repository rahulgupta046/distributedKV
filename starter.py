#start 3 different server processes
import socket
import subprocess
import yaml
import time
import os 

#config file read
with open('config.yaml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

model = cfg['model']


#start 3 server processes
for i in range(1,4):
    file = os.path.join(os.getcwd(), model + '_server.py')
    store = os.path.join(os.getcwd(),'store%s.csv'%(i) )
    subprocess.Popen('python %s %s %s'%(file, i, store))
    time.sleep(1)
