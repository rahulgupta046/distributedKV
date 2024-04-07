import subprocess

file_name = 'test_client.py'
for i in range(4):
    subprocess.Popen('python ' + file_name+' '+str(i+1))
