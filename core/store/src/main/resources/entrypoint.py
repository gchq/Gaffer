import socket
import json
import sys
import importlib

# Dynamically import the script
scriptNameParam = sys.argv[1]
scriptName = importlib.import_module(scriptNameParam)
print('scriptName is ', scriptName);

HOST = socket.gethostbyname(socket.gethostname())
PORT = 8080
print('Listening for connections from host: ', socket.gethostbyname(socket.gethostname())) #172.17.0.2

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup the port and get it ready for listening for connections
    s.bind((HOST,PORT))
    s.listen()
    print('Yaaas queen it worked')
    print('Waiting for incoming connections...')
    conn, addr = s.accept() # Wait for incoming connections # Causes nothing to be printed to logs
    print('Connected to: ', addr)
    dataReceived = False
    while not dataReceived:
        data = conn.recv(1024)
        if data:
            jdata = data.decode("utf-8", errors="ignore")
            jdata = json.dumps(jdata)
            print(type(data))
            print('Recieved data : ', data)
            dataReceived = True
            #data = scriptName.run(data)
            print('Resulting data : ', data)
            conn.sendall(data)  # Send the data back