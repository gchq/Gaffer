import socket
import pandas
import struct
import sys
import importlib

from DataInputStream import DataInputStream

# Dynamically import the script
scriptNameParam = sys.argv[1]
scriptName = importlib.import_module(scriptNameParam)
print('scriptName is ', scriptName);

HOST = socket.gethostbyname(socket.gethostname())
PORT = 8080
print('Listening for connections from host: ', socket.gethostbyname(
    socket.gethostname()))  # 172.17.0.2

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup the port and get it ready for listening for connections
    s.bind((HOST, PORT))
    s.listen(1)
    print('Waiting for incoming connections...')
    conn, addr = s.accept()  # Wait for incoming connections
    conn.sendall(struct.pack('?', True))
    print('Connected to: ', addr)
    dataReceived = False
    while not dataReceived:
        dis = DataInputStream(conn)
        if dis:
            dataReceived = True
            tableData = pandas.read_json(dis.read_utf(), orient="records")
            print('Tabled Data : ', tableData)
            data = pandas.DataFrame.to_json(tableData, orient="records")
            data = scriptName.run(data)
            print('Result Data : ', data)
            conn.send(struct.pack('>H', len(data)))
            conn.sendall(data.encode('utf-8'))  # Return the data
