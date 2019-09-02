import importlib
import socket
import struct
import re
import pandas
import sys
from ast import literal_eval

from DataInputStream import DataInputStream

# Dynamically import the script
scriptNameParam = sys.argv[1]
scriptName = importlib.import_module(scriptNameParam)
print('scriptName is ', scriptName)
scriptParameters = sys.argv[2]
try:
    dictParameters = literal_eval(scriptParameters)
except SyntaxError:
    dictParameters = dict()
print('scriptParams is ', scriptParameters)
scriptInputType = sys.argv[3]

HOST = socket.gethostbyname(socket.gethostname())
PORT = 80
print('Listening for connections from host: ', socket.gethostbyname(
    socket.gethostname()))  # 172.17.0.2

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
            rawData = None
            currentPayload = dis.read_utf()
            while currentPayload != bytes(']', encoding='utf-8'):
                if rawData is None:
                    rawData = bytes() + currentPayload
                else:
                    currentPayload = dis.read_utf()
                    rawData += currentPayload

            # Convert data into the right form based on scriptInputType
            if scriptInputType == 'DATAFRAME':
                data = pandas.read_json(rawData, orient="records")
                print('Tabled Data : \n', data)
            elif scriptInputType == 'JSON':
                data = rawData.decode('utf-8')

            # Run the script passing in the parameters
            data = scriptName.run(data, dictParameters)

            # Convert the data back into JSON
            print('type of output data before is: ', type(data))
            print('type of dataframe is: ', type(pandas.DataFrame([0])))
            if isinstance(data, type(pandas.DataFrame([0]))):
                data = pandas.DataFrame.to_json(data, orient="records")

            # Send the results back to the server
            print('type of output data is: ', type(data))
            i = 0
            conn.sendall(struct.pack('>i', len(data)))
            if len(data) > 65000:
                splitData = re.findall(('.' * 65000), data)
                while i < (len(data) / 65000) - 1:
                    conn.sendall(struct.pack('>H', 65000))
                    conn.sendall(splitData[i].encode('utf-8'))
                    i += 1
            conn.sendall(struct.pack('>H', len(data) % 65000))
            conn.sendall(data[65000 * i:].encode('utf-8'))
