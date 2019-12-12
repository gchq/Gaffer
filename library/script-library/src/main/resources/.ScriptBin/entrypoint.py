import importlib
import socket
import struct
import re
import pandas
import sys
from ast import literal_eval

from DataInputStream import DataInputStream

# Get the script name and dynamically import the script
scriptNameParam = sys.argv[1]
scriptName = importlib.import_module(scriptNameParam)
print('scriptName is ', scriptName)

# Get the script parameters
scriptParameters = sys.argv[2]
try:
    dictParameters = literal_eval(scriptParameters)
except SyntaxError:
    dictParameters = dict()
print('scriptParams is ', scriptParameters)

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

            # Decode the data
            data = rawData.decode('utf-8')

            # Run the script passing in the parameters
            error = None
            try:
                data = scriptName.run(data, dictParameters)
            except Error as error:
                print(error)
                print('Failure while running the script.')
                data = None

            # Send the results back to the server
            if (data != None):
                print('type of output data is: ', type(data))
                i = 0
                conn.sendall(struct.pack('>i', len(data)))
                if len(data) > 65000:
                    splitData = re.findall(('.' * 65000), data)
                    while i < (len(data) / 65000) - 1:
                        conn.sendall(splitData[i].encode('utf-8'))
                        i += 1
                conn.sendall(data[65000 * i:].encode('utf-8'))
            else:
                conn.sendall(struct.pack('>i', 130000))
                conn.sendall(struct.pack('>H', 65000))
                conn.sendall('Error'.encode('utf-8'))
                conn.sendall(struct.pack('>H', 65000))
                conn.sendall(error.encode('utf-8'))