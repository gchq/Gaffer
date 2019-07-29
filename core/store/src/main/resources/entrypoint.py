import json
import socket

from DataInputStream import DataInputStream

HOST = socket.gethostbyname(socket.gethostname())
PORT = 8080
print('Listening for connections from host: ', socket.gethostbyname(
    socket.gethostname()))  # 172.17.0.2

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup the port and get it ready for listening for connections
    s.bind((HOST, PORT))
    s.listen(1)
    print('Yaaas queen it worked')
    print('Waiting for incoming connections...')
    conn, addr = s.accept()  # Wait for incoming connections
    print('Connected to: ', addr)
    dataReceived = False
    while not dataReceived:
        dis = DataInputStream(conn)
        if dis:
            sdata = dis.read_utf()
            jdata = json.loads(sdata)
            print(type(jdata))
            print('Received data : ', jdata)
            dataReceived = True
            #  data = pythonOperation1(data)
            print('Resulting data : ', jdata)
            conn.sendall(sdata)  # Return the data
