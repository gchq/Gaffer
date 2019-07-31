"""
Reading from Java DataInputStream format.
"""

import struct


def recvall(sock, count):
    buf = b''
    while count:
        newbuf = sock.recv(count)
        if not newbuf:
            return None
        buf += newbuf
        count -= len(newbuf)
    return buf


class DataInputStream:
    def __init__(self, stream):
        self.stream = stream

    def read_boolean(self):
        return struct.unpack('?', self.stream.recv(1))[0]

    def read_byte(self):
        return struct.unpack('b', self.stream.recv(1))[0]

    def read_unsigned_byte(self):
        return struct.unpack('B', self.stream.recv(1))[0]

    def read_char(self):
        return chr(struct.unpack('>H', self.stream.recv(2))[0])

    def read_double(self):
        return struct.unpack('>d', self.stream.recv(8))[0]

    def read_float(self):
        return struct.unpack('>f', self.stream.recv(4))[0]

    def read_short(self):
        return struct.unpack('>h', self.stream.recv(2))[0]

    def read_unsigned_short(self):
        return struct.unpack('>H', self.stream.recv(2))[0]

    def read_long(self):
        return struct.unpack('>q', self.stream.recv(8))[0]

    def read_utf(self):
        utf_length = struct.unpack('>H', self.stream.recv(2))[0]
        return recvall(self.stream, utf_length)

    def read_int(self):
        return struct.unpack('>i', self.stream.recv(4))[0]
