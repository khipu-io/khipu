import socket
import time

UDP_IP = "0.0.0.0"
UDP_PORT = 5005

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
print "received message:", data
print addr
time.sleep(4) 
for i in xrange(10):
    print "Trying Address Restricted"
    sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    sock1.sendto('Address Restricted Cone NAT',addr)
    print "Trying Port Restricted"
    sock.sendto('Port Restricted Cone NAT',addr)

