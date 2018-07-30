import socket
import time
import sys

UDP_IP = sys.argv[1]
UDP_PORT = 5005
MESSAGE = ""

arc_nat = 0 
prc_nat = 0
timeouts = 0 
data = ''
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))

while True:
    try:
        sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))
        sock.settimeout(10)
        data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
        if data == "Address Restricted Cone NAT":
            arc_nat += 1
        elif data == "Port Restricted Cone NAT":
            prc_nat += 1
    except:
        timeouts += 1
        if timeouts > 6:
            break 

if arc_nat > 0:
    print "This is Address Restricted Cone NAT"
elif prc_nat > 0:
    print "This is Port Restricted Cone NAT."

if data == '':
        print "This is a Symmetric Cone NAT or there is a firewall in between."
