NATDiscovery
============

This script will determine what kind of NAT you are sitting behind.
* Run server.py on any publicly accessible server.
```
python NATDiscovery/server.py 
```
* Run client.py on any machine inside the NAT and pass the IP of the server as the argument.
```
python NATDiscovery/client.py <server_ip>
```
* Client may take up to 2 minutes to determine the NAT type. 

Entire code in this repo is GPL'ed
