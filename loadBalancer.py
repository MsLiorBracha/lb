import socket
import sys
import thread
import Queue
import threading

from server import *

servers = range(1, 11)

def from_char_to_service(c):
    if c == 'M':
        return Service.MUSIC
    elif c == 'V':
        return Service.VIDEO
    elif c == 'P':
        return Service.PHOTO

def handle_client(client_sock, client_addr, servers_sockets):
    data = client_sock.recv(2).decode('utf-8')
    if data == '':
        print >>sys.stderr, 'connection ended with %s' % client_addr
        client_sock.close()
        return
    print >>sys.stderr, 'received "%s" from client %s' % (data, client_addr)
    service = from_char_to_service(data[1])
    new_request = Request(int(data[0]), service, data)
    new_request.set_client(client_sock, client_addr)
    random_servers = random.sample(servers,  2)
    server1 = servers_sockets[random_servers[0]]
    server2 = servers_sockets[random_servers[1]]
    time1 = server1.time_to_finish(new_request)
    time2 = server2.time_to_finish(new_request)
    if time1<time2:
        print >>sys.stderr, 'requested "%s" from server %s' % (data, random_servers[0])
        server1.add_new_request(new_request)
    else:
        print >>sys.stderr, 'requested "%s" from server %s' % (data, random_servers[1])
        server2.add_new_request(new_request)
    

server_addrs = [(('192.168.0.100', 80), Service.VIDEO), \
     (('192.168.0.101', 80), Service.VIDEO), \
         (('192.168.0.102', 80), Service.VIDEO), \
             (('192.168.0.103', 80), Service.VIDEO), \
        (('192.168.0.104', 80), Service.VIDEO), \
            (('192.168.0.105', 80), Service.VIDEO), \
                (('192.168.0.106', 80), Service.MUSIC), \
            (('192.168.0.107', 80), Service.MUSIC), \
                (('192.168.0.108', 80), Service.MUSIC), \
                    (('192.168.0.109', 80), Service.MUSIC)]
listening_addr = ('10.0.0.1', 80) # our address for listening to clients
servers_sockets = {}
workers = []
id = 0
# connect to all servers
for (addr, service) in server_addrs:
    server = Server(id, addr, service)
    servers_sockets[id] = server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print >> sys.stderr, 'connecting to server %s port %s' % addr
    sock.connect(addr)
    server.attach_socket(sock)
    id = id + 1

# Create a TCP/IP socket for the load balancer
loadBalancer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print >>sys.stderr, 'starting up on %s port %s' % listening_addr
loadBalancer_sock.bind(listening_addr)
try:
    while True:
        # Wait for a connection
        print >>sys.stderr, 'waiting for a connection'
        connection, client_address = loadBalancer_sock.accept()
        # handle client request in a different thread
        thread.start_new_thread(handle_client, (connection, client_address, servers_sockets))

# close all the connections with the servers and our socket
finally:
    loadBalancer_sock.close()
    for server in servers.values:
        server.close_connection()
