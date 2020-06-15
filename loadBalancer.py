import socket
import sys
import thread
import Queue
import threading
import random

from server import *

servers = range(0, 10)

def choose_best_server(random_servers, server_sockets, new_request, client_addr):
    server1 = servers_sockets[random_servers[0]]
    server2 = servers_sockets[random_servers[1]]
    time1 = server1.time_to_finish(new_request)
    time2 = server2.time_to_finish(new_request)
    if time1<time2:
        print >>sys.stderr, 'Client %s got servers %s with times %s and chose %s for request' % (client_addr, random_servers, (time1, time2), random_servers[0], new_request.message)
        server1.add_new_request(new_request)
    else:
        print >>sys.stderr, 'Client %s got servers %s with times %s and chose %s for request' % (client_addr, random_servers, (time1, time2), random_servers[0], new_request.message)
        server2.add_new_request(new_request)

def handle_client(client_sock, client_addr, servers_sockets):
    data = client_sock.recv(2).decode('utf-8')
    if data == '':
        print >>sys.stderr, 'Connection ended with %s' % client_addr
        client_sock.close()
        return
    print >>sys.stderr, 'Received "%s" from client %s' % (data, client_addr)
    new_request = Request(int(data[1]), data[0], data)
    new_request.set_client(client_sock, client_addr)
    random_servers = random.sample(servers,  2)
    choose_best_server(random_servers, servers_sockets, new_request, client_addr)
    # server1 = servers_sockets[random_servers[0]]
    # server2 = servers_sockets[random_servers[1]]
    # time1 = server1.time_to_finish(new_request)
    # time2 = server2.time_to_finish(new_request)
    # if time1<time2:
    #     print >>sys.stderr, 'requested "%s" from server %s' % (data, random_servers[0])
    #     server1.add_new_request(new_request)
    # else:
    #     print >>sys.stderr, 'requested "%s" from server %s' % (data, random_servers[1])
    #     server2.add_new_request(new_request)
    

server_addrs = [(('192.168.0.100', 80), 'V'), \
     (('192.168.0.101', 80), 'V'), \
         (('192.168.0.102', 80), 'V'), \
             (('192.168.0.103', 80), 'V'), \
        (('192.168.0.104', 80), 'V'), \
            (('192.168.0.105', 80), 'V'), \
                (('192.168.0.106', 80), 'M'), \
            (('192.168.0.107', 80), 'M'), \
                (('192.168.0.108', 80), 'M'), \
                    (('192.168.0.109', 80), 'M')]
listening_addr = ('10.0.0.1', 80) # our address for listening to clients
servers_sockets = {}
workers = []
id = 0
# connect to all servers
for (addr, service) in server_addrs:
    server = Server(id, addr, service)
    servers_sockets[id] = server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print >> sys.stderr, 'Connecting to server %s port %s' % addr
    sock.connect(addr)
    server.attach_socket(sock)
    id = id + 1

# Create a TCP/IP socket for the load balancer
loadBalancer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
loadBalancer_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
print >>sys.stderr, 'Starting up LB on %s port %s' % listening_addr
loadBalancer_sock.bind(listening_addr)
loadBalancer_sock.listen(5)
try:
    while True:
        # Wait for a connection
        print >>sys.stderr, 'Waiting for a connection...'
        connection, client_address = loadBalancer_sock.accept()
        # handle client request in a different thread
        thread.start_new_thread(handle_client, (connection, client_address, servers_sockets))

# close all the connections with the servers and our socket
finally:
    print >>sys.stderr, 'Closing sockets...'
    loadBalancer_sock.close()
    for server in servers_sockets.values:
        server.close_connection()
