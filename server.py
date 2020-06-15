import socket
import sys
import thread
import datetime
import Queue
import threading


class Request:
    def __init__(self, time, service_type, message):
        self.time = time
        self.service_type = service_type
        self.message = message

    def set_time_by_service_type(self, service_type):
        if service_type == 'V':
            if self.service_type == 'M':
                self.time = 2 * self.time
        elif service_type == 'M':
            if self.service_type == 'P':
                self.time = 2 * self.time
            if self.service_type == 'V':
                self.time = 3 * self.time
    
    def started_at(self, date):
        self.started_at = date
    
    def remaining_work(self):
        remain = self.time - (datetime.datetime.now() - self.started_at).total_seconds()
        if remain<0:
            return 0
        return remain
    
    def set_client(self, client_socket, client_addr):
        self.client_socket = client_socket
        self.client_addr = client_addr

class Server:
    def __init__(self, id, addr, service_type):
        self.id = id
        self.addr = addr
        self.service_type = service_type
        self.work_q = Queue.Queue()
        self.lock = threading.Lock()
        self.cur_req = None

    def attach_socket(self, socket):
        self.socket = socket
        self.thread = threading.Thread(target=manage_connection, args=(self,))
        self.thread.setDaemon(True)
        self.thread.start()

    def close_connection(self):
        if self.socket is not None:
            self.socket.close()
        self.lock.release()
    
    def time_to_finish(self, new_request):
        sum = 0
        print >>sys.stderr, 'server %s Acquiring lock in time to finish' %self.id
        queue = []
        with self.lock:
            if self.cur_req is not None:
                sum = sum + self.cur_req.remaining_work()
            queue = self.work_q.queue
        print>>sys.stderr, 'server %s released lock in time to finish' %self.id
        for req in queue:
            sum = sum + req.time
        sum = sum + self.get_request_time_by_service_type(new_request)
        return sum

    def get_request_time_by_service_type(self, request):
        time = request.time
        if self.service_type == 'V':
            if request.service_type == 'M':
                time = 2 * time
        elif self.service_type == 'M':
            if request.service_type == 'P':
                time = 2 * time
            if self.service_type == 'V':
                time = 3 * time
        return time
    
    def add_new_request(self, new_request):
        new_request.set_time_by_service_type(self.service_type)
        print>>sys.stderr, 'server %s Acquiring lock in add new request' %self.id
        with self.lock:
            self.work_q.put(new_request)
        print >>sys.stderr, 'server %s released lock in add new request' %self.id
        print >>sys.stderr, 'Request %s sent to server %s and will take %s' %(new_request.message, self.id, new_request.time)

    def get_first_request(self):
        r = self.work_q.get()
        return r

    def current_request(self, current_request):
        with self.lock:
            self.cur_req = current_request
            self.cur_req.started_at(datetime.datetime.now())

def manage_connection(server):
    try:
        while True:
            req = server.get_first_request()
            print >>sys.stderr, 'sending "%s" to server %s' % (req.message, server.id)
            sent = server.socket.send(req.message)
            if sent == 0:
                print >>sys.stderr, 'connection ended with %s' % server.addr
                return
            print >>sys.stderr, 'sent "%s" to server %s' % (req.message, server.id)
            server.current_request(req)            
            data = server.socket.recv(2).decode('utf-8')
            if data == '':
                print >>sys.stderr, 'connection ended with %s' % server.addr
                return
            print >>sys.stderr, 'received "%s" from server %s after %s' % (data, server.id, req.remaining_work())
            req.client_socket.send(data)
            print >>sys.stderr, 'sent "%s" to client %s and closing the connection' % (req.message, req.client_addr)
            req.client_socket.close()
    except:
        if data == '':
            print >>sys.stderr, 'connection ended with %s' % server.addr
            return
    finally:
        server.close_connection()
