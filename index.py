import threading,time
import socket
import random
import json
import copy
NODE_PORTS = {
    1:5001,
    2:5002,
    3:5003,
    4:5004
} 

class Node:
    host ='127.0.0.1'
    def __init__(self,node_id:int, nei:set): 
        self.heartbeat = 0
        self.node_id = node_id
        self.nei = {}
        self.lock = threading.Lock()
        #alive is for debug
        self.alive = True
        self.send = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.recv = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.recv.bind(('localhost',NODE_PORTS[self.node_id]))
        for ne in nei:
            self.nei[ne]={"heartbeat":0,"last_updated":time.time(),"status":"ALIVE"}
    def update_heartbeat(self):
        with self.lock:
            if self.alive:
                self.heartbeat+=1
    def watchdog(self):
        T_suspect = 10
        T_fail = 20
        while True:
            with self.lock:
                for ne in list(self.nei.keys()):
                    if time.time() - self.nei[ne]["last_updated"] > T_suspect:
                        self.nei[ne]["status"] = "SUSPECTED"
                    if time.time() - self.nei[ne]["last_updated"] > T_fail:
                        self.nei[ne]["status"] = "DEAD"
            time.sleep(5)

#{'heartbeat': 1, 'node_id': 1, 'nei': {2: {'heartbeat': 0, 'last_updated': 1775458511.2642975}, 4: {'heartbeat': 0, 'last_updated': 1775458511.2642992}}}
#My intial idea was sending the whole node that we would like to merge with but in a real distributed systems we would like to keep some metadata hidden , either for condidentiality or just reduce system load, so we will just send a neighbor list
    def merge_list(self,nei):
        with self.lock:
            for ne in nei:
                if ne in self.nei and nei[ne]["heartbeat"]>self.nei[ne]["heartbeat"]:
                    self.nei[ne]["heartbeat"] = nei[ne]["heartbeat"]
                    self.nei[ne]["last_updated"] = time.time()
                    self.nei[ne]["status"] = "ALIVE"
                elif ne not in self.nei:
                    self.nei[ne]={"heartbeat":nei[ne]["heartbeat"], "last_updated":time.time(), "status":"ALIVE"}

    def send_gossip(self):
        while True:
            if not self.alive:
                return
            with self.lock:
                random_node = random.choice(list(self.nei.keys()))
                send_dict = copy.deepcopy(self.nei)
                send_dict[self.node_id] = {"heartbeat":self.heartbeat,"last_updated":time.time()}
                #how do we send the info to the random node, over socket?
            self.send.sendto(json.dumps(send_dict).encode(), (self.host, NODE_PORTS[random_node]))
            time.sleep(2)
    def receive_gossip(self):
        while True:
            data,client_address = self.recv.recvfrom(1024)
            data = {int(k): v for k, v in json.loads(data.decode()).items()}
            self.merge_list(data)
        


node_1 = Node(1,{2,4})
node_2 = Node(2,{1,3})
node_3 = Node(3,{2,4})
node_4 = Node(4,{3,1,2})

Nodes = [node_1,node_2,node_3,node_4]


def update_heartbeat(node):
    while True:
        node.update_heartbeat()
        time.sleep(1)
        

# t1= threading.Thread(target=update_heartbeat,args=(node_1,),daemon=True)
# t2= threading.Thread(target=update_heartbeat,args=(node_2,),daemon=True)
# t3= threading.Thread(target=update_heartbeat,args=(node_3,),daemon=True)
# t4= threading.Thread(target=update_heartbeat,args=(node_4,),daemon=True)

# t1.start()
# t2.start()
# t3.start()
# t4.start()


for node in Nodes:
    threading.Thread(target=update_heartbeat,args=(node,),daemon=True).start()
    threading.Thread(target=node.send_gossip,daemon=True).start()
    threading.Thread(target=node.receive_gossip,daemon=True).start()
    threading.Thread(target=node.watchdog,daemon=True ).start()


#Need to take an incoming list and merge it
#We check two things if the node is in the gossip list of the receiver 
#And also if the hearbeat value is higher in the senders list, time updated on the recevier side will be the current time



#In a real system nodes discover each other dynamically. For our simulation we'll hardcode it — each node listens on a fixed port on localhost.

#Also we can't send a python dict over a socket. Socket send bytes.
#So using json module dict->json.dumps()->.encode()->bytes Serialization
#And bytes->.decode() -> json.loads() ->dict Deserialization

# print(node_1.__dict__)

#First we need to create a socket

#AF_INET  Specifies the address family: Currently we are using IPV4
# The second parameters specifies the conection type (tcp,udp), we are using UDP here i.e. SOCK_DGRAM (SOCK_STREAM for tcp)


#connection is then done using
time.sleep(5)
node_4.alive=False
try:
    while True:
        for node in Nodes:
            print(node.nei)
        time.sleep(2)  
        print("---")
        pass
except KeyboardInterrupt:
    print("Shutting down")


