#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 16:33:26 2021

@author: paydenbrown
"""

import socket
import sys
from random import seed
from random import random

# global variables to be maintained
List = []   # List of registered clients
ring = 0    # ring size
setup = 0   # 0= DHT not set up, 1 = DHT set up

def DieWithError(error):
    print(error)
    exit(1)
    
def regCheck(name):
    global List
    size = len(List)
    found = -1
    i = 0
    while i < size:
        if List[i][0] == name:
            found = i
        i+=1
            
    return found
    
    
argc = len(sys.argv)

if argc != 2:
    DieWithError("(server) Incorrect number of arguments")
    
PORT = int(sys.argv[1])
HOST = "" 

stringtosend = "Well hello there."
bytestosend = str.encode(stringtosend)

#HOST = socket.htonl(int(socket.gethostname()))

with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:

    s.bind((HOST,PORT))
         
    print('(server) Socket bind complete. Listening to port: ' + str(PORT))

    
    while True:
        data = s.recvfrom(1024)
        
        msg = data[0].decode()
        addr = data[1]
        temp = msg.split(" ")
        
        print('(server) received command ' + temp[0] + ' from client ' + temp[1])
        
        if temp[0] == "register":    
                                 ############## Register command   ################### 
                                    
            found = 0
            for client in List:
                if client[0] == temp[1]:
                    found = 1
            if found == 1:
                msg = str.encode("357")
                s.sendto(msg,addr)
            else: 
                List.append([temp[1], addr[0], temp[2], temp[3], temp[4], "Free"])
                msg = str.encode("104")
                s.sendto(msg,addr)
                print(List)
        elif temp[0] == "setup-dht":   
                                 ############## setup-dht command   ################### 
                                    
            inList = regCheck(temp[1])
            n = int(temp[2])
            if inList < 0:
                msg = str.encode("3571")
                s.sendto(msg,addr)
            elif n < 2:
                msg = str.encode("3572")
                s.sendto(msg,addr)
            elif setup == 1:
                msg = str.encode("3573")
                s.sendto(msg,addr)
            elif n > len(List):
                msg = str.encode("3574")
                s.sendto(msg,addr)
            else:
                strtosend = List[inList][1]+" "+ List[inList][2] +" "+ List[inList][3] +" "+ "0" + " "+ List[inList][0]
                List[inList][5] = "Leader"
                i = 1 
                while i < n:
                    ind = (inList + i)%len(List)
                    strtosend += " " + List[ind][1]+" "+ List[ind][2] +" "+ List[ind][3] +" "+ str(i)+" " + List[ind][0]
                    List[ind][5] = "InDHT"
                    i+=1
                s.sendto(str.encode(strtosend), addr)
                print("(server) DHT setup request sent to " + List[inList][0])
                d = s.recvfrom(1024)
                temp = d[0].decode().split(" ")
                if temp[0] == "complete":
                    ring = n
                    setup = 1
                    print(List)
                    print("(server) DHT setup and ready for queries")
        elif temp[0] == "deregister":
                                 ############## Deregister command   ###################
                                 
            inList = regCheck(temp[1])
            if inList < 0:
                msg = str.encode("357")
                s.sendto(msg,addr)
            else:
                List.pop(inList)
                print(List)
                msg = str.encode("104")
                s.sendto(msg,addr)
        elif temp[0] == "query-dht":
                                ############## query-dht command    ###################
            inList = regCheck(temp[1])
            if inList < 0:
                msg = str.encode("3571")
                s.sendto(msg,addr)
            elif setup == 0:
                msg = str.encode("3572")
                s.sendto(msg,addr)
            else:
                seed(1)
                r = random()
                query = int(r*100)%n
                sent = 0
                for client in List:
                    if client[5] != "Free" and query != 0:
                        query -= 1
                    elif client[5] != "Free" and query == 0 and sent == 0: 
                        strtosend = client[0] + " " + client[1] + " " + client[2]
                        msg = str.encode(strtosend)
                        s.sendto(msg,addr)
                        print("(server) Query access granted to " + temp[1] +" to contact DHT starting with client " + client[0])
                        sent = 1
                        
                        
                
                
                
    


