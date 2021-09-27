#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 17:55:50 2021

@author: paydenbrown
"""

import socket  # to create sockets for communications between processes
import sys     # to read command line input
import select 

# Global variables to be maintained
id = -1         # -1 as long as the client is free, id in DHT
In = ("",-1)    # which IP and port it will receive from in DHT
Out = ("",-1)   # which IP and port it will send to in DHT
ring = 0        # ring size
hashT = []      # hash table remains empty list as long as iots free


# exit function displaying error message
def DieWithError(error):
    print(error)
    exit(1)
  
# called when a register command is given, registers client with server
def register(name, pl, pr, pq, sock, serv):
    strtosend = "register " + name + " " + str(pl) + " " + str(pr) + " " + str(pq)
    msg = str.encode(strtosend)
    print("(" + NAME + ") register request sent")
    sock.sendto(msg, serv)
    b = sock.recvfrom(1024)
    res = b[0].decode()
    if res == "104":
        print("(" + NAME + ") sucessfully registered")
    elif res == "357":
        print("(" + NAME + ") fail: already registered")
        
# sends message to server upon completion of DHT setup  
def dht_complete(name, sock, serv):
    strtosend = "complete " + name 
    msg = str.encode(strtosend)
    sock.sendto(msg, serv)
    print("(" + NAME + ") dht-complete massage sent")

# opens data file and begins to send instances around the DHT loop to be stored
def populateHT(s):
    global id
    global In
    global Out
    global ring
    global hashT
    
    f = open("StatsCountry.csv", encoding='cp1252')
    first = True
    for line in f:
        if first == True:
            first = False
        else:
            temp = line.split(",")
            sum = 0
            for char in temp[3]:
                sum += int(ord(char))
            ind = sum%353
            sendid = ind%ring
            
            if sendid != id:
                strtosend = "store " + str(sendid) + " "+ str(ind) +" "+ temp[0] + " "+ temp[1]+" "+temp[2] +" "+ temp[3]+" "+temp[4] +" "+ temp[5]+" "+temp[6] +" "+temp[7] +" "+temp[8] 
                s.sendto(str.encode(strtosend),Out)
                print("(" + NAME + ") instance passed to next DHT member")
            elif sendid == id:
                hashT[ind].append([temp[0], temp[1], temp[2], temp[3], temp[4], temp[5], temp[6], temp[7], temp[8]])
                print("(" + NAME + ") instance stored")
            
        
    
    
# communicates with server and chosen clients to set up DHT loop  
def setup_dht(name, n, sin, sout, sq, serv):
    global id
    global In
    global Out
    global ring
    global hashT

    done = 0
    strtosend = "setup-dht " + name + " " + str(n)
    msg = str.encode(strtosend)
    print("(" + NAME + ") setup-dht request sent")
    sout.sendto(msg, serv)
    b = sout.recvfrom(1024)
    res = b[0].decode()
    if res == "3571":
        print("(" + NAME + ") fail: not registered with server")
    elif res == "3572":
        print("(" + NAME + ") fail: invalid ring size")
    elif res == "3573":
        print("(" + NAME + ") fail: dht already setup")
    elif res == "3574":
        print("(" + NAME + ") fail: not enough registered users")
    else:
        print("(" + NAME + ") setting up DHT")
        temp = res.split(" ")
        id = 0
        ring = n
        hashT = [[]]*353
        
        dht_list = []
        i = 0 
        size = len(temp)
        while i < size:
            cIP = temp[i]
            i+=1
            cpl = temp[i]
            i+=1
            cpr = temp[i]
            i+=1
            cID = temp[i]
            i+=1
            cName = temp[i]
            
            dht_list.append([cIP,cpl,cpr,cID,cName])
            i+=1
            
        In = (dht_list[n-1][0], int(dht_list[n-1][2]))
        Out = (dht_list[1][0], int(dht_list[1][1]))
        

        i = 1
        while i < n:
            cID = dht_list[i][3]
            cName = dht_list[i][4]
            fromIP = dht_list[i-1][0]
            fromPort = dht_list[i-1][2] 
            toIP = dht_list[(i+1)%n][0]
            toPort = dht_list[(i+1)%n][1] 
            strtosend = "set-id " + cID + " " + fromIP + " " + fromPort + " " + toIP + " " + toPort + " " + str(n)
            msg = str.encode(strtosend)
            sout.sendto(msg,(dht_list[i][0],int(dht_list[i][1])))
            print("(" + NAME + ") set-id request sent to " + cName)
            b = sout.recvfrom(1024)
            r = b[0].decode()
            r = "OK"
            if r == "OK":
                done += 1
            i+=1
            
        if done == n-1:
            populateHT(sout)
        
        dht_complete(name, sout, serv)
        InDHT(sin,sout,sq)
            
            
def find(name, ret, sout):
    global id
    global In
    global Out
    global ring
    global hashT
    
    name = name.replace(",", " ")
    found = 0
    sum = 0
    for char in name:
        sum += int(ord(char))
    ind = sum%353
    sendid = ind%ring
    
    if sendid != id:
        name = name.replace(" ", ",")
        strtosend = "query " + name + " "+ ret[0] + " "+ str(ret[1]) 
        sout.sendto(str.encode(strtosend),Out)
        print("(" + NAME + ") query passed to next DHT member")
    elif sendid == id:
        print("(" + NAME + ") searching for " + name)
        for record in hashT[ind]:
            if record[3] == name:
                strtosend = record[0] + " " + record[1] + " " + record[2] + " " + record[3] + " " + record[4] + " " + record[5] + " " + record[6] + " " + record[7] + " " + record[8] 
                sout.sendto(str.encode(strtosend), ret)
                found = 1
                
        if found == 0:
            strtosend = "357" 
            sout.sendto(str.encode(strtosend), ret)
        
    
    
    
    
    
        
# client in the DHT will stay in this function listening to its left and query port      
def InDHT(sin,sout,sq):
    global Out
    global In
    global ring
    global id
    global hashT
    
    
    inputSources = [sin.fileno(), sq.fileno()]
    while True:
        inputready, outputready, exceptready = select.select(inputSources, [], [])
        for x in inputready:
            if x == sin.fileno():
                b = sin.recvfrom(1024)
                temp = b[0].decode().split(" ")
                if temp[0] == "store":
                    if int(temp[1]) != id:
                        sout.sendto(b[0],Out)
                        print("(" + NAME + ") instance passed to next DHT member")
                    elif int(temp[1]) == id:
                        ind = int(temp[2])
                        hashT[ind].append([temp[3], temp[4], temp[5], temp[6], temp[7], temp[8], temp[9], temp[10], temp[11]])
                        print("(" + NAME + ") instance stored") 
                elif temp[0] == "query" and len(temp) > 2:
                    find(temp[1], (temp[2], int(temp[3])), sout)
                elif temp[0] == "query" and len(temp) == 2:  
                    find(temp[1], b[1], sout)
            elif x == sq.fileno():
                b = sin.recvfrom(1024)
                temp = b[0].decode().split(" ")
                if temp[0] == "query":
                    find(temp[1], b[1], sout)
                    
                    
                    
            
# to ask the server to make a query, and then send query to DHT 
def query(s,name,serv):
    strtosend = "query-dht " + name
    msg = str.encode(strtosend)
    print("(" + NAME + ") query-dht request sent")
    s.sendto(msg, serv)
    b = s.recvfrom(1024)
    res = b[0].decode()
    if res == "3571":
        print("(" + NAME + ") fail: not registered")
    elif res == "3572":
        print("(" + NAME + ") fail: DHT not set up")
    else: 
        print("(" + NAME + ") sucessfully connected for query")
        temp = res.split(" ")
        query = input("Enter country name: ")
        print("(" + NAME + ") sending query " + query+ " to DHT starting with client " + temp[0])
        query = query.replace(" ", ",")
        s.sendto(str.encode("query " + query), (temp[1], int(temp[2])))
        b = s.recvfrom(1024)
        res = b[0].decode()
        if res == "357":
            print("(" + NAME + ") " + query + " not found in DHT")
        else: 
            print(res)
        
        

    
    
# send a request to server to deregister client    
def deregister(s,name,serv):
    strtosend = "deregister " + name
    msg = str.encode(strtosend)
    print("(" + NAME + ") deregister request sent")
    s.sendto(msg, serv)
    b = s.recvfrom(1024)
    res = b[0].decode()
    if res == "104":
        print("(" + NAME + ") sucessfully deregistered")
    elif res == "357":
        print("(" + NAME + ") fail: not registered")
         
        
    
    
argc = len(sys.argv)

if argc != 7:
    DieWithError("(client) Incorrect number of arguments")
    
SPORT = int(sys.argv[1])   
SHOST = sys.argv[2]

NAME = sys.argv[3]
PORTL = int(sys.argv[4])
PORTR = int(sys.argv[5])
PORTQ = int(sys.argv[6])

HOST = ""

servTuple = (SHOST, SPORT)

# set up left socket (incoming communication)
sl = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sl.bind((HOST, PORTL))

# set up right socket (outgoing communication)
sr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sr.bind((HOST, PORTR))

# set up query socket (socket for queries)
sq = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sq.bind((HOST, PORTQ))

inputSources = [sl.fileno()] # used to make non-blocking recvfrom()
        

comm = input("(" + NAME + ") command: ")
while comm != "quit": 
    temp = comm.split(' ')
    if temp[0] == "register":
        register(NAME, PORTL, PORTR, PORTQ, sr, servTuple)
    elif temp[0] == "setup-dht":
        setup_dht(NAME, int(temp[1]), sl, sr, sq, servTuple)
        #print("In DHT ready for queries")
        #InDHT(sl,sr,sq)
    elif temp[0] == "query-dht":
        query(sr,NAME,servTuple)
    elif temp[0] == "deregister":
        deregister(sr,NAME,servTuple)
    elif temp[0] =="":
        inputready, outputready, exceptready = select.select(inputSources, [], []) # nonblocking
        if inputready[0] == sl.fileno():
            data = sl.recvfrom(1024)
            temp = data[0].decode().split(" ")
            if temp[0] == "set-id":
                id = int(temp[1])
                ring = int(temp[6])
                In = (temp[2], int(temp[3]))
                Out = (temp[4], int(temp[5]))
                
                
                hashT = [[]]*353
    
                sr.sendto(str.encode("OK"),data[1])
                InDHT(sl,sr,sq)
            
    comm = input("(" + NAME + ") command: ")
    



            
            