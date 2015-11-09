#!/usr/bin/env python

nid2ip = {}
ip2nid = {}
num_node = 0

def read_machinelist(logdir):
  global nid2ip
  global ip2nid
  global num_node
  for line in open(logdir + '/machinelist'):
    nid, ip = line.split()
    nid = int(nid)
    nid2ip[nid] = ip
    ip2nid[ip] = nid
  num_node = len(nid2ip)


