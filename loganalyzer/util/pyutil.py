#!/usr/bin/env python

nid2ip = {}
ip2nid = {}
num_node = 0
num_nodes = 0

def read_machinelist(logdir):
  global nid2ip
  global ip2nid
  global num_node
  global num_nodes
  for line in open(logdir + '/machinelist'):
    nid, ip = line.split()
    nid = int(nid)
    nid2ip[nid] = ip
    ip2nid[ip] = nid
  num_node = len(nid2ip)
  num_nodes = len(nid2ip)

observer_nid2ip = {}
observed_node_nid2ip = {}
observer_ip2nid = {}
observed_node_ip2nid = {}

def read_simlist(logdir):
  global observer_nid2ip
  global observed_node_nid2ip
  global observer_ip2nid
  global observed_node_ip2nid
  for line in open(logdir + '/observer'):
    nid, ip = line.split()
    nid = int(nid)
    observer_nid2ip[nid] = ip
    observer_ip2nid[ip] = nid
  for line in open(logdir + '/observed_node'):
    nid, ip = line.split()
    nid = int(nid)
    observed_node_nid2ip[nid] = ip
    observed_node_ip2nid[ip] = nid
