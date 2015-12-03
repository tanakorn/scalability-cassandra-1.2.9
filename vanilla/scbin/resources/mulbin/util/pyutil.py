#!/usr/bin/env python

#nid should be 1 to n
nid2ip = {}
ip2nid = {}
num_mach = 0
num_ins = 0

ind2ip = {}
ind2nid = {}
nid2ind = {}

# Fixed
ins_per_mach = 4

def read_machinelist(logdir):
  global nid2ip
  global ip2nid
  global num_mach
  global num_ins
  global ind2ip
  global ind2nid
  machinelist_file = open(logdir + '/machinelist')
  all_entries = machinelist_file.readlines()
  num_mach = len(all_entries)
  num_ins = ins_per_mach * num_mach
  for i in range(num_mach):
    ind2ip[i + 1] = { }
    ind2nid[i + 1] = { } 
    for j in range(ins_per_mach):
      nid = i * ins_per_mach + j + 1
      ip = '192.168.%d.%d' % (i + 1, j + 1)
      nid2ip[nid] = ip
      ip2nid[ip] = nid
      ind2ip[i + 1][j + 1] = ip
      ind2nid[i + 1][j + 1] = nid
      nid2ind[nid] = (i + 1, j + 1)

