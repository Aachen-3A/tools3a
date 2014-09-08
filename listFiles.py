#!/usr/bin/env python

import sys
import subprocess
import time



#mem_limit = 500000000#[bytes]
def getdcachelist(dir,Tag,mem_limit=500000000):

    if len(Tag)==0:
        cmd_readdcache = ["uberftp","grid-ftp.physik.rwth-aachen.de",r"ls /pnfs/physik.rwth-aachen.de/cms/store/user/%s" % (dir)]
    else:
        cmd_readdcache = ["uberftp","grid-ftp.physik.rwth-aachen.de",r"ls /pnfs/physik.rwth-aachen.de/cms/store/user/%s" % (dir)]
        try:
            p = subprocess.Popen(cmd_readdcache,stdout=subprocess.PIPE)
            (stringdcache,stringdcache_err) = p.communicate()
            dcachelistraw = stringdcache.split("\n")
            dcachelistraw = dcachelistraw[2:-1]
        except:
            time.sleep(10)
            p = subprocess.Popen(cmd_readdcache,stdout=subprocess.PIPE)
            (stringdcache,stringdcache_err) = p.communicate()
            dcachelistraw = stringdcache.split("\n")
            dcachelistraw = dcachelistraw[2:-1]

    filelistlist = []

    filelistlist.append([])
    memory = 0

    l = 1
    if len(dcachelistraw)==1:
        filelistlist[-1].append("dcap://grid-dcap.physik.rwth-aachen.de/pnfs/physik.rwth-aachen.de/cms/store/user/%s/%s" %(dir,dcachelistraw[0].split()[8]))
        return filelistlist
    for tmpstring in dcachelistraw :
        memory += int(tmpstring.split()[4])
        if memory>mem_limit:
            filelistlist.append([])
            memory = 0
            l+=1
        filelistlist[-1].append("dcap://grid-dcap.physik.rwth-aachen.de/pnfs/physik.rwth-aachen.de/cms/store/user/%s/%s" %(dir,tmpstring.split()[8]))
    if len(filelistlist[-1]) == 0:
      filelistlist.pop()
    return filelistlist




