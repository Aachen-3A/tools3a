#!/usr/bin/env python

import sys
import subprocess
import time



def getdcachelist(dir , Tag , mem_limit = 500000000, filetag= '.pxlio'):

    cmd_readdcache = ["uberftp","grid-ftp.physik.rwth-aachen.de",r"ls -r /pnfs/physik.rwth-aachen.de/cms/store/user/%s" % (dir)]
    try:
        p = subprocess.Popen(cmd_readdcache,stdout=subprocess.PIPE)
        (stringdcache,stringdcache_err) = p.communicate()
        dcachelistraw = stringdcache.split("\n")
        dcachelistraw = filter(lambda line:filetag in line in line, dcachelistraw)
    except:
        time.sleep(10)
        p = subprocess.Popen(cmd_readdcache,stdout=subprocess.PIPE)
        (stringdcache,stringdcache_err) = p.communicate()
        dcachelistraw = stringdcache.split("\n")
        dcachelistraw = filter(lambda line:filetag in line in line, dcachelistraw)

    filelistlist = []

    filelistlist.append([])
    memory = 0

    l = 1
    if len(dcachelistraw)==1:
        filelistlist[-1].append(("dcap://grid-dcap.physik.rwth-aachen.de/%s" %(dcachelistraw[0].split()[7])).replace("//pnfs","/pnfs"))
        return filelistlist
    for tmpstring in dcachelistraw :
        memory += int(tmpstring.split()[3])
        if memory>mem_limit:
            filelistlist.append([])
            memory = 0
            l+=1
        if ".pxlio" in tmpstring or ".root" in tmpstring:
            filelistlist[-1].append(("dcap://grid-dcap.physik.rwth-aachen.de/%s" %(tmpstring.split()[7])).replace("//pnfs","/pnfs"))
    if len(filelistlist[-1]) == 0:
      filelistlist.pop()
    return filelistlist




