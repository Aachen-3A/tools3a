#!/usr/bin/env python
from __future__ import division
#import csv
import os
import optparse
#import sys
#import re
import time
import datetime
#import subprocess
import cesubmit
#import getpass
import multiprocessing
import multiprocessing.pool
import logging
#import signal

def getRunTimeFromHistory(history):
    starttime = [int(item[1]) for item in history if item[0] == "RUNNING"]
    endtime = [int(item[1]) for item in history if "DONE" in item[0]]
    if len(starttime)!=1:
        return ""
    if len(endtime)!=1:
        timedelta = datetime.datetime.now() - datetime.datetime.fromtimestamp(starttime[0])
    else:
        timedelta = datetime.datetime.fromtimestamp(endtime[0]) - datetime.datetime.fromtimestamp(starttime[0])
    #in seconds
    return timedelta

def checkTask(item):
    """perform actions on a task
    resubmit jobs, get status, get output and get status again
    """
    task, resubmitJobs, killJobs=item
    #print "get %s"%task.name
    if len(killJobs) > 0:
        task.kill(killJobs)
    if len(resubmitJobs) > 0:
        task.resubmit(resubmitJobs)
    if task.frontEndStatus=="RETRIEVED":
        return task
    status = task.getStatus()
    task.getOutput()
    status = task.getStatus()
    #if task.frontEndStatus=="RETRIEVED":
        #print "finished %s"%task.name
    return task

def linearStatusgetter(taskList,resubmitList,killList):
    q=[]
    finished=[]
    for itask,task in enumerate(taskList):
        if task.frontEndStatus=="RETRIEVED":
            finished.append(task)
            continue
        q.append([task,resubmitList[itask],killList[itask]])
    pool = multiprocessing.Pool(10)
    result = pool.map_async(checkTask, q)
    pool.close()
    #pool.join()
    while pool._cache:
        time.sleep(1)
    res = result.get()
    taskList=res+finished
    return taskList

def resubmitByStatus(taskList, resubmitList, status):
    """add jobs with a certain status to the resubmit list
    """
    myTaskIds, myTaskList=range(len(taskList)), taskList
    for (t, task) in zip(myTaskIds, myTaskList):
        for (j, job) in zip(range(len(task.jobs)), task.jobs):
            if job.status in status:
                if (job.status == "DONE-OK" and job.infos["ExitCode"]!="0") or job.status != "DONE-OK":
                    resubmitList[t].add(j)

def resubmitBytime(taskList, resubmitList, time):
    """add jobs with a certain status to the resubmit list
    """
    myTaskIds, myTaskList=range(len(taskList)), taskList
    for (t, task) in zip(myTaskIds, myTaskList):
        for (j, job) in zip(range(len(task.jobs)), task.jobs):
            #print getRunTimeFromHistory(job.infos["history"]), time
            if not isinstance(getRunTimeFromHistory(job.infos["history"]),datetime.timedelta) or getRunTimeFromHistory(job.infos["history"])>time:
                if (job.status == "DONE-OK" and job.infos["ExitCode"]!="0") or job.status != "DONE-OK":
                    resubmitList[t].add(j)

def main( options, args):
    # Logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging._levelNames[options.debug.upper()])
    logQueue = multiprocessing.Queue()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #handler.setFormatter(formatter)
    #logger.addHandler(handler)

    # catch sigterm to terminate gracefully
    taskList, resubmitList, killList = [], [], []
    # load tasks from directories
    for directory in args:
        try:
            task = cesubmit.Task.load(directory)
        except:
            continue
        taskList.append(task)
        resubmitList.append(set())
        killList.append(set())
    taskList=linearStatusgetter(taskList,resubmitList,killList)




    if options.resubmit:

        taskList, resubmitList, killList = [], [], []
        # load tasks from directories
        for directory in args:
            try:
                task = cesubmit.Task.load(directory)
            except:
                continue
            taskList.append(task)
            resubmitList.append(set())
            killList.append(set())

        resubmitByStatus(taskList, resubmitList, ["ABORTED","DONE-FAILED","None","CANCELLED", None])
        for itask,task in enumerate(taskList):
            if len(resubmitList[itask])==0:
                continue
            if options.verbose:
                print task.name
            task.resubmit(resubmitList[itask],processes=8)
    if options.resubmit_failed:

        taskList, resubmitList, killList = [], [], []
        # load tasks from directories
        for directory in args:
            try:
                task = cesubmit.Task.load(directory)
            except:
                continue
            taskList.append(task)
            resubmitList.append(set())
            killList.append(set())

        resubmitByStatus(taskList, resubmitList, ["DONE-OK"])
        for itask,task in enumerate(taskList):
            if len(resubmitList[itask])==0:
                continue
            if options.verbose:
                print task.name
            task.resubmit(resubmitList[itask],processes=8)

    if options.local:

        taskList, resubmitList, killList = [], [], []
        # load tasks from directories
        for directory in args:
            try:
                task = cesubmit.Task.load(directory)
            except:
                continue
            taskList.append(task)
            resubmitList.append(set())
            killList.append(set())

        resubmitByStatus(taskList, resubmitList, ["ABORTED","DONE-FAILED","None", None])
        for itask,task in enumerate(taskList):
            if len(resubmitList[itask])==0:
                continue
            if options.verbose:
                print task.name
            task.resubmitLocal(resubmitList[itask],processes=8)
    if options.resubmitTime:
        taskList, resubmitList, killList = [], [], []
        # load tasks from directories
        for directory in args:
            try:
                task = cesubmit.Task.load(directory)
            except:
                continue
            taskList.append(task)
            resubmitList.append(set())
            killList.append(set())

        resubmitBytime(taskList, resubmitList, datetime.timedelta(hours=1))
        for itask,task in enumerate(taskList):
            if len(resubmitList[itask])==0:
                continue
            if options.verbose:
                print task.name
            task.resubmit(resubmitList[itask],processes=8)



    if options.verbose:
        #taskList, resubmitList, killList = [], [], []
        ## load tasks from directories
        #for directory in args:
            #try:
                #task = cesubmit.Task.load(directory)
            #except:
                #continue
            #taskList.append(task)
            #resubmitList.append(set())
            #killList.append(set())
        taskStati={}
        jobStati={}
        for task in taskList:
            #if task.frontEndStatus!="RETRIEVED":
            if task.frontEndStatus in taskStati:
                taskStati[task.frontEndStatus]+=1
            else:
                taskStati[task.frontEndStatus]=1
            for job in task.jobs:
                if job.status in jobStati:
                    jobStati[job.status]+=1
                else:
                    jobStati[job.status]=1

        print "Task summery:"
        for stati in taskStati:
            print "%s: %d"%(stati,taskStati[stati])
        print ""
        print "Job summery:"
        for stati in jobStati:
            print "%s: %d"%(stati,jobStati[stati])



if __name__ == "__main__":
    parser = optparse.OptionParser( description='Monitor for ce tasks', usage='usage: %prog directories')
    parser.add_option("--debug", action="store", dest="debug", help="Debug level (DEBUG, INFO, WARNING, ERROR, CRITICAL)", default="WARNING")
    parser.add_option("-v","--verbose",  action="store_true", dest="verbose", help="verbose output", default=False)
    parser.add_option("-r","--resubmit",  action="store_true", dest="resubmit", help="resubmit Failed/NONE jobs", default=False)
    parser.add_option("-f","--resubmit-failed",  action="store_true", dest="resubmit_failed", help="resubmit that have a exit code !=1 jobs", default=False)
    parser.add_option("-l","--local",  action="store_true", dest="local", help="resubmit localy Failed/NONE jobs", default=False)
    parser.add_option("-t","--resubmitTime",  action="store_true", dest="resubmitTime", help="resubmit jobs that run longer than 1h jobs", default=False)
    (options, args) = parser.parse_args()
    main( options, args)

