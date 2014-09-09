#!/usr/bin/env python
from __future__ import division
import csv
import os
import optparse
import sys
import re
import time
import datetime
import curses
import subprocess
import cesubmit
import getpass
import multiprocessing
import curseshelpers
import pprint

def addtime(tfor,tsince,tto):
   if tsince==None:
      return tfor
   deltat=tto-tsince
   if tfor==None:
      tfor=deltat
   else:
      tfor=tfor+deltat
   return tfor
def timerepr(deltat):
    if deltat.days<0:
        return "now"
    hours, seconds=divmod(deltat.seconds, 60*60)
    minutes, seconds=divmod(seconds, 60)
    if deltat.days: return "in {0}d {1}h {2}m {3}s".format(deltat.days, hours, minutes, seconds)
    if hours: return "in {0}h {1}m {2}s".format(hours, minutes, seconds)
    if minutes: return "in {0}m {1}s".format(minutes, seconds)
    return "in {0}s".format(seconds)

def checkTask(task):
    #try:
        status = task.getStatus()
        task.getOutput()
    #except:
        #pass
        return task



def changePad(currentpad, overview, taskOverviews, direction):
    if currentpad==overview and direction=="down":
        currentpad=taskOverviews[currentpad.cursor]
    elif currentpad in taskOverviews and direction=="up":
        currentpad=overview
        
def resubmit(taskList, status, overview):
    if overview.level==0:
        myTaskList=taskList
    elif overview.level==1:
        myTaskList=[taskList[overview.overview.cursor]]
    else:
        myTaskList=[]
    jobsWithStatus=[]
    for task in myTaskList:
        for job in task.jobs:
            if job.status in status:
                jobsWithStatus.append(job)

class Overview:
    def __init__(self, stdscr, tasks):
        self.level = 0
        self.taskId = 0
        self.cursor = 0
        self.stdscr = stdscr
        self.taskOverviews = []
        self.overview = curseshelpers.SelectTable(stdscr, top=10, maxrows=100+len(tasks))
        self.overview.setColHeaders(["Task", "Status", "Total", "Pre Running", "Run.","RRun.","Abrt.","Fail.","OK","None","Retr."])
        for task in tasks:
            taskOverview = curseshelpers.SelectTable(stdscr, top=10, maxrows=100+len(task.jobs))
            taskOverview.setColHeaders(["Job", "Status", "In Status since", "FE-Status", "Exit Code"])
            self.taskOverviews.append(taskOverview)
        self.update(tasks)
    def update(self, tasks):
        self.overview.clear()
        for (taskOverview, task) in zip(self.taskOverviews, tasks):
            statusnumbers=task.jobStatusNumbers()
            #formatting
            if statusnumbers['DONE-FAILED'] or statusnumbers['ABORTED']:
                printmode=curses.color_pair(1) | curses.A_BOLD
            elif task.frontEndStatus=="RETRIEVED":
                printmode=curses.color_pair(2)
            elif task.frontEndStatus=="RUNNING":
                printmode=curses.color_pair(2) | curses.A_BOLD
            else:
                printmode=curses.A_BOLD
            #prepare and add row
            cells = [task.name, task.frontEndStatus, statusnumbers['total'], statusnumbers['PENDING']+ statusnumbers['IDLE']+statusnumbers['SUBMITTED']+statusnumbers['REGISTERED'], statusnumbers['RUNNING'], statusnumbers['REALLY-RUNNING'], statusnumbers['ABORTED'], statusnumbers['DONE-FAILED'], statusnumbers['DONE-OK'], statusnumbers[None], statusnumbers['RETRIEVED']]
            self.overview.addRow(cells, printmode)
            taskOverview.clear()
            for job in task.jobs:
                try:
                    jobid = job.jobid
                except AttributeError:
                    jobid = ""
                try:
                    jobstatus = job.status
                except AttributeError:
                    jobstatus = ""
                try:
                    jobfestatus = job.frontEndStatus
                except AttributeError:
                    jobfestatus = ""
                try:
                    jobreturncode=job.infos["ExitCode"]
                except:
                    jobreturncode=""
                try:
                    jobsince = datetime.datetime.fromtimestamp(int(job.infos["history"][-1][1])).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    jobsince=""
                cells = [jobid, jobstatus, jobsince, jobfestatus, jobreturncode]
                if jobstatus in ['DONE-FAILED', 'ABORTED']:
                    printmode=curses.color_pair(1) | curses.A_BOLD
                elif jobfestatus=="RETRIEVED":
                    printmode=curses.color_pair(2)
                elif "RUNNING" in jobstatus:
                    printmode=curses.color_pair(2) | curses.A_BOLD
                else:
                    printmode=curses.A_BOLD
                taskOverview.addRow(cells, printmode)
        self._refresh()
    #@property
    #def currentView(self):
        #if self.level==0:
            #return self.overview
        #elif self.level==1:
            #return self.taskOverview[self.currentTask]
        #else:
            #x=curseshelpers.Text(top=10)
            #x.setText("Blub")
            #return x
    @property
    def currentTask(self):
        return self.overview.cursor
    @property
    def currentJob(self):
        return self.taskOverviews[self.currentTask].cursor
    def up(self):
        self.level=max(self.level-1,0)
        self._refresh()
    def down(self):
        self.level=min(self.level+1,2)
        self._refresh()
    def _refresh(self, taskList=None):
        if self.level==0:
            self.currentView = self.overview
        elif self.level==1:
            self.currentView = self.taskOverviews[self.currentTask]
        else:
            pp = pprint.PrettyPrinter(indent=4)
            x=curseshelpers.Text(self.stdscr, top=10,maxrows=100)
            try:
                x.setText(pp.pformat(taskList[self.currentTask].jobs[self.currentJob].infos))
            except:
                x.setText("No infos found")
            self.currentView=x
        self.currentView.refresh()
        
def main(stdscr, options, args, passphrase):
   
    curses.init_pair(1, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_BLUE, curses.COLOR_BLACK)
    taskList=[]
    for directory in args:
        task = cesubmit.Task.load(directory)
        taskList.append(task)
    #stdscr = curses.initscr()
    curses.noecho()
    stdscr.keypad(1)

    updateInterval=60
    lastUpdate=datetime.datetime.now()

    stdscr.addstr(0, 0, "cejobmonitor"+30*" ", curses.A_REVERSE)
    stdscr.addstr(1, 0, "Exit: q  Raise/lower update interval: +/- ("+str(updateInterval)+")  Update:  <SPACE>")
    stdscr.timeout(1000)
    curses.curs_set(0)
    stdscr.refresh()
    certtime=datetime.datetime.now()+datetime.timedelta(seconds=cesubmit.timeLeftVomsProxy())
    waitingForUpdate, waitingForExit = False, False
    overview=Overview(stdscr, taskList)
    markForResubmission=[]
    pool=None
    while True:
        stdscr.addstr(1, 0, "Exit: q  Raise/lower update interval: +/- ("+str(updateInterval)+")  Update:  <SPACE>     ")
        stdscr.addstr(2, 0, "Resubmit   (1): ABORTED,  (2): DONE-FAILED,  (3): (REALLY-)RUNNING,  (4): None")
        stdscr.addstr(3, 0, "Next update {0}       ".format(timerepr(lastUpdate+datetime.timedelta(seconds=updateInterval)-datetime.datetime.now())))
        stdscr.addstr(4, 0, "Certificate expires {0}       ".format(timerepr(certtime-datetime.datetime.now())))
        if waitingForExit:
            stdscr.addstr(6,0,"Exiting... Waiting for status retrieval to finish...")
        stdscr.refresh()
        overview.currentView.refresh()
        #stdscr.move(30,0)
        i=0
        if lastUpdate+datetime.timedelta(seconds=updateInterval)<datetime.datetime.now() or waitingForUpdate:
            #stdscr.clrtobot()
            if waitingForUpdate:
                if not pool._cache:
                    #pool.join()
                    if result.successful():
                        taskList = result.get()
                        overview.update(taskList)
                    lastUpdate = datetime.datetime.now()
                    waitingForUpdate = False
            else:
                if passphrase:
                    cesubmit.checkAndRenewVomsProxy(648000, passphrase=passphrase)
                    certtime=datetime.timedelta(seconds=cesubmit.timeLeftVomsProxy())+datetime.datetime.now()
                if False:  #set to true for serious debugging
                    for task in taskList:
                        checkTask(task)
                    overview.update(taskList)
                    lastUpdate = datetime.datetime.now()
                else:
                    for job in markForResubmission:
                        job.resubmit()
                    markForResubmission=[]
                    pool = multiprocessing.Pool()
                    result = pool.map_async(checkTask, taskList)
                    pool.close()
                    waitingForUpdate=True
        c = stdscr.getch()
        if c == ord('+'):
            updateInterval+=60
        elif c == ord('-'):
            updateInterval=max(30,updateInterval-30)
        elif c == ord('q') or c == 27:
            if overview.level:
                overview.up()
            else:
                waitingForExit=True
        elif c == ord(' '):
            lastUpdate = datetime.datetime.now()-datetime.timedelta(seconds=2*updateInterval)
        elif c == curses.KEY_DOWN:
            overview.currentView.goDown()
        elif c == curses.KEY_UP:
            overview.currentView.goUp()
        elif c == curses.KEY_NPAGE:
            overview.currentView.pageDown()
        elif c == curses.KEY_PPAGE:
            overview.currentView.pageUp()
        elif c == curses.KEY_HOME:
            overview.currentView.home()
        elif c == curses.KEY_END:
            overview.currentView.end()
        elif c == ord('1'):
            markForResubmission.append(resubmit(taskList, ["ABORTED"], overview))
            overview.update(taskList)
        elif c == ord('2'):
            markForResubmission.append(resubmit(taskList, ["DONE-FAILED"], overview))
            overview.update(taskList)
        elif c == ord('3'):
            markForResubmission.append(resubmit(taskList, ["RUNNING", "REALLY-RUNNING"], overview))
            overview.update(taskList)
        elif c == ord('4'):
            markForResubmission.append(resubmit(taskList, ["None", None], overview))
            overview.update(taskList)
        elif c == 10:
            overview.down()
        elif c==ord('r') and overview.level==1:
            markForResubmission.append(taskList[overview.currentTask].jobs[overview.currentJob])
            taskList[overview.currentTask].jobs[overview.currentJob].frontEndStatus="Marked for resubmission"
        else:
            pass
            #stdscr.addstr(8,0,str(c))
        if waitingForExit and not waitingForUpdate:
            break
    curses.nocbreak(); stdscr.keypad(0); curses.echo()
    curses.endwin()



if __name__ == "__main__":
    parser = optparse.OptionParser( description='Monitor for ce tasks', usage='usage: %prog directories')
    parser.add_option("--dump", action="store_true", dest="dump", help="Dump dictionary of task info", default=False)
    parser.add_option("-p", "--proxy", action="store_true", dest="proxy", help="Do not ask for password and use current proxy", default=False)
    (options, args) = parser.parse_args()
    if options.dump:
        for directory in args:
            task = cesubmit.Task.load(directory)
            pp = pprint.PrettyPrinter(indent=2)
            pp.pprint(task.__dict__)
            for job in task.jobs:
                pp.pprint(job.__dict__)
    else:
        #cesubmit.checkAndRenewVomsProxy(345600)
        if options.proxy:
            passphrase=None
        else:
            print "You may enter you grid password here. Do not enter anything to use the available proxy. Program will terminate when proxy expires"
            passphrase = getpass.getpass()
            if passphrase=="":
                passphrase = None
            else:
                cesubmit.renewVomsProxy(passphrase=passphrase)
        curses.wrapper(main, options, args, passphrase)
    
