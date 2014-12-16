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
import logging
import collections

def addtime(tfor,tsince,tto):
    """Add two time intervals
    tfor to tto-tsince
    """
    if tsince==None:
        return tfor
    deltat=tto-tsince
    if tfor==None:
        tfor=deltat
    else:
        tfor=tfor+deltat
    return tfor
def timerepr(deltat):
    """Return formatted time interval
    """
    if deltat.days<0:
        return "now"
    hours, seconds=divmod(deltat.seconds, 60*60)
    minutes, seconds=divmod(seconds, 60)
    if deltat.days: return "in {0}d {1}h {2}m {3}s".format(deltat.days, hours, minutes, seconds)
    if hours: return "in {0}h {1}m {2}s".format(hours, minutes, seconds)
    if minutes: return "in {0}m {1}s".format(minutes, seconds)
    return "in {0}s".format(seconds)

def checkTask(task, resubmitJobs, killJobs):
    """perform actions on a task
    resubmit jobs, get status, get output and get status again
    """
    if len(killJobs) > 0:
        task.kill(killJobs)
    if len(resubmitJobs) > 0:
        task.resubmit(resubmitJobs)
    status = task.getStatus()
    task.getOutput(4)
    status = task.getStatus()
    return task

def nextUpdate(lastUpdate, updateInterval, nextTaskId):
    if nextTaskId==0:
        return lastUpdate+datetime.timedelta(seconds=updateInterval)-datetime.datetime.now()
    else:
        return datetime.timedelta(seconds=-1)

def resubmit(taskList, resubmitList, status, overview):
    """add jobs with a certain status to the resubmit list
    """
    if overview.level==0:
        myTaskIds, myTaskList=range(len(taskList)), taskList
    elif overview.level==1:
        myTaskIds, myTaskList=[overview.currentTask], [taskList[overview.currentTask]]
    else:
        myTaskIds, myTaskList=[], []
    for (t, task) in zip(myTaskIds, myTaskList):
        for (j, job) in zip(range(len(task.jobs)), task.jobs):
            if job.status in status:
                if (job.status == "DONE-OK" and job.infos["ExitCode"]!="0") or job.status != "DONE-OK":
                    resubmitList[t].add(j)

def kill(taskList, killList, overview):
    """add jobs to the kill list
    """
    if overview is True:
        # kill all tasks
        for t in range(len(taskList)):
            for j in range(len(taskList[t].jobs)):
                killList[t].add(j)
    elif overview.level==0:
        # kill one task
        for j in range(len(taskList[overview.currentTask].jobs)):
            killList[overview.currentTask].add(j)
    elif overview.level==1:
        # kill one job
        killList[overview.currentTask].add(overview.currentJob)

def getRunTimeFromHistory(history):
    starttime = [int(item[1]) for item in history if item[0] == "RUNNING"]
    endtime = [int(item[1]) for item in history if "DONE" in item[0]]
    if len(starttime)!=1:
        return ""
    if len(endtime)!=1:
        timedelta = datetime.datetime.now() - datetime.datetime.fromtimestamp(starttime[0])
    else:
        timedelta = datetime.datetime.fromtimestamp(endtime[0]) - datetime.datetime.fromtimestamp(starttime[0])
    return timerepr(timedelta)[3:]

class Overview:
    """This class incorporates the 'graphical' overviews of tasks, jobs and jobinfo.
    Tasks and jobs overviews are stored persistantly in order to be aware of the selected task/job.
    Jobinfo is created on the fly.
    """
    def __init__(self, stdscr, tasks, resubmitList, killList, nextTaskId):
        self.level = 0
        self.taskId = 0
        self.cursor = 0
        self.stdscr = stdscr
        self.taskOverviews = []
        self.height=stdscr.getmaxyx()[0]-16
        self.overview = curseshelpers.SelectTable(stdscr, top=10, height=self.height, maxrows=100+len(tasks), footer=True)
        widths=[2, 100, 12, 12, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9]
        self.overview.setColHeaders(["", "Task", "Status", "Performance", "Total", "Prep.", "Run.", "RRun.", "Abrt.", "Fail.", "OK", "Good", "None", "Retr."], widths)
        for task in tasks:
            taskOverview = curseshelpers.SelectTable(stdscr, top=10, height=self.height, maxrows=100+len(task.jobs))
            widths=[100, 16, 22, 22, 16, 10]
            taskOverview.setColHeaders(["Job", "Status", "In Status since", "Running for", "FE-Status", "Exit Code"], widths)
            self.taskOverviews.append(taskOverview)
        self.update(tasks, resubmitList, killList, nextTaskId)
        self.tasks = tasks
    def update(self, tasks, resubmitList, killList, nextTaskId):
        self.tasks = tasks
        self.overview.clear()
        totalstatusnumbers = collections.defaultdict(int)
        for (taskId, taskOverview, task) in zip(range(len(tasks)), self.taskOverviews, tasks):
            statusnumbers=task.jobStatusNumbers()
            if statusnumbers['good'] + statusnumbers['bad'] == 0:
                performance = None
                strperformance = ''
            else:
                performance = statusnumbers['good']/(statusnumbers['good']+statusnumbers['bad'])
                strperformance = '{0:>6.1%}'.format(performance)
            #formatting
            if performance is None:
                # blue
                printmode = curses.color_pair(4)
            elif performance <=0.95:
                # red
                printmode = curses.color_pair(1)
            elif 0.95<performance<1:
                # yellow
                printmode = curses.color_pair(3)
            else:
                #green
                printmode = curses.color_pair(2)
            if task.frontEndStatus != "RETRIEVED":
                printmode = printmode | curses.A_BOLD
            #prepare and add row
            if nextTaskId == taskId:
                icon = ">"
            else:
                icon = " "
            cells = [icon, task.name, task.frontEndStatus, strperformance, statusnumbers['total'], statusnumbers['PENDING']+ statusnumbers['IDLE']+statusnumbers['SUBMITTED']+statusnumbers['REGISTERED'], statusnumbers['RUNNING'], statusnumbers['REALLY-RUNNING'], statusnumbers['ABORTED'], statusnumbers['DONE-FAILED'], statusnumbers['DONE-OK'], statusnumbers['good'], statusnumbers[None]+statusnumbers["None"], statusnumbers['RETRIEVED']]
            self.overview.addRow(cells, printmode)
            for key in statusnumbers:
                totalstatusnumbers[key]+=statusnumbers[key]
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
                    jobreturncode = ""
                try:
                    jobsince = datetime.datetime.fromtimestamp(int(job.infos["history"][-1][1])).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    jobsince = ""
                try:
                    jobrunningfor = getRunTimeFromHistory(job.infos["history"])
                except KeyError:
                    jobrunningfor = ""
                cells = [jobid, jobstatus, jobsince, jobrunningfor, jobfestatus, jobreturncode]
                if job.nodeid in resubmitList[taskId] or job.nodeid in killList[taskId]:
                    printmode = curses.color_pair(5) | curses.A_BOLD
                elif jobstatus in ['DONE-FAILED', 'ABORTED']:
                    printmode = curses.color_pair(1) | curses.A_BOLD
                elif jobfestatus == "RETRIEVED":
                    if jobreturncode == "0":
                        printmode=curses.color_pair(2)
                    else:
                        printmode = curses.color_pair(1) | curses.A_BOLD
                elif "RUNNING" in jobstatus:
                    printmode = curses.color_pair(2) | curses.A_BOLD
                else:
                    printmode = curses.A_BOLD
                taskOverview.addRow(cells, printmode)
        if totalstatusnumbers['good'] + totalstatusnumbers['bad'] == 0:
            performance = None
            strperformance = ''
        else:
            performance = totalstatusnumbers['good']/(totalstatusnumbers['good']+totalstatusnumbers['bad'])
            strperformance = '{0:>6.1%}'.format(performance)
        cells = ["", "TOTAL", "", strperformance, totalstatusnumbers['total'], totalstatusnumbers['PENDING']+ totalstatusnumbers['IDLE']+totalstatusnumbers['SUBMITTED']+totalstatusnumbers['REGISTERED'], totalstatusnumbers['RUNNING'], totalstatusnumbers['REALLY-RUNNING'], totalstatusnumbers['ABORTED'], totalstatusnumbers['DONE-FAILED'], totalstatusnumbers['DONE-OK'], totalstatusnumbers['good'], totalstatusnumbers[None]+totalstatusnumbers["None"], totalstatusnumbers['RETRIEVED']]
        self.overview.setFooters(cells)
        self._refresh()
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
    def _refresh(self):
        if self.level==0:
            self.currentView = self.overview
        elif self.level==1:
            self.currentView = self.taskOverviews[self.currentTask]
        else:
            pp = pprint.PrettyPrinter(indent=4)
            x = curseshelpers.TabbedText(self.stdscr, top=10, height=self.height-2)
            try:
                x.addText("Status information",pp.pformat(self.tasks[self.currentTask].jobs[self.currentJob].infos))
            except:
                x.addText("Status information", "No information available")
            if self.tasks[self.currentTask].jobs[self.currentJob].frontEndStatus=="RETRIEVED":
                try:
                    x.addFile("stdout",os.path.join(self.tasks[self.currentTask].directory, self.tasks[self.currentTask].jobs[self.currentJob].outputSubDirectory,"out.txt"))
                except:
                    x.addText("stdout","could not find stdout"+ os.path.join(self.tasks[self.currentTask].directory, self.tasks[self.currentTask].jobs[self.currentJob].outputSubDirectory,"out.txt"))
                try:
                    x.addFile("stderr",os.path.join(self.tasks[self.currentTask].directory, self.tasks[self.currentTask].jobs[self.currentJob].outputSubDirectory,"err.txt"))
                except:
                    x.addText("stderr","could not find stderr")
            x.addFile("jdl file", os.path.join(self.tasks[self.currentTask].directory,self.tasks[self.currentTask].jobs[self.currentJob].jdlfilename))
                    
            self.currentView=x
        self.currentView.refresh()

def main(stdscr, options, args, passphrase):
    # Logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging._levelNames[options.debug.upper()])
    logQueue = multiprocessing.Queue()
    logText = curseshelpers.BottomText(stdscr, stdscr.getmaxyx()[0]-5)
    handler = curseshelpers.CursesMultiHandler(stdscr, logText, logQueue, level=logging._levelNames[options.debug.upper()])
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # curses color pairs 
    curses.init_pair(1, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_BLUE, curses.COLOR_BLACK)
    curses.init_pair(5, curses.COLOR_MAGENTA, curses.COLOR_BLACK)
    taskList, resubmitList, killList = [], [], []
    # load tasks from directories
    for directory in args:
        task = cesubmit.Task.load(directory)
        taskList.append(task)
        resubmitList.append(set())
        killList.append(set())
    curses.noecho()
    stdscr.keypad(1)
    updateInterval=600
    lastUpdate=datetime.datetime.now()
    
    # paint top rows
    stdscr.addstr(0, 0, ("{0:^"+str(stdscr.getmaxyx()[1])+"}").format("television"), curses.A_REVERSE)
    stdscr.timeout(1000)
    curses.curs_set(0)
    stdscr.refresh()
    # get validity of the certificate
    certtime=datetime.datetime.now()+datetime.timedelta(seconds=cesubmit.timeLeftVomsProxy())
    # waitingForUpdate stores the current task when its updated. waitingForExit is needed to wait for all jobs to finish before exiting
    waitingForUpdate, waitingForExit = None, False
    nextTaskId=0
    overview = Overview(stdscr, taskList, resubmitList, killList, nextTaskId)
    # this is the pool for the update task.
    pool = None
    while True:
        # main loop
        stdscr.addstr(1, 0, "Exit <q>  Raise/lower update interval <+>/<-> ("+str(updateInterval)+")  More information <return>  Update <space>   ")
        stdscr.addstr(2, 0, "Resubmit job <r>   By Status:  Aborted <1>, Done-Failed <2>, (Really-)Running <3>, None <4>, Done-Ok exit!=0 <5>")
        stdscr.addstr(3, 0, "Kill job/task <k>   Kill all tasks <K>")
        stdscr.addstr(4, 0, "Next update {0}       ".format(timerepr(nextUpdate(lastUpdate, updateInterval, nextTaskId))))
        stdscr.addstr(5, 0, "Certificate expires {0}       ".format(timerepr(certtime-datetime.datetime.now())))
        if waitingForExit:
            stdscr.addstr(6,0,"Exiting... Waiting for status retrieval to finish...", curses.color_pair(1) | curses.A_BOLD)
        stdscr.refresh()
        # refresh overview (the task/job table or the jobinfo text)
        overview.currentView.refresh()
        logText.refresh()
        if nextUpdate(lastUpdate, updateInterval, nextTaskId).days<0 or waitingForUpdate is not None:
            # should an update be performed or is ongoing?
            if waitingForUpdate is not None:
                # update ongoing
                if not pool._cache:
                    if result.successful():
                        # rewrite the task into the tasklist, this is necessary as the multiprocessing pickles the object
                        taskList[waitingForUpdate] = result.get()
                        overview.update(taskList, resubmitList, killList, nextTaskId)
                    lastUpdate = datetime.datetime.now()
                    waitingForUpdate = None
            else:
                # no update ongoing, then start a new task to update
                if passphrase:
                    cesubmit.checkAndRenewVomsProxy(648000, passphrase=passphrase)
                    certtime=datetime.timedelta(seconds=cesubmit.timeLeftVomsProxy())+datetime.datetime.now()
                if False:  #set to true for serious debugging, this disables the multiprocessing
                    for task in taskList:
                        checkTask(task)
                    overview.update(taskList, resubmitList, killList, nextTaskId)
                    lastUpdate = datetime.datetime.now()
                else:
                    # prepare parameters
                    parameters = [taskList[nextTaskId], resubmitList[nextTaskId], killList[nextTaskId]]
                    # use one process only, actual multiprocessing is handled within this process (multiple jobs per tasks are retrieved)
                    pool = multiprocessing.Pool(1)
                    result = pool.apply_async(checkTask, parameters)
                    pool.close()
                    # reset resubmit list for this task
                    resubmitList[nextTaskId], killList[nextTaskId] = set(), set()
                    waitingForUpdate = nextTaskId
                    nextTaskId = (nextTaskId+1) % len(taskList)
                    
        # user key press processing
        c = stdscr.getch()
        if c == ord('+'):
            updateInterval+=60
        elif c == ord('-'):
            updateInterval=max(0,updateInterval-60)
        elif c == ord('q') or c == 27 or c == curses.KEY_BACKSPACE:
            # q escape or backspace
            if overview.level:
                overview.up()
            else:
                waitingForExit=True
        elif c == ord(' '):
            lastUpdate = datetime.datetime.now()-datetime.timedelta(seconds=2*updateInterval)
        elif c == curses.KEY_LEFT:
            overview.currentView.goLeft()
        elif c == curses.KEY_RIGHT:
            overview.currentView.goRight()
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
            resubmit(taskList, resubmitList, ["ABORTED"], overview)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c == ord('2'):
            resubmit(taskList, resubmitList, ["DONE-FAILED"], overview)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c == ord('3'):
            resubmit(taskList, resubmitList, ["RUNNING", "REALLY-RUNNING"], overview)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c == ord('4'):
            resubmit(taskList, resubmitList, ["None", None], overview)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c == ord('5'):
            resubmit(taskList, resubmitList, ["DONE-OK"], overview)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c==ord('r') and overview.level==1:
            resubmitList[overview.currentTask].add(overview.currentJob)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c==ord('k'):
            kill(taskList, killList, overview)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c==ord('K'):
            kill(taskList, killList, True)
            overview.update(taskList, resubmitList, killList, nextTaskId)
        elif c==ord('t'):
            logger.warning("warning")
            logger.info("info")
        elif c == 10:   #enter key
            overview.down()
        else:
            pass
        if waitingForExit and waitingForUpdate is None:
            break
    curses.nocbreak(); stdscr.keypad(0); curses.echo()
    curses.endwin()

if __name__ == "__main__":
    parser = optparse.OptionParser( description='Monitor for ce tasks', usage='usage: %prog directories')
    parser.add_option("--dump", action="store_true", dest="dump", help="Dump dictionary of task info", default=False)
    parser.add_option("--debug", action="store", dest="debug", help="Debug level (DEBUG, INFO, WARNING, ERROR, CRITICAL)", default="WARNING")
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
        if options.proxy:
            passphrase=None
        else:
            print "You may enter your grid password here. Do not enter anything to use the available proxy."
            passphrase = getpass.getpass()
            if passphrase=="":
                passphrase = None
            else:
                cesubmit.renewVomsProxy(passphrase=passphrase)
        curses.wrapper(main, options, args, passphrase)
        #curseshelpers.outputWrapper(main, 5, options, args, passphrase)
    
