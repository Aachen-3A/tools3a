#!/usr/bin/env python
from multiprocessing import Process, Condition, Lock
from multiprocessing.managers import BaseManager
import threading
import os,glob,sys
import optparse
import logging
import fnmatch
import datetime
import time
import curses
import multiprocessing
import Queue

# custom modules
import aix3adb
from  aix3adb import Aix3adbException
import curseshelpers
# so far no additional command line parsing needed
import gridFunctions

# Command line parsing is added in commandline_parsing
import crabFunctions


serverLock = threading.Lock()
optionsLock = threading.Lock()
mylogger = logging.getLogger("watchfrog")

def runserver( options, args):
    # Start a shared manager server and access its queues
    manager = make_server_manager(5001, "blablibliub")
    optionsLock.acquire()
    options.shared_job_q = manager.get_job_q()
    options.shared_result_q = manager.get_result_q()
    options.shared_log_q = manager.get_log_q()
    optionsLock.release()

    mp_crab_worker(options.shared_job_q , options.shared_result_q , max(options.nCores-1 , 1) )

    time.sleep(2)
    serverLock.acquire()
    manager.shutdown()
    serverLock.release()

#~ def main( options , args):
def main(  ):
    print printFrogArt()
    (options, args ) = commandline_parsing()
    #~ curseshelpers.outputWrapper(runGui, 5,options,args)
    curses.wrapper(runGui, options, args)

def runGui(stdscr , options, args):
    class CrabManager( multiprocessing.managers.BaseManager ):
        pass
    job_q = multiprocessing.Queue()
    result_q = multiprocessing.Queue()
    log_q = multiprocessing.Queue()

    #~ multiprocessing.freeze_support()
    CrabManager.register('Controller', crabFunctions.CrabController)

    CrabManager.register('get_job_q', callable=lambda: job_q)
    CrabManager.register('get_result_q', callable=lambda: result_q)
    CrabManager.register('get_log_q', callable=lambda: log_q)

    manager = CrabManager(address=('', 5001), authkey='blabliblub')
    manager.start()
    optionsLock.acquire()
    options.shared_job_q = manager.get_job_q()
    options.shared_result_q = manager.get_result_q()
    options.shared_log_q = manager.get_log_q()
    optionsLock.release()

    ch =logging.FileHandler('frog.log', mode='a', encoding=None, delay=False)
    ch.setLevel(logging.INFO)

    mylogger.addHandler(ch)
    #~ mylogger.setLevel(logging.DEBUG)
    mylogger.setLevel(logging.INFO)

    crabController = manager.Controller( )

    crabWorkers = mp_crab_worker(options.shared_job_q , options.shared_result_q , options.shared_log_q ,max(options.nCores-1 , 1) )

    # curses color pairs
    curses.init_pair(1, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_BLUE, curses.COLOR_BLACK)
    curses.init_pair(5, curses.COLOR_MAGENTA, curses.COLOR_BLACK)

    # setting up curses
    curses.noecho()
    stdscr.keypad(1)

    curses.curs_set(0)
    stdscr.refresh()
    logText = curseshelpers.BottomText(stdscr,top=40)
    # handler without multiprocessing layer
    #ch = curseshelpers.CursesHandler(stdscr,logText)
    #~ ch = curseshelpers.CursesMultiHandler( stdscr, logText, options.shared_log_q )


    stdscr.timeout(1000)
    curses.curs_set(0)

    waitingForExit =  False
    count = 0

    lastUpdate=datetime.datetime.now()

    # get all needed tasks
    crabFolderList = getAllCrabFolders(options)
    tasknameList = []
    for folder in crabFolderList:
        tasknameList.append( folder.replace("crab_","") )

    logText.clear()
    resubmitList = []
    overview = Overview(stdscr, tasknameList , resubmitList, job_q , options.dblink)

    logText._redraw()
    updateFlag = True
    mylogger.info(" Finished init. Running GUI now")
    while not waitingForExit:
        count+=1
        stdscr.addstr(2, 0, "Next update {0}       ".format(timerepr(lastUpdate+datetime.timedelta(seconds=options.updateInterval)-datetime.datetime.now())))
        overview.currentView.refresh()
        #~ ch.receive()
        # check if new update should be started and add crabTasks to q
        if lastUpdate+datetime.timedelta(seconds=options.updateInterval)<datetime.datetime.now() or updateFlag:
            tasks = overview.tasks
            #filter tasks which are still updating
            tasks = filter(lambda task: not task.isUpdating, tasks)
            # check if we can skip crab for finalized samples
            [task.isFinal for task in tasks if "NOSTATE" in task.state]
            #filter tasks which are already marked as complete
            tasks[:] = [task for task in tasks if not task.state =="FINAL" ]

            optionsLock.acquire()
            for task in tasks:
                mylogger.info("adding task %s with state %s updateTime %s to queue"% ( task.name , task.state, task.lastUpdate ))

                #resubmit failed tasks
                if "FAILED" in task.state:
                    task.state == "RESUBMIT"
                elif "COMPLETED" in task.state:
                    task.state = "FINALIZING"
                else:
                    task.state = "UPDATING"
                options.shared_job_q.put(( task.state , task))
                time.sleep(0.1)
            optionsLock.release()
            updateFlag = False
            lastUpdate = datetime.datetime.now()

        overview.update()
        try:
            #~ finishedTask = options.shared_result_q.get()
            #~ mylogger.info("Tring to get updated Task from queue")

            finishedTask = options.shared_result_q.get_nowait()
            overview.tasks[:] = [task for task in overview.tasks if not finishedTask.uuid == task.uuid]
            mylogger.info("Appending Task %s with update time %s"% ( finishedTask.name, finishedTask.lastUpdate ) )
            overview.tasks.insert(finishedTask.taskId, finishedTask)
            overview.taskStats = crabFunctions.TaskStats( overview.tasks )
        except Queue.Empty:
            pass

        stdscr.refresh()
        logText.refresh()
        addInfoHeader(stdscr, options)
        c = stdscr.getch()
        #~ if c < 256 and c > 0:
            #~ mylogger.info(chr(c))
        #~ elif c>0:
            #~ mylogger.info(str(c))
        if c == ord('q') or c == 27 or c == curses.KEY_BACKSPACE:
            # q escape or backspace
            if overview.level:
                overview.up()
            else:
                waitingForExit=True
        elif c == ord('+'):
            options.updateInterval+=30
        elif c == ord('-'):
            options.updateInterval=max(30,options.updateInterval-30)
        elif c == ord(' '):
            overview.update_currentTask()
        elif c == ord('u'):
            updateFlag = True
        elif c == ord('r'):
            overview.resubmit_currentTask()
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
        elif c == 10:   #enter key
            overview.down()
        time.sleep(0.01)
    # free shell from curses
    curses.nocbreak(); stdscr.keypad(0); curses.echo()
    curses.endwin()

    del overview
    for p in crabWorkers:
        try:
            p.terminate()
        except:
            p.terminate()
    time.sleep(2)
    manager.shutdown()

def printFrogArt():
    return "              _     __        __    _       _      __                      _                \n"\
           "  __   ___.--'_`.   \ \      / /_ _| |_ ___| |__  / _|_ __ ___   __ _    .'_`--.___   __    \n"\
           " ( _`.'. -   'o\ )   \ \ /\ / / _` | __/ __| '_ \| |_| '__/ _ \ / _` |  ( /o`   - .`.'_ )   \n"\
           " _\.'_'      _.-'     \ V  V / (_| | || (__| | | |  _| | | (_) | (_| |   `-._      `_`./_   \n"\
           "( \`. )    //\`        \_/\_/ \__,_|\__\___|_| |_|_| |_|  \___/ \__, |     '/\\    ( .'/ )  \n"\
           " \_`-'`---'\\__,                                                 |___/    ,__//`---'`-'_/   \n"\
           "  \`        `-\                                                            /-'        '/    \n"\
           "   `                                                                                 '      \n"\
           " Upquark                       ... setting up the watchfrog ...               DownQuark     \n"


def mp_crab_worker(shared_job_q, shared_result_q, shared_log_q, nprocs):
    """ Split the work with jobs in shared_job_q and results in
        shared_result_q into several processes. Launch each process with
        factorizer_worker as the worker function, and wait until all are
        finished.
    """
    procs = []
    for i in range(nprocs):
        p = multiprocessing.Process(
                target=crab_worker,
                args=(shared_job_q, shared_result_q, shared_log_q))
        procs.append(p)
        p.start()

    #~ for p in procs:
        #~ p.join()
    return procs


def crab_worker(job_q, result_q, log_q):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q -. When the job is done,
        the result is placed into result_q. Runs until job_q is empty.
    """
    import random
    ch = logging.FileHandler('frog.log', mode='a', encoding=None, delay=False)
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
    time.sleep(random.randint(0, 150) / 100.  )
    # add formatter to ch
    ch.setFormatter(formatter)
    mylogger = logging.getLogger('worker')
    mylogger.setLevel(logging.DEBUG)
    mylogger.addHandler(ch)
    while True:
        try:
            #~ mylogger.info('tring to git from q')
            ( state, crabTask ) = job_q.get_nowait()
            mylogger.info('in worker updating Task %s now state %s '% (crabTask.name ,state) )
            if "RESUBMIT" in state:
                failedJobIds = []
                now = datetime.datetime.now()
                mylogger.info('in worker rseubmit taks')
                crabTask.resubmit_failed()
            if "FINALIZING" in state:
                if not crabTask.isFinal:
                    crabTask.finalizeTask()
            else:
                crabTask.update()
            mylogger.info('in worker updated Task %s now state %s '% (crabTask.name, crabTask.state ) )
            result_q.put( crabTask )
        except Queue.Empty:
            #~ mylogger.info("finished all crab update tasks in q")
            time.sleep(1.)
            time.sleep( random.randint(0, 100) / 100. )
        except Exception as e:
            mylogger.error('something went wrong in the worker: \n %s' % str(e))

def getAllCrabFolders(options):
    # get all crab folders in working directory
    crabFolders = [f for f in os.listdir(options.workingArea) if os.path.isdir(os.path.join(options.workingArea, f)) and "crab_" in f]
    # check if only folders, which match certain patterns should be watched
    if options.only:
        filteredFolders = []
        # loop over all user specified patterns
        for pattern in options.only:
            #loop over all crabFolders in working directory and
            # them to filteredFolder list if they match the pattern
            for crabFolder in crabFolders:
                if fnmatch.fnmatchcase( crabFolder, pattern ):
                    filteredFolders.append(crabFolder)
        crabFolders = filteredFolders
    if len(crabFolders) < 1:
        mylogger.error("found no folder with crab_ in working directory")
        sys.exit(1)
    return crabFolders


class Overview:
    def __init__(self, stdscr, taskNameList, resubmitList, shared_job_q, dblink):
        self.level = 0
        self.taskId = 0
        self.cursor = 0
        self.stdscr = stdscr
        self.shared_job_q = shared_job_q
        self.taskOverviews = []
        self.tasks = []
        # can be deleted in cleanup ?
        for taskName in taskNameList:
            self.tasks.append( crabFunctions.CrabTask( taskName, initUpdate = False, dblink= dblink) )
        self.height=stdscr.getmaxyx()[0]-16
        self.height=stdscr.getmaxyx()[0]-16
        self.tasktable = curseshelpers.SelectTable(stdscr, top=4, height=self.height, maxrows=50+len(self.tasks))
        widths=[5, 100, 13, 11, 11, 11, 11, 11, 11, 11 , 11, 20]
        self.tasktable.setColHeaders(["#","Task", "Status", "nJobs", "Unsubmitted", "Idle", "Run.","Cooloff","Fail.","Trans","Finished", "last Update"],widths)
        for task in self.tasks:
            #~ taskOverview = curseshelpers.SelectTable(stdscr, top=4, height=self.height, maxrows=100+task.nJobs)
            taskOverview = curseshelpers.SelectTable(stdscr, top=4, height=self.height, maxrows=50+task.nJobs)
            self.taskOverviews.append(taskOverview)
            widths=[5, 15, 22, 5, 5, 35 ,20 ,20 ,20]
            taskOverview.setColHeaders(["#","JobID", "State", "Retries", "Restarts", "Sites","SubmitTime" , "StartTime","EndTime"], widths)
        self.taskStats = crabFunctions.TaskStats(self.tasks)
        self.currentView = self.tasktable


        self.update()
    def __del__(self):
        serverLock.release()

    def update(self):
        self.tasktable.clear()

        for (taskId, taskOverview, task) in zip(range(len(self.tasks)), self.taskOverviews, self.tasks):
            printmode = self.getPrintmode(task)
            task.taskId = taskId
            cells = [task.taskId, task.name ,task.state ,task.nJobs , task.nUnsubmitted , task.nIdle, task.nRunning , task.nCooloff , task.nFailed, task.nTransferring , task.nFinished , task.lastUpdate]
            self.tasktable.addRow( cells , printmode )
            taskOverview.clear()
            for jobkey in task.jobs.keys():
                job = task.jobs[jobkey]
                if not 'EndTimes' in job.keys():
                    jobendtimes = ''
                elif len(job['EndTimes']) > 0:
                    jobendtimes = formatedUnixTimestamp(job['EndTimes'][-1])
                else:
                    jobendtimes = ''
                try:
                    taskOverview.addRow( [jobkey,
                                          job['JobIds'][-1],
                                          job['State'],
                                          job['Retries'],
                                          job['Restarts'],
                                          ' '.join(job['SiteHistory']),
                                          formatedUnixTimestamp(job['SubmitTimes'][-1]),
                                          formatedUnixTimestamp(job['StartTimes'][-1]),
                                           jobendtimes] )
                except:
                    pass
        self.tasktable.refresh()
        cells = [self.taskStats.nTasks, "Tasks total", "Job Stats" , sum(len(t.jobs) for t in self.tasks ) , self.taskStats.nUnsubmitted, self.taskStats.nIdle, self.taskStats.nRunning, self.taskStats.nCooloff,self.taskStats.nFailed, self.taskStats.nTransferring , self.taskStats.nFinished ]
        self.tasktable.setFooters(cells)
        self._refresh()

    def getPrintmode(self,task):
        if task.state in ["UPDATING", "FINALIZING"]:
            # blue
            printmode = curses.color_pair(4)
            #~ printmode = printmode | A_BLINK
        elif "SUBMITTED" in task.state:
            if task.nRunning > 0:
                # blue
                printmode = curses.color_pair(4)
            else:
                # yellow
                printmode = curses.color_pair(3)
        elif "COMPLETE" in task.state:
            #green
            printmode = curses.color_pair(2)
        elif "FINAL" in task.state:
            #green
            printmode = curses.color_pair(2)
        elif "DONE" in task.state:
            printmode = curses.color_pair(2)
            printmode = printmode | curses.A_BOLD
        else:
            # red
            printmode = curses.color_pair(1)
        return printmode

    @property
    def currentTask(self):
        return self.tasktable.cursor

    def update_currentTask(self):
        if self.level == 0:
            if self.tasks[self.currentTask].state == "COMPLETED":
                self.tasks[self.currentTask].state = "FINALIZING"
            elif self.tasks[self.currentTask].state == "FINALIZING":
                return
            elif self.tasks[self.currentTask].state == "FINAL":
                return
            else:
                self.tasks[self.currentTask].state = "UPDATING"
            self.shared_job_q.put_nowait( (self.tasks[self.currentTask].state
                                        ,self.tasks[self.currentTask]) )
    def resubmit_currentTask(self):
        if self.level == 0:
            self.tasks[self.currentTask].state = "RESUBMIT"
            self.shared_job_q.put_nowait( (self.tasks[self.currentTask].state
                                        ,self.tasks[self.currentTask]) )
    def down(self):
        self.stdscr.clear()
        self.level=min(self.level+1,2)
        self._refresh()
    def up(self):
        self.stdscr.clear()
        self.level=max(self.level-1,0)
        self._refresh()
    def _refresh(self):
        if self.level==0:
            self.currentView = self.tasktable
        elif self.level==1:
            self.currentView = self.taskOverviews[self.currentTask]
        else:
            print "no recognized level for overview"
        self.currentView.refresh()

def addInfoHeader(stdscr, options):
    stdscr.addstr(0, 0, ("{0:^"+str(stdscr.getmaxyx()[1])+"}").format("watchfrog quark...quark"), curses.A_REVERSE)
    #~ self.stdscr.addstr(0, 0, ("{0:^"+str(self.stdscr.getmaxyx()[1])+"}").format(self.asciiFrog), curses.A_REVERSE)
    #~ self.stdscr.addstr(8, 0, "Exit: q  Raise/lower update interval: +/- ("+str(options.updateInterval)+"s)  Update:  <SPACE>")
    stdscr.addstr(1, 0, "Exit: q  Raise/lower update interval: +/- ("+str(options.updateInterval)+"s)  Update Task:  <SPACE> Update all: <u> Resubmit failed: <r>")

def createDBlink():
    # Create a database object.
    dblink = aix3adb.aix3adb()
    crab = crabFunctions.CrabController()
    # Authorize to database.
    #~ print( "Connecting to database: 'http://cern.ch/aix3adb'" )
    dblink.authorize(username = crab.checkusername())
    #~ log.info( 'Authorized to database.' )
    return dblink

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

def formatedUnixTimestamp (unixTimeStamp):
    return datetime.datetime.fromtimestamp( int(unixTimeStamp) ).strftime('%Y-%m-%d %H:%M:%S')


def commandline_parsing():
    parser = optparse.OptionParser( description='Watchfrog helps you to care for your jobs',  usage='usage: %prog [options]' )
    parser.add_option( '-o', '--only', metavar='PATTERNS', default=None,
                       help='Only check samples matching PATTERNS (bash-like ' \
                            'patterns only, comma separated values. ' \
                            'E.g. --only QCD* ). [default: %default]' )
    parser.add_option( '-u','--user', help='Alternative username [default is HN-username]')
    parser.add_option( '--workingArea',metavar='DIR',help='The area (full or relative path) where the CRAB project directories are saved. ' \
                     'Defaults to the current working directory.'       )
    parser.add_option( '--updateInterval', default=600,help='Time between two updates for crab tasks in seconds.')
    parser.add_option( '--nCores', default=multiprocessing.cpu_count(),help='Number of cores to use [default: %default]')



    #~ parsingController = crabFunctions.CrabController(logger = mylogger)
    parsingController = crabFunctions.CrabController()
    # we need to add the parser options from other modules
    #get crab command line options
    parsingController.commandlineOptions(parser)

    (options, args ) = parser.parse_args()
    now = datetime.datetime.now()
    isodatetime = now.strftime( "%Y-%m-%d_%H.%M.%S" )
    options.isodatetime = isodatetime

    if options.workingArea:
        options.workingArea = os.path.abspath(options.workingArea)
    else:
        options.workingArea = os.path.abspath(os.getcwd())

    options.runServer = True
    # get pass before starting

    options.dblink = createDBlink()
    # check if user has valid proxy
    import gridFunctions
    import getpass
    proxytime = gridFunctions.checkVomsProxy()
    if not proxytime:
        passphrase = getpass.getpass('Please enter your GRID pass phrase:')
        gridFunctions.checkAndRenewVomsProxy( passphrase = passphrase)




    #get current user HNname
    if not options.user:
        options.user = parsingController.checkusername()

    return (options, args )

if __name__ == '__main__':
    # get command line arguments

    #~ runserver( options, args )
    #~ main( options, args )
    main(  )



