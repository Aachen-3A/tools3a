#!/usr/bin/env python
import os,glob
import optparse
import logging
import fnmatch
import datetime
import curses
import multiprocessing
# custom modules
import curseshelpers
# so far no additional command line parsing needed
import gridFunctions 

# Command line parsing is added in commandline_parsing
import crabFunctions

mylogger = logging.getLogger("watchfrog")

def main(stdscr,options,args):
    
    # setting up curses
    curses.noecho()
    stdscr.keypad(1)
    
    ch.setLevel(logging.INFO)
    
    #~ logging.getLogger("watchfrog").addHandler(ch)
    #~ logging.getLogger("watchfrog").setLevel(logging.DEBUG)
    mylogger.addHandler(ch)
    mylogger.setLevel(logging.DEBUG)
    #~ options.logger = mylogger
    
    # first setup command line parsing and parse input
    #~ tag = "2014-10-13"
    
    curses.curs_set(0)
    stdscr.refresh()
    logText = curseshelpers.BottomText(stdscr,top=40)
    ch = curseshelpers.CursesHandler(stdscr,logText)
    #~ sample = "WToMuNu_M_200_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v2MINI_P8"
    sample = "WprimeToENu_M_3800_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v1MINI_P8"


    # get list of crab Folders in working Area
    crabFolders = getAllCrabFolders(options)
    
    #create list of CrabTask ojects
    #~ taskList = []
    #~ for folder in crabFolders:
        #~ taskList.append( crabFunctions.CrabTask(  ) )

    for handler in logging.getLogger().handlers:
        #~ mylogger.warning(handler.__dict__)
        mylogger.warning(handler)

    #~ testTask = crabFunctions.CrabTask(sample)
    #~ taskNameList = [testTask]
    taskNameList = [sample]
    logText.clear()
    resubmitList = []
    overview=Overview(stdscr, taskNameList ,resubmitList,options)

    waitingForExit =  False
    count = 0
    
    lastUpdate=datetime.datetime.now()
    
    pool = multiprocessing.Pool(processes = options.nCores)
    
    while not waitingForExit:
        stdscr.addstr(2, 0, "Next update {0}       ".format(timerepr(lastUpdate+datetime.timedelta(seconds=options.updateInterval)-datetime.datetime.now())))
        
        if lastUpdate+datetime.timedelta(seconds=options.updateInterval)<datetime.datetime.now():
            currenTasks = overview.tasks
            pool.map_async(updateTask,currentTasks)
            lastUpdate = datetime.datetime.now()
        
        count+=1
        mylogger.info("update %d\n"%count)
        stdscr.refresh()
        overview.update()
        #~ #overview.currentView.refresh()
        c = stdscr.getch()
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
    # free shell from curses
    curses.nocbreak(); stdscr.keypad(0); curses.echo()
    curses.endwin()
#~ class Overview:
    #~ def __init__(self, stdscr, tasks, resubmitList):

def getoutlfn(crabFolder):
    with open(os.path.join(crabFolder + "crab.log")) as logfile:
        lines = logfile.readlines
        dCacheFolder = filter(lambda line:"config.Data.outlfn" in line in line, lines)
        dCacheFolder = dCacheFolder[0]
        return dCacheFolder 

def getAllCrabFolders(options):
    # get all crab folders in working directory
    crabFolders = [f for f in os.listdir(options.workingArea) if os.path.isdir(os.path.join(options.workingArea, f))]
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
        print "found no folder with crab_ in working directory"
        sys.exit()
    return crabFolders
        
def updateTask(task):
    task.update()
    return task

class Overview:
    def __init__(self, stdscr, taskNameList, resubmitList,options):
        self.level = 0
        self.taskId = 0
        self.cursor = 0
        self.stdscr = stdscr
        self.taskOverviews = []
        self.crabController = crabFunctions.CrabController(workingArea = options.workingArea, logger = mylogger)
        self.tasks = []
        
        
        
        for taskName in taskNameList:
            self.tasks.append( crabFunctions.CrabTask( taskName, self.crabController ) )
        self.height=stdscr.getmaxyx()[0]-16
        self.addInfoHeader(options)
        self.height=stdscr.getmaxyx()[0]-16
        #~ self.tasktable = curseshelpers.SelectTable(stdscr, top=4, height=self.height, maxrows=100+len(tasks))
        self.tasktable = curseshelpers.SelectTable(stdscr, top=4, height=self.height, maxrows=50+len(self.tasks))
        widths=[5, 100, 11, 11, 11, 11, 11, 11, 11, 11 , 11, 20]
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
        
    def update(self):
        self.tasktable.clear()
        #~ self.logbox.refresh()
        
        for (taskId, taskOverview, task) in zip(range(len(self.tasks)), self.taskOverviews, self.tasks):
            self.tasktable.addRow( [taskId, task.name ,task.state ,task.nJobs , task.nUnsubmitted , task.nIdle, task.nRunning , task.nCooloff , task.nFailed, task.nTransferring , task.nFinished , task.lastUpdate] )
            taskOverview.clear()
            for jobkey in task.jobs.keys():
                job = task.jobs[jobkey]
                taskOverview.addRow( [jobkey, job['JobIds'][-1], job['State'], job['Retries'], job['Restarts'], ' '.join(job['SiteHistory']), formatedUnixTimestamp(job['SubmitTimes'][-1]), formatedUnixTimestamp(job['StartTimes'][-1]), formatedUnixTimestamp(job['EndTimes'][-1]), ] )
        self.tasktable.refresh()
        cells = ["", "TOTAL", "", self.taskStats.nTasks, self.taskStats.nUnsubmitted, self.taskStats.nIdle, self.taskStats.nRunning, self.taskStats.nCooloff,self.taskStats.nFailed, self.taskStats.nTransferring , self.taskStats.nFinished ]
        self.tasktable.setFooters(cells)
        self._refresh()
    
    def addInfoHeader(self,options):
        self.stdscr.addstr(0, 0, ("{0:^"+str(self.stdscr.getmaxyx()[1])+"}").format("watchfrog quark...quark"), curses.A_REVERSE)
        #~ self.stdscr.addstr(0, 0, ("{0:^"+str(self.stdscr.getmaxyx()[1])+"}").format(self.asciiFrog), curses.A_REVERSE)
        #~ self.stdscr.addstr(8, 0, "Exit: q  Raise/lower update interval: +/- ("+str(options.updateInterval)+"s)  Update:  <SPACE>")
        self.stdscr.addstr(1, 0, "Exit: q  Raise/lower update interval: +/- ("+str(options.updateInterval)+"s)  Update:  <SPACE>")
    
    @property
    def currentTask(self):
        return self.tasktable.cursor
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
            self.addInfoHeader(options)
            self.currentView = self.tasktable
        elif self.level==1:
            self.currentView = self.taskOverviews[self.currentTask]
        else:
            print "no recognized level for overview"
        self.currentView.refresh()
    @property
    def asciiFrog(self):
        return "              _     __        __    _       _      __                      _                \n"\
               "  __   ___.--'_`.   \ \      / /_ _| |_ ___| |__  / _|_ __ ___   __ _    .'_`--.___   __    \n"\
               " ( _`.'. -   'o\ )   \ \ /\ / / _` | __/ __| '_ \| |_| '__/ _ \ / _` |  ( /o`   - .`.'_ )   \n"\
               " _\.'_'      _.-'     \ V  V / (_| | || (__| | | |  _| | | (_) | (_| |   `-._      `_`./_   \n"\
               "( \`. )    //\`        \_/\_/ \__,_|\__\___|_| |_|_| |_|  \___/ \__, |     '/\\    ( .'/ )  \n"\
               " \_`-'`---'\\__,                                                 |___/    ,__//`---'`-'_/   \n"\
               "  \`        `-\                                                            /-'        '/    \n"\
               "   `                                                                                 '      \n"
               #~ " Upquark                       ... setting up the watchfrog ...               DownQuark     \n"                  

def formatedUnixTimestamp (unixTimeStamp):
    return datetime.datetime.fromtimestamp( int(unixTimeStamp) ).strftime('%Y-%m-%d %H:%M:%S')   

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


    
    parsingController = crabFunctions.CrabController(logger = mylogger)
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
    
    # check if user has valid proxy
    gridFunctions.checkAndRenewVomsProxy()
    
    #get current user HNname
    if not options.user:
        options.user = parsingController.checkHNname()
    
    return (options, args )
     
def testFunction(options):
    
    #~ # first setup command line parsing and parse input
    #~ tag = "2014-10-13"
    #~ 
    #~ sample = "WToMuNu_M_200_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v2MINI_P8"
    #~ sample = "WprimeToENu_M_3800_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v1MINI_P8"
    #~ sample = "WToTauNu_M_200_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v1MINI_P8"
    #~ sample = "QCD_Pt-170to300_13TeVSpring14miniaod-castor_PU20bx25_POSTLS170_V5-v1MINI_P8"
    #~ user = "tpook"
    #~ folder="/%s/MUSiC/%s/%s" % (user,tag,sample)
    #~ dCacheFileList = gridFunctions.getdcachelist(folder,tag) 
    #~ testTask = crabFunctions.CrabTask(sample)
    #~ taskList = [testTask]
    #~ testTask.updateJobStats()
    #~ print testTask.nJobs
    #~ print len( testTask.jobs.keys() )
    #~ print testTask.jobs
    # get list of crab Folders in working Area
    crabFolders = getAllCrabFolders(options)
    print options.workingArea
    #create list of CrabTask ojects
    taskList = []
    for handler in logging.getLogger().handlers:
        #~ mylogger.warning(handler.__dict__)
        mylogger.warning(handler)
    for folder in crabFolders:
        print folder
        #~ taskList.append( crabFunctions.CrabTask(  )
            
if __name__ == '__main__':
    # get command line arguments
    (options, args ) = commandline_parsing()
    #~ testFunction(options)
    curseshelpers.outputWrapper(main, 5,options,args)
