#! /usr/bin/env python2
from __future__ import division
import logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
import cesubmit
import os
import time
import glob
import subprocess
import ConfigParser
import optparse
import aix3adb
import math
import shlex
import tarfile
import gridFunctions

def main():
    parser = optparse.OptionParser(usage="usage: %prog [options] arguments")
    groupRun = optparse.OptionGroup(parser, "Run mode Options")
    groupRun.add_option("-p", "--prepare", action="store_true", help="Prepare television tasks (default)", default=True)
    groupRun.add_option("-m", "--merge", action="store_true", help="Merge output files", default=False)
    parser.add_option_group(groupRun)

    groupCommon = optparse.OptionGroup(parser, "Common Options")
    parser.add_option_group(groupCommon)

    groupMerge = optparse.OptionGroup(parser, "Merge Options","Specify the television directories as arguments.")
    parser.add_option_group(groupMerge)

    groupPrepare = optparse.OptionGroup(parser, "Prepare Options","Specify one config file as argument")
    groupPrepare.add_option("-d", "--directory", action="store", help="Main television directory", default="./")
    groupPrepare.add_option("--section", action="append", help="Only prepare the following section. May be specified multiple times for multiple sections. If not specified, all sections are prepared.", default=[])
    groupPrepare.add_option("--test", action="store_true", help="Run one task per section with one small job only.", default=False)
    #groupPrepare.add_option("--local", action="store_true", help="Run the tasks on local computer.", default=False)
    #groupPrepare.add_option("--testlocal", action="store_true", help="Run only one task with one small job locally.", default=False)
    groupPrepare.add_option("-s", "--skipcheck", action="store_true", help="Skip check if grid pack is outdated.", default=False)
    parser.add_option_group(groupPrepare)
    (options, args) = parser.parse_args()
    gridFunctions.checkAndRenewVomsProxy()
    if options.merge:
        merge(options, args)
    elif options.prepare:
        prepare(options, args)

def prepare(options, args):
    config = MyConfigParser()
    config.read(args[0])
    dblink = aix3adb.aix3adb()
    executable = config.get("DEFAULT","executable")
    outputfiles = shlex.split(config.get("DEFAULT","outputfiles"))
    gridoutputfiles = shlex.split(config.get("DEFAULT","gridoutputfiles"))
    localgridpackdirectory = config.get("DEFAULT","localgridpackdirectory")
    gridpackfiles = config.get("DEFAULT","gridpackfiles")
    localgridtarfile = config.get("DEFAULT","localgridtarfile")
    remotegridtarfile = config.get("DEFAULT","remotegridtarfile")
    cmssw = config.get("DEFAULT","cmssw")
    maxeventsoption = config.get("DEFAULT","maxeventsoption")
    skipeventsoption = config.get("DEFAULT","skipeventsoption")
    eventsperjob = config.getint("DEFAULT","eventsperjob")
    filesperjob = config.getint("DEFAULT","filesperjob")
    datasections = config.get("DEFAULT","datasections")
    basedir = options.directory
    if not options.skipcheck:
        checkGridpack(localgridtarfile, remotegridtarfile, localgridpackdirectory, gridpackfiles)
    dblink = aix3adb.aix3adb()
    for section in config.sections():
        if section in ["DEFAULT"]: continue
        if options.sections != [] and section not in options.section: continue
        mc = not (section in datasections)
        print "Preparing tasks for section {0}.".format(section)
        identifiers=config.optionsNoDefault(section)
        print "Found {0} tasks.".format(len(identifiers))
        for identifier in identifiers:
            arguments = shlex.split(config.get(section, identifier))
            if identifier[0]=="/":
                #it's a datasetpath
                if mc:
                    skim, sample = dblink.getMCLatestSkimAndSampleByDatasetpath(identifier)
                else:
                    skim, sample = dblink.getDataLatestSkimAndSampleByDatasetpath(identifier)
            elif identifier.isdigit():
                #it's a skim id
                if mc:
                    skim, sample = dblink.getMCSkimAndSampleBySkim(identifier)
                else:
                    skim, sample = dblink.getDataSkimAndSampleBySkim(identifier)
            else:
                #it's a sample name
                if mc:
                    skim, sample = dblink.getMCLatestSkimAndSampleBySample(identifier)
                else:
                    skim, sample = dblink.getDataLatestSkimAndSampleBySample(identifier)
            makeTask(skim, sample, basedir, section, executable, arguments, cmssw, eventsperjob, filesperjob, maxeventsoption, skipeventsoption, outputfiles, gridoutputfiles, remotegridtarfile, test=options.test)
            if options.test:
                break

def merge(options, args):
    print "Merging..."
    for directory in args:
        print directory
        rootfiles = set([os.path.basename(f) for f in glob.glob(os.path.join(directory, "grid*","*.root"))])
        for outfilename in rootfiles:
            joinfilenames=glob.glob(os.path.join(directory,"grid*",outfilename))
            mergedfilename=os.path.join(directory,"merged_"+outfilename)
            cmd = ["hadd","-f",mergedfilename]+joinfilenames
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            (stdout, stderr) = p.communicate()

def makeTask(skim, sample, basedir, section, executable, arguments, cmssw, eventsperjob, filesperjob, maxeventsoption, skipeventsoption, outputfiles, gridoutputfiles, remotegridtarfile, test):
    name = sample.name+"-skimid"+str(skim.id)
    print "Preparing task", name
    task=cesubmit.Task(name, directory=os.path.join(basedir,section,name), cmsswVersion=cmssw, mode="CREATE")
    task.executable=executable
    task.uploadexecutable=False
    task.outputfiles.extend(outputfiles)
    task.addGridPack(remotegridtarfile)
    for gridoutputfile in gridoutputfiles:
        task.copyResultsToDCache(gridoutputfile)
    jobchunks=getJobChunks(skim.files, eventsperjob, filesperjob, maxeventsoption, skipeventsoption, test)
    print "Number of jobs: ", len(jobchunks)
    for chunk in jobchunks:
        job=cesubmit.Job()
        job.arguments.extend(arguments)
        job.arguments.extend(chunk)
        task.addJob(job)
    print "Submitting..."
    task.submit(6)
    print "[Done]"

## Determines the adler32 check sum of a file
#
#@type path: string
#@param path: The path to the file.
#@return string The adler32 check sum value.
def adler32(path):
    cmd = ["adler32", path]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    return stdout.strip()

## Asks a yes/no question at the prompt
#
#@type question: string
#@param question: The question to ask
#@return boolean returns True if the user answered y, False if the user answered n.
def ask(question):
    while True:
        print question
        answer = raw_input("[y/n]")
        if answer in "yY":
            return True
        elif answer in "nN":
            return False

## Check a grid pack and asks if it should be updated
#
# Two checks are performed. (1) the timestamp of all files that should enter the grid pack
# is compared to the local grid pack timestamp. If at least one file is newer than the grid pack,
# it is suggested to upgrade the grid pack. (2) The adler32 check sum of the remote grid pack
# is compared to the local grid pack. If they differ or no remote grid pack is found, it is suggested
# to upload the grid pack.
#@type local: string
#@param local: The local path where the grid tar.gz file is stored.
#@type remote: string
#@param remote: The remote path where the grid tar.gz file is stored.
#@type gpdir: string
#@param gpdir: The local directory containing all the files and subdirectories that are part of the gridpack.
#@type gpfilestring: string.
#@param gpfilestring: A string describing all files that should be contained in the grid pack. Usual shell syntax, such as astersiks is allowed. Directories are included recursively.
def checkGridpack(local, remote, gpdir, gpfilestring):
    print "Comparing gridpack files timestamps..."
    gppaths = expandFiles(gpdir, gpfilestring)
    # check time stamps
    outdated = False
    createnew = False
    try:
        timestampzipped = os.path.getmtime(local)
    except OSError:
        print "Local grid pack file does not exist. Creating..."
        createnew=True
    if not createnew:
        for path in gppaths:
            if os.path.getmtime(path)>timestampzipped:
                print "Found outdated file",path
                outdated=True
        if outdated:
            if ask("Local grid pack zip file is outdated. Would you like to create a current version?"):
                createnew=True
    if createnew:
        createGridpack(local, gpdir, gpfilestring)
    # check hashes
    try:
        adlerRemote = gridFunctions.adler32(os.path.join(cesubmit.getCernUserName(), remote))
    except gridFunctions.FileNotFound:
        print "Remote gridpack not found"
        adlerRemote = None
    adlerLocal = adler32(local)
    if adlerLocal!=adlerRemote:
        if ask("Remote gridpack differs from local gridpack. Do you want to upload the current local gridpack now?"):
            cesubmit.uploadGridPack(local, remote)

## Creates a local grid pack tar.gz file
#
#@type localpath: string
#@param localpath: The local path where the grid tar.gz file is stored.
#@type gpdir: string
#@param gpdir: The local directory containing all the files and subdirectories that are part of the gridpack.
#@type gpfilestring: string.
#@param gpfilestring: A string describing all files that should be contained in the grid pack. Usual shell syntax, such as astersiks is allowed. Directories are included recursively.
def createGridpack(localpath, gpdir, gpfilestring):
    gppaths = expandFiles(gpdir, gpfilestring)
    tar = tarfile.open(localpath, "w:gz")
    for name in gppaths:
        extractname=name[len(gpdir):].lstrip("/")
        tar.add(name, extractname)
    tar.close()

## Returns a list of paths from a base directory and a shell conform list of subpath
#
# The function prepends the base directory and resolves wildcards
#@type gpdir: string
#@param gpdir: The local directory containing all the files and subdirectories that are part of the gridpack.
#@type gpfilestring: string.
#@param gpfilestring: A string describing all files that should be contained in the grid pack. Usual shell syntax, such as astersiks is allowed. Directories are included recursively.
#@return: A list of all paths corresponding to the input
def expandFiles(gpdir, gpfilestring):
    gpfiles = shlex.split(gpfilestring)
    # get a list of all filepaths (resolving asterisks and joining the basedir to the path)
    gppaths = [filename for sublist in [glob.glob(p) for p in [os.path.join(gpdir,f) for f in gpfiles]] for filename in sublist]
    return gppaths

def getJobChunks(files, eventsperjob, filesperjob, maxeventsoption, skipeventsoption, test):
    if test:
        return [[maxeventsoption, "100", files[0].path]]
    if eventsperjob and not filesperjob:
        result = determineJobChunksByEvents(files, eventsperjob)
        return [[maxeventsoption, str(eventsperjob), skipeventsoption, str(skip)]+x for (skip, x) in result]
    elif filesperjob:
        result = determineJobChunksByFiles(files, filesperjob)
        return result
    raise Exception("Please specify either eventsperjob or filesperjob")

def determineJobChunksByFiles(files, filesperjob):
    filenames = [f['path'] for f in files]
    return list(chunks(filenames, filesperjob))

def determineJobChunksByEvents(files, eventsperjob):
    events=[int(f['nevents']) for f in files]
    cummulativeHigh=list(cummulative(events))
    cummulativeLow=[0]+cummulativeHigh[:-1]
    totalevents=cummulativeHigh[-1]
    result=[]
    for i in xrange(int(math.ceil(totalevents/eventsperjob))):
        skipEventsGlobal=eventsperjob*i
        skipEventsLocal=getSkipEventsLocal(cummulativeLow, skipEventsGlobal)
        filenames=getFileList(files, cummulativeHigh, cummulativeLow, skipEventsGlobal, eventsperjob)
        result.append((skipEventsLocal,filenames))
    return result

def getFileList(files, cummulativeHigh, cummulativeLow, skipEventsGlobal, eventsperjob):
    filenames = [files[i]['path'] for i in xrange(len(files)) if skipEventsGlobal<=cummulativeHigh[i] and skipEventsGlobal+eventsperjob>cummulativeLow[i]]
    return filenames

def getSkipEventsLocal(cummulativeLow, skipEventsGlobal):
    skipEventsLocal=0
    for i in xrange(len(cummulativeLow)):
        if skipEventsGlobal>=cummulativeLow[i]:
            skipEventsLocal=skipEventsGlobal-cummulativeLow[i]
        else:
            return skipEventsLocal
    return skipEventsLocal

## Yield successive n-sized chunks from l. The last chunk may be smaller than n.
#
#@type l: iterable
#@param l: The iterable from which chunks are determiend.
#@type l: iterable
#@param l: The iterable from which chunks are determiend.
#@return A list of chunks from l.
def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def cummulative(l):
    """Yield cummulative sums."""
    n=0
    for i in xrange(len(l)):
        n+=l[i]
        yield n

class MyConfigParser(ConfigParser.SafeConfigParser):
    """Can get options() without defaults
    """
    def optionsNoDefault(self, section):
        """Return a list of option names for the given section name."""
        try:
            opts = self._sections[section].copy()
        except KeyError:
            raise NoSectionError(section)
        if '__name__' in opts:
            del opts['__name__']
        return opts.keys()
    def optionxform(self, optionstr):
        return optionstr

if __name__=="__main__":
    main()