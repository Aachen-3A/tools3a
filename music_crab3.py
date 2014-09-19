#!/usr/bin/env python
## @package music_crab3
# MUSiC wrapper for crab3
#
# music_crab is a wrapper script that has been developed
# to submit the large number of tasks (more or less) automatically, 
# needed to get all data and MC. The script automatically creates CRAB 
# config files according to the dataset file provided. 
# crab3 rebuilds the previous crab2 functionality and relies on the work
# in the previous version
# @author Tobias Pook

import datetime
import os
import sys
import time
import re
import logging
import optparse
import glob
import subprocess


# some general definitions
COMMENT_CHAR = '#'
log_choices = [ 'ERROR', 'WARNING', 'INFO', 'DEBUG' ]
date = '%F %H:%M:%S'

skimmer_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject' )
lumi_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/lumi' )
config_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/configs' )

# Parse user input    
from crabConfigParser import CrabConfigParser
parser = optparse.OptionParser( description='Submit MUSiCSkimmer jobs for all samples listed in DATASET_FILE',  usage='usage: %prog [options] DATASET_FILE' )
parser.add_option( '-c', '--config', metavar='FILE', help='Use FILE as CMSSW config file, instead of the one declared in DATASET_FILE' )
parser.add_option( '--config-dir', metavar='DIR', default=config_dir, help='Directory containing CMSSW configs [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/configs]' )
parser.add_option( '--lumi-dir', metavar='DIR', default=lumi_dir, help='Directory containing luminosity-masks [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/lumi]' )
parser.add_option( '-o', '--only', metavar='PATTERNS', default=None,
                   help='Only submit samples matching PATTERNS (bash-like ' \
                        'patterns only, comma separated values. ' \
                        'E.g. --only QCD* ). [default: %default]' )
parser.add_option( '-S', '--submit', action='store_true', default=False,
                   help='Force the submission of jobs, even if a CRAB task with the given process name already exists. [default: %default]' )
parser.add_option( '-d', '--dbsUrl', metavar='DBSURL',default='global', help='Set DBS instance URL to use (e.g. for privately produced samples published in a local DBS).' )
parser.add_option( '--dry-run', action='store_true', default=False, help='Do everything except calling CRAB or registering samples to the database.' )
parser.add_option( '--debug', metavar='LEVEL', default='INFO', choices=log_choices,
                   help='Set the debug level. Allowed values: ' + ', '.join( log_choices ) + ' [default: %default]' )
parser.add_option( '--no-tag', action='store_true', default=False,
                   help="Do not create a tag in the skimmer repository. [default: %default]" )
parser.add_option( '-b', '--blacklist', metavar='SITES', help='Blacklist SITES in addition to T0,T1 separated by comma, e.g. T2_DE_RWTH,T2_US_Purdue  ' )
##              
# new options since crab3
##
#new  options for General section
parser.add_option( '--workingArea',metavar='DIR',default=None,help='The area (full or relative path) where to create the CRAB project directory. ' 
                         'If the area doesn\'t exist, CRAB will try to create it using the mkdir command' \
                         ' (without -p option). Defaults to the current working directory.'       )  
parser.add_option( '-t', '--transferOutput', action='store_true',default=True,help="Whether to transfer the output to the storage site"
                                                'or leave it at the runtime site. (Not transferring the output might'\
                                                ' be useful for example to avoid filling up the storage area with'\
                                                ' useless files when the user is just doing some test.) ' )  
parser.add_option( '--log', action='store_true',default=False,help='Whether or not to copy the cmsRun stdout /'\
                                                'stderr to the storage site. If set to False, the last 1 MB'\
                                                ' of each job are still available through the monitoring in '\
                                                'the job logs files and the full logs can be retrieved from the runtime site with') 
parser.add_option( '--failureLimit', help='The number of jobs that may fail permanently before the entire task is cancelled. '\
                                            'Defaults to 10% of the jobs in the task. ')
# new feature alternative username
parser.add_option( '-u','--user', help='Alternative username if local user name is not equal to HN-username [default is local name]')
                                    
# new options for JobType       
parser.add_option('--pyCfgParams',default =None, help="List of parameters to pass to the CMSSW parameter-set configuration file, as explained here. For example, if set to "\
"[\'myOption\',\'param1=value1\',\'param2=value2\'], then the jobs will execute cmsRun JobType.psetName myOption param1=value1 param2=value2. ")
parser.add_option('--inputFiles',help='List of private input files needed by the jobs. ')
parser.add_option('--outputFiles',help='List of output files that need to be collected, besides those already specified in the output'\
                                            ' modules or TFileService of the CMSSW parameter-set configuration file.  ')
parser.add_option('--allowNonProductionCMSSW', action='store_true',default=False,help='Set to True to allow using a CMSSW release possibly not available at sites. Defaults to False. ') 
parser.add_option('--maxmemory',help=' Maximum amount of memory (in MB) a job is allowed to use. ')
parser.add_option('--maxjobruntime', default=72,help="Overwrite the maxjobruntime if present in samplefile [default: 72]" ) 
parser.add_option('--numcores', help="Number of requested cores per job. [default: 1]" ) 
parser.add_option('--priority', help='Task priority among the user\'s own tasks. Higher priority tasks will be processed before lower priority.'\
                                                ' Two tasks of equal priority will have their jobs start in an undefined order. The first five jobs in a'\
                                                ' task are given a priority boost of 10. [default  10] ' ) 
parser.add_option('-n','--name',help="Music Process name [default: /Music/{current_date}/]")
parser.add_option('--publish',default = False,help="Switch to turn on publication of a processed sample [default: False]")

#new options for Data
parser.add_option('--eventsPerJob',default=10000,help="Number of Events per Job for MC [default: 10.000]")
parser.add_option('--ignoreLocality',action='store_true',default=False,help="Set to True to allow jobs to run at any site,"
                                                    "regardless of whether the dataset is located at that site or not. "\
                                                    "Remote file access is done using Xrootd. The parameters Site.whitelist"\
                                                    " and Site.blacklist are still respected. This parameter is useful to allow "\
                                                    "jobs to run on other sites when for example a dataset is available on only one"\
                                                    " or a few sites which are very busy with jobs. Defaults to False. ")

(options, args ) = parser.parse_args()

now = datetime.datetime.now()
isodatetime = now.strftime( "%Y-%m-%d_%H.%M.%S" )
options.isodatetime = isodatetime

# Write everything into a log file in the directory you are submitting from.
log = logging.getLogger( 'music_crab' )

formatter = logging.Formatter( format )
log_file_name = 'music_crab_' + options.isodatetime + '.log'
hdlr = logging.FileHandler( log_file_name, mode='w' )
hdlr.setFormatter( formatter )
log.addHandler( hdlr )

#setup logging
format = '%(levelname)s (%(name)s) [%(asctime)s]: %(message)s'
logging.basicConfig( level=logging._levelNames[ options.debug ], format=format, datefmt=date )   


# define some module-wide switches
runOnMC = False
runOnData = False
runOnGen = False


#get current user HNname
if options.user:
    user = options.user
else:
    user = os.getenv( 'LOGNAME' )

if options.blacklist:
    options.blacklist = 'T0,T1,'+options.blacklist
else:
    options.blacklist = 'T0,T1'

def main():

    
    if(len(args))> 0:
        SampleFileInfoDict = readSampleFile( args[0] )
        SampleDict =  SampleFileInfoDict['sampledict']
    else:
        #~ print "no config file specified"
        log.error("no config file specified.")
        sys.exit(1)
    #~ print SampleFileInfoDict
    
    # Check if the current commit is tagged or tag it otherwise
    #~ if not options.no_tag:
        #~ try:
            #~ gitTag = createTag( options, skimmer_dir )
        #~ except Exception, e:
            #~ log.error( e )
            #~ sys.exit( 3 )
    
    # first check if user has permission to write to selected site
    #~ if not crab_checkwrite("T2_DE_RWTH"):
        #~ log.error( "music_crab3 stopped due to site permission error on site: %s"%site )
        #~ sys.exit(1)
    #~ log.info("after tag")
        
    for key in SampleDict.keys():
        writeSampleConfig(SampleFileInfoDict,SampleDict[key])
        #~ crab_submit(key)
    
def writeSampleConfig(SampleFileInfoDict, sampleinfo):
    global runOnMC 
    global runOnData
    global runOnGen 
    config = CrabConfigParser()
    if runOnData:
        (name,sample,lumi_mask,lumisPerJob) = sampleinfo
    else:
        (name,sample) = sampleinfo
    
    ### General section
    config.add_section('General')
    config.set( 'General', 'requestName', name )
    if options.workingArea:
        config.set( 'General', 'workArea', options.workingArea )
    if options.transferOutput:
        config.set( 'General', 'transferOutput', 'True')
    if options.log:
        config.set( 'General', 'saveLogs', 'True' )
    
    ### JobType section
    config.add_section('JobType')
    config.set( 'JobType', 'pluginName', 'Analysis' )
    config.set( 'JobType', 'psetName', SampleFileInfoDict['pset'] )
    if options.failureLimit:
        try:
            config.set( 'JobType', 'failureLimit', "%.2f"%float(options.failureLimit) )
        except:
            log.error('No failureLimit set. failureLimit needs float')
    if options.pyCfgParams:
       config.set( 'JobType', 'pyCfgParams', options.pyCfgParams )   
    if options.inputFiles:
        config.set( 'JobType', 'inputFiles', options.inputFiles ) 
    if options.outputFiles:
        config.set( 'JobType', 'outputFiles', options.outputFiles ) 
    if options.allowNonProductionCMSSW:
        config.set( 'JobType', 'allowNonProductionCMSSW', 'True' ) 
    if options.maxmemory:
        try:
            config.set( 'JobType', 'maxmemory', "%d"%int(options.maxmemory ) )
        except:
            log.error('Option maxmemory not used. maxmemory needs integer')
    if options.maxjobruntime:
        try:
            config.set( 'JobType', 'maxjobruntime', "%d"%int(options.maxjobruntime ) )
        except:
            log.error('Option maxjobruntime not used. maxjobruntime needs integer')
    if options.numcores:
        try:
            config.set( 'JobType', 'numcores', "%d"%int(options.numcores ) )
        except:
            log.error('Option numcores not used. numcores needs integer')
    
    
    ### Data section        
    config.add_section('Data')
    config.set( 'Data', 'inputDataset', sample )
    config.set( 'Data', 'dbsUrl', options.dbsUrl)
    config.set( 'Data', 'publication', 'False' )
    config.set( 'Data','publishDbsUrl','phys03')
    config.set( 'Data','publishDataName',name)
    
    if runOnData:
        config.set( 'Data', 'splitting', 'LumiBased' )
        config.set( 'Data', 'unitsPerJob', lumisPerJob )
        if ".json" in lumi_mask:
            config.set( 'Data', 'lumiMask', lumi_dir + lumi_mask )
        else:
            config.set( 'Data', 'lumiMask', SampleFileInfoDict['DCSOnly_json'] )
            config.set( 'Data', 'runRange', lumi_mask )
    else:
        config.set( 'Data', 'splitting', 'FileBased' )
        DatasetSummary = getDatasetSummary(sample)
        try:
            print DatasetSummary
            filesPerJob =  int((float(options.eventsPerJob) * int(DatasetSummary['numFiles'])) /  int(DatasetSummary['numEvents']) )
            if filesPerJob < 1:
                filesPerJob = 1
        except:
            log.error("events per job needs an integer")
            sys.exit(1)
        config.set( 'Data', 'splitting', 'FileBased' )
        config.set( 'Data', 'unitsPerJob', '%d'%filesPerJob)
    
    config.set( 'Data', 'outlfn', "/store/user/%s/MUSiC/%s/%s/"%(user,datetime.date.today().isoformat(),name))
    
    ## set default for now, will change later
    config.set( 'Data', 'publishDataName', 'dummy' )
    if options.publish:
        config.set( 'Data', 'publication', 'True')
        # seems to be the only valid choice at the moment
        config.set( 'Data', 'publishDbsUrl', 'phys03')
    
    if options.ignoreLocality:
        config.set( 'Data', 'ignoreLocality', 'True')
        
    ### Site section    
    config.add_section('Site')
    config.set( 'Site', 'storageSite', 'T2_DE_RWTH' )
    
    if options.blacklist:
        config.set( 'Site', 'blacklist', options.blacklist )
    
    if options.workingArea:
        runPath = options.workingArea
        if not runPath.strip()[-1] == "/":
            runPath+="/"
    else:
        runPath ="./"
        
    filename = '%s/crab_%s_cfg.py'%(runPath,name)
    print "created crab config file %s"%filename
    config.writeCrabConfig(filename)
    log.info( 'created crab config file %s'%filename )
    
def crab_checkwrite(site):    
    cmd = ['crab checkwrite --site %s'%site ]
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
    (stringlist,string_err) = p.communicate()
    if len(string_err) > 0:
        log.error( "The crab checkwrite command failed for site: %s"%site )
        return False
    else:
        return True
        
def crab_submit(name):
    cmd = 'crab submit %s_cfg.py'%name
    if options.dry-run:
        log.info( 'Dry-run: Created config file. crab command would have been: ', cmd )
    else:
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
        (stringlist,string_err) = p.communicate()

def getRunRange():
    return 'dummy'

def readSampleFile(filename):
    global runOnMC 
    global runOnData
    global runOnGen 
    outdict = {}
    sampledict = {}
    afterConfig = False
    existing = getExistingProcesses()
    
    #check if only samples mathing a certain paatern should be added
    if options.only:
    # 'PATTERNS' should be a comma separated list of strings.
    # Remove all empty patterns and those containing only whitespaces.
        options.only = options.only.split( ',' )
        options.only = filter( lambda x: x.strip(), options.only )
        log.debug( "Only submitting samples matching patterns '%s'." % ','.join( options.only ) )
    
    with open(filename,'rb') as sample_file:
        for line in sample_file:
            line = line.strip()
            if not line or line.startswith( COMMENT_CHAR ): continue
            if COMMENT_CHAR in line:
                line, comment = line.split( COMMENT_CHAR, 1 )
            if line.startswith( 'generator' ):
                generator = line.split( '=' )[1].strip()
                outdict.update({'generator':generator})
                runOnMC = True
            if line.startswith( 'maxjobruntime' ):
                generator = line.split( '=' )[1].strip()
                outdict.update({'maxjobruntime':options.maxjobruntime})
                runOnMC = True
            if line.startswith( 'CME' ):
                energy = line.split( '=' )[1].strip()
                outdict.update({'energy':energy})
            if line.startswith( 'DCSOnly' ):
                DCSOnly_json = line.split( '=' )[1].strip()
                outdict.update({'DCSOnly_json':DCSOnly_json})
                # set a default
                outdict.update({'defaultUnitsPerJob':"20"})
                runOnData = True
            if line.startswith( 'defaultUnitsPerJob' ):
                defaultLumisPerJob= line.split( '=' )[1].strip()
                outdict.update({'defaultLumisPerJob':defaultLumisPerJob})
            if line.startswith( 'config' ):
                (junk,pset) = line.split( '=' )
                pset = os.path.join( options.config_dir, pset.strip() )
                outdict.update({'pset':pset})
                afterConfig = True
            if afterConfig and not "config" in line: 
                
                skip = False
                if options.only:
                    for pattern in options.only:
                        if fnmatch.fnmatchcase( line, pattern ):
                    # One of the patterns does match, no need to continue.
                            break
                        else:
                            # Found none matching, skip submission.
                            skip = True
                if skip:
                    log.debug( "Skipping sample '%s' (not matching any patterns)." % line )
                    continue
                #lumi-mask and lumis-per-job can be specified in the command line
                if ';' in line:
                    split_line = line.split( ';' )
                    first_part = split_line[ 0 ]
                    if ".json" in split_line[1]:
                        lumi_mask = os.path.join( options.lumi_dir, split_line[ 1 ] )
                    else:
                        lumi_mask = split_line[1].strip()
                    if len( split_line ) > 2:
                        lumisPerJob = int( split_line[ 2 ] )
                    else:
                        lumisPerJob = lumisPerJob
                else:
                    first_part = line
                    lumi_mask = None

                (name,sample) = first_part.split( ':' )
                if name in existing.keys():
                    log.warning( "Found existing CRAB task (%s) with process name '%s'!" % ( existing[ name ], name ) )
                    if not options.submit:
                        log.warning( "Skipping sample '%s'..." % sample )
                        log.info( "If you want to submit it anyway, run again with '--submit'." )
                        skipped[ name ] = sample
                        continue
                        
                if runOnData:
                    sampledict.update({name:(name,sample,lumi_mask,lumisPerJob)})
                else:
                    sampledict.update({name:(name,sample)})
    # add sampledict to outdict
    outdict.update({'sampledict':sampledict})
    # overwrite pset if --config option is used
    if options.config:
        pset = options.config
        outdict.update({'pset':pset})
        
    if 'pset' in outdict:
        return outdict 
    else:
        log.error( 'No CMSSW config file specified!' )
        log.error( 'Either add it to the sample file or add it to the command line.' )
        sys.exit(1)
            
def getExistingProcesses():
    workdir = os.getcwd()

    checkDirs = [
        workdir,
        os.path.join( workdir, 'done' ),
        ]

    processes = {}
    for dir in checkDirs:
        log.debug( "Checking directory '%s' for existing CRAB tasks..." % dir )
        for task in glob.glob( os.path.join( dir, 'crab_?_*' ) ):
            log.debug( "Found existing CRAB task '%s'." % task )
            config = pickle.load( open( os.path.join( task, 'job', 'CMSSW.py.pkl' ) ) )

            process = config.Skimmer.Process.pythonValue().strip( "' " )
            log.debug( "Process name is '%s'." % process )

            processes[ process ] = task

    return processes

def getDatasetSummary( dataset ):
    from dbs.apis.dbsClient import DbsApi
    dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    dbsApi = DbsApi( url = dbsUrl )
    datasetSummary = {}
    datasetBlocks = dbsApi.listBlockSummaries( dataset = dataset )
    datasetSummary.update({"numEvents":sum( [ block['num_event'] for block in datasetBlocks ] )} )
    datasetSummary.update({"numFiles":sum( [ block['num_file'] for block in datasetBlocks ] )} )
    datasetSummary.update({"totalFileSize":sum( [ block['file_size'] for block in datasetBlocks ] ) })
    return datasetSummary

def createTag( options, skimmer_dir ):
    # Save the current working directory to get back here later.
    workdir = os.getcwd()

    def gitCheckRepo( skimmer_dir ):
        os.chdir( skimmer_dir )

        cmd = [ 'git', 'diff-index', '--name-only', 'HEAD' , '--' ]
        proc = subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
        output = proc.communicate()[0]
        retcode = int( proc.returncode )

        if retcode != 0:
            log.warning( "Failed command: " + ' '.join( cmd ) )
            log.debug( 'Full output:\n' + output )
            return False
        else:
            if output:
                error  = "Repository in '%s' has uncommitted changes.\n" % skimmer_dir
                error += "It is strongly recommended that you commit your changes and rerun this script.\n"
                error += "If you know what you are doing, you can use the '--no-tag' option to submit anyway!"
                log.error( error )
                return False
            return True

    def gitTag( tag, skimmer_dir ):
        os.chdir( skimmer_dir )

        log.info( "Creating tag in '%s'" % skimmer_dir )

        message = "'Auto-tagged by music_crab!'"
        cmd = [ 'git', 'tag', '-a', tag, '-m', message ]
        log.debug( 'Calling git command: ' + ' '.join( cmd ) )
        retcode = subprocess.call( cmd )

        if retcode != 0:
            log.warning( "Failed command: " + ' '.join( cmd ) )
            return False
        else:
            log.info( "Created git tag '%s' in '%s'" % ( tag, skimmer_dir ) )
            return True

    if not gitCheckRepo( skimmer_dir ):
        raise Exception( "git repository in '%s' dirty!" % skimmer_dir )

    # The tag is always the date and time with a 'v' prefixed.
    tag = 'v' + options.isodatetime

    # Call git to see if the commit is already tagged.
    cmd = [ 'git', 'log', '-1', '--pretty=%h%d', '--decorate=full' ]
    log.debug( 'Checking commit for tags: ' + ' '.join( cmd ) )
    proc = subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    output = proc.communicate()[0]

    # There should only be one line.
    line = output.splitlines()[0]

    success = False
    if not 'tags' in line:
        success = gitTag( tag, skimmer_dir )
    else:
        commit, line = line.split( ' ', 1 )
        info = line.split( ',' )
        head = info[0].strip( '() ' )
        branch = info[-1].strip( '() ' )

        del info[0]
        del info[-1]

        tags = []

        for part in info:
            if 'tags' in part:
                tags.append( part.strip( '() ' ).split( '/' )[-1] )

        log.debug( "In commit '" + commit + "', found tags: " + ', '.join( tags ) )

        pattern = r'v\d\d\d\d-\d\d-\d\d_\d\d.\d\d.\d\d'
        for t in tags:
            matched = re.match( pattern, t )
            if matched:
                log.info( "Found tag '%s', not creating a new one!" % t )
                tag = t
                break
        else:
            success = gitTag( tag, skimmer_dir )

    os.chdir( workdir )

    log.info( "Using Skimmer version located in '%s'." % skimmer_dir )

    if success:
        log.info( "Using Skimmer version tagged with '%s'." % tag )

    return tag

if __name__ == '__main__':
  main()

