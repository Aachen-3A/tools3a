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
import os, csv
import sys
import shutil
import time
import re
import logging
import optparse
import glob
import subprocess
import imp
import pickle
import fnmatch

#custom libs
from crabFunctions import *
import aix3adb
from  aix3adb import Aix3adbException
import dbutilscms

# some general definitions
COMMENT_CHAR = '#'
log_choices = [ 'ERROR', 'WARNING', 'INFO', 'DEBUG' ]
date = '%F %H:%M:%S'



# Write everything into a log file in the directory you are submitting from.
log = logging.getLogger( 'music_crab' )

# define some module-wide switches
runOnMC = False
runOnData = False
runOnGen = False

import FWCore.ParameterSet.Config as cms

def main():
    # get controller object for contacts with crab
    #~ controller =  CrabController(logger = log)
    controller =  CrabController(debug = 0)
    # Parse user input from command line
    (options, args ) = commandline_parsing( controller )
    # Setup logging for music_crab
    setupLogging(options)
    # adjust options for CrabController
    controller.dry_run = options.dry_run
    log.info("Starting music_crab3")

    # Read additional information from music_crab config file
    if(len(args))> 0:
        #get SampleFile from command line argument
        sampleFileName = args[0]
        SampleFileInfoDict = readSampleFile( sampleFileName ,options)
        SampleDict =  SampleFileInfoDict['sampledict']
        SampleFileInfoDict['sampleFileName'] = sampleFileName
    else:
        log.error("no config file specified.")
        sys.exit(1)

    # Check if the current commit is tagged or tag it otherwise
    if options.noTag or options.overrideTag == "noTag":
        try:
            gitTag = createTag( options )
            SampleFileInfoDict.update({'gitTag':gitTag})
        except Exception, e:
            log.error( e )
            sys.exit( 3 )
    else:
        SampleFileInfoDict.update( { 'gitTag':options.overrideTag } )

    log.info("after tag")
    log.info("tag: "+SampleFileInfoDict['gitTag'])
    log.info(controller.checkusername())

    # first check if user has permission to write to selected site
    if not controller.checkwrite():sys.exit(1)

    # extract the global tag for the used config file
    globalTag =  getGlobalTag(options)
    log.info("using global tag: %s" % globalTag)
    SampleFileInfoDict.update({'globalTag':globalTag})

    # create a connection to aix3adb if necessary
    #~ if options.db:
    dblink = createDBlink(options)
    #~ else:
        #~ dblink = None

    # create variables for job submission stats
    tasks = []
    tasks_submitted = []
    tasks_existing = []
    tasks_noSubmit = []
    tasks_successful = []

    # create crab config files and submit tasks
    for key in SampleDict.keys():
        # initalize crabTaks frob crabFunctions lib
        if runOnData:
            (name,sample,lumi_mask,lumisPerJob) = SampleDict[key]
            json_file = lumi_mask
        else:
            json_file = None
        task = CrabTask( key , initUpdate = True,
                dblink= dblink,
                globalTag = globalTag,
                skimmer_version = gitTag,
                json_file = json_file )
        if task.inDB and not options.force:
            tasks_existing.append( task )
            continue

        CrabConfig = createCrabConfig(SampleFileInfoDict,SampleDict[key],options)
        log.info("Created crab config object")
        try:
            configFileName = writeCrabConfig(key,CrabConfig,options)
        except IOError as e:
            log.error( "I/O error({0}): {1}".format(e.errno, e.strerror) )
            continue

        if not options.resubmit :
            #check if crab folder exists
            if not os.path.isdir( "crab_" + key ):
                try:
                    controller.submit( configFileName )
                except HTTPException:
                    # Try resubmit once
                    controller.submit( configFileName )
                if runOnMC:
                    submitSample2db_dump_csv( key,"success", SampleDict[ key ][1], SampleFileInfoDict, options )
                tasks_submitted.append( ( task, time.time() ) )
            # Delete folder and submit if --force option is used
            elif options.force:
                shutil.rmtree( "crab_" + key )
                controller.submit( configFileName )
                if runOnMC:
                    submitSample2db_dump_csv( key,"success", SampleDict[ key ][1], SampleFileInfoDict, options )
                tasks_submitted.append( ( task, time.time() ) )
            else:
                log.warning('Existing CRAB folder for tasks: %s not '\
                            'found (use force to submit anyway)' % key)
                #~ if runOnMC:
                    #~ submitSample2db_dump_csv( key,"fail", SampleDict[ key ][1], SampleFileInfoDict, options )
                #~ tasks_existing.append( ( task, 'EXISTING' ) )
        if options.resubmit:
            controller.resubmit( configFileName )
            tasks_submitted.append( ( task, time.time() ) )
            submitSample2db_dump_csv( key,"success", SampleDict[ key ][1], SampleFileInfoDict, options )

    # inform about existing skims
    log.info( "some samples are skipped due to existing skims" )
    for task in tasks_existing:
        log.info( '{:<12} {:<90} '.format( task.dbSkim.owner, task.name ) )

    log.info( "Submitted samples (entered to db)" )
    # check if submission was successful and add to aix3adb if --db option used
    for taskTuple in tasks_submitted:
        task = taskTuple[ 0 ]
        timeDelta = time.time() - taskTuple[ 1 ]
        # give crab3 server some time to create jobs
        log.info( "Give crab3 30 seconds for job submission %d left" % int(timeDelta) )
        while timeDelta < 30:
            time.sleep(2)
            timeDelta = time.time() - taskTuple[ 1 ]
        task.update( )
        log.info( '{:<12} {:<90} '.format( task.dbSkim.state, task.name ) )



def createCrabConfig(SampleFileInfoDict, sampleinfo,options):
    global runOnMC
    global runOnData
    global runOnGen
    # Parse user input
    from crabConfigParser import CrabConfigParser
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
    if options.transferOutputs:
        config.set( 'General', 'transferOutputs', 'True')
    if options.nolog:
        config.set( 'General', 'transferLogs', 'False' )
    else:
        config.set( 'General', 'transferLogs', 'True' )

    ### JobType section
    config.add_section('JobType')
    config.set( 'JobType', 'pluginName', 'Analysis' )
    config.set( 'JobType', 'psetName', SampleFileInfoDict['pset'] )


    if options.failureLimit:
        try:
            config.set( 'JobType', 'failureLimit', "%.2f"%float(options.failureLimit) )
        except:
            log.error('No failureLimit set. failureLimit needs float')
    # add name, datasetpath, globalTag (optional) and custom Params (optional)
    # arguments starting with '-' or '--' are not allowed, because they
    # are interpreted as cmsRun options
    paramlist = ['name=%s'%name,'datasetpath=%s'%sample]
    #~ if options.globalTag:
        #~ paramlist.extend(['--psglobalTag',options.globalTag])
    paramlist.extend(['globalTag=%s'%SampleFileInfoDict['globalTag']])
    if options.pyCfgParams:
        paramlist.append(options.pyCfgParams)
    config.set( 'JobType', 'pyCfgParams', paramlist )
    if options.inputFiles:
        config.set( 'JobType', 'inputFiles', options.inputFiles )
    if options.outputFiles:
        config.set( 'JobType', 'outputFiles', options.outputFiles )
    else:
        config.set( 'JobType', 'outputFiles', [name+".pxlio"]  )
    if options.allowUndistributedCMSSW:
        config.set( 'JobType', 'allowUndistributedCMSSW', 'True' )
    if options.maxmemory:
        try:
            config.set( 'JobType', 'maxmemory', "%d"%int(options.maxmemory ) )
        except:
            log.error('Option maxmemory not used. maxmemory needs integer')
    if options.maxJobRuntimeMin:
        try:
            config.set( 'JobType', 'maxJobRuntimeMin', "%d"%int(options.maxJobRuntimeMin ) )
        except:
            log.error('Option maxJobRuntimeMin not used. maxJobRuntimeMin needs integer')
    if options.numcores:
        try:
            config.set( 'JobType', 'numcores', "%d"%int(options.numcores ) )
        except:
            log.error('Option numcores not used. numcores needs integer')



    #####This is a dirty hack to get the JEC
    #when the JEC is in the global tag remove this part!!!
    files_to_copy=['Summer15_50nsV5_DATA.db', 'Summer15_50nsV5_MC.db', 'Summer15_25nsV5_DATA.db', 'Summer15_25nsV5_DATA_Uncertainty_AK4PF.txt', 'Summer15_25nsV5_DATA_Uncertainty_AK4PFchs.txt']
    for era in files_to_copy:
        shutil.copyfile(os.path.abspath(os.environ['CMSSW_BASE']+"/src/PxlSkimmer/Skimming/data/"+era), os.path.abspath("./"+era))
    config.set( 'JobType','inputFiles', files_to_copy)
    #up to here
    #by the way if anyone finds a way to get an abs path into a cmssw python file tell me!!! ggrrr!

    ### Data section
    config.add_section('Data')
    config.set( 'Data', 'inputDataset', sample )
    config.set( 'Data', 'inputDBS', options.inputDBS)
    config.set( 'Data', 'publication', 'False' )
    config.set( 'Data','publishDBS','phys03')
    config.set( 'Data','publishDataName',name)

    if runOnData:
        config.set( 'Data', 'splitting', 'LumiBased' )
        config.set( 'Data', 'unitsPerJob', lumisPerJob )
        if ".json" in lumi_mask:
            config.set( 'Data', 'lumiMask', os.path.join(options.lumi_dir , lumi_mask) )
        else:
            config.set( 'Data', 'lumiMask', SampleFileInfoDict['DCSOnly_json'] )
            config.set( 'Data', 'runRange', lumi_mask )
    else:
        config.set( 'Data', 'splitting', 'FileBased' )
        dasHelper = dbutilscms.dasClientHelper()
        DatasetSummary = dasHelper.getDatasetSummary( sample )
        SampleFileInfoDict.update({'dasInfos':DatasetSummary})
        try:
            #~ print DatasetSummary
            filesPerJob =  int((float(options.eventsPerJob) * int(DatasetSummary['nfiles'])) /  int(DatasetSummary['nevents']) )
            if filesPerJob < 1:
                filesPerJob = 1
        except:
            log.error("events per job needs an integer")
            sys.exit(1)
        config.set( 'Data', 'splitting', 'FileBased' )
        config.set( 'Data', 'unitsPerJob', '%d'%filesPerJob)

    if options.outLFNDirBase:
        outdir = os.path.join( '/store/user/', options.user, options.outLFNDirBase )
    else:
        outdir = os.path.join( '/store/user/', options.user, options.name, SampleFileInfoDict['gitTag'], name )
    config.set( 'Data', 'outLFNDirBase', outdir )

    ## set default for now, will change later
    config.set( 'Data', 'publishDataName', SampleFileInfoDict['globalTag'] )
    if options.publish:
        config.set( 'Data', 'publication', 'True')
        # seems to be the only valid choice at the moment
        config.set( 'Data', 'publishDBS', 'phys03')

    if options.ignoreLocality:
        config.set( 'Data', 'ignoreLocality', 'True')

    ### Site section
    config.add_section('Site')
    config.set( 'Site', 'storageSite', 'T2_DE_RWTH' )

    if options.whitelist:
        whitelists = options.whitelist.split(',')
        config.set( 'Site', 'whitelist', whitelists )

    if options.blacklist:
        blacklists = options.blacklist.split(',')
        config.set( 'Site', 'blacklist', blacklists )


    ### User section
    config.add_section('User')
    config.set( 'User', 'voGroup', 'dcms' )

    return config

def writeCrabConfig(name,config,options):
    if options.workingArea:
        runPath = options.workingArea
        if not runPath.strip()[-1] == "/":
            runPath+="/"
    else:
        runPath ="./"
    filename = '%s/crab_%s_cfg.py'%(runPath,name)
    #try:
    if os.path.exists(filename):
        raise IOError("file %s alrady exits"%(filename))
    config.writeCrabConfig(filename)
    log.info( 'created crab config file %s'%filename )
    #except  Exception as e:
        #log.error("Could not create crab config file %s"%e)
        #sys.exit()

    return filename


def getRunRange():
    return 'dummy'

def readSampleFile(filename,options):
    global runOnMC
    global runOnData
    global runOnGen
    outdict = {}
    sampledict = {}
    afterConfig = False
    existing = [] #]getExistingProcesses()
    #check if only samples matching a certain pattern should be added
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
            if line.startswith( 'maxJobRuntimeMin' ):
                generator = line.split( '=' )[1].strip()
                outdict.update({'maxJobRuntimeMin':options.maxJobRuntimeMin})
            if line.startswith( 'CME' ):
                energy = line.split( '=' )[1].strip()
                outdict.update({'energy':energy})
            if line.startswith( 'DCSOnly' ):
                DCSOnly_json = line.split( '=' )[1].strip()
                outdict.update({'DCSOnly_json':DCSOnly_json})
                # set a default
                outdict.update({'defaultUnitsPerJob':"20"})
            if line.startswith( 'defaultUnitsPerJob' ):
                defaultLumisPerJob= line.split( '=' )[1].strip()
                outdict.update({'defaultLumisPerJob':defaultLumisPerJob})
            if line.startswith( 'isData' ):
                runOnData = bool(line.split( '=' )[1].strip())
                runOnMC = not (runOnData)
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
                    lumi_mask = split_line[ 1 ].strip()
                    if len( split_line ) > 2:
                        lumisPerJob = int( split_line[ 2 ] )
                    else:
                        lumisPerJob = options.unitsPerJob
                else:
                    first_part = line
                    lumi_mask = None
                try:
                    ( sample, datasetpath ) = first_part.split( ':' )
                except:
                    log.error("could not parse line: '%s'"%(first_part))
                    sys.exit(1)
                #~ if name in existing.keys():
                    #~ log.warning( "Found existing CRAB task (%s) with process name '%s'!" % ( existing[ name ], name ) )
                    #~ if not options.submit:
                        #~ log.warning( "Skipping sample '%s'..." % sample )
                        #~ log.info( "If you want to submit it anyway, run again with '--submit'." )
                        #~ skipped[ name ] = sample
                        #~ continue

                if runOnData:
                    sampledict.update({sample : ( sample, datasetpath, lumi_mask, lumisPerJob )})
                else:
                    sampledict.update({sample : ( sample, datasetpath )})
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


def getGlobalTag(options):
    someCondition = False
    if options.globalTag:
        globalTag =  options.globalTag
    elif someCondition:
        log.info("this is a place where Global tags will be defined during the run")
    else:
        if options.DefaultGlobalTag:
            #find default globalTag
            from Configuration.AlCa.autoCond import autoCond
            if runOnData:
                #~ globalTag = cms.string( autoCond[ 'com10' ] )
                globalTag =  autoCond[ 'com10' ]
            else:
                globalTag = autoCond[ 'startup' ]
        else:
            log.info( "Global tag not specified, aborting! Specify global tag or run with --DefaultGlobalTag. " )
            quit()
    return globalTag

def createDBlink(options):


    # Create a database object.
    dblink = aix3adb.aix3adb()

    # Authorize to database.
    log.info( "Connecting to database: 'http://cern.ch/aix3adb'" )
    dblink.authorize(username = options.user)
    log.info( 'Authorized to database.' )
    return dblink

def sampleInDB(options, dblink, sample):
    try:
        if options.notInDB:
            dbSample = dblink.getMCSample( samplename )
        return True
    except Aix3adbException:
        return False

def submitSample2db_dump_csv( samplename, prefix, datasetpath, SampleFileInfoDict, options ):
    csv_filename = 'aix3adb_%s_%s.csv' % ( prefix, SampleFileInfoDict['generator'])
    if not os.path.exists( csv_filename ):
        writeheader = True
    else:
        writeheader = False
    with open(csv_filename, 'a') as outcsv:
        tempwriter = csv.writer( outcsv, delimiter=',', quotechar='"')
        mcmutil = dbutilscms.McMUtilities()
        mcmutil.readURL( datasetpath )

        if runOnMC:
            if writeheader:
                tempwriter.writerow( ['name', 'datasetpath','gitTag','Analysis','generator', 'xs', 'filtler_effi', 'filter_effi_ref', 'kfactor','energy', 'globalTag', 'CMSSW_Version', 'numEvents'] )
            line = [samplename,
                   datasetpath,\
                   SampleFileInfoDict['gitTag'],\
                   options.name,\
                   SampleFileInfoDict['generator'],\
                   mcmutil.getCrossSection(), \
                   mcmutil.getGenInfo('filter_efficiency'),\
                   'McM',\
                   1.,\
                   SampleFileInfoDict['energy'],
                   SampleFileInfoDict['globalTag'],
                   os.getenv( 'CMSSW_VERSION' ),
                   SampleFileInfoDict['dasInfos']['nevents'] ,
                   ]
        tempwriter.writerow(line)


def createTag( options ):
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
                error += "If you know what you are doing, you can use the '--noTag' option to submit anyway!"
                log.error( error )
                return False
            return True

    if not gitCheckRepo( options.ana_dir ):
        raise Exception( "git repository in '%s' dirty!" % options.ana_dir )

    # Call git to see if the commit is already tagged.
    cmd = [ 'git', 'log', '-1', '--pretty=%h%d', '--decorate=full' ]
    log.debug( 'Checking commit for tags: ' + ' '.join( cmd ) )
    proc = subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    output = proc.communicate()[0]

    # There should only be one line.
    line = output.splitlines()[0]

    success = False
    if not 'tags' in line:
        return 'noTag'
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
    tag = tags[0]
    os.chdir( workdir )

    log.info( "Using Skimmer version located in '%s'." % options.ana_dir )

    if success:
        log.info( "Using Skimmer version tagged with '%s'." % tag )

    return tag

def setupLogging(options):
    #setup logging
    format = '%(levelname)s (%(name)s) [%(asctime)s]: %(message)s'
    logging.basicConfig( level=logging._levelNames[ options.debug ], format=format, datefmt=date )
    log.setLevel(logging._levelNames[ options.debug ])
    formatter = logging.Formatter( format )
    log_file_name = 'music_crab_' + options.isodatetime + '.log'
    hdlr = logging.FileHandler( log_file_name, mode='w' )
    hdlr.setFormatter( formatter )
    log.addHandler( hdlr )
    logging.getLogger('CRAB3').propagate = False  # Avoid any CRAB message to propagate up to the handlers of the root logger.

def commandline_parsing( parsingController ):
    ##parse user input
    ####################################
    # The following options
    # were already present in muic_crab
    ####################################
    skimmer_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/PxlSkimmer/Skimming' )
    lumi_dir    = os.path.join( skimmer_dir, 'test/lumi' )
    config_dir  = os.path.join( skimmer_dir, 'test/configs' )
    parser = optparse.OptionParser( description='Submit MUSiCSkimmer jobs for all samples listed in DATASET_FILE',  usage='usage: %prog [options] DATASET_FILE' )
    music_crabOpts = optparse.OptionGroup(parser, "Options for music_crab3")

    music_crabOpts.add_option( '-c', '--config', metavar='FILE', help='Use FILE as CMSSW config file, instead of the one declared in DATASET_FILE.\n Correspond to crab3 JobType.psetName' )
    music_crabOpts.add_option( '--ana-dir', metavar='ANADIR', default=skimmer_dir,
                       help='Directory containing the analysis. If set, ANADIR is used '\
                            'as the base directory for CONFDIR and LUMIDIR. [default: '\
                            '%default]' )
    music_crabOpts.add_option( '--config-dir', metavar='CONFDIR', default=config_dir,
                       help='Directory containing CMSSW configs. Overwrites input from '\
                            'ANADIR. [default: %default]' )
    music_crabOpts.add_option( '--lumi-dir', metavar='LUMIDIR', default=lumi_dir,
                       help='Directory containing luminosity-masks. Overwrites input '\
                            'from ANADIR. [default: %default]' )
    music_crabOpts.add_option( '-o', '--only', metavar='PATTERNS', default=None,
                       help='Only submit samples matching PATTERNS (bash-like ' \
                            'patterns only, comma separated values. ' \
                            'E.g. --only QCD* ). [default: %default]' )
    music_crabOpts.add_option( '-S', '--submit', action='store_true', default=False,
                       help='Force the submission of jobs, even if a CRAB task with the given process name already exists. [default: %default]' )
    music_crabOpts.add_option( '--dry-run', action='store_true', default=False, help='Do everything except calling CRAB or registering samples to the database.' )
    music_crabOpts.add_option( '--debug', metavar='LEVEL', default='INFO', choices=log_choices,
                       help='Set the debug level. Allowed values: ' + ', '.join( log_choices ) + ' [default: %default]' )
    #~ music_crabOpts.add_option( '--noTag', action='store_true', default=False,
    music_crabOpts.add_option( '--noTag', action='store_true', default=False,help="Do not create a tag in the skimmer repository. [default: %default]" )
    music_crabOpts.add_option( '--overrideTag', default="noTag",help="Same as noTag but with custom string replacement for the tag name. [default: %default]" )

    music_crabOpts.add_option( '-D', '--db', action='store_true', default=False,
                       help="Register all datasets at the database: 'https://cern.ch/aix3adb/'. [default: %default]" )
    #///////////////////////////////
    #// new options since crab3
    #//////////////////////////////

    # new feature alternative username
    music_crabOpts.add_option( '-u', '--user', metavar='USERNAME', help='Alternative username [default: HN-username]' )
    music_crabOpts.add_option( '-g','--globalTag', help='Override globalTag from pset')
    music_crabOpts.add_option( '--DefaultGlobalTag', action='store_true', default=False, help='Allow submission without stating globalTag (use default)')
    music_crabOpts.add_option( '--resubmit',action='store_true', default=False, help='Try to resubmit jobs instead of submit')
    music_crabOpts.add_option( '--force',action='store_true', default=False, help='Delete existing crab folder and resubmit tasks')
    music_crabOpts.add_option( '--notInDB',action='store_true', default=False, help='Only submit samples if not in aix3aDB')
    parser.add_option_group(music_crabOpts)
    ###########################################
    # new  options for General section in pset
    ##########################################
    generalOpts = optparse.OptionGroup(parser, "\n SECTION General - Options for crab3 config section General ")
    generalOpts.add_option( '--workingArea',metavar='DIR',default=os.getcwd(),help='The area (full or relative path) where to create the CRAB project directory. '
                             'If the area doesn\'t exist, CRAB will try to create it using the mkdir command' \
                             ' (without -p option). Defaults to the current working directory.'       )
    generalOpts.add_option( '-t', '--transferOutputs', action='store_true',default=True,help="Whether to transfer the output to the storage site"
                                                    'or leave it at the runtime site. (Not transferring the output might'\
                                                    ' be useful for example to avoid filling up the storage area with'\
                                                    ' useless files when the user is just doing some test.) ' )
    generalOpts.add_option( '--nolog', action='store_true',default=False,help='Whether or not to copy the cmsRun stdout /'\
                                                    'stderr to the storage site. If set to False, the last 1 MB'\
                                                    ' of each job are still available through the monitoring in '\
                                                    'the job logs files and the full logs can be retrieved from the runtime site with')
    generalOpts.add_option( '--failureLimit', help='The number of jobs that may fail permanently before the entire task is cancelled. '\
                                                'Defaults to 10% of the jobs in the task. ')
    parser.add_option_group( generalOpts )
    ########################################
    # new options for JobType in pset
    ########################################
    jobTypeOpts = optparse.OptionGroup(parser, "\n SECTION JobType - Options for crab3 config section JobType ")
    jobTypeOpts.add_option('--pyCfgParams',default =None, help="List of parameters to pass to the CMSSW parameter-set configuration file, as explained here. For example, if set to "\
    "[\'myOption\',\'param1=value1\',\'param2=value2\'], then the jobs will execute cmsRun JobType.psetName myOption param1=value1 param2=value2. ")
    jobTypeOpts.add_option('--inputFiles',help='List of private input files needed by the jobs. ')
    jobTypeOpts.add_option('--outputFiles',help='List of output files that need to be collected, besides those already specified in the output'\
                                                ' modules or TFileService of the CMSSW parameter-set configuration file.  ')
    jobTypeOpts.add_option( '--allowUndistributedCMSSW', action='store_true', default=False,
                       help='Allow using a CMSSW release potentially not available at sites. [default: %default]' )
    jobTypeOpts.add_option('--maxmemory',help=' Maximum amount of memory (in MB) a job is allowed to use. ')
    jobTypeOpts.add_option('--maxJobRuntimeMin',help="Overwrite the maxJobRuntimeMin if present in samplefile [default: 72] (set by crab)" )
    jobTypeOpts.add_option('--numcores', help="Number of requested cores per job. [default: 1]" )
    jobTypeOpts.add_option('--priority', help='Task priority among the user\'s own tasks. Higher priority tasks will be processed before lower priority.'\
                                                    ' Two tasks of equal priority will have their jobs start in an undefined order. The first five jobs in a'\
                                                    ' task are given a priority boost of 10. [default  10] ' )
    jobTypeOpts.add_option('-n','--name', default="PxlSkim" ,
                      help="Name for this analysis run (E.g. Skim Campaign Name) [default: %default]")
    jobTypeOpts.add_option('--publish',default = False,help="Switch to turn on publication of a processed sample [default: %default]")
    parser.add_option_group( jobTypeOpts )

    ####################################
    # new options for Data in pset
    ####################################
    dataOpts = optparse.OptionGroup(parser, "\n SECTION Data - Options for crab3 config section Data")
    dataOpts.add_option('--eventsPerJob',default=10000,help="Number of Events per Job for MC [default: %default]")
    dataOpts.add_option( '-d', '--inputDBS', metavar='inputDBS',default='global', help='Set DBS instance URL to use (e.g. for privately produced samples published in a local DBS).' )
    parser.add_option_group( dataOpts )

    ####################################
    # new options for Site in pset
    ####################################
    siteOpts = optparse.OptionGroup(parser, "\n SECTION Site - Options for crab3 config section Site ")
    siteOpts.add_option( '--outLFNDirBase', metavar='OUTLFNDIRBASE', default=None,
                       help="Set dCache directory for crab output to '/store/user/USERNAME/"\
                            "OUTLFNDIRBASE'. [default: 'store/user/USERNAME/PxlSkim/git-tag/']" )
    siteOpts.add_option( '-w', '--whitelist', metavar='SITES', help="Whitelist SITES in a comma separated list, e.g. 'T2_DE_RWTH,T2_US_Purdue'." )
    siteOpts.add_option( '-b', '--blacklist', metavar='SITES', help='Blacklist SITES in addition to T0,T1 separated by comma, e.g. T2_DE_RWTH,T2_US_Purdue  ' )
    siteOpts.add_option('--unitsPerJob',default="20",help="Suggests (but not impose) how many units (i.e. files, luminosity sections or events [1] -depending on the splitting mode-) to include in each job.  [default: %default]")
    siteOpts.add_option('--ignoreLocality',action='store_true',default=False,help="Set to True to allow jobs to run at any site,"
                                                        "regardless of whether the dataset is located at that site or not. "\
                                                        "Remote file access is done using Xrootd. The parameters Site.whitelist"\
                                                        " and Site.blacklist are still respected. This parameter is useful to allow "\
                                                        "jobs to run on other sites when for example a dataset is available on only one "\
                                                        "or a few sites which are very busy with jobs. It is strongly recommended "\
                                                        "to provide a whitelist of sites physically close to the input dataset's host "\
                                                        "site. This helps reduce file access latency. [default: %default]" )
    parser.add_option_group( siteOpts )

    # we need to add the parser options from other modules
    #get crab command line options
    parsingController.commandlineOptions(parser)

    (options, args ) = parser.parse_args()
    now = datetime.datetime.now()
    isodatetime = now.strftime( "%Y-%m-%d_%H.%M.%S" )
    options.isodatetime = isodatetime

    # check if user has valid proxy
    import gridFunctions
    gridFunctions.checkAndRenewVomsProxy()

    #get current user HNname
    if not options.user:
        options.user = parsingController.checkusername()

    # Set CONFDIR and LUMIDIR relative to ANADIR if ANADIR is set
    # but the other two are not.
    if not options.ana_dir == skimmer_dir:
        # ANADIR was set (it is not at its default value).
        if options.lumi_dir == lumi_dir:
            # LUMIDIR was not set (it is at its default value).
            options.lumi_dir = os.path.join( options.ana_dir, 'test/lumi' )
        if options.config_dir == config_dir:
            # CONFDIR was not set (it is at its default value).
            options.config_dir = os.path.join( options.ana_dir, 'test/configs' )

    return (options, args )

if __name__ == '__main__':
    main()

