#!/usr/bin/env python

import datetime
import glob
import os
import sys
import subprocess
import time
import imp
import pickle
import optparse
import re
import logging
import ConfigParser

log = logging.getLogger( 'music_crab' )

def getNumberOfEvents( dataset ):
    query = 'find sum(block.numevents) where dataset = ' + dataset
    dbs_cmd = [ 'dbs', 'search', '--query', query ]
    dbs_output = subprocess.Popen( dbs_cmd, stdout = subprocess.PIPE ).communicate()[0]

    for i, line in enumerate( dbs_output.splitlines() ):
        if 'sum_block.numevents' in line:
            numEvents = dbs_output.splitlines()[ i + 2 ]
            if numEvents.isdigit():
                return int( numEvents )
            else:
                return 0


def parseCrabOutput( output ):
    totalNumJobs = 0
    workDir = ''

    log.debug( 'Full output:\n' + output )

    for line in output.splitlines():
        if 'working directory' in line:
            workDir = line.split()[2]
            log.debug( "Created working directory '%s'." % workDir )

        if 'Total number of created jobs:' in line:
            totalNumJobs = int( line.split()[-1] )
            log.debug( "Created a total of %i jobs." % totalNumJobs )

        if line.startswith( 'crab:' ):
            if 'Total of' in line and 'jobs created' in line:
                totalNumJobs = int( line.split()[3] )
                log.debug( "Created a total of %i jobs." % totalNumJobs )

    return workDir, totalNumJobs


def crabSubmit( options, workDir, first=None, last=None ):
    cmd = [ 'crab' ]

    if first != None and last != None:
        range = '%i-%i' % ( first, last )
        cmd += [ '-submit', range ]
    else:
        cmd += [ '-submit' ]

    if workDir:
        cmd += [ '-c', workDir ]

    if options.dry_run:
        log.info( 'Dry-run: crab command would have been: ' + ' '.join( cmd ) )
    else:
        log.info( 'Done and submitting...' )
        subprocess.call( cmd )


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


skimmer_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject' )
lumi_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/lumi' )
config_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/configs' )

COMMENT_CHAR = '#'

log_choices = [ 'ERROR', 'WARNING', 'INFO', 'DEBUG' ]
parser = optparse.OptionParser( description='Submit MUSiCSkimmer jobs for all samples listed in DATASET_FILE',  usage='usage: %prog [options] DATASET_FILE' )
parser.add_option( '-c', '--config', metavar='FILE', help='Use FILE as CMSSW config file, instead of the one declared in DATASET_FILE' )
parser.add_option( '--config-dir', metavar='DIR', default=config_dir, help='Directory containing CMSSW configs [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/configs]' )
parser.add_option( '-n', '--name', metavar='NAME', help='Output will be written in store/user/{your_name}/NAME/{short_dataset_name} [default: MUSiC/{current_date}]' )
parser.add_option( '-r', '--runs', metavar='RUNS', help='Only analyze the given runs (comma separated list)' )
parser.add_option( '-l', '--lumimask', metavar='FILE', help='Use JSON file FILE as lumi mask' )
parser.add_option( '--lumi-dir', metavar='DIR', default=lumi_dir, help='Directory containing luminosity-masks [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/lumi]' )
parser.add_option( '-t', '--total', metavar='NUMBER', default='-1', help='Only analyze NUMBER events/lumis [default: %default; means all]' )
parser.add_option( '-j', '--perJob', metavar='NUMBER', default='unset',
                   help="Analyze NUMBER events per job (MC) or lumis per job (data) [default: 50000 events or 20 lumis]. Use '--perJob=auto' for an automatic number that implies submitting < 500 jobs when running without the server." )
parser.add_option( '-s', '--server', action='store_true', default=False, help='Use the CRAB server [default: %default]' )
parser.add_option( '-g', '--scheduler', metavar='SCHEDULER', default='remoteGlidein',
                   help="Use scheduler SCHEDULER. ('--scheduler=glidein' implies '--server', '--scheduler=remoteGlidein' implies server cannot be used). \
In case '--scheduler=remoteGlidein', the number of events/lumi per job is adjusted, so that the total number of jobs in each task does not exceed 5000. [default: %default]" )
parser.add_option( '-b', '--blacklist', metavar='SITES', help='Blacklist SITES in addition to T0,T1' )
parser.add_option( '-d', '--dbs-url', metavar='DBSURL', help='Set DBS instance URL to use (e.g. for privately produced samples published in a local DBS).' )
parser.add_option( '--dry-run', action='store_true', default=False, help='Do everything except calling CRAB' )
parser.add_option( '-m', '--no-more-time', action='store_false', default=False,
                   help="By default, the limit on the wall clock time and cpu time will be increased to 72 h with help of config files (this is a workaround for 'remoteGlidein' only). Use this option, if you don't want this behaviour [default: %default]!" )
parser.add_option( '--debug', metavar='LEVEL', default='INFO', choices=log_choices,
                   help='Set the debug level. Allowed values: ' + ', '.join( log_choices ) + ' [default: %default]' )
parser.add_option( '--no-tag', action='store_true', default=False,
                   help="Do not create a tag in the skimmer repository. [default: %default]" )
parser.add_option( '-S', '--submit', action='store_true', default=False,
                   help='Force the submission of jobs, even if a CRAB task with the given process name already exists. [default: %default]' )

(options, args ) = parser.parse_args()

now = datetime.datetime.now()
isodatetime = now.strftime( "%Y-%m-%d_%H.%M.%S" )
options.isodatetime = isodatetime

date = '%F %H:%M:%S'
format = '%(levelname)s (%(name)s) [%(asctime)s]: %(message)s'
logging.basicConfig( level=logging._levelNames[ options.debug ], format=format, datefmt=date )

# Write everything into a log file in the directory you are submitting from.
formatter = logging.Formatter( format )
log_file_name = 'music_crab_' + options.isodatetime + '.log'
hdlr = logging.FileHandler( log_file_name, mode='w' )
hdlr.setFormatter( formatter )
log.addHandler( hdlr )

if len( args ) < 1:
    parser.error( 'DATASET_FILE required' )

if options.scheduler == 'glidein':
    options.server = True
if options.scheduler == 'remoteGlidein' and options.server:
    parser.error( 'You cannot run remoteGlidein with the server. Aborting!' )

if options.name:
    outname = options.name
    while outname.startswith( '/' ):
        outname = outname[ 1: ]
    while outname.endswith( '/' ):
        outname = outname[ :-1 ]
else:
    outname = 'MUSiC/'+datetime.date.today().isoformat()
outname += '/'

lumisPerJob = 20
eventsPerJob = 50000
maxNumJobs = 500
maxNumJobsRG = 5000

if options.perJob != 'unset' and options.perJob != 'auto':
    lumisPerJob = options.perJob
    eventsPerJob = options.perJob

if options.blacklist:
    options.blacklist = 'T0,T1,'+options.blacklist
else:
    options.blacklist = 'T0,T1'
    

sample_file = open( args[0] )
if options.config:
    pset = options.config
else:
    for line in sample_file:
        line = line.strip()
        if not line or line.startswith( COMMENT_CHAR ): continue
        if COMMENT_CHAR in line:
            line, comment = line.split( COMMENT_CHAR, 1 )
        if line.startswith( 'config' ):
            (junk,pset) = line.split( '=' )
            pset = os.path.join( options.config_dir, pset.strip() )
            break
    else:
        log.error( 'No CMSSW config file specified!' )
        log.error( 'Either add it to the sample file or add it to the command line.' )
        sys.exit(1)

log.info( "Reading config: '%s'" % pset )
file = open( pset )
cfo = imp.load_source("pycfg", pset, file )
del file
process = cfo.process
del cfo


#check if the user is not in dcms
user = os.getenv( 'LOGNAME' )
dcms_blacklist = [ 'malhotra' ]
allow_dcms = not user in dcms_blacklist

if not options.no_tag:
    try:
        createTag( options, skimmer_dir )
    except Exception, e:
        log.error( e )
        sys.exit( 3 )


# Before submitting anything, check existing jobs:
existing = getExistingProcesses()

skipped = {}
for line in sample_file:
    line = line.strip()
    if not line or line.startswith( COMMENT_CHAR ): continue
    if COMMENT_CHAR in line:
        line, comment = line.split( COMMENT_CHAR, 1 )

    #lumi-mask and lumis-per-job can be specified in the command line
    if ';' in line:
        split_line = line.split( ';' )
        first_part = split_line[ 0 ]
        lumi_mask = os.path.join( options.lumi_dir, split_line[ 1 ] )
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

    setJobsNumber = False

    if options.perJob == 'auto' and not options.server:
        numEvents = getNumberOfEvents( sample )

        if numEvents / maxNumJobs > eventsPerJob:
            setJobsNumber = True

    elif options.scheduler == 'remoteGlidein':
        numEvents = getNumberOfEvents( sample )

        if numEvents / maxNumJobsRG > eventsPerJob:
            setJobsNumber = True

    if options.scheduler == 'remoteGlidein' and not options.no_more_time:
        # The filenames are NOT optional!
        # Don't change them, unless you know exactly what you do.
        wall_filename = 'wallLimit'
        cpu_filename  = 'cpuLimit'

        # Set max. time to 3*24*60*60 sec = 3 days
        max_time = 259200.0
        if os.path.isfile( wall_filename ):
            log.info( "Using file '%s'..." % wall_filename )
        else:
            log.info( "Generating file '%s'..." % wall_filename )
            wall_file = open( wall_filename, 'wb' )
            wall_file.write( str( max_time ) )

        if os.path.isfile( cpu_filename ):
            log.info( "Using file '%s'..." % cpu_filename )
        else:
            log.info( "Generating file '%s'..." % cpu_filename )
            cpu_file = open( cpu_filename, 'wb' )
            cpu_file.write( str( max_time ) )

    log.info( "Working on '%s'..." % name )
    log.info( 'Generating CRAB cfg...' )
    config = ConfigParser.RawConfigParser()
    config.add_section( 'CRAB' )
    config.set( 'CRAB', 'jobtype', 'cmssw' )
    config.set( 'CRAB', 'scheduler', options.scheduler )
    if options.server:
        config.set( 'CRAB', 'use_server', '1' )
    config.add_section( 'CMSSW' )
    config.set( 'CMSSW', 'datasetpath', sample )
    config.set( 'CMSSW', 'pset', name+'_cfg.py' )
    if options.lumimask or lumi_mask:
        if setJobsNumber:
            if options.scheduler == 'remoteGlidein':
                config.set( 'CMSSW', 'number_of_jobs', maxNumJobsRG - 50 )
            else:
                config.set( 'CMSSW', 'number_of_jobs', maxNumJobs - 50 )
        else:
            config.set( 'CMSSW', 'total_number_of_lumis', options.total )
        config.set( 'CMSSW', 'lumis_per_job', lumisPerJob )
        if options.lumimask:
            config.set( 'CMSSW', 'lumi_mask', options.lumimask )
        else:
            config.set( 'CMSSW', 'lumi_mask', lumi_mask )
    else:
        config.set( 'CMSSW', 'total_number_of_events', options.total )
        if setJobsNumber:
            if options.scheduler == 'remoteGlidein':
                config.set( 'CMSSW', 'number_of_jobs', maxNumJobsRG - 50 )
            else:
                config.set( 'CMSSW', 'number_of_jobs', maxNumJobs - 50 )
        else:
            config.set( 'CMSSW', 'events_per_job', eventsPerJob )
    if options.runs:
        config.set( 'CMSSW', 'runselection', options.runs )
    config.set( 'CMSSW', 'output_file', name+'.pxlio' )
    if options.dbs_url:
        config.set( 'CMSSW', 'dbs_url', options.dbs_url )
    config.add_section( 'USER' )
    config.set( 'USER', 'return_data', '0' )
    config.set( 'USER', 'copy_data', '1' )
    if options.scheduler == 'remoteGlidein' and not options.no_more_time:
        config.set( 'USER', 'additional_input_files', '%s,%s' % ( wall_filename, cpu_filename ) )
    if allow_dcms:
        config.set( 'USER', 'storage_element', 'T2_DE_RWTH' )
        config.set( 'USER', 'user_remote_dir', outname+name )
    else:
        config.set( 'USER', 'storage_element', 'grid-srm.physik.rwth-aachen.de' )
        config.set( 'USER', 'storage_port', '8443' )
        config.set( 'USER', 'storage_path', '/srm/managerv2?SFN=/pnfs/physik.rwth-aachen.de/cms' )
        config.set( 'USER', 'user_remote_dir', '/store/user/pieta/'+user+'/'+outname+name )
    config.add_section( 'GRID' )
    config.set( 'GRID', 'rb', 'CERN' )
    if allow_dcms:
        config.set( 'GRID', 'group', 'dcms' )
    config.set( 'GRID', 'max_cpu_time', '4200' )
    config.set( 'GRID', 'max_wall_clock_time', '4200' )
    config.set( 'GRID', 'se_black_list', options.blacklist )
    config.set( 'GRID', 'additional_jdl_parameters', 'rank=-other.GlueCEStateEstimatedResponseTime+(other.GlueCEStateFreeJobSlots > 10 ? 86400 : 0)-(other.GlueCEStateWaitingJobs > 10 ? 0 : 86400);' )

    cfg_file = open(name+'.cfg', 'wb')
    config.write( cfg_file )
    del config
    del cfg_file

    log.info( 'Generating CMSSW config...' )
    process.Skimmer.FileName = name+'.pxlio'
    process.Skimmer.Process = name

    # This is a little hack for the Fall11 production.
    # We need different global tags for different samples to get the right jet
    # energy corrections. So set them here.
    #
    # FIXME: This should be removed (or updated) for SU12 and newer samples.
    #
    log.debug( "Full sample name:\n" + sample )
    if 'Fall11' in sample:
        if 'START42' in sample:
            process.GlobalTag.globaltag = 'START44_V12::All'
        elif 'START44_V9B' in sample:
            process.GlobalTag.globaltag = 'START44_V9C::All'
        elif 'START44_V5' in sample:
            process.GlobalTag.globaltag = 'START44_V5D::All'
        elif 'START44_V10' in sample:
            process.GlobalTag.globaltag = 'START44_V10D::All'
        elif 'START44' in sample:
            log.warning( "Unknown sample type: '%s'. Using Global Tag from config file." % sample )
        else:
            log.error( "Sample '%s' apparently not processed with CMSSW 4XY. Aborting!" % sample )
            sys.exit( 2 )

        log.info( "Using global tag %s" % ( process.GlobalTag.globaltag.pythonValue() ) )

    pset_file = open( name+'_cfg.py', 'w' )
    pset_file.write( "import FWCore.ParameterSet.Config as cms\n" )
    pset_file.write( "import pickle\n" )
    pset_file.write( "pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps( process ) )
    pset_file.write("process = pickle.loads(pickledCfg)\n")
    pset_file.close()

    command_create = [ 'crab', '-create', '-cfg', name + '.cfg' ]
    if options.scheduler != 'remoteGlidein':
        command_submit = [ 'crab', '-create', '-submit', '-cfg', name + '.cfg' ]
        if not options.dry_run:
            log.info( 'Done and submitting...' )
            subprocess.call( command_submit )
        else:
            log.info( 'Done and creating crab jobs...' )
            subprocess.call( command_create )
            log.info( 'Dry-run: Created task. crab command would have been: ', ' '.join( command_submit ) )
    else:
        log.info( 'Creating crab task...' )

        proc = subprocess.Popen( command_create, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
        output = proc.communicate()[0]

        # Check how many jobs have been created.
        workDir, totalNumJobs = parseCrabOutput( output )

        # With 'remoteGlidein' you can submit up to 5000 jobs per task.
        # But you can submit only max. 500 jobs at once.
        if totalNumJobs <= 500:
            # Submit all:
            crabSubmit( options, workDir )
        else:
            start = 1
            while start + 499 < totalNumJobs:
                crabSubmit( options, workDir, first=start, last=start + 499 )
                start += 500

            # Last one:
            crabSubmit( options, workDir, first=start, last=totalNumJobs )

log.info( 'Done submitting!' )

if skipped:
    longest_process = max( map( len, skipped.keys() ) )
    longest_sample  = max( map( len, skipped.values() ) )

    info = "Not submitted the following samples:"
    for process, sample in sorted( skipped.items() ):
        info += '\n'
        info += str( process + ': ' ).ljust( longest_process + 2 )
        info += sample.ljust( longest_sample )

    log.info( info )
