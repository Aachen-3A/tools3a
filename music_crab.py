#!/usr/bin/env python

import os
import sys
import subprocess
import imp
import pickle
import optparse
import datetime
import ConfigParser

lumi_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/lumi' )
config_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/configs' )


parser = optparse.OptionParser( description='Submit MUSiCSkimmer jobs for all samples listed in DATASET_FILE',  usage='usage: %prog [options] DATASET_FILE' )
parser.add_option( '-c', '--config', metavar='FILE', help='Use FILE as CMSSW config file, instead of the one declared in DATASET_FILE' )
parser.add_option( '--config-dir', metavar='DIR', default=config_dir, help='Directory containing CMSSW configs [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/configs]' )
parser.add_option( '-n', '--name', metavar='NAME', help='Output will be written in store/user/{your_name}/NAME/{short_dataset_name} [default: MUSiC/{current_date}]' )
parser.add_option( '-r', '--runs', metavar='RUNS', help='Only analyze the given runs (comma separated list)' )
parser.add_option( '-l', '--lumimask', metavar='FILE', help='Use JSON file FILE as lumi mask' )
parser.add_option( '--lumi-dir', metavar='DIR', default=lumi_dir, help='Directory containing luminosity-masks [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/lumi]' )
parser.add_option( '-t', '--total', metavar='NUMBER', default='-1', help='Only analyze NUMBER events/lumis [default: %default; means all]' )
parser.add_option( '-j', '--perJob', metavar='NUMBER', default='unset', help='Anlyze NUMBER events.lumis per job [default: 50000 events or 35 lumis]' )
parser.add_option( '-s', '--server', action='store_true', default=False, help='Use the CRAB server [default: %default]' )
parser.add_option( '-g', '--scheduler', default='glite', help='Scheduler to use (glidein implies --server) [default: %default]' )
parser.add_option( '-b', '--blacklist', metavar='SITES', help='Blacklist SITES in addition to T0,T1' )

(options, args ) = parser.parse_args()

if len( args ) < 1:
    parser.error( 'DATASET_FILE required' )

del parser


if options.scheduler == 'glidein':
    options.server = True

if options.name:
    outname = options.name
    while outname.startswith( '/' ):
        outname = outname[ 1: ]
    while outname.endswith( '/' ):
        outname = outname[ :-1 ]
else:
    outname = 'MUSiC/'+datetime.date.today().isoformat()
outname += '/'

if options.perJob == 'unset':
    defaultLumiPerJob = '100'
    eventsPerJob = '50000'
else:
    defaultLumiPerJob = options.perJob
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
        line = line[:-1]
        if not line or line.startswith( '#' ): continue
        if line.startswith( 'config' ):
            (junk,pset) = line.split( '=' )
            pset = os.path.join( options.config_dir, pset )
            break
    else:
        print 'No CMSSW config file specified!'
        print 'Either add it to the sample file or add it to the command line.'
        sys.exit(1)

print 'Reading config', pset
file = open( pset )
cfo = imp.load_source("pycfg", pset, file )
del file
process = cfo.process
del cfo


#check if the user is not in dcms
user = os.getenv( 'LOGNAME' )
dcms_blacklist = [ 'malhotra' ]
allow_dcms = not user in dcms_blacklist


for line in sample_file:
    line = line[:-1]
    if not line or line.startswith( '#' ): continue

    #lumi-mask and lumis-per-job can be specified in the command line
    if ';' in line:
        split_line = line.split( ';' )
        first_part = split_line[ 0 ]
        lumi_mask = os.path.join( options.lumi_dir, split_line[ 1 ] )
        if len( split_line ) > 2:
            lumiPerJob = int( split_line[ 2 ] )
        else:
            lumiPerJob = defaultLumiPerJob
    else:
        lumiPerJob = defaultLumiPerJob
        first_part = line
        lumi_mask = None

    (name,sample) = first_part.split( ':' )

    print name, ': Generating CRAB cfg...',
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
        config.set( 'CMSSW', 'total_number_of_lumis', options.total )
        config.set( 'CMSSW', 'lumis_per_job', lumiPerJob )
        if options.lumimask:
            config.set( 'CMSSW', 'lumi_mask', options.lumimask )
        else:
            config.set( 'CMSSW', 'lumi_mask', lumi_mask )
    else:
        config.set( 'CMSSW', 'total_number_of_events', options.total )
        config.set( 'CMSSW', 'events_per_job', eventsPerJob )
    if options.runs:
        config.set( 'CMSSW', 'runselection', options.runs )
    config.set( 'CMSSW', 'output_file', name+'.pxlio' )
    config.add_section( 'USER' )
    config.set( 'USER', 'return_data', '0' )
    config.set( 'USER', 'copy_data', '1' )
    config.set( 'USER', 'storage_element', 'T2_DE_RWTH' )
    config.set( 'USER', 'user_remote_dir', outname+name )
    config.add_section( 'GRID' )
    config.set( 'GRID', 'rb', 'CERN' )
    if allow_dcms:
        config.set( 'GRID', 'group', 'dcms' )
    config.set( 'GRID', 'max_cpu_time', '1400' )
    config.set( 'GRID', 'max_wall_clock_time', '1400' )
    config.set( 'GRID', 'se_black_list', options.blacklist )
    config.set( 'GRID', 'additional_jdl_parameters', 'rank=-other.GlueCEStateEstimatedResponseTime+(other.GlueCEStateFreeJobSlots > 10 ? 86400 : 0)-(other.GlueCEStateWaitingJobs > 10 ? 0 : 86400);' )

    cfg_file = open(name+'.cfg', 'wb')
    config.write( cfg_file )
    del config
    del cfg_file

 
    print 'Generating CMSSW config...',
    process.Skimmer.FileName = name+'.pxlio'
    process.Skimmer.Process = name

    pset_file = open( name+'_cfg.py', 'w' )
    pset_file.write( "import FWCore.ParameterSet.Config as cms\n" )
    pset_file.write( "import pickle\n" )
    pset_file.write( "pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps( process ) )
    pset_file.write("process = pickle.loads(pickledCfg)\n")
    pset_file.close()

    print 'done and submitting...'
    subprocess.call( [ 'crab', '-create', '-submit', '-cfg', name+'.cfg' ] )
