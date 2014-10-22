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
import imp
import pickle
import fnmatch

# some general definitions
COMMENT_CHAR = '#'
log_choices = [ 'ERROR', 'WARNING', 'INFO', 'DEBUG' ]
date = '%F %H:%M:%S'

skimmer_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject' )
lumi_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/lumi' )
config_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/configs' )



# Write everything into a log file in the directory you are submitting from.
log = logging.getLogger( 'music_crab' )

# define some module-wide switches
runOnMC = False
runOnData = False
runOnGen = False


import FWCore.ParameterSet.Config as cms

def main():
    # Parse user input from command line
    (options, args ) = commandline_parsing()
    # Setup logging for music_crab
    setupLogging(options)
    

    
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
    if not options.noTag:
        try:
            gitTag = createTag( options, skimmer_dir )
            SampleFileInfoDict.update({'gitTag':gitTag})
        except Exception, e:
            log.error( e )
            sys.exit( 3 )
    log.info("after tag")
    
    
    # first check if user has permission to write to selected site
    if not crab_checkwrite("T2_DE_RWTH",options):sys.exit(1)
    
    
    
    # extract the global tag for the used config file
    #~ globalTag = log.info("using global tag: %s" % getGlobalTag(SampleFileInfoDict))
    globalTag =  getGlobalTag(options)
    log.info("using global tag: %s" % globalTag)
    SampleFileInfoDict.update({'globalTag':globalTag})
    # create crab config files and submit tasks
    for key in SampleDict.keys():
        CrabConfig = createCrabConfig(SampleFileInfoDict,SampleDict[key],options)
        log.info("Created crab config object")
        writeCrabConfig(key,CrabConfig,options)
        crab_submit(key,options)
        if options.db and not options.dry_run:
            submitSample2db(key, SampleDict[key][1],SampleFileInfoDict)
        else:
            log.warning('No -db option or dry run, no sample information is submitted to aix3adb')
    
                
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
    if options.transferOutput:
        config.set( 'General', 'transferOutput', 'True')
    if options.log:
        config.set( 'General', 'saveLogs', 'True' )
    
    ### JobType section
    config.add_section('JobType')
    config.set( 'JobType', 'pluginName', 'Analysis' )
    config.set( 'JobType', 'psetName', SampleFileInfoDict['pset'] )
    
    # next two lines use old (pickle way)
    #~ preloadProcess(name,sample,SampleFileInfoDict)
    #~ config.set( 'JobType', 'psetName', name+'_cfg.py' )
    
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
            #~ print DatasetSummary
            filesPerJob =  int((float(options.eventsPerJob) * int(DatasetSummary['numFiles'])) /  int(DatasetSummary['numEvents']) )
            if filesPerJob < 1:
                filesPerJob = 1
        except:
            log.error("events per job needs an integer")
            sys.exit(1)
        config.set( 'Data', 'splitting', 'FileBased' )
        config.set( 'Data', 'unitsPerJob', '%d'%filesPerJob)
    
    config.set( 'Data', 'outlfn', "/store/user/%s/MUSiC/%s/%s/"%(options.user,datetime.date.today().isoformat(),name))
    
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
        blacklists = options.blacklist.split(',')
        config.set( 'Site', 'blacklist', blacklists )
    
    
    ### User section
    config.add_section('User')
    config.set( 'User', 'voGroup', 'dcms' )
      
    return config

def preloadProcess(name,sample,SampleFileInfoDict):
    pset = SampleFileInfoDict['pset'] 
    file = open( pset )
    cfo = imp.load_source("pycfg", pset, file )
    del file
    process = cfo.process
    del cfo

    process.Skimmer.FileName = name+'.pxlio'
    process.Skimmer.Process = name
    process.Skimmer.Dataset = sample

    pset_file = open( name+'_cfg.py', 'w' )
    pset_file.write( "import FWCore.ParameterSet.Config as cms\n" )
    pset_file.write( "import pickle\n" )
    pset_file.write( "pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps( process ) )
    pset_file.write("process = pickle.loads(pickledCfg)\n")
    pset_file.close()

def writeCrabConfig(name,config,options):
    if options.workingArea:
        runPath = options.workingArea
        if not runPath.strip()[-1] == "/":
            runPath+="/"
    else:
        runPath ="./"
        filename = '%s/crab_%s_cfg.py'%(runPath,name)
        try:
            config.writeCrabConfig(filename)
            log.info( 'created crab config file %s'%filename )
        except:
            log.error("Could not create crab config file")
    

def getRunRange():
    return 'dummy'

def crab_checkwrite(site,options,path='noPath'):    
    log.info("Checking if user can write in output storage")
    cmd = ['crab checkwrite --site %s --voGroup=dcms'%site ]
    if not 'noPath' in path:
        cmd[0] +=' --lfn=%s'%(path)
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
    (stringlist,string_err) = p.communicate()
    if not "Able to write to /store/user/%s on site %s"%(options.user,site)  in stringlist:
        log.error( "The crab checkwrite command failed for site: %s"%site )
        log.error(string_err)
        return False
    else:
        log.info("Checkwrite was sucessfully called.")
        return True
        
def crab_submit(name,options):
    cmd = 'crab submit crab_%s_cfg.py'%name
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    if options.dry_run:
        log.info( 'Dry-run: Created config file. crab command would have been: %s'%cmd )
    else:
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
        (stringlist,string_err) = p.communicate()
        log.info("crab sumbit called for task %s"%name) 

def crab_checkHNname(options):
    cmd = 'crab checkHNname --voGroup=dcms'
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
    (string_out,string_err) = p.communicate()
    string_out = string_out.split("\n")
    for line in string_out:
        if "Your CMS HyperNews username is" in line:
            hnname = line.split(":")[1].strip()
            return hnname
    return "noHNname"



def readSampleFile(filename,options):
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


#~ def getGlobalTag(SampleFileInfoDict):
    #~ pset = SampleFileInfoDict['pset'] 
    
    #~ with open(pset,"rb" ) as psetfile:
        #~ cfo = imp.load_source("pycfg", pset, psetfile )
        #~ globalTag = cfo.getGlobalTag()
        #~ del cfo
        #~ return globalTag
def getGlobalTag(options):
    someCondition = False
    if options.globalTag:
        globalTag =  options.globalTag 
    elif someCondition:
        log.info("this is a place where Global tags will be defined during the run")
    else:
        #find default globalTag
        from Configuration.AlCa.autoCond import autoCond
        if runOnData:
            #~ globalTag = cms.string( autoCond[ 'com10' ] )
            globalTag =  autoCond[ 'com10' ]
        else:
            globalTag = autoCond[ 'startup' ] 
    return globalTag

def createDBlink():
    import MUSiCProject.Tools.aix3adb as aix3adb
    
    # Create a database object.
    dblink = aix3adb.aix3adb()

    # Authorize to database.
    log.info( "Connecting to database: 'http://cern.ch/aix3adb'" )
    dblink.authorize()
    log.info( 'Authorized to database.' )
    return dblink

def submitSample2db(name,sample,SampleFileInfoDict,dblink):
    
    datasetInfos[ 'original_name' ] = sample
    datasetInfos[ 'energy' ] = SampleFileInfoDict['energy']
    datasetInfos[ 'iscreated' ] = 1
    datasetInfos[ 'skimmer_name' ] = 'MUSiCSkimmer'
    datasetInfos[ 'skimmer_cmssw' ] = os.getenv( 'CMSSW_VERSION' )
    datasetInfos[ 'skimmer_globaltag' ] = SampleFileInfoDict['globalTag']
    
    if options.no_tag:
        datasetInfos[ 'skimmer_version' ] = 'Not tagged'
    else:
        datasetInfos[ 'skimmer_version' ] = SampleFileInfoDict['gitTag']
    
    datasetTags = dict()
    datasetTags[ 'MUSiC_Skimming_cfg' ] = SampleFileInfoDict['pset']
    datasetTags[ 'MUSiC_Processname' ] = name

    log.info( "Registering '%s' at database 'http://cern.ch/aix3adb'. " % name )

    # Create a text file in the crab folder to log the database ID.
    # This ID is unique for each database entry.
    DBconfig = ConfigParser.SafeConfigParser()
    DBconfig.add_section( 'DB' )    
    
    if runOnMC == True:
        datasetInfos[ 'generator' ] = SampleFileInfoDict['generator']
        datasetInfos[ 'tags' ] = datasetTags

        if not options.dry_run:
            # Send all info on this MC sample to the database.
            DBentry = dblink.registerMCSample( datasetInfos )
            log.debug( DBentry )
            DBconfig.set( 'DB', 'ID', str( DBentry[ 'id' ] ) )
            DBconfig.set( 'DB', 'Table', 'MC samples' )
        else:
            log.info( 'Dry-run: Would have registered this MC sample at database:\n%s' % datasetInfos )

    elif runOnData == True:
        # Syntax of the lumimask file: DCS-firstrun-lastrun.json
        ( firstrun, lastrun ) = os.path.basename( lumi_mask).split( '.' )[0].split( '-' )[1:3]
        datasetInfos[ 'firstrun' ] = firstrun
        datasetInfos[ 'lastrun' ] = lastrun
        datasetInfos[ 'jsonfile' ] = lumi_mask
        datasetTags[ 'DCSOnly_JSON' ] = DCSOnly_json
        datasetInfos[ 'tags' ] = datasetTags
    
        if not options.dry_run:
            # Send all info on this Data sample to the database.
            DBentry = dblink.registerDataSample( datasetInfos )
            log.debug( DBentry )
            DBconfig.set( 'DB', 'ID', str( DBentry[ 'id' ] ) )
            DBconfig.set( 'DB', 'Table', 'data samples' )
        else:
            log.info( 'Dry-run: Would have registered this Data sample at database:\n%s' % datasetInfos )
    else:
        log.error( "Not all necessary arguments given in config: '%s'." % sample_file )
        
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

def setupLogging(options):
    #setup logging
    format = '%(levelname)s (%(name)s) [%(asctime)s]: %(message)s'
    logging.basicConfig( level=logging._levelNames[ options.debug ], format=format, datefmt=date )   
    formatter = logging.Formatter( format )
    log_file_name = 'music_crab_' + options.isodatetime + '.log'
    hdlr = logging.FileHandler( log_file_name, mode='w' )
    hdlr.setFormatter( formatter )
    log.addHandler( hdlr )

def commandline_parsing():
    ##parse user input
    ####################################
    # The following options
    # were already present in muic_crab
    ####################################
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
    #~ parser.add_option( '--noTag', action='store_true', default=False,
    parser.add_option( '--noTag', action='store_true', default=False,help="Do not create a tag in the skimmer repository. [default: %default]" )
    parser.add_option( '-b', '--blacklist', metavar='SITES', help='Blacklist SITES in addition to T0,T1 separated by comma, e.g. T2_DE_RWTH,T2_US_Purdue  ' )
    parser.add_option( '-D', '--db', action='store_true', default=False,
                       help="Register all datasets at the database: 'https://cern.ch/aix3adb/'. [default: %default]" )
    #///////////////////////////////              
    #// new options since crab3
    #//////////////////////////////
    
    # new feature alternative username
    parser.add_option( '-u','--user', help='Alternative username [default is HN-username]')
    parser.add_option( '-g','--globalTag', help='Override globalTag from pset')
    ###########################################
    # new  options for General section in pset
    ##########################################
    parser.add_option( '--workingArea',metavar='DIR',default=os.getcwd(),help='The area (full or relative path) where to create the CRAB project directory. ' 
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
    ########################################                                    
    # new options for JobType in pset
    ########################################
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
    ####################################
    # new options for Data in pset
    ####################################
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
    
    # check if user has valid proxy
    import gridFunctions 
    gridFunctions.checkAndRenewVomsProxy()
    
    #get current user HNname
    if not options.user:
        options.user = crab_checkHNname(options)
        
    return (options, args )
    
if __name__ == '__main__':
    main()

