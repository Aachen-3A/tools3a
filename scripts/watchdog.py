#!/usr/bin/env python
import os,sys,glob
import tarfile
import xml.etree.ElementTree as ET
import subprocess
import imp
import datetime
import argparse

import crabFunctions
import gridFunctions
import dbutilscms
import aix3adb
from  aix3adb import Aix3adbException
# define some module-wide switches
runOnMC = True
runOnData = False
runOnGen = False

def commandline_parsing():
    descr = 'Simple Tool for crab job monitoring and submission of metainformation to aix3adb after completion'
    parser = argparse.ArgumentParser(description= descr)
    parser.add_argument('-f' ,'--noFinalize', action='store_true', help='Do not finalize (get metainfo and submit to aix3adb)')
    parser.add_argument('-r' ,'--rFailed',    action='store_true', help='resubmit failed tasks')
    parser.add_argument('-i', '--kIdle',     action='store_true', help='Try to kill stuck idle jobs')
    parser.add_argument('-u', '--update',    action='store_true', help='Update latest skim instead of creating a new one. This sets the --ignoreComplete option true')
    parser.add_argument('--ignoreComplete',  action='store_true', help='Do not skip previously finalized samples')
    parser.add_argument('--addIncomplete',  action='store_true', help='Submit all samples to db, even if they are not finished')
    parser.add_argument('--only', action='store', dest='folder_tag', default='', help='Only loop over folder containing a string')
    args = parser.parse_args()

    if args.update: args.ignoreComplete = True
    return args

def read_crabconfig( sample ):
    pset = 'crab_%s_cfg.py' % sample
    with open( pset, 'r') as cfgfile:
        cfo = imp.load_source("pycfg", pset, cfgfile )
        config = cfo.config
        del cfo
    return config

def getdCacheFiles(sample):
    return

def readLogArch(logArchName):
    JobNumber = logArchName.split("/")[-1].split("_")[1].split(".")[0]
    log = {'readEvents' : 0}
    with tarfile.open( logArchName, "r") as tar:
        try:
            JobXmlFile = tar.extractfile('FrameworkJobReport-%s.xml' % JobNumber)
            root = ET.fromstring( JobXmlFile.read() )
            for child in root:
                if child.tag == 'InputFile':
                    for subchild in child:
                        if subchild.tag == 'EventsRead':
                            nEvents = int(subchild.text)
                            log.update({'readEvents' : nEvents})
                            break
                    break
        except:
            print "Can not parse / read %s" % logArchName
    return log

def finalizeSample(sample,dblink, args):

    config = read_crabconfig( sample )
    outlfn = config.Data.outLFNDirBase.split('/store/user/')[1]
    if outlfn.endswith("/"): outlfn =outlfn[:-1]
    crab = crabFunctions.CrabController()
    # Check files for each job
    dCacheFiles = gridFunctions.getdcachelist( outlfn )
    #~ success , failed = crab.getlog( sample )
    cmd = 'crab log %s' % crab._prepareFoldername(sample)
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
    (out,err) = p.communicate()
    #~ print out
    sample = sample.strip()
    crabFolder = crab._prepareFoldername( sample )
    #~ print crabFolder
    #~ print "%s/%s/results/*.log.tar.gz" % (config.General.workArea,crabFolder)
    logArchs = glob.glob("%s/%s/results/*.log.tar.gz" % (config.General.workArea,crabFolder))
    #~ print logArchs
    finalFiles = []
    totalEvents = 0
    #~ sys.exit()
    #~ print dCacheFiles
    for logArchName in logArchs:
        JobNumber = logArchName.split("/")[-1].split("_")[1].split(".")[0]
        #~ print logArchName
        log = readLogArch( logArchName )
        # check if file on dCache
        dfile = []
        for layer in dCacheFiles:
            dfile += [s for s in layer if "%s_%s.pxlio" %( sample, JobNumber ) in s ]
        if len(dfile)  > 0 and log['readEvents'] > 0 :
            finalFiles.append(  {'path':dfile[0], 'nevents':log['readEvents']} )
            totalEvents += log['readEvents']
    global runOnMC
    global runOnData
    # find out if we work on dat or mc
    try:
        test = config.Data.lumiMask
        runOnMC = False
    except:
        runOnMC = True
    runOnData = not runOnMC

    if runOnMC:
        addMC2db(sample, dblink, args, config, finalFiles, totalEvents)
    elif runOnData:
        addData2db(sample, dblink, args, config, finalFiles, totalEvents)

    #~ elif runOnGen:
        #~ log.info("Gen Samples are not saved to db")
    with open('finalSample','a') as outfile:
        outfile.write("%s:%s\n" % (sample,  config.Data.inputDataset))

def addData2db(sample, dblink, args, config, finalFiles, totalEvents):
    crab = crabFunctions.CrabController()
    # try to get sample db entry and create it otherwise
    newInDB = False
    try:
        dbSample = dblink.getDataSample( sample  )
    except Aix3adbException:
        newInDB = True
        dbSample = aix3adb.DataSample( )
        dbSample.name = sample
    # update fields
    dbSample.energy = 13

    if newInDB:
        dblink.insertDataSample( dbSample )
        # get sample again with its new id
        dbSample = dblink.getDataSample( sample  )
    else:
        dblink.editDataSample( dbSample )

    if args.update and not newInDB:
        dbSkim, dbSample  =  dblink.getDataLatestSkimAndSampleBySample( dbSample.name )
    else:
        dbSkim = aix3adb.MCSkim()

    fillCommonSkimFields( dbSample, dbSkim , config, finalFiles, totalEvents )
    ## fill additional fields for data
    dbSkim.jsonfile = config.Data.lumiMask.split("/")[-1]
    if args.update:
        dblink.editDataSkim( dbSkim )
    else:
        dblink.insertDataSkim( dbSkim )

def addMC2db(sample, dblink, args, config, finalFiles, totalEvents):
    crab = crabFunctions.CrabController()
    generators = {}
    generators.update({ 'MG':'madgraph' })
    generators.update({ 'PH':'powheg' })
    generators.update({ 'HW':'herwig6' })
    generators.update({ 'HP':'herwigpp' })
    generators.update({ 'HW':'herwig' })
    generators.update({ 'SP':'sherpa' })
    generators.update({ 'MC':'mcatnlo' })
    generators.update({ 'AG':'alpgen' })
    generators.update({ 'CA':'calchep' })
    generators.update({ 'CO':'comphep'  })
    generators.update({ 'P6':'pythia6' })
    generators.update({ 'P8':'pythia8' })
    generators.update({ 'PY':'pythia8' })
    newInDB = False
        # get infos from McM
    mcmutil = dbutilscms.McMUtilities()
    mcmutil.readURL( config.Data.inputDataset )
    # try to get sample db entry and create it otherwise
    try:
        dbSample = dblink.getMCSample( sample )
    except Aix3adbException:
        dbSample = aix3adb.MCSample()
        newInDB = True
        dbSample.name = sample
        dbSample.generator = generators[ sample.split("_")[-1] ]
        dbSample.crosssection = str(mcmutil.getCrossSection())
        dbSample.crosssection_reference = 'McM'
        dbSample.filterefficiency = mcmutil.getGenInfo('filter_efficiency')
        dbSample.filterefficiency_reference = 'McM'
        dbSample.kfactor = 1.
        dbSample.kfactor_reference = "None"
        dbSample.energy = mcmutil.getEnergy()

    if newInDB:
        dbSample = dblink.insertMCSample(dbSample)
    else:
        dbSample = dblink.editMCSample(dbSample)

    if args.update and not newInDB:
        mcSkim, mcSample  =  dblink.getMCLatestSkimAndSampleBySample( dbSample.name )
    else:
        mcSkim = aix3adb.MCSkim()

    fillCommonSkimFields( dbSample, mcSkim , config, finalFiles, totalEvents )

    if args.update:
        dblink.editMCSkim( mcSkim )
    else:
        dblink.insertMCSkim( mcSkim )

def fillCommonSkimFields( dbSample, dbSkim , config, finalFiles, totalEvents ):
    # create relation to dbsample object
    dbSkim.sampleid = dbSample.id
    dbSkim.datasetpath = config.Data.inputDataset
    crab = crabFunctions.CrabController()
    dbSkim.owner = crab.checkusername()
    dbSkim.iscreated = 1
    dbSkim.isfinished = 1
    dbSkim.isdeprecated  = 0
    now = datetime.datetime.now()
    dbSkim.files = finalFiles
    dbSkim.created_at = now.strftime( "%Y-%m-%d %H-%M-%S" )
    # where to get the skimmer name ??? MUSiCSkimmer fixed
    dbSkim.skimmer_name = "PxlSkimmer"
    outlfn = config.Data.outLFNDirBase.split('/store/user/')[1]
    dbSkim.skimmer_version = outlfn.split("/")[2]
    dbSkim.skimmer_cmssw = os.getenv( 'CMSSW_VERSION' )
    dbSkim.skimmer_globaltag = [p.replace("globalTag=","").strip() for p in config.JobType.pyCfgParams if "globalTag" in p][0]
    dbSkim.nevents = str(totalEvents)

def createDBlink():
    # Create a database object.
    dblink = aix3adb.aix3adb()
    crab = crabFunctions.CrabController()
    # Authorize to database.
    #~ print( "Connecting to database: 'http://cern.ch/aix3adb'" )
    dblink.authorize(username = crab.checkusername())
    #~ log.info( 'Authorized to database.' )
    return dblink

def main():
    args = commandline_parsing()
    crab = crabFunctions.CrabController()
    #~ crabFolder = crab.crabFolders[0]
    crabSamples = [crabFolder.replace('crab_','') for crabFolder in crab.crabFolders]
    if args.folder_tag!='':
        crabSamples = filter(lambda x: args.folder_tag in x, crabSamples)
    sample = crabSamples[0]
    dblink = createDBlink()
    skipped_final = []
    if not os.path.exists('finalSample'):
        open('finalSample', 'a').close()
    with open('finalSample','r') as finalFile:
        finalsamples = finalFile.read()
        finalsamples = finalsamples.split("\n")
    i = 0
    for sample in crabSamples:

        if not any(sample in s for s in finalsamples) or args.ignoreComplete:
            if i % 50 == 0:
                print '{:<90} {:<12} {:<8} {:<8} {:<8} {:<8} {:<8}'.format('Sample','State','Running','Idle','Failed','Transfer','Finished')
            i+=1
            #~ state,jobs = crab.status(sample)
            task = crabFunctions.CrabTask(sample)
            if "COMPLETE" == task.state and not args.noFinalize or args.addIncomplete:
                finalizeSample( sample, dblink, args )
            if args.rFailed and ("FAILED" == task.state or "NOSTATE" == task.state):# or "KILLED" == task.state:
                if task.failureReason is not None:
                    if "The CRAB3 server backend could not resubmit your task because the Grid scheduler answered with an error." in task.failureReason:
                        cmd = 'mv %s bak_%s' %(crab._prepareFoldername(sample),crab._prepareFoldername(sample))
                        p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
                        (out,err) = p.communicate()

                        cmd = 'crab submit %s_cfg.py' %(crab._prepareFoldername(sample))
                        p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
                        (out,err) = p.communicate()
                        print out,err
                    else:
                        cmd = 'crab resubmit --wait %s' %crab._prepareFoldername(sample)
                        p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
                        (out,err) = p.communicate()
                else:
                    cmd = 'crab resubmit --wait %s' %crab._prepareFoldername(sample)
                    p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
                    (out,err) = p.communicate()
                task.update()
                print out
            if  args.kIdle and "SUBMITTED" == task.state and task.nRunning < 1 and task.nIdle > 0 and task.nTransferring <1:
                idlejobs = [id for id in task.jobs.keys() if "idle" in task.jobs[id]['State']]
                idlejobs = ','.join(idlejobs)
                print "IDLE",idlejobs
                cmd = 'crab kill --jobids=%s %s' %(idlejobs,crab._prepareFoldername(sample))
                #~ cmd = 'crab resubmit --force --jobids=%s %s' %(idlejobs,crab._prepareFoldername(sample))
                #~ cmd = 'crab resubmit --wait %s' %crab._prepareFoldername(sample)
                p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
                (out,err) = p.communicate()
                #~ print out


            print '{:<90} {:<12} {:<8} {:<8} {:<8} {:<8} {:<8}'.format(sample,task.state,str(task.nRunning),str(task.nIdle),str(task.nFailed),str(task.nTransferring),str(task.nFinished))
    sys.exit(1)

if __name__ == '__main__':
    main()
