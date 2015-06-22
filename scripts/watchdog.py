#!/usr/bin/env python
import os,sys,glob
import tarfile
import xml.etree.ElementTree as ET
import subprocess
import imp
import datetime

import crabFunctions
import gridFunctions
import dbutilscms
import aix3adb
from  aix3adb import Aix3adbException
# define some module-wide switches
runOnMC = True
runOnData = False
runOnGen = False


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

def finalizeSample(sample,dblink):
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
    finalFiles = {}
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
        if len(dfile)  > 0:
            finalFiles.update({ dfile[0]:log['readEvents']})
            totalEvents += log['readEvents']
    newInDB = False
    #~ print finalFiles
    #~ sys.exit(1)
    if runOnMC:
        # get infos from McM
        mcmutil = dbutilscms.McMUtilities()
        mcmutil.readURL( config.Data.inputDataset )
        # try to get sample db entry and create it otherwise
        try:
            dbSample = dblink.getMCSample( sample )
            #~ return 0
        except Aix3adbException:
            dbSample = aix3adb.MCSample()
            newInDB = True
        dbSample.datasetpath = config.Data.inputDataset
        dbSample.name = sample
        dbSample.generator = generators[ sample.split("_")[-1] ]
        dbSample.crosssection = str(mcmutil.getCrossSection())
        dbSample.crosssection_reference = 'McM'
        dbSample.filter_efficiency = mcmutil.getGenInfo('filter_efficiency')
        dbSample.filter_efficiency_reference = 'McM'
        dbSample.kfactor = 1.
        dbSample.kfactor_reference = "None"
        dbSample.energy = mcmutil.getEnergy()
        if newInDB:
            dbSample = dblink.insertMCSample(dbSample)
        else:
            dbSample = dblink.editMCSample(dbSample)
        # create a new McSkim object
        mcSkim = aix3adb.MCSkim()
        # create relation to dbsample object
        mcSkim.sampleid = dbSample.id
        mcSkim.datasetpath = config.Data.inputDataset
        mcSkim.owner = crab.checkusername()
        mcSkim.is_created = 1
        mcSkim.is_finished = 1
        mcSkim.is_deprecated  = 0
        now = datetime.datetime.now()
        mcSkim.files = finalFiles
        mcSkim.created_at = now.strftime( "%Y-%m-%d %H-%M-%S" )
        # where to get the skimmer name ??? MUSiCSkimmer fixed
        mcSkim.skimmer_name = "MUSiCSkimmer"
        mcSkim.skimmer_version = outlfn.split("/")[2]
        mcSkim.skimmer_cmssw = os.getenv( 'CMSSW_VERSION' )
        mcSkim.skimmer_globaltag = [p.replace("globalTag=","").strip() for p in config.JobType.pyCfgParams if "globalTag" in p][0]
        mcSkim.nevents = str(totalEvents)
        dblink.insertMCSkim( mcSkim )


    elif runOnData:
        # try to get sample db entry and create it otherwise
        dbSample = dblink.getDataSample( datasetpath )
        if not dbSample:
            dbSample = aix3adb.DataSample( datasetpath )
            dblink.insertDataSample( dbsample )
        dataSkim = aix3adb.DataSkim( dbSample )

    #~ elif runOnGen:
        #~ log.info("Gen Samples are not saved to db")
    with open('finalSample','a') as outfile:
        outfile.write("%s:%s\n" % (sample,  config.Data.inputDataset))
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

    crab = crabFunctions.CrabController()
    #~ crabFolder = crab.crabFolders[0]
    crabSamples = [crabFolder.replace('crab_','') for crabFolder in crab.crabFolders]
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

        if not any(sample in s for s in finalsamples):
            if i % 20 == 0:
                print '{:<90} {:<12} {:<8} {:<8} {:<8} {:<8} {:<8}'.format('Sample','State','Running','Idle','Failed','Transfer','Finished')
            i+=1
            #~ state,jobs = crab.status(sample)
            task = crabFunctions.CrabTask(sample)
            if "COMPLETE" == task.state:
            #~ print state, sample
                finalizeSample(sample,dblink)
            if "FAILED" == task.state:# or "KILLED" == task.state:
                cmd = 'crab resubmit --wait %s' %crab._prepareFoldername(sample)
                p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
                (out,err) = p.communicate()
                print out
            #~ if  "SUBMITTED" == task.state and task.nRunning < 1 and task.nIdle > 0 and task.nTransferring <1:
                #~ idlejobs = [id for id in task.jobs.keys() if "idle" in task.jobs[id]['State']]
                #~ idlejobs = ','.join(idlejobs)
                #~ print "IDLE",idlejobs
                #cmd = 'crab kill --force --jobids=%s %s' %(idlejobs,crab._prepareFoldername(sample))
                #~ cmd = 'crab resubmit --force --jobids=%s %s' %(idlejobs,crab._prepareFoldername(sample))
                #~ p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
                #~ (out,err) = p.communicate()
                #~ print out


            print '{:<90} {:<12} {:<8} {:<8} {:<8} {:<8} {:<8}'.format(sample,task.state,str(task.nRunning),str(task.nIdle),str(task.nFailed),str(task.nTransferring),str(task.nFinished))
    sys.exit(1)

if __name__ == '__main__':
    main()
