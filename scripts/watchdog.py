#!/usr/bin/env python
import os,sys,glob
import tarfile
import xml.etree.ElementTree as ET
import subprocess
import imp
import datetime
import argparse

from multiprocessing import Process

from music_crab3 import readSampleFile as readMusicCrabConfig

import crabFunctions
import gridFunctions
import dbutilscms
import aix3adb
from  aix3adb import Aix3adbException

COMMENT_CHAR = '#'

def commandline_parsing():
    descr = 'Simple Tool for crab job monitoring and submission of metainformation to aix3adb after completion'
    parser = argparse.ArgumentParser(description= descr)
    parser.add_argument('-f' ,'--noFinalize', action='store_true', help='Do not finalize (get metainfo and submit to aix3adb)')
    parser.add_argument('-r' ,'--rFailed',    action='store_true', help='resubmit failed tasks')
    parser.add_argument('-i', '--kIdle',     action='store_true', help='Try to kill stuck idle jobs')
    parser.add_argument('-u', '--update',    action='store_true', help='Update latest skim instead of creating a new one. This sets the --ignoreComplete option true')
    parser.add_argument('-m', '--musicCrab3Input',  metavar='FILES', type=str, nargs='+',
                   help='A list of music_crab input files. Gives summary of clompetness')
    parser.add_argument('--ignoreComplete',  action='store_true', help='Do not skip previously finalized samples')
    parser.add_argument('--addIncomplete',  action='store_true', help='Submit all samples to db, even if they are not finished')
    parser.add_argument('--only', action='store', dest='folder_tag', default='', help='Only loop over folder containing a string')
    parser.add_argument('--debug', action='store_true', default='', help='Show debug output')
    args = parser.parse_args()

    if args.update: args.ignoreComplete = True

    if args.musicCrab3Input:
        args.maxJobRuntimeMin = -1
        args.config = ""
        args.config_dir = ""
        args.unitsPerJob = -1

    #~ args.user = crab.checkusername()
    return args

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
    finalSampleList = []
    finalizeProcs = []
    finalizeSamples = []
    for sample in crabSamples:

        task = crabFunctions.CrabTask(sample, initUpdate = False, dblink= dblink)
        if any(sample in s for s in finalsamples) and not args.ignoreComplete:
            if task.isFinal:
                finalSampleList.append( sample )
                task.state = "FINAL"
            else:
                print "Sample in final but not aix3aDB ! "
            continue
        else:
            task.update()
        if i % 50 == 0:
            print '{:<90} {:<12} {:<8} {:<8} {:<8} {:<8} {:<8}'.format('Sample','State','Running','Idle','Failed','Transfer','Finished')
        i+=1
        #~ state,jobs = crab.status(sample)

        if "COMPLETE" == task.state and not args.noFinalize or args.addIncomplete:
            if sample in finalSampleList or args.update: task.finalizeTask( update = args.update )
            if task.isFinal: finalizeSamples.append( task )
            #~ finalizeSample( sample, dblink, crabConfig, args )
        if args.rFailed and "FAILED" == task.state:# or "KILLED" == task.state:
            cmd = 'crab resubmit --wait %s' %crab._prepareFoldername(sample)
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
            (out,err) = p.communicate()
            task.update()
            if args.debug: print out
        if  args.kIdle and "SUBMITTED" == task.state and task.nRunning < 1 and task.nIdle > 0 and task.nTransferring <1:
            idlejobs = [id for id in task.jobs.keys() if "idle" in task.jobs[id]['State']]
            idlejobs = ','.join(idlejobs)
            if args.debug: print "IDLE",idlejobs
            cmd = 'crab kill --jobids=%s %s' %(idlejobs,crab._prepareFoldername(sample))
            #~ cmd = 'crab resubmit --force --jobids=%s %s' %(idlejobs,crab._prepareFoldername(sample))
            #~ cmd = 'crab resubmit --wait %s' %crab._prepareFoldername(sample)
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
            (out,err) = p.communicate()
            #~ print out


        print '{:<90} {:<12} {:<8} {:<8} {:<8} {:<8} {:<8}'.format(sample,task.state,str(task.nRunning),str(task.nIdle),str(task.nFailed),str(task.nTransferring),str(task.nFinished))
    # check if everything was added correctly to aix3adb
    for sample in finalizeSamples:
        if task.isFinal:
            finalSampleList.append( sample )
    #~ print "samples in final and added to aix3adb"
    #~ for sample in finalSampleList: print sample

    if args.musicCrab3Input:
        musicCrabSamples = []
        allUnfinished = []
        allNoFolder = []
        allMonitored = []
        missingFiles = []
        for path in args.musicCrab3Input:
            mcrArgs = args
            if not args.folder_tag:
                mcrArgs.only = False
            outdict = readMusicCrabConfig( path, mcrArgs )
            musicCrabSamples = outdict['sampledict'].keys()
            unfinished = [ sample for sample in musicCrabSamples if not sample in finalSampleList]
            monitored = [ sample for sample in unfinished if sample in crabSamples]
            allNoFolder += [ sample for sample in unfinished if not sample in crabSamples]
            allUnfinished += unfinished
            allMonitored += monitored
            splitPath = path.split("/")
            missFileName = splitPath.pop(-1)
            missPath =  "missing_" + missFileName
            with open( path , "r" ) as mcrFile:
                missConfig = mcrFile.readlines()
            with open( missPath , "wb" ) as missfile:
                for line in missConfig:
                    skip = False
                    for sample in monitored:
                        if sample in line: skip = True
                    for sample in finalSampleList:
                        if sample in line: skip = True
                    if not skip: missfile.write( line  )
                missingFiles.append( missPath )
        if len( finalSampleList )> 0:
            print "\n++++++++++++++++++++++++++++++++++++++++++++"
            print "Finalized samples from music_crab3 configs"
            print "++++++++++++++++++++++++++++++++++++++++++++"
            for sample in finalSampleList: print sample

        if len( allMonitored )> 0:
            print "\n++++++++++++++++++++++++++++++++++++++++++++"
            print "Submitted samples which are not final yet from music_crab3 configs"
            print "++++++++++++++++++++++++++++++++++++++++++++"
            for sample in allMonitored: print sample

        if len( allNoFolder )> 0:
            print "++++++++++++++++++++++++++++++++++++++++++++"
            print "Samples without crab folder ( unsubmitted ?) "
            print "++++++++++++++++++++++++++++++++++++++++++++"
            for sample in allNoFolder: print sample
            print "\n Created music_crab configs with unsubmitted sample:"
            for mcrFile in missingFiles: print mcrFile
    sys.exit(0)

if __name__ == '__main__':
    main()
