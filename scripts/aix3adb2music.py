#!/usr/bin/env python

## @package aix3a2music
# Create MUSiC config files from aix3db
#
#
# @author Tobias Pook

import datetime
import os, csv
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

#custom libs
import aix3adb
from  aix3adb import Aix3adbException

# some general definitions
COMMENT_CHAR = '#'
log_choices = [ 'ERROR', 'WARNING', 'INFO', 'DEBUG' ]
date = '%F %H:%M:%S'

# Write everything into a log file in the directory you are submitting from.
log = logging.getLogger( 'aix3a2music' )

def commandline_parsing():
    import argparse
    from cesubmit import getCernUserName
    user = getCernUserName()
    parser = argparse.ArgumentParser(description='Create music config files from DB')
    parser.add_argument('files', metavar='FILES', type=str, nargs='+',
                   help='A list of input files')
    parser.add_argument('--SkimmerTag',  type=str,
                   help='A timestamp used on dCache')
    parser.add_argument('--user',  type=str, default = user,
                   help='Change user name for dCache position default % s' % user)
    parser.add_argument('--inputType', default='music_crab',
                        choices=['music_crab','television'], # should add simple file lists 'skimid','sampleid','samplename'
                        help='Choose your input file format ') # sampleid and samplename use the latest skim
    #~ parser.add_argument('borders', metavar='N', type=int, nargs='+',
                   #~ help='A list min max  aix3adb Sample IDs [min1 max1 min2 max2]')
    args = parser.parse_args()
    return args

def readSampleFile(filename):
    outdict = {}
    sampledict = {}
    afterConfig = False
    existing = [] #]getExistingProcesses()

    with open(filename,'rb') as sample_file:
        for line in sample_file:
            line = line.strip()
            if not line or line.startswith( COMMENT_CHAR ): continue
            if COMMENT_CHAR in line:
                line, comment = line.split( COMMENT_CHAR, 1 )
            if line.startswith( 'generator' ):
                generator = line.split( '=' )[1].strip()
                outdict.update({'generator':generator})

            if line.startswith( 'config' ):
                afterConfig = True
            if afterConfig and not "config" in line:
                #lumi-mask and lumis-per-job can be specified in the command line
                (name,sample) = line.split( ':' )
                sampledict.update({name:(name,sample)})
    return outdict,sampledict


def getSkimAndSampleList(args):

    outlist = []
    for inputFile in args.files:
        if args.inputType == "music_crab":
            outdict, sampledict = readSampleFile( samplefile )
            for samplename in sampledict.keys():
                try:
                    dbSkim, dbSample = dblink.getMCLatestSkimAndSampleBySample( samplename )
                    outlist.append( (dbSkim, dbSample) )
                except Aix3adbException:
                    print "Sample %s not found in aix3adb" % samplename
        if args.inputType == "television":
            #little hack to use the remoteAnalysis Tool to read input files for it
            class ConfigDummy: pass
            from remoteAnalysis import readConfig
            dummy = ConfigDummy()
            dummy.__dict__['prepareConfigs'] = 'None'
            remoteDict = readConfig( dummy, [inputFile] )
            outlist += flattenedRemoteSkimDict( dummy, inputFile )

    return outlist


# @param datasections a list of section which contain data samples
def flattenRemoteSkimDict( remoteDict , datasections):
    remoteList = []
    for section in remoteDict.keys():
        if section in datasections:
            continue
        for ( skim, sample, arguments ) in remoteDict[section]:
            remoteList.append( (skim, sample) )
    return remoteList

def getConfigDicts( skimlist ):
    playlistdict = {}
    scalesdict = {}

    for ( dbSkim, dbSample ) in skimlist:

        if not dbSample.generator in playlistdict.keys():
            playlistdict.update( { dbSample.generator:[] } )
        if not dbSample.generator in scalesdict.keys():
            scalesdict.update( { dbSample.generator:[] } )

        dCachedir = '%s/PxlSkim/%s' %( dbSkim.owner, dbSkim.skimmer_version )
        outstring = "%s = %s::%s::MC_miniAOD.cfg" % (dbSample.id, dbSample.name, dCachedir ) #2015-02-08
        playlistdict[ dbSample.generator ].append( outstring )

        scalesdict[ dbSample.generator ].append( dbSample.name + '.' + 'aix3adbID = ' + dbSample.id )
        scalesdict[ dbSample.generator ].append( dbSample.name + '.' + 'XSec = ' + dbSample.crosssection )
        if dbSample.filterefficiency:
            scalesdict[ dbSample.generator ].append( dbSample.name + '.' + 'FilterEff = ' + dbSample.filterefficiency )
        else:
            scalesdict[ dbSample.generator ].append( dbSample.name + '.' + 'FilterEff = 1'  )
        scalesdict[ dbSample.generator ].append( dbSample.name + '.' + 'kFactor = ' + dbSample.kfactor )
        scalesdict[ dbSample.generator ].append( dbSample.name + '.' + 'ErrorMapping = ' + 'LO.CrosssectionError\n' )
        #~ print dbSkim.files

    return scalesdict, playlistdict

def writeConfigDicts( scalesdict, playlistdict , configdir='', lumi =1):
    lines = []
    lines.append('[GENERAL]')
    lines.append('type = mc')
    for generator in playlistdict.keys():
        lines.append( "\n[" + generator + "]")
        lines += playlistdict[ generator ]

    scalelines = []
    scalelines.append( "Lumi = %d" % lumi)
    scalelines.append( "LumiUnit = pb-1" )
    scalelines.append( "# Lumi error (CMS PAS LUM-13-001):" )
    scalelines.append( "Global.ScalefactorError = 0.026" )

    for generator in scalesdict.keys():
        scalelines.append( "\n###" + generator + "###")
        scalelines += scalesdict[ generator ]

    #~ print lines
    with open( os.path.join( configdir, 'playlist.txt'), 'wb' ) as playlistfile:
        playlistfile.write( '\n'.join( lines ) )

    with open( os.path.join( configdir, 'scales.txt'), 'wb' ) as scalelistfile:
        scalelistfile.write( '\n'.join( scalelines ) )

def main():

    args = commandline_parsing()

    dblink = aix3adb.createDBlink( args.user )

    skimlist = getSkimAndSampleList( args )

    scalesdict, playlistdict = getConfigDicts( skimlist )

    writeConfigDicts( scalesdict, playlistdict )


if __name__ == '__main__':
    main()
