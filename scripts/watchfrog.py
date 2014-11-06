#!/usr/bin/env python
import os,glob
import optparse
import logging
import fnmatch
# so far no additional command line parsing needed
import gridFunctions 

# Command line parsing is added in commandline_parsing
import crabFunctions

def main():
    
    # check if user has valid proxy
    gridFunctions.checkAndRenewVomsProxy()
    
    # first setup command line parsing and parse input
    tag = "2014-10-13"
    
    #~ sample = "WToMuNu_M_200_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v2MINI_P8"
    sample = "WprimeToENu_M_3800_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v1MINI_P8"
    #~ sample = "WToTauNu_M_200_13TeVSpring14miniaod-PU20bx25_POSTLS170_V5-v1MINI_P8"
    #~ sample = "QCD_Pt-170to300_13TeVSpring14miniaod-castor_PU20bx25_POSTLS170_V5-v1MINI_P8"
    user = "tpook"
    folder="/%s/MUSiC/%s/%s" % (user,tag,sample)
    dCacheFileList = gridFunctions.getdcachelist(folder,tag)

    #strip down FileList
    dCacheFileList =  [item for sublist in dCacheFileList for item in sublist]

    print len(dCacheFileList)
    print dCacheFileList
    
    os.chdir("./crab_%s"%sample)
    for file in glob.glob("*.pxlio"):
        print file
    
    os.chdir("..")
    

    
    crabController = crabFunctions.CrabController()
    
    print crabController.status(sample)
    
    import json
    statusJSON = crabController.status(sample)
    #~ print json.dumps(statusJSON, sort_keys=True, indent=2, separators=(',', ': '))
    import ast
    statusDict = ast.literal_eval(statusJSON)
    jobKeys = sorted(statusDict.keys())
    try:
        intJobkeys = [int(x) for x in jobKeys]
    except:
        print "error parsing job numers to int" 
        
    maxjobnumber = max(intJobkeys)
    print statusDict['1']
    
    nComplete = 0
    nFinished = 0
    
    # loop through jobs
    for key in jobKeys:
        job = statusDict[key]
         #check if all completed files are on decache
        if 'finished' in job['State']:
            nFinished +=1
            outputFilename = "%s_%s"%( sample, key)
            #~ print outputFilename
            if any(outputFilename in s for s in dCacheFileList):
                nComplete +=1
                
    print "finished %d"%nFinished
    print "completed (file found on dCache ) %d"%nComplete

#~ class Overview:
    #~ def __init__(self, stdscr, tasks, resubmitList):


def commandline_parsing():
    parser = optparse.OptionParser( description='Watchfrog helps you to care for your jobs',  usage='usage: %prog [options]' )
    parser.add_option( '-o', '--only', metavar='PATTERNS', default=None,
                       help='Only check samples matching PATTERNS (bash-like ' \
                            'patterns only, comma separated values. ' \
                            'E.g. --only QCD* ). [default: %default]' )
    parser.add_option( '-u','--user', help='Alternative username [default is HN-username]')
    parser.add_option( '--workingArea',metavar='DIR',default=os.getcwd(),help='The area (full or relative path) where the CRAB project directories are saved. ' \
                     'Defaults to the current working directory.'       )  

    parsingController = crabFunctions.CrabController()
    # we need to add the parser options from other modules
    #get crab command line options
    parsingController.crab_commandlineOptions(parser)

    (options, args ) = parser.parse_args()
    now = datetime.datetime.now()
    isodatetime = now.strftime( "%Y-%m-%d_%H.%M.%S" )
    options.isodatetime = isodatetime    
    
    #get current user HNname
    if not options.user:
        options.user = parsingController.checkHNname()
    
    return (options, args )

def getAllCrabFolders(options):
    # get all crab folders in working directory
    crabFolders = [f for f in os.listdir(options.workingArea) if os.path.isdir(os.path.join(options.workingArea, f))]
    # check if only folders, which match certain patterns should be watched
    if options.only:
        filteredFolders = []
        # loop over all user specified patterns
        for pattern in options.only:
            #loop over all crabFolders in working directory and 
            # them to filteredFolder list if they match the pattern
            for crabFolder in crabFolders:
                if fnmatch.fnmatchcase( crabFolder, pattern ):
                    filteredFolders.append(crabFolder)
        crabFolders = filteredFolders
    if len(crabFolders) < 1:
        print "found no folder with crab_ in working directory"
        sys.exit()
    return crabFolders
        
    
if __name__ == '__main__':
    # get command line arguments
    (options, args ) = commandline_parsing()
    #~ test(options)
    curseshelpers.outputWrapper(main, 5,options,args)
