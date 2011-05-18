#!/usr/bin/env python

import sys
import os
import optparse
from ConfigParser import SafeConfigParser
import subprocess
from fnmatch import fnmatch
from FWCore.PythonUtilities.LumiList import LumiList


lumi_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/lumi' )
lumi_map_file = os.path.join( lumi_dir, 'lumi-map.txt' )

parser = optparse.OptionParser( description='Calculate the integrated luminosity of CRAB tasks after applying additional luminosity-masks',  usage='usage: %prog [options] CRAB_TASKS...' )
parser.add_option( '-d', '--lumi-dir', metavar='DIR', default=lumi_dir, help='Directory containing luminosity-masks [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/lumi]' )
parser.add_option( '-l', '--lumi-map', metavar='FILE', default=lumi_map_file, help='Pattern file to map tasks on lumi-masks [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/lumi/lumi-map.txt]' )
parser.add_option( '-a', '--all-lumi', action='store_true', default=False, help='Do not apply further lumi-masks [default: %default]' )

(options, tasks ) = parser.parse_args()

if not tasks:
   parser.error( 'Needs at least one task to work on.' )

del parser


#generate the lumi-mask map
lumi_map = []
if not options.all_lumi:
   for line in open( options.lumi_map ):
      if not line.startswith( '#' ):
         line = line.split( '#' )[0].strip()
         if line:
            (pattern,file) = line.split( ':' )
            lumis = LumiList( os.path.join( options.lumi_dir, file ) )
            lumi_map.append( (pattern, file, lumis) )

jsons_to_read = []
#now work on all tasks
for task in tasks:
   #get the datasetpath and output file name
   parser = SafeConfigParser()
   parser.read( os.path.join( task, 'share/crab.cfg' ) )
   output_file_name = os.path.splitext( os.path.basename( parser.get( 'CMSSW', 'output_file' ) ) )[0] + '.json'
   datasetpath = parser.get( 'CMSSW', 'datasetpath' )
   #read the analyzed lumis
   ana_lumis = LumiList( os.path.join( task, 'res/lumiSummary.json' ) )
   
   if not options.all_lumi:
      #match it on a lumi-mask
      for pattern,file,cert_lumis in lumi_map:
         if fnmatch( datasetpath, pattern ):
            print 'Dataset %s matched with pattern %s to lumis %s' % (datasetpath, pattern, file)
            #build the output lumi list and write it
            output_lumis = cert_lumis & ana_lumis
            #take the first match, so break
            break
      else:
         print 'No valid pattern found!'
         sys.exit(1)
   else:
      #no matching and masking to be done, output=input
      output_lumis = ana_lumis

   #now write the result
   output_lumis.writeJSON( output_file_name )
   jsons_to_read.append( output_file_name )
   print 'Task %s with dataset %s written to %s' % (task, datasetpath, output_file_name)


print '\nCalculating integrated lumi:\n'
#loop over generated lumi-files and get the lumi
for json in jsons_to_read:
   print os.path.splitext( json )[0], '\t',
   #get the lumi information
   proc = subprocess.Popen( ['lumiCalc.py', '-b', 'stable', 'overview', '-i', json ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
   output = proc.communicate()[0]
   if proc.returncode != 0:
      print '\nCalling lumiCalc.py failed. Output:'
      print output
      sys.exit(1)
   #parse the output
   lines = output.splitlines()
   line = iter( lines )
   #look for the line containint 'Total'
   for i in line:
      if 'Total' in i:
         break
   else:
      print '\nUnexpected output from lumiCalc.py:'
      print output
      sys.exit(1)
   #drop two lines
   line.next()
   line.next()
   #now get the right line
   split_line = line.next().split( '|' )
   rec_lumi = float( split_line[4] )
   print rec_lumi
