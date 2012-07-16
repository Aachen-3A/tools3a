#!/usr/bin/env python

import datetime
import sys
import os
import optparse
import re
from ConfigParser import SafeConfigParser
import subprocess
from fnmatch import fnmatch
from FWCore.PythonUtilities.LumiList import LumiList


lumi_dir = os.path.join( os.environ[ 'CMSSW_BASE' ], 'src/MUSiCProject/Skimming/test/lumi' )
lumi_map_file = os.path.join( lumi_dir, 'lumi-map.txt' )

date = datetime.datetime.today().isoformat('\0')

parser = optparse.OptionParser( description='Calculate the integrated luminosity of CRAB tasks after applying additional luminosity-masks',  usage='usage: %prog [options] CRAB_TASKS...' )
parser.add_option( '-d', '--lumi-dir', metavar='DIR', default=lumi_dir, help='Directory containing luminosity-masks [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/lumi]' )
parser.add_option( '-l', '--lumi-map', metavar='FILE', default=lumi_map_file, help='Pattern file to map tasks on lumi-masks [default: $CMSSW_BASE/src/MUSiCProject/Skimming/test/lumi/lumi-map.txt]' )
parser.add_option( '-a', '--all-lumi', action='store_true', default=False, help='Do not apply further lumi-masks [default: %default]' )
parser.add_option( '-L', '--lumiCalc', action = 'store_true', default = False,
                   help = 'Use lumiCalc.py instead of pixelLumiCalc.py. [default: %default]' )
parser.add_option(       '--lumiCalc2', action = 'store_true', default = False,
                   help = 'Use lumiCalc2.py instead of pixelLumiCalc.py .[default: %default]' )
parser.add_option( '-o', '--output', metavar='OUTFILE', help='Store output in OUTFILE [default: lumi-<date>.txt]' )
parser.add_option( '-w', '--without', action = 'store_true', default = False,
                   help = 'Do not use correction in lumi calculation (lumiCalc2.py and pixelLumiCalc.py only). [default: %default]' )

(options, tasks ) = parser.parse_args()

# default unit is /pb
units = dict( [ ['/\xce\xbcb', 1e-6 ], [ '/ub', 1e-6 ], [ '/nb', 1e-3 ], [ '/pb', 1 ], [ '/fb', 1e3 ] ] ) # \xce\xbc represents the 'micro sign' in unicode

if not tasks:
   parser.error( 'Needs at least one task to work on.' )

if options.lumiCalc and options.lumiCalc2:
   parser.error( 'Not allowed to use both --lumiCalc and --lumiCalc2 at the same time!' )

del parser

if options.lumiCalc:
   options.lumi = 'lumiCalc.py'
elif options.lumiCalc2:
   options.lumi = 'lumiCalc2.py'
else:
   options.lumi = 'pixelLumiCalc.py'

if not options.output:
   options.output = 'lumi-' + date + '.txt'

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

# use tee to redirect stdout to a file
tee = subprocess.Popen( [ 'tee', options.output ], stdin = subprocess.PIPE )
os.dup2( tee.stdin.fileno(), sys.stdout.fileno() )

print '\nCalculating integrated lumi in pb^-1:\n'

#loop over generated lumi-files and get the lumi
for json in jsons_to_read:
   print os.path.splitext( json )[0], '\t',

   #get the lumi information
   args = [ options.lumi, 'overview', '-i', json ]
   if options.lumiCalc or options.lumiCalc2:
      args = [ options.lumi, '-b', 'stable', 'overview', '-i', json ]
   if options.without and not options.lumiCalc:
      args += [ '--without-correction' ]
   proc = subprocess.Popen( args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )

   output = proc.communicate()[0]
   if proc.returncode != 0:
      print '\nCalling %s failed. Output:' %options.lumi
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
      print '\nUnexpected output from %s:' %options.lumi
      print output
      sys.exit(1)

   # get unit
   unit_line = line.next().split( '|' )
   recorded = unit_line[4]
   unit = re.compile('\(([^)]*)\)').search( recorded )
   if unit:
      unit = unit.groups()[0]
   else:
      print '\nUnexpected output from %s:' %options.lumi
      print output
      sys.exit(1)
   conversion = units[ unit ]
   #drop one line
   line.next()
   #now get the right line
   split_line = line.next().split( '|' )
   rec_lumi = float( split_line[4] )
   rec_lumi *= conversion
   print rec_lumi
