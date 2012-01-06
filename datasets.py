#!/usr/bin/env python

import optparse

datastreams = [ 'SingleMu', 'SingleElectron', 'Jet', 'MET', 'Photon', 'DoubleMu', 'Tau', 'TauPlusX' ]

usage = """./%prog DATASET DCSFILES
The DSCFILES must be in the form of DCS-<runmin>-<runmax>.json"""

description = """This script helps to create a .txt file with information needed
for data skimming. It assignes the given DSCFILES to the datastreams depending
on the run range of the DSCFILE."""

parser = optparse.OptionParser( usage = usage, description = description )
parser.add_option( '-o', '--output', metavar = 'FILENAME', default = 'data.txt',
                   help = 'Set the filename where the results shall be stored.  [default = %default]' )
parser.add_option( '-c', '--config', metavar = 'CONFIGNAME',
                   help = 'Set the name of the config file that shall be used for this dataset(s). No default.' )

( options, args ) = parser.parse_args()

if len( args ) < 2:
    parser.error( 'Exactly one DATASET and at least one DCSFILE needed!' )

dataset = args[0]
del args[0]

file = open( options.output , 'w' )

if options.config:
    print >> file, 'config = ' + options.config
    print >> file

for filename in args:
    run_min = filename.split( '-' )[1]
    run_max = filename.split( '-' )[2].split( '.' )[0]

    if not filename.startswith( 'DCS-' ) or not filename.endswith( '.json' ) or not run_min.isdigit() or not run_max.isdigit():
        parser.error( "Expected files in the form of DCS-<runmin>-<runmax>.json but found: '%s'!" % filename )

    for datastream in datastreams:
        string = 'Data' + '_' + run_min + '_' + run_max + '_' + datastream + ':/' + datastream + '/' + dataset + ';' + filename
        print >> file, string

    # Do not put an empty line at the end of the file.
    if not filename == args[-1]:
        print >> file

file.close()
