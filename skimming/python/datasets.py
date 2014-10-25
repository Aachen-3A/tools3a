#!/usr/bin/env python

import optparse

datastreams = [
               'SingleMu',
               'SingleElectron',
               'Tau',
               'TauPlusX',
               'Photon',
               'Jet',
               'MET',
               'METBTag',
               'DoubleMu',
               'DoubleElectron',
               'MuEG',
               ]

usage = "%prog RECO DCSFILES"

description  = "This script helps to create a .txt file with information needed "
description += "for data skimming. It assignes the given DSCFILES to the "
description += "datastreams depending on the run range in the DSCFILE names. "
description += "RECO is the full (Re)Reco label and version plus data tier, e.g. 'Run2011A-08Nov2011-v1/RECO' or 'Run2012A-PromptReco-v1/AOD'. "
description += "DSCFILES is a list of files in the form of DCS-<runmin>-<runmax>.json. "

parser = optparse.OptionParser( usage = usage, description = description )
parser.add_option( '-o', '--output', metavar = 'FILENAME', default = 'data.txt',
                   help = 'Set the filename where the results shall be stored.  [default = %default]' )
parser.add_option( '-c', '--config', metavar = 'CONFIGNAME', default = 'data_cfg.py',
                   help = 'Set the name of the config file that shall be used for this dataset(s). [default = %default]' )
parser.add_option( '-d', '--datastreams', metavar = 'DATASTREAMS', default = ','.join( datastreams ),
                   help = 'Set the datastream(s) (aka. Primary Datasets) you want to skim. [default = %default]' )

( options, args ) = parser.parse_args()

if len( args ) < 2:
    parser.error( 'Exactly one RECO and at least one DCSFILE needed!' )

reco = args[0].strip( ' /' )
del args[0]

if options.datastreams:
    options.datastreams = options.datastreams.split( ',' )
else:
    options.datastreams = datastreams

file = open( options.output , 'w' )

if options.config:
    print >> file, 'config = ' + options.config
    print >> file

for filename in args:
    run_min = filename.split( '-' )[1]
    run_max = filename.split( '-' )[2].split( '.' )[0]

    if not filename.startswith( 'DCS-' ) or not filename.endswith( '.json' ) or not run_min.isdigit() or not run_max.isdigit():
        parser.error( "Expected files in the form of DCS-<runmin>-<runmax>.json but found: '%s'!" % filename )

    for datastream in options.datastreams:
        string = 'Data' + '_' + run_min + '_' + run_max + '_' + datastream + ':/' + datastream + '/' + reco + ';' + filename
        print >> file, string

    # Do not put an empty line at the end of the file.
    if not filename == args[-1]:
        print >> file

file.close()
