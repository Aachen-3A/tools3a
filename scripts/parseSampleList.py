#!/usr/bin/env python

import logging
import optparse
import os
import re
import sys

from collections import defaultdict

log = logging.getLogger( 'parseSampleList' )

def main():

    usage = '%prog [options] SAMPLELIST'
    description = 'This script parses a given file SAMPLELIST, created e.g. by checkSampleStatus.py, and writes a customizable list that can be given to music_crab.'

    parser = optparse.OptionParser( usage = usage, description = description, version = '%prog 0' )
    parser.add_option( '-c', '--config', action = 'store', default = 'mc_cfg.py', metavar = 'CONFIG',
                       help = 'Specify the CMSSW config file you want to use for skimming. [default = %default]' )
    parser.add_option( '-e', '--cme', action = 'store', default = '13', metavar = 'ENERGY',
                       help = 'The center-of-mass energy for this sample' )
    parser.add_option( '-p', '--prefix', action = 'store', default = None, metavar = 'PREFIX',
                       help = 'Specify a PREFIX for your output filename (e.g.  production version). [default = %default]' )
    parser.add_option( '-P', '--postfix', action = 'store', default = None, metavar = 'POSTFIX',
                       help = 'Specify a POSTFIX for every process name in the output file. [default = %default]' )
    parser.add_option( '-t', '--no-timestamp', action = 'store_true', default = False,
                       help = 'Do not split the output files by timestamps. [default = %default]' )
    parser.add_option( '-f', '--force', action = 'store_true', default = False,
                       help = 'Force overwriting of output files.' )
    parser.add_option(       '--debug', metavar = 'LEVEL', default = 'INFO',
                       help = 'Set the debug level. Allowed values: ERROR, WARNING, INFO, DEBUG. [default: %default]' )
    ( options, args ) = parser.parse_args()

    # Set loggig format and level
    #
    format = '%(levelname)s (%(name)s) [%(asctime)s]: %(message)s'
    date = '%F %H:%M:%S'
    logging.basicConfig( level = logging._levelNames[ options.debug ], format = format, datefmt = date )

    if options.prefix:
        options.prefix += '_'
    if options.prefix == None:
        options.prefix = ''

    log.debug( 'Parsing files: ' + ' '.join( args ) )

    if len( args ) != 1:
        parser.error( 'Exactly 1 argument needed: state the file you want to parse.' )

    samples_by_timestamps = getSamplesByTimestamps( options, args[0] )

    generators = []
    generators.append( ( 'madgraph', 'MG', defaultdict( list ) ) )
    generators.append( ( 'powheg',   'PH', defaultdict( list ) ) )
    generators.append( ( 'herwig6',  'HW', defaultdict( list ) ) )
    generators.append( ( 'herwigpp', 'HP', defaultdict( list ) ) )
    generators.append( ( 'herwig',   'HW', defaultdict( list ) ) )
    generators.append( ( 'sherpa',   'SP', defaultdict( list ) ) )
    generators.append( ( 'mcatnlo',  'MC', defaultdict( list ) ) )
    generators.append( ( 'alpgen',   'AG', defaultdict( list ) ) )
    generators.append( ( 'calchep',  'CA', defaultdict( list ) ) )
    generators.append( ( 'comphep',  'CO', defaultdict( list ) ) )
    generators.append( ( 'lpair',    'LP', defaultdict( list ) ) )
    generators.append( ( 'pythia6',  'P6', defaultdict( list ) ) )
    generators.append( ( 'pythia8',  'P8', defaultdict( list ) ) )
    generators.append( ( 'pythia',   'PY', defaultdict( list ) ) )
    generators.append( ( 'gg2ww',    'GG', defaultdict( list ) ) )
    generators.append( ( 'gg2zz',    'GG', defaultdict( list ) ) )
    generators.append( ( 'gg2vv',    'GG', defaultdict( list ) ) )

    print_list = {}

    unkown_gen  = list()

    for timestamp, datasets in samples_by_timestamps.items():

        for dataset in datasets:
            samplename = abbrDatasetName( dataset )

            for gen in generators:
                key     = gen[0]
                value   = gen[1]

                # subn() performs sub(), but returns tuple (new_string, number_of_subs_made)
                ( samplename, n ) = re.subn( r'(?i).' + key, '', samplename )

                if n > 0:
                    if options.postfix: samplename += '_' + options.postfix
                    samplename += '_' + value

                    # If the showering is done with pythia or herwig and
                    # this is also given in the original sample name delete
                    # it from the sample name.
                    shower = [ 'pythia8',
                               'pythia6',
                               'pythia',
                               'herwigpp',
                               ]
                    for g in shower:
                        samplename = re.sub( r'(?i).' + g, '', samplename )

                    # Fill the list of samplenames + datasets into a defaultdict by timestamp
                    #
                    gen[2][ timestamp ].append( samplename + ':' + dataset )
                    break
            else:
                unkown_gen.append( samplename )
            # The following ist needed for the console output.
            #
            samplename += ':'
            print_list[ dataset ] = samplename


    for gen in generators:
        number = 1
        if gen[2].items():
            for timestamp, datasets in sorted( gen[2].items() ):

                # Create generic filenames by incrementing 'number' for the amount of timestamps
                # found and adding 'number' to the end of the filename.
                #
                if not options.no_timestamp:
                    gen_file_name = 'mc_' + options.prefix + gen[0] + '_%02d'%number + '.txt'
                # If not split by timestamps, we only need one filename.
                #
                else:
                    gen_file_name = 'mc_' + options.prefix + gen[0] + '.txt'

                if os.path.exists( gen_file_name ) and not options.force:
                    raise Exception( "Found file '%s'! If you want to overwrite it use --force." % gen_file_name )
                else:
                    file = open( gen_file_name, 'w' )
                    print >> file, 'generator = ' + gen[0].upper()
                    print >> file, 'CME = ' + options.cme
                    print >> file, 'config = ' + options.config
                    print >> file

                for dataset in sorted( datasets ):
                    print >> file, dataset

                file.close()
                number += 1

    length = max( map( len, print_list.values() ) )
    processes_samples = list()
    for name, sample in sorted( print_list.items() ):
        processes_samples.append( sample.ljust( length ) + name )

    log.info( 'Process names and sample names:\n' + '\n'.join( processes_samples ) )

    log.info( '----> Total number of datasets: %s  ' % len( print_list ) )

    if unkown_gen:
        log.warning( 'Dataset produced with unkown generator:\n' + '\n'.join( unkown_gen ) )


def getSamplesByTimestamps( options, filename ):
    file = open( filename, 'r' )

    file.seek( 0 )

    # Store samples by timestamps.
    samples_by_timestamps = defaultdict( list )

    for line in file.readlines():
        if options.no_timestamp:
            timestamp = 'noTimestamp'
        else:
            if line.startswith( '#' ):
                # Identify timestamp with regexs.
                if re.search( '# \d\d\d\d-\d\d-\d\d \d\d:\d\d', line ):
                    timestamp = line.strip()
        if line.startswith( "/" ):
            samples_by_timestamps[ timestamp ].append( line.strip() )

    file.close()

    return samples_by_timestamps


def abbrDatasetName( datasetName ):
    datasetName = datasetName.replace( '/', '' )
    # FIXME: This should be in a config file or something.
    #
    tags = [ 'MINIAODSIM',
             'AODSIM',
             '.8TeV',
             '.7TeV',
             #~ '13TeV',
             'DR74',
             '25ns',
             'RunIISpring15',
             'Fall11',
             '.Asympt',
             'Summer12_DR53X',
             'Summer12',
             #~ '.Spring15',
             '.TuneZ2Star',
             '.TuneCUETP8M1',
             '.TuneZ2',
             '.Tune4C',
             '.TuneD6T',
             '.Tune23',
             '.CT10',
             '.CTEQ6L1',
             '.tarball',
             '.cff',
             '.START.._V.-v.',
             '.START.._V..-v.',
             '.START.._V...-v.',
             '.START.._V.',
             '.START.._V..',
             '.START.._V...',
             '.MCRUN2_.._V.-v.',
             '.MCRUN2_.._V..-v.',
             '.MCRUN2.._V...-v.',
             '.MCRUN2.._V.',
             '.MCRUN2.._V..',
             '.MCRUN2.._V...',
             '.tauola',
             '.evtgen',
             '.photos',
             '.2MuEtaFilter',
             '.2MuPEtaFilter',
             ]

    for tag in tags:
        find = r'(?i)' + tag                              # using (?i) at the beginning of a regular expression makes it case insensitive
        datasetName = re.sub( find, '', datasetName )
    return datasetName

if __name__ == '__main__':
    main()
