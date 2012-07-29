#!/usr/bin/env python

import logging
import optparse
import os
import re
import sys

log = logging.getLogger( 'parseSampleList' )

def main():

    usage = '%prog [options] SAMPLELIST'
    description = 'This script parses a given file SAMPLELIST, created e.g. by checkSampleStatus.py, and writes a customizable list that can be given to music_crab.'

    parser = optparse.OptionParser( usage = usage, description = description, version = '%prog 0' )
    parser.add_option( '-c', '--config', action = 'store', default = 'mc_cfg.py', metavar = 'CONFIG',
                       help = 'Specify the CMSSW config file you want to use for skimming. [default = %default]' )
    parser.add_option( '-p', '--prefix', action = 'store', default = None, metavar = 'PREFIX',
                       help = 'Specify a PREFIX for your output filename (e.g.  production version). [default = %default]' )
    parser.add_option( '-P', '--postfix', action = 'store', default = None, metavar = 'POSTFIX',
                       help = 'Specify a POSTFIX for every process name in the output file. [default = %default]' )
    parser.add_option( '-t', '--timestamp', action = 'store', default = None, metavar = 'TIMESTAMP',
                       help = 'Only parse parts of list that were written after a specific date, e.g., use when additional new samples are avaliable. [default = %default]' )
    parser.add_option( '-f', '--force', action = 'store_true', default = False,
                       help = 'Force overwriting of output files.' )
    parser.add_option(       '--debug', metavar = 'LEVEL', default = 'INFO',
                       help = 'Set the debug level. Allowed values: ERROR, WARNING, INFO, DEBUG. [default: %default]' )
    ( options, args ) = parser.parse_args()

    # Set loggig format and level
    #
    format = '%(levelname)s at %(asctime)s: %(message)s'
    logging.basicConfig( filename = 'log_parseSampleList.txt', filemode = 'w', level = logging._levelNames[ options.debug ], format = format, datefmt = '%F %H:%M:%S' )

    console = logging.StreamHandler()
    console.setLevel( logging._levelNames[ options.debug ] )
    log.addHandler( console )

    if options.prefix:
        options.prefix += '_'
    if options.prefix == None:
        options.prefix = ''

    log.debug( 'Parsing files: ' + ' '.join( args ) )

    if len( args ) != 1:
        parser.error( 'Exactly 1 argument needed: state the file you want to parse.' )

    generators = []
    generators.append( [ 'madgraph', 'MG', [] ] )
    generators.append( [ 'powheg',   'PH', [] ] )
    generators.append( [ 'herwig6',  'HW', [] ] )
    generators.append( [ 'herwigpp', 'HP', [] ] )
    generators.append( [ 'herwig',   'HW', [] ] )
    generators.append( [ 'sherpa',   'SP', [] ] )
    generators.append( [ 'mcatnlo',  'MC', [] ] )
    generators.append( [ 'alpgen',   'AG', [] ] )
    generators.append( [ 'pythia6',  'P6', [] ] )
    generators.append( [ 'pythia8',  'P8', [] ] )
    generators.append( [ 'pythia',   'PY', [] ] )
    generators.append( [ 'comphep',  'CH', [] ] )

    print_list = {}

    sample_file = open( args[0], 'r' )

    valid_ts = ''
    starting_line = 0
    if options.timestamp:
        for num, line in enumerate( sample_file ):
            if options.timestamp in line:
                log.info( 'Found timestamp: ' + options.timestamp + '\n' )
                valid_ts = options.timestamp
                starting_line = num
                break
        else:
            log.info( "Timestamp: '%s' does not appear in the file  -> exiting..." % str( options.timestamp ) )
            sys.exit( 0 )

    sample_file.seek( 0 )

    all_samples = list()
    samples     = list()

    for num, line in enumerate( sample_file ):
        if line.startswith( '/' ):
            if num > starting_line:
                dataset    = line.strip()
                samplename = abbrDatasetName( dataset )
                for gen in generators:
                    key     = gen[0]
                    value   = gen[1]
                    samples = gen[2]

                    ( samplename, n ) = re.subn( r'(?i).' + key, '', samplename )       # subn() performs sub(), but returns tuple (new_string, number_of_subs_made)

                    if n > 0:
                        if options.postfix: samplename += '_' + options.postfix
                        samplename += '_' + value

                        # If the showering is done with pythia and this is also given in
                        # the original sample name delete 'pythia' from samplename
                        #
                        for g in [ 'pythia8', 'pythia6', 'pythia' ]:
                            samplename = re.sub( r'(?i).' + g, '', samplename )

                        samples.append( samplename + ':' + dataset )
                        break
                else:
                    log.warning( 'Dataset produced with unkown generator: %s' %samplename )
                samplename += ':'
                print_list[ dataset ] = samplename


    for gen in generators:
        if gen[2]:
            gen_file_name = 'mc_' + options.prefix + gen[0] + '.txt'
            if os.path.exists( gen_file_name ) and not options.force:
                raise Exception( "Found file '%s'! If you want to overwrite it use --force." % gen_file_name )
            else:
                file = open( gen_file_name, 'w' )
            print >> file, 'config = ' + options.config
            print >> file

            for sample in sorted( gen[2] ):
                print >> file, sample
            file.close()

    length = max( map( len, print_list.values() ) )
    for name, sample in sorted( print_list.items() ):
        log.info( sample.ljust( length ) + name )

    log.info( '\n----> Total number of datasets: %s\n' % len( print_list ) )


def abbrDatasetName( datasetName ):
    datasetName = datasetName.replace( '/', '' )
    # FIXME: This should be in a config file or something.
    #
    tags = [ 'AODSIM',
             '.8TeV',
             '.7TeV',
             'Fall11',
             'Summer12',
             '.TuneZ2Star',
             '.TuneZ2',
             '.Tune4C',
             '.TuneD6T',
             '.Tune23',
             '.CT10',
             '.tarball',
             '.cff',
             '.START42_V14B-v.',
             '.START44_V4-v.',
             '.START44_V5-v.',
             '.START44_V9B-v.',
             '.START44_V10-v.',
             '.START50_V15-v.',
             '.START52_V5-v.',
             '.START52_V9-v.',
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
