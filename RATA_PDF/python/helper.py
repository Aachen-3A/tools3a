#!/usr/bin/env python

import os
import sys
import subprocess
sys.path.append("config/")
sys.path.append("lib/")
from configobj import ConfigObj
import time
from datetime import datetime
import ROOT as r
import logging
from ctypes import *

info = r.ProcInfo_t()

class bcolors:
    HEADER = '\033[35m'
    OKBLUE = '\033[34m'
    OKGREEN = '\033[32m'
    WARNING = '\033[33m'
    FAIL = '\033[31m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.CYAN = ''
        self.WHITE = ''
        self.ENDC = ''

# getTerminalSize() : Displays the width and height of the current terminal
def getTerminalSize():
    env = os.environ
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct, os
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
        '1234'))
        except:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))
    return int(cr[1]), int(cr[0])

# update_progress() : Displays or updates a console progress bar
## Accepts a float between 0 and 1. Any int will be converted to a float.
## A value under 0 represents a 'halt'.
## A value at 1 or bigger represents 100%
def update_progress(progress):
    (width, height) = getTerminalSize()
    barLength = width-30 # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = bcolors.FAIL+"error: progress var must be float\r\n"+bcolors.ENDC
    if progress < 0:
        progress = 0
        status = bcolors.WARNING+"Halt...\r\n"+bcolors.ENDC
    if progress >= 1:
        progress = 1
        status = bcolors.OKGREEN+"Done...\r\n"+bcolors.ENDC
    block = int(round(barLength*progress))
    text = "\rPercent: [{0}] {1}% {2}".format( bcolors.HEADER+"#"*block+bcolors.ENDC + "-"*(barLength-block), int(progress*100), status)
    sys.stdout.write(text)
    sys.stdout.flush()

def welcome_output():
    logging.info("starting RATA PDF")
    logging.info("")
    print("")
    print(bcolors.HEADER+"8 888888888o.            .8.    8888888 8888888888   .8.                    8 888888888o   8 888888888o.      8 8888888888   ")
    print("8 8888    `88.          .888.         8 8888        .888.                   8 8888    `88. 8 8888    `^888.   8 8888         ")
    print("8 8888     `88         :88888.        8 8888       :88888.                  8 8888     `88 8 8888        `88. 8 8888         ")
    print("8 8888     ,88        . `88888.       8 8888      . `88888.                 8 8888     ,88 8 8888         `88 8 8888         ")
    print("8 8888.   ,88'       .8. `88888.      8 8888     .8. `88888.                8 8888.   ,88' 8 8888          88 8 888888888888 ")
    print("8 888888888P'       .8`8. `88888.     8 8888    .8`8. `88888.               8 888888888P'  8 8888          88 8 8888         ")
    print("8 8888`8b          .8' `8. `88888.    8 8888   .8' `8. `88888.              8 8888         8 8888         ,88 8 8888         ")
    print("8 8888 `8b.       .8'   `8. `88888.   8 8888  .8'   `8. `88888.             8 8888         8 8888        ,88' 8 8888        ") 
    print("8 8888   `8b.    .888888888. `88888.  8 8888 .888888888. `88888.            8 8888         8 8888    ,o88P'   8 8888       ")  
    print("8 8888     `88. .8'       `8. `88888. 8 8888.8'       `8. `88888.           8 8888         8 888888888P'      8 8888      "+bcolors.ENDC)
    print("")
    print("RWTH Aachen Three A Parton Distribution Functions calculator")
    print("")
    print(time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
    print("")

def farewell_output(t0,t1):
	print("")
	print("-"*20)
	print(bcolors.HEADER + "\t All calculations done" + bcolors.ENDC)
	print("-"*20)
	print("\t" + time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
	print("\truntime in seconds : ")
	print("\t" + bcolors.OKGREEN + str(time.clock() - t0) + bcolors.ENDC + " (process time)")
	print("\t" + bcolors.OKGREEN + str(time.time() - t1) + bcolors.ENDC + " (wall time)")
	print("")
	r.gSystem.GetProcInfo(info)
	print("\tmemory in MB : ")
	print("\t" + bcolors.OKGREEN + str(info.fMemResident/1000.) + bcolors.ENDC + " (resident) ")
	print("\t" + bcolors.OKGREEN + str(info.fMemVirtual/1000.) + bcolors.ENDC + " (virtual) ")
	print("-"*20)
	print("")
	print("")
	raw_input("The program will now crash")

def Usage():
	return '%prog [options] CONFIG_FILE'

def option_parsing():
    import optparse

    date_time = datetime.now()
    usage = Usage()
    parser = optparse.OptionParser( usage = usage )
    parser.add_option( '--debug', metavar = 'LEVEL', default = 'INFO',
                       help= 'Set the debug level. Allowed values: ERROR, WARNING, INFO, DEBUG. [default = %default]' )
    parser.add_option( '--logfile', default = 'log_file.log',
                       help= 'Set the logfile. [default = %default]' )

    run_group = optparse.OptionGroup( parser, 'Run options', 'The same as for the Run executable!' )

    run_group.add_option( '-s', '--Signal', action = 'store_true', default = False,
                            help = 'Run on Signal samples. [default = %default]' )
    run_group.add_option( '-b', '--Background', action = 'store_true', default = False,
                            help = 'Run on Background samples. [default = %default]' )
    parser.add_option_group( run_group )

    cfg_group = optparse.OptionGroup( parser, 'Cfg options', 'The same as for the Run executable!' )

    cfg_group.add_option( '-a', '--SignalCfg', default = 'Sig.cfg', metavar = 'DIRECTORY',
                            help = 'Signal sample config file. [default = %default]' )
    cfg_group.add_option( '-e', '--BackgroundCfg', default = 'Bag.cfg', metavar = 'DIRECTORY',
                            help = 'Signal sample config file. [default = %default]' )
    cfg_group.add_option( '-c', '--XsCfg', default = 'xs.cfg', metavar = 'DIRECTORY',
                            help = 'Cross section config file. [default = %default]' )
    cfg_group.add_option( '-d', '--PDFCfg', default = 'pdf.cfg', metavar = 'DIRECTORY',
                            help = 'PDF config file. [default = %default]' )
    parser.add_option_group( cfg_group )

    ( options, args ) = parser.parse_args()

    if not options.Signal and not options.Background:
        parser.error( 'Specify to either run on the Signal or on the Background samples!' )
        
    numeric_level = getattr(logging, options.debug.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % options.debug)
    if os.path.exists(options.logfile):
        os.remove(options.logfile)
    logging.basicConfig(filename=options.logfile,level=numeric_level,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    return options,numeric_level

def config_parsing(options):
    if options.Signal:
        try:
            mc_cfg = ConfigObj(options.SignalCfg)
            logging.info('read config file %s',options.SignalCfg)
        except IOError as e:
            print("There was a error reading the File "+options.SignalCfg)
            print(e)
            logging.error("There was a error reading the File "+options.SignalCfg)
            logging.error(e)
            exit()
    else:
        try:
            mc_cfg = ConfigObj(options.BackgroundCfg)
            logging.info('read config file %s',options.BackgroundCfg)
        except IOError as e:
            print("There was a error reading the File "+options.BackgroundCfg)
            print(e)
            logging.error("There was a error reading the File "+options.BackgroundCfg)
            logging.error(e)
            exit()
    try:
        xs_cfg = ConfigObj(options.XsCfg)
        logging.info('read config file %s',options.XsCfg)
    except IOError as e:
        print("There was a error reading the File "+options.XsCfg)
        print(e)
        logging.error("There was a error reading the File "+options.XsCfg)
        logging.error(e)
        exit()
    try:
        pdf_cfg = ConfigObj(options.PDFCfg)
        logging.info('read config file %s',options.PDFCfg)
    except IOError as e:
        print("There was a error reading the File "+options.PDFCfg)
        print(e)
        logging.error("There was a error reading the File "+options.PDFCfg)
        logging.error(e)
        exit()
    return mc_cfg,xs_cfg,pdf_cfg

def control_output(options,mc_cfg,pdf_cfg,xs_cfg):
	print("\n"+"-"*20)
	if options.Signal:
		print("\t Running on "+bcolors.OKGREEN+"Signal"+bcolors.ENDC+" samples")
		logging.info("Running on Signal samples")
	else:
		print("\t Running on "+bcolors.OKGREEN+"Background"+bcolors.ENDC+" samples")
		logging.info("Running on Background samples")
	print("-"*20)
	print("\t PDF sets to be used:")
	logging.info("PDF sets to be used:")
	for pdfs in pdf_cfg["PDFs"]:
		print("\t  -"+bcolors.OKGREEN+pdfs+bcolors.ENDC)
		logging.info("-"+pdfs)
	print("-"*20)
	print("\t MC samples to be used:")
	logging.info("MC samples to be used:")
	for sample in mc_cfg["samples"]:
		print("\t  -"+bcolors.OKGREEN+sample+bcolors.ENDC+"  xs: "+xs_cfg[sample]["xs"])
		logging.info("-"+sample+"  xs: "+xs_cfg[sample]["xs"])

def get_event_number_list(mc_cfg,pdf_cfg,path):
	filelist = []
	total_events = 0.
	eventlist = {}
	for sg in mc_cfg["samples"]:
		dummy_events = get_event_number(path+sg+".root",pdf_cfg["Tree"]["tree_name"],pdf_cfg["Tree"]["cut_string"])
		if dummy_events > 0:
			total_events += dummy_events
			eventlist.update({sg:dummy_events})
			filelist.append(sg)
	print("-"*20)
	print("\t Running on "+bcolors.OKGREEN+str(int(total_events))+bcolors.ENDC+" events")
	print("-"*20+"\n")
	return filelist,total_events,eventlist

def get_event_number(file_name,tree_name,cut_string):
	try:
		tfile = r.TFile(file_name,"READ")
		tree = tfile.Get(tree_name)
		dummy_file = r.TFile("tmp/tmpFile2_.root","RECREATE")
		smallerTree = tree.CopyTree(cut_string)
		nentries = smallerTree.GetEntries()
		tfile.Close()
		dummy_file.Close()
		return nentries
	except:
		print("-"*20)
		print("\t Can't read "+bcolors.FAIL+file_name+bcolors.ENDC+", skipping it")
		print("-"*20)
		return 0

def check_file(file_name):
	tfile = r.TFile(file_name,"READ")
	hists = tfile.GetListOfKeys().GetSize()
	if hists <= 1:
		print("-"*20)
		print("\t Output of "+bcolors.FAIL+file_name+bcolors.ENDC+" is not okay, will not be used for PDF calculation")
		print("-"*20)
		return False
	else:
		return True

def final_file_check(options,run_samples,pdf_cfg):
	print("-"*20)
	print("\t Now checking all output files")
	print("-"*20+"\n")
	if options.Signal:
		for sg in run_samples:
			if not check_file(sg):
				run_samples.remove(sg)
	else:
		for sg in run_samples:
			if not check_file(sg):
				run_samples.remove(sg)
		print("-"*20)
		print("\t Now merging all background files")
		print("-"*20+"\n")
		command= "hadd -f9 "+pdf_cfg["general"]["temp_path"]+"allMCs.root "
		for i in run_samples:
			command += " " + i
		p = subprocess.Popen(command,shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
		out, err = p.communicate()
		logging.debug(out)
		logging.debug(err)
		run_samples = [pdf_cfg["general"]["temp_path"]+"allMCs.root"]
	return run_samples

def make_c_parameters(mc_cfg,pdf_cfg):
	paras = {}
	logging.info('preparing paramters for C++ functions ...')
	# Get the path of the MC samples:
	path = mc_cfg["general"]["path"]
	paras.update({"path":path})
	logging.debug('path: %s',path)
	# Get the name of the tree branches:
	c_branches = (c_char_p * 7)()
	logging.debug('branches: %s, %s, %s, %s, %s, %s, %s',
	pdf_cfg["Tree"]["b_pdf_scale"],
	pdf_cfg["Tree"]["b_pdf_id1"],
	pdf_cfg["Tree"]["b_pdf_id2"],
	pdf_cfg["Tree"]["b_pdf_x1"],
	pdf_cfg["Tree"]["b_pdf_x2"],
	pdf_cfg["Tree"]["b_observe"],
	pdf_cfg["Tree"]["b_weight"])
	paras.update({"branches":c_branches})
	c_branches[:] = [pdf_cfg["Tree"]["b_pdf_scale"],
	pdf_cfg["Tree"]["b_pdf_id1"],
	pdf_cfg["Tree"]["b_pdf_id2"],
	pdf_cfg["Tree"]["b_pdf_x1"],
	pdf_cfg["Tree"]["b_pdf_x2"],
	pdf_cfg["Tree"]["b_observe"],
	pdf_cfg["Tree"]["b_weight"]]
	# Get the names of the PDF sets:
	n_pdf_sets = 0
	PDFsets = []
	for i_pdf in pdf_cfg["PDFs"]:
		n_pdf_sets+=1
		PDFsets.append(i_pdf)
		logging.debug('append pdf set: %s',i_pdf)
	logging.debug('number of pdf sets: %i',n_pdf_sets)
	c_PDFsets = (c_char_p * n_pdf_sets)()
	c_PDFsets[:] = PDFsets
	paras.update({"PDFSets":c_PDFsets})
	paras.update({"n_pdfs":c_int(n_pdf_sets)})
	# Get the path of the PDF sets
	PDFPath = pdf_cfg["general"]["PDFpath"]
	paras.update({"PDF_path":create_string_buffer(PDFPath)})
	logging.debug('pdf_path: %s',PDFPath)
	# Get the histogram binning
	dummy_binning = []
	logging.debug('binning:')
	for i in mc_cfg["general"]["binning"]:
		dummy_binning.append(float(i))
		logging.debug(i)
	c_binning = (c_double * len(dummy_binning))()
	c_binning[:] = dummy_binning
	paras.update({"n_bins":c_int(len(dummy_binning))})
	paras.update({"binning":c_binning})
	paras.update({"tree_name":create_string_buffer(pdf_cfg["Tree"]["tree_name"])})
	paras.update({"cut_string":create_string_buffer(pdf_cfg["Tree"]["cut_string"])})
	paras.update({"lumi":c_double(float(mc_cfg["general"]["lumi"]))})

	logging.debug('number of bins: %i',len(dummy_binning))
	logging.info('done')
	return paras

def helper():
	print(bcolors.OKBLUE+"#"*100,"\n","#"*49+bcolors.FAIL+"#"+bcolors.OKBLUE+"#"*50)
	print("#"*49+bcolors.FAIL+"#"*3+bcolors.OKBLUE+"#"*48)
	print("#"*48+bcolors.FAIL+"#"*5+bcolors.OKBLUE+"#"*47)
	print("#"*100)
	print("#"*46+bcolors.FAIL+"#"*9+bcolors.OKBLUE+"#"*45)
	print("#"*45+bcolors.FAIL+"#"*11+bcolors.OKBLUE+"#"*44)
	print("#"*100)
	print("#"*43+bcolors.FAIL+"#"*15+bcolors.OKBLUE+"#"*42)
	print("#"*42+bcolors.FAIL+"#"*17+bcolors.OKBLUE+"#"*41)
	print("#"*100)
	print("#"*40+bcolors.FAIL+"#"*21+bcolors.OKBLUE+"#"*39)
	print("#"*39+bcolors.FAIL+"#"*23+bcolors.OKBLUE+"#"*38)
	print("#"*100)
	print("#"*38+bcolors.FAIL+"#"*25+bcolors.OKBLUE+"#"*37)
	print("#"*37+bcolors.FAIL+"#"*27+bcolors.OKBLUE+"#"*36)
	print("#"*100)
	print("#"*35+bcolors.FAIL+"#"+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*20+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"+bcolors.OKBLUE+"#"*35)
	print("#"*34+bcolors.FAIL+"#"*3+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*18+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*3+bcolors.OKBLUE+"#"*34)
	print("#"*100)
	print("#"*32+bcolors.FAIL+"#"*7+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*14+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*7+bcolors.OKBLUE+"#"*32)
	print("#"*31+bcolors.FAIL+"#"*9+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*12+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*9+bcolors.OKBLUE+"#"*31)
	print("#"*100)
	print("#"*30+bcolors.FAIL+"#"*11+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*9+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*13+bcolors.OKBLUE+"#"*29)
	print("#"*29+bcolors.FAIL+"#"*13+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*7+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*15+bcolors.OKBLUE+"#"*28)
	print("#"*100)
	print("#"*27+bcolors.FAIL+"#"*17+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*4+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*17+bcolors.OKBLUE+"#"*27)
	print("#"*26+bcolors.FAIL+"#"*19+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"++"+bcolors.OKBLUE+"#"*4+bcolors.FAIL+"#"*19+bcolors.OKBLUE+"#"*26)
	print("#"*100)
	print("#"*24+bcolors.FAIL+"#"*23+bcolors.OKBLUE+"#"*7+bcolors.FAIL+"#"*23+bcolors.OKBLUE+"#"*23)
	print("#"*23+bcolors.FAIL+"#"*25+bcolors.OKBLUE+"#"*5+bcolors.FAIL+"#"*25+bcolors.OKBLUE+"#"*22)
	print("#"*100)
	print("#"*22+bcolors.FAIL+"#"*27+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*26+bcolors.OKBLUE+"#"*22)
	print("#"*21+bcolors.FAIL+"#"*28+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*28+bcolors.OKBLUE+"#"*20)
	print("#"*100)
	print("#"*19+bcolors.FAIL+"#"*30+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*30+bcolors.OKBLUE+"#"*18)
	print("#"*18+bcolors.FAIL+"#"*31+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*31+bcolors.OKBLUE+"#"*17)
	print("#"*100)
	print("#"*16+bcolors.FAIL+"#"*33+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*33+bcolors.OKBLUE+"#"*15)
	print("#"*15+bcolors.FAIL+"#"*34+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*34+bcolors.OKBLUE+"#"*14)
	print("#"*100)
	print("#"*14+bcolors.FAIL+"#"*35+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*35+bcolors.OKBLUE+"#"*13)
	print("#"*13+bcolors.FAIL+"#"*36+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*37+bcolors.OKBLUE+"#"*11)
	print("#"*100)
	print("#"*11+bcolors.FAIL+"#"*38+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*39+bcolors.OKBLUE+"#"*9)
	print("#"*10+bcolors.FAIL+"#"*39+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*40+bcolors.OKBLUE+"#"*8)
	print("#"*100)
	print("#"*8+bcolors.FAIL+"#"*41+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*41+bcolors.OKBLUE+"#"*7)
	print("#"*7+bcolors.FAIL+"#"*42+bcolors.OKBLUE+"#"*3+bcolors.FAIL+"#"*42+bcolors.OKBLUE+"#"*6)
	print("#"*100)
	print("#"*100)
	print("#"*9+bcolors.WHITE+"#"*13+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*5+bcolors.WHITE+"#"*7+bcolors.OKBLUE+"#"*8+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*6+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*3+bcolors.WHITE+"#"*12+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"*7)
	print("#"*8+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*3+bcolors.WHITE+"#"*9+bcolors.OKBLUE+"#"*8+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*5+bcolors.OKBLUE+"#"*5+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*13+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"*7)
	print("#"*8+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*4+bcolors.OKBLUE+"#"*3+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*8+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*6+bcolors.OKBLUE+"#"*4+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"*7)
	print("#"*8+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*12+bcolors.WHITE+"#"*8+bcolors.OKBLUE+"#"*4+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*8+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*18+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*12)
	print("#"*8+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*7+bcolors.OKBLUE+"#"*6+bcolors.WHITE+"#"*13+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*5+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"*7+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*12)
	print("#"*8+bcolors.WHITE+"#"*22+bcolors.OKBLUE+"#"*7+bcolors.WHITE+"#"*12+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*8+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"*7+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*12)
	print("#"*20+bcolors.WHITE+"#"*12+bcolors.OKBLUE+"#"*14+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*3+bcolors.WHITE+"#"*7+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*18+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*12)
	print("#"*8+bcolors.WHITE+"#"*18+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*5+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*4+bcolors.WHITE+"#"*6+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"*7+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*14)
	print("#"*8+bcolors.WHITE+"#"*14+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*4+bcolors.WHITE+"#"*4+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*13+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*5+bcolors.WHITE+"#"*5+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*13+bcolors.OKBLUE+"#"*7+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*12)
	print("#"*8+bcolors.WHITE+"#"*13+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*6+bcolors.WHITE+"#"*5+bcolors.OKBLUE+"#"+bcolors.WHITE+"#"*10+bcolors.OKBLUE+"##"+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*7+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*3+bcolors.WHITE+"#"*12+bcolors.OKBLUE+"#"*7+bcolors.WHITE+"#"*3+bcolors.OKBLUE+"#"*12)
	print("#"*100)
	print("#"*100)
	print("#"*100+bcolors.ENDC)
