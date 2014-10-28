#!/usr/bin/env python

######################################################################
# RATA_PDF  RWTH Aachen Three A Parton Distribution Functions
# calculate systematic uncertainties due to PDFs and alpha_S on
# background and signal Monte Carlo
#
# (C) Soeren Erdweg 2013-2014


from helper import *
from array import array
import multiprocessing

def init_pdfs(c_par,log_level,lib):
	print("-"*20)
	print("\t Now starting PDF initialisation")
	logging.info("Now starting PDF initialisation...")
	debug_level = 1
	if int(log_level) >= 20:
		debug_level = 1
	else:
		debug_level = 4
	lib.init_bg.restype = None
	lib.init_bg(c_par["PDF_path"],c_par["n_pdfs"],c_par["PDFSets"],c_int(debug_level))
	logging.info("done")
	print("\t"+bcolors.OKGREEN+" done"+bcolors.ENDC)
	print("-"*20)
	print("\t Now starting PDF histogram calculation")
	print("-"*20)
	update_progress(0.)

	# TBD: change to multiprocessing
	# TBD: calculate expected run time
def pdf_file_loop(eventlist,total_events,filelist,c_par,xs_cfg,mc_cfg,pdf_cfg,lib):
	run_samples = []
	done_events = 0
	#submit_list = []
	#for sg in filelist:
		#submit_list.append([create_string_buffer(path+sg+".root"),
		#create_string_buffer(pdf_cfg["Tree"]["tree_name"]),
		#create_string_buffer(pdf_cfg["Tree"]["cut_string"]),
		#c_branches,
		#c_double(float(mc_cfg["general"]["lumi"])),
		#c_double(float(xs_cfg[sg]["xs"])),
		#c_int(n_pdf_sets),
		#c_PDFsets,
		#create_string_buffer(mc_cfg["samples"][sg]["PDFset"]),
		#create_string_buffer(mc_cfg["general"]["histname"]),
		#create_string_buffer(pdf_cfg["general"]["temp_path"]+sg+".root"),
		#c_int(len(dummy_binning)),
		#c_binning])
		#run_samples.append(pdf_cfg["general"]["temp_path"]+sg+".root")
		#done_events += eventlist[sg]
		#update_progress(done_events/total_events)
  
	#lib.make_hists.restype = None
	#pool = multiprocessing.Pool()
	#results = pool.map_async(lib.make_hists, submit_list)
	#remainingOut = 0
	#while True:
		#time.sleep(2)
		#remaining = results._number_left
		#if remainingOut!=remaining:
			#print "Waiting for", remaining, "tasks to complete..."
			#remainingOut=remaining
		#if not pool._cache: break
	#pool.close()
	#pool.join()
	#res=(results.get())
	for sg in filelist:
		# Call C++ 'make_hists' function to create PDF histograms
		lib.make_hists.restype = None
		lib.make_hists(create_string_buffer(c_par["path"]+sg+".root"),
		c_par["tree_name"],
		c_par["cut_string"],
		c_par["branches"],
		c_par["lumi"],
		c_double(float(xs_cfg[sg]["xs"])),
		c_par["n_pdfs"],
		c_par["PDFSets"],
		create_string_buffer(mc_cfg["samples"][sg]["PDFset"]),
		create_string_buffer(mc_cfg["general"]["histname"]),
		create_string_buffer(pdf_cfg["general"]["temp_path"]+sg+".root"),
		c_par["n_bins"],
		c_par["binning"])

		run_samples.append(pdf_cfg["general"]["temp_path"]+sg+".root")
		done_events += eventlist[sg]
		update_progress(done_events/total_events)

	if os.path.exists("tmpFile_.root"):
		os.remove("tmpFile_.root")
		
	return run_samples

	# TBD: outfile parameters
	# TBD: include copier 'signal_mover.py'
def calc_pdf_uncer(run_samples,options,c_par,pdf_cfg,mc_cfg,lib):
	print("-"*20)
	print("\t Now starting PDF uncertainty calculation")
	print("-"*20+"\n")

	for sg in run_samples:
		out_file_1 = ""
		if options.Signal:
			out_file_1 = c_par["path"]+sg.replace(pdf_cfg["general"]["temp_path"],"")
		else:
			out_file_1 = mc_cfg["general"]["outfile"]
		out_par_1 = "UPDATE"
		for i_pdf in pdf_cfg["PDF_Groups"]:
			if pdf_cfg["PDF_Groups"][i_pdf]["type"] == "Hessian":
				dummy_pdf_members = []
				dummy_pdf_members.append(pdf_cfg["PDF_Groups"][i_pdf]["main"])
				for i in pdf_cfg["PDF_Groups"][i_pdf]["members"]:
					if i != pdf_cfg["PDF_Groups"][i_pdf]["main"]:
						dummy_pdf_members.append(str(i))
				c_pdf_1 = (c_char_p * len(dummy_pdf_members))()
				c_pdf_1[:] = dummy_pdf_members
				n_pdf_sets_1 = len(dummy_pdf_members)
				c_as_plus_number = (c_int * 2)()
				dummy_as_plus_number = []
				for i in pdf_cfg["PDF_Groups"][i_pdf]["as_plus_member"]:
					dummy_as_plus_number.append(int(i))
				c_as_plus_number[:] = dummy_as_plus_number
				c_as_minus_number = (c_int * 2)()
				dummy_as_minus_number = []
				for i in pdf_cfg["PDF_Groups"][i_pdf]["as_minus_member"]:
					dummy_as_minus_number.append(int(i))
				c_as_minus_number[:] = dummy_as_minus_number
				lib.pdf_calcer_hessian.restype = None
				lib.pdf_calcer_hessian(c_int(n_pdf_sets_1), 
				c_pdf_1,
				create_string_buffer(out_file_1),
				create_string_buffer(out_par_1), 
				create_string_buffer(sg), 
				create_string_buffer(mc_cfg["general"]["histname"]), 
				create_string_buffer(i_pdf),
				c_double(float(pdf_cfg["PDF_Groups"][i_pdf]["norm_pdf"])),
				c_double(float(pdf_cfg["PDF_Groups"][i_pdf]["norm_as_plus"])),
				c_double(float(pdf_cfg["PDF_Groups"][i_pdf]["norm_as_minus"])),
				c_as_plus_number,
				c_as_minus_number
				)
			if pdf_cfg["PDF_Groups"][i_pdf]["type"] == "MC":
				dummy_pdf_members = []
				for i in pdf_cfg["PDF_Groups"][i_pdf]["members"]:
					dummy_pdf_members.append(str(i))
				c_pdf_1 = (c_char_p * len(dummy_pdf_members))()
				c_pdf_1[:] = dummy_pdf_members
				n_pdf_sets_1 = len(dummy_pdf_members)
				lib.pdf_calcer_MC.restype = None
				lib.pdf_calcer_MC(c_int(n_pdf_sets_1), 
				c_pdf_1,
				create_string_buffer(out_file_1),
				create_string_buffer(out_par_1), 
				create_string_buffer(sg), 
				create_string_buffer(mc_cfg["general"]["histname"]), 
				create_string_buffer(i_pdf)
				)


def main():
	# measure process time
	t0 = time.clock()

	# measure wall time
	t1 = time.time()

	############################
	# Parse all given options
	#
	options,log_level = option_parsing()

	############################
	# Start with welcome output
	#
	welcome_output()
	
	############################
	# Parse all three config files
	#
	mc_cfg,xs_cfg,pdf_cfg = config_parsing(options)
	
	############################
	# Output to check if all parameters are correct
	#
	control_output(options,mc_cfg,pdf_cfg,xs_cfg)
	
	############################
	# Check if tmp folder exist, otherwise create it
	#
	if not os.path.exists(pdf_cfg["general"]["temp_path"]):
		os.mkdir(pdf_cfg["general"]["temp_path"])
		logging.debug('creating tmp/ directory')

	############################
	# Read C++ libraray
	#
	# export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH
	logging.debug('loading library.so ...')
        temp = os.path.abspath(__file__)
        temp = os.path.realpath(temp)
        temp = os.path.dirname(temp)
        temp = os.path.join(temp, "lib/library.so")
	lib = CDLL("/home/home1/institut_3a/erdweg/Desktop/Software/tools3a/RATA_PDF/local/lib/library.so")
	logging.debug('done')
	
	############################
	# Sort parameters for C++ functions
	#
	c_par = make_c_parameters(mc_cfg,pdf_cfg)

	############################
	# Get the event number for all samples
	#
	filelist,total_events,eventlist = get_event_number_list(mc_cfg,pdf_cfg,c_par["path"])

	############################
	# Init all pdf informations
	#
	init_pdfs(c_par,log_level,lib)

	############################
	# Loop over all MC samples
	# TBD: change to multiprocessing
	# TBD: calculate expected run time
	#
	run_samples = pdf_file_loop(eventlist,total_events,filelist,c_par,xs_cfg,mc_cfg,pdf_cfg,lib)

	############################
	# Check files and merge background samples
	#
	run_samples = final_file_check(options,run_samples,pdf_cfg)

	############################
	# Calculate PDF uncertainties
	# TBD: outfile parameters
	# TBD: include copier 'signal_mover.py'
	#
	calc_pdf_uncer(run_samples,options,c_par,pdf_cfg,mc_cfg,lib)
        
	############################
	# End with farewell output
	#
	farewell_output(t0,t1)

main()
