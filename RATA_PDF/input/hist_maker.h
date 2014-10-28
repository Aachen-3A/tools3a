#include <TROOT.h>
#include <TH2.h>
#include <TStyle.h>
#include <TCanvas.h>
#include <iostream>
#include <fstream>
#include "TSystem.h"
#include <TChain.h>
#include <TFile.h>
#include <TH1.h>
#include <TEfficiency.h>
#include <THnSparse.h>
#include <TKey.h>
#include <vector>
#include <map>
#include <TStopwatch.h>
#include <string>
//#include "RunPDFErrors.h"

#include "LHAPDF/LHAPDF.h"

using namespace std;

/** \file Utilities.h
 *
 * \brief Various preprocessor macros, variables, functions, and procedures
 * that misfit elsewhere.
 *
 */

// global Variable to log errors (1), warnings (2), info (3), debug(4,5,...)
int gLogLevel = 4;

#ifndef NDEBUG
#define LOG(level, message) { if (gLogLevel >= level) { switch (level) { \
case 1: std::cerr << "ERROR: " << message << std::endl; break; \
case 2: std::cerr << "WARNING: " << message << std::endl; break; \
case 3: std::cout << "INFO: " << message << std::endl; break; \
default: std::cout << "DEBUG: " << message << std::endl; } } }
#else
#define LOG(level, message) ;
#endif

#define ERROR(message) LOG(1, message);
#define WARNING(message) LOG(2, message);
#define INFO(message) LOG(3, message);
#define DEBUG(message) LOG(10000, message);

// throw an exception that tells me where the exception happened
#define THROW(errmsg) throw (std::string( __PRETTY_FUNCTION__ )+std::string(" (file: ")+std::string( __FILE__ )+std::string(", line: ")+std::string( Form("%d", __LINE__) )+std::string(") ")+std::string(errmsg));


ProcInfo_t info;

void raw_input(TString question);
double get_scalefactor(char* f_name,double lumi,double data_mc_scf,double xs);
extern "C" void init_bg(char* PDFpath, int n_pdfs, char** PDFsets, int loglevel);
TTree* read_tree(char* filename, char* tree_name, char* tree_cuts);
bool analyser(TTree* tree, char** branches, char* histname, int n_pdfs, char** PDFsets, double scalefactor);
bool writer(char* histname, int n_pdfs, char** PDFsets);
extern "C" void make_hists(char* input_file, char* tree_name, char* tree_cuts, char** branches, double lumi, double cross_section, int n_pdfs, char** PDFsets, char* PDFnorm, char* histname, char* output_file, int n_binning, double* binning);

vector< vector<Float_t> > hessian_pdf_asErr(vector< vector< TH1D* > > hist_scaled, double pdf_correction, double as_plus_correction, double as_minus_correction, int* as_plus_number, int* as_minus_number);
extern "C" void pdf_calcer_hessian(int n_pdfSet, char** pdf_sets, char* outfile, char* out_par, char* infile, char* main_hist, char* shortname, double pdf_correction, double as_plus_correction, double as_minus_correction, int* as_plus_number, int* as_minus_number);
vector< vector<Float_t> > NNPDF_weighted_mean( vector< vector< TH1D* > > hist_scaled);
extern "C" void pdf_calcer_MC(int n_pdfSet, char** pdf_sets, char* outfile, char* out_par, char* infile, char* main_hist, char* shortname);

LHAPDF::PDF* Mainpdf;
vector<vector< LHAPDF::PDF* > > allPdfsets;

static map<TString, TH1D * > histo;

namespace HistClass {
        static void CreateHisto(Int_t n_histos,const char* name,const char* title, Int_t nbinsx, Double_t xlow, Double_t xup,TString xtitle = ""){
            for(int i = 0; i < n_histos; i++){
                string dummy = Form("h1_%d_%s", i, name);
                histo[dummy] = new TH1D(Form("h1_%d_%s", i, name), title, nbinsx, xlow, xup);
                histo[dummy] -> Sumw2();
                histo[dummy] -> GetXaxis() ->SetTitle(xtitle);
            }
        }

        static void ClearHisto(Int_t n_histos,const char* name){
            for(int i = 0; i < n_histos; i++){
                string dummy = Form("h1_%d_%s", i, name);
                histo[dummy] -> Reset();
            }
        }

        static void RebinHisto(Int_t n_histos,const char* name, Int_t n_rebin, Double_t* bins){
            for(int i = 0; i < n_histos; i++){
                string dummy = Form("h1_%d_%s", i, name);
                char* cdummy = Form("h1_%d_%s", i, name);
                histo[dummy] = (TH1D*)histo[dummy] -> Rebin(n_rebin,cdummy, bins);
            }
        }
    
        static void Fill(Int_t n_histo,const char * name, double value,double weight)
        {
          string dummy = Form("h1_%d_%s", n_histo, name);
          histo[dummy]->Fill(value,weight);
        }
        
        static void Write(Int_t n_histo,const char * name)
        {
          string dummy = Form("h1_%d_%s", n_histo, name);
          histo[dummy]->Write();
        }
        
        static void SetToZero(Int_t n_histo,const char * name)
        {
          string dummy = Form("h1_%d_%s", n_histo, name);
          int Nbins2 = histo[dummy] -> GetNbinsX();
          for ( int bb = 0; bb < Nbins2+1; bb++) {
            double binValue = histo[dummy] -> GetBinContent(bb);
            if (binValue < 0) {
              //cout << "Bin " << bb << "  " << dummy << " is negative: " << binValue << "  and is being set to zero!" << endl;
              histo[dummy] -> SetBinContent(bb,0.);
            }
          }
        }
        
        static TH1D* ReturnHist(Int_t n_histo,const char * name)
        {
          string dummy = Form("h1_%d_%s", n_histo, name);
          return histo[dummy];
        }
        
        static void DeleteHisto(Int_t n_histos,const char* name){
            for(int i = 0; i < n_histos; i++){
                string dummy = Form("h1_%d_%s", i, name);
                delete histo[dummy];
            }
        }
        
      }
