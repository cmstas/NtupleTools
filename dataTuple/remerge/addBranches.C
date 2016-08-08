#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include "TFile.h"
#include "TTree.h"
#include "TBranch.h"
#include "TString.h"

using namespace std;

int addBranches(string mData, string inFileName, string outFileName);

int addCMS3Branches(TString infname, TString outfile, ULong64_t events, ULong64_t events_effective,
					Float_t xsec, Float_t kfactor,
					Float_t filt_eff, bool SortBasketsByEntry = false);


int addBranches(string mData, string inFileName, string outFileName){
  
  
  cout<<"File Name = "<<mData.c_str()<<endl;
  ifstream file(mData.c_str());
  if (!file.good()){
  	cout<<Form("%s is not a good file. exiting..",mData.c_str())<<endl;
  	return 3;
  }

  ULong64_t nEvents = 0;
  ULong64_t nEvents_effective = 0;
  float kFactor = 0;
  float filtEff = 0;
  float xSect = 0;


  while(file){

	//This assigns the variables such as cross section etc given by the metaData.txt file
	string varName = "";
	string varValue = "";
	file>>varName>>varValue;

	  if(varName.compare("n:") == 0){
		nEvents = atoi(varValue.c_str());
		cout<<"nEvents = "<<nEvents<<endl;
	  }
	  if(varName.compare("effN:") == 0){
		nEvents_effective = atoi(varValue.c_str());
		cout<<"nEvents_effective = "<<nEvents_effective<<endl;
	  }
	  if(varName.compare("k:") == 0){
		kFactor = (float)atof(varValue.c_str());
		cout<<"kFactor = "<<kFactor<<endl;
	  }
	  if(varName.compare("f:") == 0){
		filtEff = atof(varValue.c_str());
		cout<<"filtEff = "<<filtEff<<endl;
	  }
	  if(varName.compare("x:") == 0){
		xSect = atof(varValue.c_str());
		cout<<"xSect = "<<xSect<<endl;
	  }
	  
  }

  addCMS3Branches(inFileName, outFileName, nEvents, nEvents_effective, xSect, kFactor, filtEff );

  return 0;

}

int addCMS3Branches(TString infname, TString outfile, ULong64_t events, ULong64_t events_effective,
		      Float_t xsec, Float_t kfactor,
		      Float_t filt_eff, bool SortBasketsByEntry ) {
  
  cout << "Processing File " << infname << endl;

  TFile *f = TFile::Open(infname.Data(), "READ");
  if (! f || f->IsZombie()) {
    cout << "File does not exist!" << endl;
    return 1;
  }
  
  TTree* t = (TTree*)f->Get("Events");
  if (! t || t->IsZombie()) {
    cout << "Tree does not exist!" << endl;
    return 2;
  }

  bool isdata = false;
  // check for CMS3 first then CMS2
  if (t->GetBranch("int_eventMaker_evtisRealData_CMS3.obj") != 0) {
    isdata = (Int_t)t->GetMaximum("int_eventMaker_evtisRealData_CMS3.obj");
  }
  else if (t->GetBranch("int_eventMaker_evtisRealData_CMS2.obj") != 0) {
    isdata = (Int_t)t->GetMaximum("int_eventMaker_evtisRealData_CMS2.obj");
  }
  
  //-------------------------------------------------------------
  // Removes all non *_CMS*.* branches
  //-------------------------------------------------------------`
  t->SetBranchStatus("*", 0);
  t->SetBranchStatus("*_CMS*.*", 1);

  // Removes the branches (if they exist) that we want to replace
  //evt_xsec_excl
  TString bName = t->GetAlias("evt_xsec_excl");
  //cout << "evt_xsec_excl " << bName << endl;
  if(bName != "") {
    bName.ReplaceAll(".obj", "*");
    t->SetBranchStatus(bName.Data(), 0); 
  }

  //evt_xsec_incl
  bName = t->GetAlias("evt_xsec_incl");
  //cout << "evt_xsec_incl " << bName << endl;
  if(bName != "") {
    bName.ReplaceAll(".obj", "*");
    t->SetBranchStatus(bName.Data(), 0);   
  }
  
  //evt_kfactor
  bName = t->GetAlias("evt_kfactor");
  //cout << "evt_kfactor " << bName << endl;
  if(bName != "") {
    bName.ReplaceAll(".obj", "*");
    t->SetBranchStatus(bName.Data(), 0); 
  }

  //evt_nEvts
  bName = t->GetAlias("evt_nEvts");
  //cout << "evt_nEvts " << bName << endl;
  if(bName != "") {
    bName.ReplaceAll(".obj", "*");
    t->SetBranchStatus(bName.Data(), 0); 
  }

  //evt_nEvts
  bName = t->GetAlias("evt_nEvts_effective");
  //cout << "evt_nEvts " << bName << endl;
  if(bName != "") {
    bName.ReplaceAll(".obj", "*");
    t->SetBranchStatus(bName.Data(), 0); 
  }

  //evt_filt_eff
  bName = t->GetAlias("evt_filt_eff");
  //cout << "evt_filt_eff " << bName << endl;
  if(bName != "") {
    bName.ReplaceAll(".obj", "*");
    t->SetBranchStatus(bName.Data(), 0); 
  }

  //evt_scale1fb
  bName = t->GetAlias("evt_scale1fb");
  //cout << "evt_scale1fb " << bName << endl;
  if(bName != "") {
    bName.ReplaceAll(".obj", "*");
    t->SetBranchStatus(bName.Data(), 0); 
  }

  TFile *out = TFile::Open(outfile.Data(), "RECREATE");
  TTree *clone;
  if(SortBasketsByEntry)
    clone = t->CloneTree(-1, "fastSortBasketsByEntry");
  else 
    clone = t->CloneTree(-1, "fast");
   

  //-------------------------------------------------------------

  //Calculate scaling factor and put variables into tree 
  Float_t scale1fb = xsec*kfactor*1000*filt_eff/(Double_t)events_effective;

  if(isdata){
    scale1fb = 1.0;
    cout<< "Data file. scale1fb: " << scale1fb << endl;
  }else{
    cout << "scale1fb: " << scale1fb << endl; 
  }
  
  TBranch* b1 = clone->Branch("evtscale1fb", &scale1fb, "evt_scale1fb/F");
  TBranch* b2 = clone->Branch("evtxsecexcl", &xsec, "evt_xsec_excl/F");
  TBranch* b3 = clone->Branch("evtxsecincl", &xsec, "evt_xsec_incl/F");
  TBranch* b4 = clone->Branch("evtkfactor", &kfactor, "evt_kfactor/F");
  TBranch* b5 = clone->Branch("evtnEvts", &events, "evt_nEvts/l");
  TBranch* b6 = clone->Branch("evtnEvtseffective", &events_effective, "evt_nEvts_effective/l");
  TBranch* b7 = clone->Branch("evtfilteff", &filt_eff, "evt_filt_eff/F");
   
  clone->SetAlias("evt_scale1fb",  "evtscale1fb");
  clone->SetAlias("evt_xsec_excl", "evtxsecexcl");
  clone->SetAlias("evt_xsec_incl",  "evtxsecincl");
  clone->SetAlias("evt_kfactor",   "evtkfactor");
  clone->SetAlias("evt_nEvts",     "evtnEvts");
  clone->SetAlias("evt_nEvts_effective", "evtnEvtseffective");
  clone->SetAlias("evt_filt_eff",     "evtfilteff");


  t->SetMakeClass(1);
  t->SetBranchStatus("*", 0);
  t->SetBranchStatus("float_genMaker_genpsweight_CMS3.obj", 1);

  float genWeight = 999;
  if(!isdata) t->SetBranchAddress("float_genMaker_genpsweight_CMS3.obj", &genWeight);
  
  Int_t nentries = t->GetEntries();
  for(Int_t i = 0; i < nentries; i++) {
    if(!isdata){
      t->GetEntry(i);
      if(genWeight < 0) scale1fb = -1*fabs(scale1fb);
      else scale1fb = fabs(scale1fb);
    } 
    b1->Fill();
    b2->Fill();
    b3->Fill();
    b4->Fill();
    b5->Fill();
    b6->Fill();
    b7->Fill();
  }
  //-------------------------------------------------------------

  clone->Write(); 
  out->Close();
  f->Close();
  return 0;
  
}






