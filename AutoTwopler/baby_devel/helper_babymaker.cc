#include "helper_babymaker.h"
#include "TString.h"
#include <string>

using namespace tas;

//Main functions
void babyMaker::MakeBabyNtuple(TString input_name, TString output_name) {

    input_filename = input_name;
    output_filename = output_name;

    //Create Baby
    BabyFile = new TFile(output_name, "RECREATE");
    BabyFile->cd();
    BabyTree = new TTree("t", "SS2015 Baby Ntuple");

    //Define Branches
    BabyTree->Branch("filename" , &filename ) ;
    BabyTree->Branch("met"      , &met      ) ;
    BabyTree->Branch("metPhi"   , &metPhi   ) ;
    BabyTree->Branch("event"    , &event    ) ;
    BabyTree->Branch("lumi"     , &lumi     ) ;
    BabyTree->Branch("run"      , &run      ) ;

}

void babyMaker::InitBabyNtuple(){

    filename = "";
    met = -1;
    metPhi = -1;
    event = -1;
    lumi = -1;
    run = -1;
} 

//Main function
int babyMaker::ProcessBaby() {
    //Initialize variables
    InitBabyNtuple();

    //Preliminary stuff
    if (tas::hyp_type().size() < 1) return 1;
    if (tas::mus_dxyPV().size() != tas::mus_dzPV().size()) return 1;

    //Fill Easy Variables
    filename = input_filename;
    met = evt_pfmet();
    metPhi = evt_pfmetPhi();
    event = tas::evt_event();
    lumi = tas::evt_lumiBlock();
    run = tas::evt_run();

    BabyTree->Fill();

    return 0; 

}
