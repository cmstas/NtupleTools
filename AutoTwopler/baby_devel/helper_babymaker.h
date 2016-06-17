#ifndef SSBABYMAKER_H
#define SSBABYMAKER_H

#include "TChain.h"
#include "TString.h"
#include "TFile.h"
#include "TTree.h"
#include "TRandom.h"
#include <string>
#include "TF1.h"
#include "TH2.h"
#include "Math/VectorUtil.h" 
#include "CORE/CMS3.h"
#include "CORE/SSSelections.h"
#include "CORE/ElectronSelections.h"
#include "CORE/IsolationTools.h"
#include "TROOT.h"
#include <vector>
#include "Math/Vector4D.h" 
#include "Math/LorentzVector.h" 

#ifdef __MAKECINT__
#pragma link C++ class ROOT::Math::PxPyPzE4D<float>+;
#pragma link C++ class ROOT::Math::LorentzVector<ROOT::Math::PxPyPzE4D<float> >+;
#pragma link C++ typedef ROOT::Math::XYZTVectorF;
#endif

typedef ROOT::Math::LorentzVector<ROOT::Math::PxPyPzE4D<float> > LorentzVector;

using namespace std;

//Classes
class babyMaker {

    public:
        babyMaker(bool debug = 0) {
            verbose = debug;
        }
        void MakeBabyNtuple(TString input_name, TString output_name);
        void InitBabyNtuple();
        void CloseBabyNtuple () { BabyFile->cd(); BabyTree->Write(); BabyFile->Close(); }
        int ProcessBaby();

    protected:
        TFile* BabyFile;
        TTree* BabyTree;
        TString input_filename;
        TString output_filename;

    private:

        //Switches
        bool verbose;

        //MET
        float met;
        float metPhi;

        //Meta Variables
        ULong64_t event;
        int lumi;
        int run;
        TString filename;

};

#endif
