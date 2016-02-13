#include <iostream>
#include <TString.h>
#include <TChain.h>

void counts(TString location, bool do_effective = false, bool single_file = false)
{
    TChain * ch = new TChain("Events");
    if(single_file) {
      ch->Add(location);
    } else {
      ch->Add(location+"/*.root");
    }
    if(do_effective){
      int pos_weight = ch->GetEntries("genps_weight > 0");
      int neg_weight = ch->GetEntries() - pos_weight;
      int nevents_effective = pos_weight - neg_weight;
      std::cout << "nevents_effective=" << nevents_effective << std::endl;
    }
    else std::cout << "nevents=" << ch->GetEntries() << std::endl;
}
