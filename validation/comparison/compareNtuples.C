#include "TH1.h"
#include "TFile.h"
#include "TTree.h"
#include "TList.h"
#include "/home/users/cgeorge/software/dataMCplotMaker/dataMCplotMaker.h"
#include <fstream>

ofstream myfile; 
TFile *file_old;
TFile *file_new;
int plotNum;
int max_events = 10000;

struct trifecta { string plotName; int plotNumber; float chi2; };

bool comparePair(trifecta a, trifecta b) {
  return a.chi2 < b.chi2;
}

vector<std::string> getAliasNames(TTree *t){

  //Vector to return results
  vector<std::string> v_aliasnames;
  
  //Skip if no entries
  if (t->GetEntriesFast() == 0) return v_aliasnames;

  //Get list of aliases
  TList *t_list =  t->GetListOfAliases();

  //Loop over list, skip entries that are vectors of vectors, or strings
  for(int i = 0; i < t_list->GetSize(); i++) {
    std::string aliasname(t_list->At(i)->GetName());
    TBranch *branch = t->GetBranch(t->GetAlias(aliasname.c_str()));
    std::string branchname(branch->GetName());
    std::string branchtitle(branch->GetTitle());

    if(branchname.find("intss") == 0 || branchname.find("floatss") == 0 || branchname.find("doubless") == 0 || branchname.find("LorentzVectorss") < branchname.length() || branchname.find("timestamp") < branchname.length() || branchname.find("selectedPatJets") < branchname.length()){
      cout << "Sorry, I dont know about vector of vectors of objects. Will be skipping " << aliasname << endl;
      continue;
    }

    if(branchname.find("std::string") < branchname.length() || branchname.find("TStrings") < branchname.length()){
      cout << "Sorry, I don't know how to graphically represent std::strings in only 3 dimensions." << " Put in a feature request. Will be skipping " << aliasname << endl;
      continue;
    }

    v_aliasnames.push_back(aliasname);
  }

  //Sort alias names alphabetically
  sort(v_aliasnames.begin(), v_aliasnames.end());
  
  //Return aliases names
  return v_aliasnames;
}

vector<std::string> getUncommonBranches(vector<std::string> aliasnames, vector<std::string> v_commonBranches){
  
  vector<std::string> v_notCommonBranches;
  for(vector<std::string>::iterator it = aliasnames.begin(); it != aliasnames.end(); it++){
    if(find(v_commonBranches.begin(), v_commonBranches.end(), *it) != v_commonBranches.end()) continue;
    v_notCommonBranches.push_back(*it);
  }

  sort(v_notCommonBranches.begin(), v_notCommonBranches.end());

  return v_notCommonBranches;

}

void doSinglePlots(vector <std::string> branches, bool isNew, TTree* tree){

  TH1F* empty = new TH1F("","", 1, 0, 1);

  for (unsigned int i = 0; i < branches.size(); i++){  

    //isLorentz
    TBranch *branch = tree->GetBranch(tree->GetAlias(branches[i].c_str()));
    TString branchname(branch->GetName());
    bool isLorentz = branchname.Contains("p4") || branchname.Contains("MathLorentzVectors"); 

    //Draw
    string alias = branches.at(i);

    //Table of contents
    myfile << "\\subsection*{" << alias << "}\\addcontentsline{toc}{subsection}{" << alias << "}" << endl;

    tree->Draw(Form("%s%s>>hist", branches[i].c_str(), isLorentz ? ".Pt()" : ""), "", "", max_events); 
    TH1F *hist = (TH1F*)gDirectory->Get("hist");
    if(hist==NULL){
      cout << "********** Branch " << branches.at(i) << " exists, but is undrawable for some reason. Skipping this branch" << endl;
      continue; 
    }

    //Scale
    hist->Scale(1./hist->GetEntries());

    //Print plot, update ToC
    vector<TH1F*> hvec;
    hvec.push_back(hist);
    vector<string> titles;
    titles.push_back(isNew ? "New" : "Old");
    dataMCplotMaker(empty, hvec, titles, "", alias, Form("--isLinear --noDivisionLabel --xAxisOverride --outputName hists/uncommon%d", plotNum));
    myfile << "\\begin{figure}[H]" << endl
           << Form("\\includegraphics[width=0.6\\textwidth]{./hists/uncommon%d.pdf}", plotNum) << endl
           << "\\end{figure}" << endl;
    plotNum++;
    delete hist;
  }

  delete empty;
}

void test(){

  //Put in the files you want to compare here
  string filename_new = "/home/users/namin/sandbox/CMSSW_8_0_20/src/CMS3/NtupleMaker/test/ntuple_new.root";
  string filename_old = "/home/users/namin/sandbox/beforelumi/CMSSW_8_0_20/src/CMS3/NtupleMaker/test/ntuple_old.root";

  //Load files
  file_old = new TFile(filename_old.c_str());
  file_new = new TFile(filename_new.c_str());

  //Load trees
  TTree *tree_old = (TTree*)file_old->Get("Events");
  TTree *tree_new = (TTree*)file_new->Get("Events");

  //LaTeX stuff
  myfile.open("overview.tex"); 
  myfile << "\\documentclass{article}" << endl
         << "\\usepackage{fullpage}" << endl
         << "\\usepackage{graphicx}" << endl
         << "\\usepackage{float}" << endl
         << "\\usepackage{listings}" << endl
         << "\\usepackage{hyperref}" << endl << endl
         << "\\begin{document}" << endl << endl
         << "\\begin{center}" << endl
         << "\\LARGE \\bf Compare Ntuples for CMS3" << endl
         << "\\end{center}" << endl
         << "\\vspace{0.5cm}" << endl << endl
         << "{\\noindent\\Large\\bf Files that are compared here:}" << endl
         << "\\begin{lstlisting}[breaklines] "  << endl
         << "OLD: " << filename_old << endl
         << "NEW: " << filename_new << endl
         << "\\end{lstlisting}" << endl
         << "\\vspace{0.5cm}" << endl << endl 
         << "\\tableofcontents" << endl 
         << "\\section*{Branches in OLD but not in NEW}\\addcontentsline{toc}{section}{Branches in Old but not in New}" 
         << endl;

  //Get aliases in both trees
  vector<std::string> oldAliasNames = getAliasNames(tree_old);
  vector<std::string> newAliasNames = getAliasNames(tree_new);

  //Holders for common and notCommonBranches
  vector<std::string> commonBranches;
  vector<std::string> oldOnlyBranches;
  vector<std::string> newOnlyBranches;
  
  //Figure out common branches
  for(vector<std::string>::iterator it = oldAliasNames.begin(); it != oldAliasNames.end(); it++) {
    if(find(newAliasNames.begin(), newAliasNames.end(), *it) != newAliasNames.end()) commonBranches.push_back(*it);
  }

  //Figure out uncommon branches 
  oldOnlyBranches = getUncommonBranches(oldAliasNames, commonBranches);
  newOnlyBranches = getUncommonBranches(newAliasNames, commonBranches);

  //Number of plots drawn
  plotNum = 0;

  //Record chi2 of each plot
  vector<trifecta> chi2pair;

  //Loop over non-common branches, update ToC for LaTeX
  doSinglePlots(oldOnlyBranches, 0, tree_old);
  myfile << "\\section*{Branches in NEW but not in OLD}\\addcontentsline{toc}{section}{Branches in New but not in Old}" << endl;
  doSinglePlots(newOnlyBranches, 1, tree_new);

  //Reinitialize number of plots drawn
  plotNum = 0;

  //Loop over common branches, test for sameness
  for (unsigned int i = 0; i < commonBranches.size(); i++){

    //Status
    cout << "Working on " << commonBranches[i] << endl;

    //Identical plots should be suppressed
    int suppress = false;

    //Determine if it's a LorentzVector
    TBranch *branch = tree_new->GetBranch(tree_new->GetAlias(commonBranches[i].c_str()));
    TString branchname(branch->GetName()); 
    bool isLorentz = branchname.Contains("p4") || branchname.Contains("MathLorentzVectors"); 

    //Make plot
    tree_old->Draw(Form("%s%s>>hist_old", commonBranches[i].c_str(), isLorentz ? ".Pt()" : ""), "", "", max_events); 
    tree_new->Draw(Form("%s%s>>hist_new", commonBranches[i].c_str(), isLorentz ? ".Pt()" : ""), "", "", max_events); 

    //Retrieve plot (this will use default binning)
    TH1F *hist_old = (TH1F*)gDirectory->Get("hist_old");
    TH1F *hist_new = (TH1F*)gDirectory->Get("hist_new");

    //Give both histos the same binning, if not already done
    int old_min = hist_old->GetXaxis()->GetBinLowEdge(1);
    int old_max = hist_old->GetXaxis()->GetBinLowEdge( hist_old->GetNbinsX() ) + hist_old->GetXaxis()->GetBinWidth( hist_old->GetNbinsX() );
    int old_nbins = hist_old->GetNbinsX();
    int new_min = hist_new->GetXaxis()->GetBinLowEdge(1);
    int new_max = hist_new->GetXaxis()->GetBinLowEdge( hist_new->GetNbinsX() ) + hist_new->GetXaxis()->GetBinWidth( hist_new->GetNbinsX() );
    int new_nbins = hist_new->GetNbinsX();
    int nbins = std::max(old_nbins, new_nbins); 
    float min = std::min(old_min, new_min); 
    float max = std::max(old_max, new_max); 
    if (old_min != new_min || old_max != new_max || old_nbins != new_nbins){
      delete hist_old;
      delete hist_new;
      hist_old = new TH1F("hist_old", "hist_old", nbins, min, max); 
      hist_new = new TH1F("hist_new", "hist_new", nbins, min, max); 
      tree_old->Draw(Form("%s%s>>hist_old", commonBranches[i].c_str(), isLorentz ? ".Pt()" : ""), "", "", max_events); 
      tree_new->Draw(Form("%s%s>>hist_new", commonBranches[i].c_str(), isLorentz ? ".Pt()" : ""), "", "", max_events); 
    }

    //If it's being fucking stupid and putting everything in 1 bin, redraw yet again.  Lowest & Highest bins shouldn't be empty
    bool keepGoing = ((std::min(hist_old->GetBinContent(1), hist_new->GetBinContent(1)) == 0) || (std::min(hist_old->GetBinContent(nbins), hist_new->GetBinContent(nbins)) == 0));
 
    //Keep track of number of iterations so it doesn't get stuck
    int niter = 0;

    //Keep looping until lowest & highest bins aren't empty
    while (keepGoing == true && niter < 15){
      //Determine highest/lowest bins that are filled
      for (int j = 1; j < nbins; j++){
        if (hist_old->GetBinContent(j) != 0){ min = hist_old->GetXaxis()->GetBinLowEdge(j); break; } 
        if (hist_new->GetBinContent(j) != 0){ min = hist_new->GetXaxis()->GetBinLowEdge(j); break; } 
      }
      for (int j = nbins; j > 0; j--){
        if (hist_old->GetBinContent(j) != 0){ max = hist_old->GetXaxis()->GetBinLowEdge(j) + 2*hist_old->GetXaxis()->GetBinWidth(j); break; } 
        if (hist_new->GetBinContent(j) != 0){ max = hist_new->GetXaxis()->GetBinLowEdge(j) + 2*hist_new->GetXaxis()->GetBinWidth(j); break; } 
      }
      //Redraw using the above bins
      delete hist_old;
      delete hist_new;
      hist_old = new TH1F("hist_old", "hist_old", nbins, min, max); 
      hist_new = new TH1F("hist_new", "hist_new", nbins, min, max); 
      tree_old->Draw(Form("%s%s>>hist_old", commonBranches[i].c_str(), isLorentz ? ".Pt()" : ""), "", "", max_events); 
      tree_new->Draw(Form("%s%s>>hist_new", commonBranches[i].c_str(), isLorentz ? ".Pt()" : ""), "", "", max_events); 

      //Decide if we fixed the problem or if we need to try again
      niter++;
      keepGoing = ((std::max(hist_old->GetBinContent(1), hist_new->GetBinContent(1)) == 0) || (std::max(hist_old->GetBinContent(nbins), hist_new->GetBinContent(nbins)) == 0));

      //No need to try again if the second-highest bin is filled (close enough, often saves a step)
     if (keepGoing && (std::max(hist_old->GetBinContent(1), hist_new->GetBinContent(1)) != 0) && (std::max(hist_old->GetBinContent(nbins-1), hist_new->GetBinContent(nbins-1)) != 0)) keepGoing = false;
    
     //No need to try again if max and min are fucking close (this will avoid problems if everything is in one bin)
     if ( (max-min)/max < .001){ keepGoing = false; suppress = true;} 
    }

    //Normalize both histos
    hist_old->Scale(1./hist_old->GetEntries());
    hist_new->Scale(1./hist_new->GetEntries());

    //Figure out chi2, decide if you want to suppress
    float chi2test = hist_new->Chi2Test(hist_old, "CHI2/NDFWWOFUF");
    if (chi2test*100 < 0.2) suppress = true;

    //Print histogram, update LaTeX stuff if not suppressed
    if (suppress) cout << " --> Suppressed!" << endl;
    if (!suppress){ 
      vector<TH1F*> old_vector;
      old_vector.push_back(hist_old);
      vector<string> titles;
      titles.push_back("Old");
      dataMCplotMaker(hist_new, old_vector, titles, Form("%.2f", chi2test*100), commonBranches[i].c_str(), Form("--noErrBars --isLinear --dataName New --topYaxisTitle New/Old --xAxisOverride --noDivisionLabel --outputName hists/diff%i", plotNum));
      trifecta chi2pair_;
      chi2pair_.plotName = commonBranches[i];
      chi2pair_.plotNumber = plotNum;
      chi2pair_.chi2 = chi2test; 
      chi2pair.push_back(chi2pair_);
      plotNum++;
    }

    //Memory management
    delete hist_old;
    delete hist_new;

  }

  //ToC stuff
  myfile << "\\section*{Branches in Common}\\addcontentsline{toc}{section}{Branches in Common}" << endl;

  //Put worst plots first
  std::sort(chi2pair.begin(), chi2pair.end(), comparePair);

  //LaTeX stuff
  for(unsigned int i = 0; i < chi2pair.size(); i++){
    myfile << "\\subsection*{" << chi2pair[i].plotName << "}\\addcontentsline{toc}{subsection}{" << chi2pair[i].plotName << "}" << endl
           << "\\begin{figure}[H]" << endl
           << Form("\\includegraphics[width=0.9\\textwidth]{./hists/diff%i.pdf}", chi2pair[i].plotNumber) << endl
           << "\\end{figure}" << endl;
    myfile << "The $\\chi^2$ test value between the Old and New is: " << chi2pair[i].chi2 << endl;
  }

  myfile << "\\end{document}" << endl;

}
