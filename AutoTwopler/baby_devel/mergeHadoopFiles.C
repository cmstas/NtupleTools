void mergeHadoopFiles(const TString& indir, const TString& outpath) {
  TChain *chain = new TChain("t");
  chain->SetMaxTreeSize(5000000000LL);

  std::cout << "Merging files from dir: " << indir << std::endl
	    << "Ouputting to: " << outpath << std::endl;

  chain->Add(indir + "*.root");
  chain->Merge(outpath, "fast");

  std::cout << "Total events merged: " << chain->GetEntries() << std::endl;
}
