{
  gSystem->Exec("mkdir -p hists/");
  gROOT->ProcessLine(".L /home/users/cgeorge/software/dataMCplotMaker/dataMCplotMaker.cc+");
  gROOT->ProcessLine(".L compareNtuples.C+");
  gROOT->ProcessLine("test()");
}
