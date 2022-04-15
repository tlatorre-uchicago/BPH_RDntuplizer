#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/EDProducer.h"
#include "FWCore/Utilities/interface/StreamID.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/ESHandle.h"

#include "DataFormats/Candidate/interface/Candidate.h"
#include "DataFormats/HepMCCandidate/interface/GenParticle.h"

#include "FWCore/ServiceRegistry/interface/Service.h"
#include "CommonTools/UtilAlgos/interface/TFileService.h"
#include "TTree.h"

#include <iostream>
#include <string>
#include <map>

#include "Hammer/Hammer.hh"
#include "Hammer/Process.hh"
#include "Hammer/Particle.hh"

#include "TLorentzVector.h"

using namespace std;

class HammerWeightsProducer : public edm::EDProducer {

public:

    explicit HammerWeightsProducer(const edm::ParameterSet &iConfig);

    ~HammerWeightsProducer() {
      if(verbose) { cout << "[Hammer]: Computing rates" << endl;}
      // Should it be without the - (i.e. only D*Munu)?
      map<string, double> outRate;
      vector<string> processes = {"B0D*-MuNu", "B0D*-TauNu"};
      // Getting overall rates
      for(auto proc : processes) {
        if(verbose) { cout << "Process: " << proc << endl;}
        float aux_den = hammer.getDenominatorRate(proc);
        if(aux_den == 0) {
          if(verbose) { cout << "Not evaluated, skipping" << endl;}
          continue;
        }
        outRate["den"] = aux_den;
        if(verbose) { cout << Form("Denominator rate: %1.3e", outRate["den"]) << endl;}

        map<string, double> settingsCLN;
        for(auto n: parNameCLN) settingsCLN["delta_" + n] = 0;
        hammer.setFFEigenvectors("BtoD*", "CLNVar", settingsCLN);
        outRate["CLNCentral"] = hammer.getRate(proc, "SchmeCLN");
        if(verbose) { cout << Form("CLN central rate: %1.3e (ratio = %.3f)", outRate["CLNCentral"], outRate["CLNCentral"]/outRate["den"]) << endl;}
        // hammer.saveOptionCard("Opts.yml", false);

        for(int i=0; i<4; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<4; k++) { //Loop over parameters
              settings["delta_" + parNameCLN[k]] = eigVarCLN[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD*", "CLNVar", settings);
            auto rate = hammer.getRate(proc, "SchmeCLN");
            string var_name = "CLN" + varNameCLN[i];
            var_name += j==0? "Up" : "Down";
            outRate[var_name] = rate;
            if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
          }
        }


        map<string, double> settingsBLPR;
        for(auto n: parNameBLPR) settingsBLPR["delta_" + n] = 0;
        hammer.setFFEigenvectors("BtoD*", "BLPRVar", settingsBLPR);
        outRate["BLPRCentral"] = hammer.getRate(proc, "SchmeBLPR");
        if(verbose) { cout << Form("BLPR central rate: %1.3e (ratio = %.3f)", outRate["BLPRCentral"], outRate["BLPRCentral"]/outRate["den"]) << endl;}
        // hammer.saveOptionCard("Opts.yml", false);

        for(int i=0; i<7; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<7; k++) { //Loop over parameters
              settings["delta_" + parNameBLPR[k]] = eigVarBLPR[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD*", "BLPRVar", settings);
            auto rate = hammer.getRate(proc, "SchmeBLPR");
            string var_name = "BLPR" + varNameBLPR[i];
            var_name += j==0? "Up" : "Down";
            outRate[var_name] = rate;
            if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
          }
        }


        map<string, double> settingsBGL;
        for(auto n: parNameBGL) settingsBGL["delta_" + n] = 0;
        hammer.setFFEigenvectors("BtoD*", "BGLVar", settingsBGL);
        outRate["BGLCentral"] = hammer.getRate(proc, "SchmeBGL");
        if(verbose) { cout << Form("BGL central rate: %1.3e (ratio = %.3f)", outRate["BGLCentral"], outRate["BGLCentral"]/outRate["den"]) << endl;}
        // hammer.saveOptionCard("Opts.yml", false);

        for(int i=0; i<10; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<10; k++) { //Loop over parameters
              settings["delta_" + parNameBGL[k]] = eigVarBGL[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD*", "BGLVar", settings);
            auto rate = hammer.getRate(proc, "SchmeBGL");
            string var_name = "BGL" + varNameBGL[i];
            var_name += j==0? "Up" : "Down";
            outRate[var_name] = rate;
            if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
          }
        }

        break;
      }


      vector<string> processes_D1 = {"B0D**1-MuNu", "B-D**10MuNu"};
      for(auto proc : processes_D1) {
        auto auxD1_den = hammer.getDenominatorRate(proc);
        if (auxD1_den == 0) {
          if (verbose) {cout << "Process: " << proc << " not evaluated, skipping" << endl;}
        }
        else {
          if (verbose) {cout << "Process: " << proc << endl;}
          outRate["den_D1"] = auxD1_den;
          map<string, double> settings;
          for(auto n: parNameDststN_BLR) settings["delta_" + n] = 0;
          hammer.setFFEigenvectors("BtoD**1", "BLRVar", settings);
          outRate["D1BLR_central"] = hammer.getRate(proc, "SchmeBLR_Dstst");
          if(verbose) { cout << Form("D1 BLR central rate: %1.3e (ratio = %.3f)", outRate["D1BLR_central"], outRate["D1BLR_central"]/outRate["den_D1"]) << endl;}

          for(int i=0; i<4; i++) { //Loop over eigenVar
            for (int j=0; j<2; j++) { //Loop over pos/neg direction
              map<string, double> settings;
              for (int k=0; k<7; k++) { //Loop over parameters
                settings["delta_" + parNameDststN_BLR[k]] = eigVarDststN_BLR[i][k][j];
              }
              hammer.setFFEigenvectors("BtoD**1", "BLRVar", settings);
              auto rate = hammer.getRate(proc, "SchmeBLR_Dstst");
              string var_name = "D1BLR_" + varNameDststN_BLR[i];
              var_name += j==0? "Up" : "Down";
              outRate[var_name] = rate;
              if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
            }
          }
          break;
        }
      }

      vector<string> processes_D2st = {"B0D**2*-MuNu", "B-D**2*0MuNu"};
      for(auto proc : processes_D2st) {
        auto auxD2st_den = hammer.getDenominatorRate(proc);
        if (auxD2st_den == 0) {
          if (verbose) {cout << "Process: " << proc << " not evaluated, skipping" << endl;}
        }
        else {
          if (verbose) {cout << "Process: " << proc << endl;}
          outRate["den_D2st"] = auxD2st_den;
          map<string, double> settings;
          for(auto n: parNameDststN_BLR) settings["delta_" + n] = 0;
          hammer.setFFEigenvectors("BtoD**2*", "BLRVar", settings);
          outRate["D2stBLR_central"] = hammer.getRate(proc, "SchmeBLR_Dstst");
          if(verbose) { cout << Form("D2* BLR central rate: %1.3e (ratio = %.3f)", outRate["D2stBLR_central"], outRate["D2stBLR_central"]/outRate["den_D2st"]) << endl;}


          for(int i=0; i<4; i++) { //Loop over eigenVar
            for (int j=0; j<2; j++) { //Loop over pos/neg direction
              map<string, double> settings;
              for (int k=0; k<7; k++) { //Loop over parameters
                settings["delta_" + parNameDststN_BLR[k]] = eigVarDststN_BLR[i][k][j];
              }
              hammer.setFFEigenvectors("BtoD**2*", "BLRVar", settings);
              auto rate = hammer.getRate(proc, "SchmeBLR_Dstst");
              string var_name = "D2stBLR_" + varNameDststN_BLR[i];
              var_name += j==0? "Up" : "Down";
              outRate[var_name] = rate;
              if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
            }
          }
          break;
        }
      }

      vector<string> processes_D1st = {"B0D**1*-MuNu", "B-D**1*0MuNu"};
      for(auto proc : processes_D1st) {
        auto auxD1st_den = hammer.getDenominatorRate(proc);
        if (auxD1st_den == 0) {
          if (verbose) {cout << "Process: " << proc << " not evaluated, skipping" << endl;}
        }
        else {
          if (verbose) {cout << "Process: " << proc << endl;}
          outRate["den_D1st"] = auxD1st_den;
          map<string, double> settings;
          for(auto n: parNameDststW_BLR) settings["delta_" + n] = 0;
          hammer.setFFEigenvectors("BtoD**1*", "BLRVar", settings);
          outRate["D1stBLR_central"] = hammer.getRate(proc, "SchmeBLR_Dstst");
          if(verbose) { cout << Form("D1* BLR central rate: %1.3e (ratio = %.3f)", outRate["D1stBLR_central"], outRate["D1stBLR_central"]/outRate["den_D1st"]) << endl;}


          for(int i=0; i<3; i++) { //Loop over eigenVar
            for (int j=0; j<2; j++) { //Loop over pos/neg direction
              map<string, double> settings;
              for (int k=0; k<5; k++) { //Loop over parameters
                settings["delta_" + parNameDststW_BLR[k]] = eigVarDststW_BLR[i][k][j];
              }
              hammer.setFFEigenvectors("BtoD**1*", "BLRVar", settings);
              auto rate = hammer.getRate(proc, "SchmeBLR_Dstst");
              string var_name = "D1stBLR_" + varNameDststW_BLR[i];
              var_name += j==0? "Up" : "Down";
              outRate[var_name] = rate;
              if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
            }
          }
          break;
        }
      }

      vector<string> processes_D2s = {"B0D(2s)-MuNu", "B-D(2s)0MuNu"};
      for(auto proc : processes_D2s) {
        auto aux_den = hammer.getDenominatorRate(proc);
        if (aux_den == 0) {
          if (verbose) {cout << "Process: " << proc << " not evaluated, skipping" << endl;}
        }
        else {
          if (verbose) {cout << "Process: " << proc << endl;}
          outRate["den_D2s"] = aux_den;
          map<string, double> settings;
          for(auto n: parNameD2S_BLOP) settings["delta_" + n] = 0;
          hammer.setFFEigenvectors("BtoD(2s)", "BLOPVar", settings);
          outRate["D2sBLOP_central"] = hammer.getRate(proc, "SchmeBLOP_D2s");
          if(verbose) { cout << Form("D(2s) BLOP central rate: %1.3e (ratio = %.3f)", outRate["D2sBLOP_central"], outRate["D2sBLOP_central"]/outRate["den_D2s"]) << endl;}


          for(int i=0; i<5; i++) { //Loop over eigenVar
            for (int j=0; j<2; j++) { //Loop over pos/neg direction
              map<string, double> settings;
              for (int k=0; k<9; k++) { //Loop over parameters
                settings["delta_" + parNameD2S_BLOP[k]] = eigVarD2S_BLOP[i][k][j];
              }
              hammer.setFFEigenvectors("BtoD(2s)", "BLOPVar", settings);
              auto rate = hammer.getRate(proc, "SchmeBLOP_D2s");
              string var_name = "D2sBLOP_" + varNameD2S_BLOP[i];
              var_name += j==0? "Up" : "Down";
              outRate[var_name] = rate;
              if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
            }
          }
          break;
        }
      }


      vector<string> processes_D2sst = {"B0D(2s)*-MuNu", "B-D(2s)*0MuNu"};
      for(auto proc : processes_D2sst) {
        auto aux_den = hammer.getDenominatorRate(proc);
        if (aux_den == 0) {
          if (verbose) {cout << "Process: " << proc << " not evaluated, skipping" << endl;}
        }
        else {
          if (verbose) {cout << "Process: " << proc << endl;}
          outRate["den_D2sst"] = aux_den;
          map<string, double> settings;
          for(auto n: parNameD2S_BLOP) settings["delta_" + n] = 0;
          hammer.setFFEigenvectors("BtoD(2s)*", "BLOPVar", settings);
          outRate["D2sstBLOP_central"] = hammer.getRate(proc, "SchmeBLOP_D2s");
          if(verbose) { cout << Form("D(2s)* BLOP central rate: %1.3e (ratio = %.3f)", outRate["D2sstBLOP_central"], outRate["D2sstBLOP_central"]/outRate["den_D2sst"]) << endl;}


          for(int i=0; i<5; i++) { //Loop over eigenVar
            for (int j=0; j<2; j++) { //Loop over pos/neg direction
              map<string, double> settings;
              for (int k=0; k<9; k++) { //Loop over parameters
                settings["delta_" + parNameD2S_BLOP[k]] = eigVarD2S_BLOP[i][k][j];
              }
              hammer.setFFEigenvectors("BtoD(2s)*", "BLOPVar", settings);
              auto rate = hammer.getRate(proc, "SchmeBLOP_D2s");
              string var_name = "D2sstBLOP_" + varNameD2S_BLOP[i];
              var_name += j==0? "Up" : "Down";
              outRate[var_name] = rate;
              if(verbose) {cout << var_name << Form(": %1.3e", rate) << endl;}
            }
          }
          break;
        }
      }


      if (outRate.size()) {
        edm::Service<TFileService> fs;
        TTree* tree = fs->make<TTree>( "Trate", Form("Rates from Hammer"));
        for(auto& kv : outRate) {
          auto k = kv.first;
          tree->Branch(k.c_str(), &(outRate[k]));
        }
        tree->Fill();
      }
    };

private:

    virtual void produce(edm::Event&, const edm::EventSetup&);

    // ----------member data ---------------------------

    edm::EDGetTokenT<vector<reco::GenParticle>> PrunedParticlesSrc_;
    edm::EDGetTokenT<int> indexBmcSrc_;

    Hammer::Hammer hammer;

    double mass_B0 = 5.27961;
    double mass_mu = 0.1056583745;
    double mass_K = 0.493677;
    double mass_pi = 0.13957018;

    int verbose = 0;

    // ################ CLN parameters #############
    const vector<string> parNameCLN = {"RhoSq", "R1", "R2", "R0"};
    // rho2, R1 and R2 from Ex: https://arxiv.org/pdf/1909.12524.pdf
    // R0 from Th: https://journals.aps.org/prd/pdf/10.1103/PhysRevD.85.094025
    const double centralValCLN[4] = {1.122, 1.270, 0.852, 1.14};
    const vector<string> varNameCLN = {"eig1", "eig2", "eig3", "R0"};
    const double eigVarCLN[4][4][2] = { // 2 sigma
      {{ -0.0420, 0.0420}, { -0.0459, 0.0459}, { 0.0331, -0.0331},  {0, 0}},
      {{ -0.0221, 0.0221}, { 0.0243, -0.0243}, { 0.0056, -0.0056},  {0, 0}},
      {{ 0.0068, -0.0068}, { 0.0032, -0.0032}, { 0.0131, -0.0131},  {0, 0}},
      {{0, 0}, {0, 0}, {0, 0}, {+0.22, -0.22}}
    };


    // ########## BLPR parameters ##############
    // Form arXiv:1703.05330v4, using scheme NoL+SR (No Lattice QCD, Yes QCD sum rules)
    const vector<string> parNameBLPR = {"RhoSq","chi21","chi2p","chi3p","eta1","etap","dV20"};
    const double centralValBLPR[7] = {1.19, -0.06, -0.00, 0.04, 0.35, -0.11, 0.};
    const vector<string> varNameBLPR = {"eig1", "eig2", "eig3", "eig4", "eig5", "eig6", "dV20"};
    const double eigVarBLPR[7][7][2] = { // 2 sigma
      {{ 0.0464, -0.0464}, { 0.0004, -0.0004}, { 0.0005, -0.0005}, { -0.0016, 0.0016}, { 0.1933, -0.1933}, { -0.3377, 0.3377}, {0, 0}},
      {{ -0.0869, 0.0869}, { 0.0095, -0.0095}, { 0.0047, -0.0047}, { -0.0162, 0.0162}, { -0.1963, 0.1963}, { -0.1242, 0.1242}, {0, 0}},
      {{ 0.1260, -0.1260}, { -0.0011, 0.0011}, { -0.0005, 0.0005}, { 0.0195, -0.0195}, { -0.0502, 0.0502}, { -0.0115, 0.0115}, {0, 0}},
      {{ 0.0043, -0.0043}, { 0.0113, -0.0113}, { 0.0056, -0.0056}, { -0.0254, 0.0254}, { 0.0003, -0.0003}, { 0.0009, -0.0009}, {0, 0}},
      {{ -0.0015, 0.0015}, { 0.0129, -0.0129}, { 0.0354, -0.0354}, { 0.0134, -0.0134}, { 0.0008, -0.0008}, { 0.0003, -0.0003}, {0, 0}},
      {{ 0.0012, -0.0012}, { -0.0349, 0.0349}, { 0.0171, -0.0171}, { -0.0116, 0.0116}, { -0.0007, 0.0007}, { -0.0002, 0.0002}, {0, 0}},
      {{ 0.0000, -0.0000}, { 0.0000, -0.0000}, { 0.0000, -0.0000}, { 0.0000, -0.0000}, { 0.0000, -0.0000}, { 0.0000, -0.0000}, { 0.0002, -0.0002}}
    };

    // ############# BGL parameters #################
    // From https://arxiv.org/pdf/2105.14019.pdf
    const vector<string> parNameBGL = {"a0", "a1", "a2", "b0", "b1", "b2", "c1", "c2", "d0", "d1"};
    const double centralValBGL[10] = {0.03200, -0.14800, -0.60000, 0.01246, 0.00380, 0.02000, 0.00008, 0.08000, 0.05260, -0.19400};
    // Default values:               {0.00038,  0.02690,  0.00000, 0.00055,-0.00204, 0.00000,-0.00043, 0.00535, 0.00700, -0.03600};
    const vector<string> varNameBGL = {"eig1", "eig2", "eig3", "eig4", "eig5", "eig6", "eig7", "eig8", "eig9", "eig10"};
    const double eigVarBGL[10][10][2] = { // 2 sigma
      {{ -0.00109, 0.00109}, { 0.03782, -0.03782}, { 0.43814, -0.43814}, { 0.00004, -0.00004}, { 0.00047, -0.00047}, { -0.06926, 0.06926}, { -0.00055, 0.00055}, { 0.03489, -0.03489}, { -0.00039, 0.00039}, { 0.02474, -0.02474} },
      {{ 0.00035, -0.00035}, { -0.02000, 0.02000}, { 0.04029, -0.04029}, { 0.00001, -0.00001}, { -0.00600, 0.00600}, { 0.22956, -0.22956}, { -0.00017, 0.00017}, { -0.00783, 0.00783}, { 0.00045, -0.00045}, { -0.02924, 0.02924} },
      {{ -0.00090, 0.00090}, { 0.04135, -0.04135}, { 0.00225, -0.00225}, { 0.00007, -0.00007}, { -0.00109, 0.00109}, { -0.00254, 0.00254}, { 0.00071, -0.00071}, { -0.05709, 0.05709}, { 0.00075, -0.00075}, { -0.02961, 0.02961} },
      {{ -0.00041, 0.00041}, { 0.01685, -0.01685}, { -0.00188, 0.00188}, { 0.00001, -0.00001}, { 0.00139, -0.00139}, { 0.01003, -0.01003}, { 0.00072, -0.00072}, { -0.02521, 0.02521}, { -0.00079, 0.00079}, { 0.07108, -0.07108} },
      {{ -0.00017, 0.00017}, { 0.00142, -0.00142}, { -0.00014, 0.00014}, { -0.00003, 0.00003}, { 0.00660, -0.00660}, { 0.00033, -0.00033}, { 0.00054, -0.00054}, { 0.00098, -0.00098}, { -0.00016, 0.00016}, { -0.00018, 0.00018} },
      {{ -0.00018, 0.00018}, { 0.00429, -0.00429}, { -0.00053, 0.00053}, { -0.00004, 0.00004}, { -0.00134, 0.00134}, { 0.00054, -0.00054}, { -0.00119, 0.00119}, { 0.00306, -0.00306}, { -0.00079, 0.00079}, { 0.00001, -0.00001} },
      {{ 0.00095, -0.00095}, { 0.00032, -0.00032}, { -0.00004, 0.00004}, { 0.00033, -0.00033}, { -0.00003, 0.00003}, { 0.00004, -0.00004}, { 0.00012, -0.00012}, { 0.00023, -0.00023}, { 0.00234, -0.00234}, { 0.00003, -0.00003} },
      {{ -0.00083, 0.00083}, { -0.00006, 0.00006}, { 0.00000, -0.00000}, { 0.00002, -0.00002}, { 0.00005, -0.00005}, { -0.00001, 0.00001}, { -0.00049, 0.00049}, { -0.00004, 0.00004}, { 0.00037, -0.00037}, { 0.00001, -0.00001} },
      {{ 0.00025, -0.00025}, { -0.00007, 0.00007}, { 0.00001, -0.00001}, { 0.00017, -0.00017}, { 0.00006, -0.00006}, { -0.00001, 0.00001}, { -0.00045, 0.00045}, { -0.00006, 0.00006}, { -0.00008, 0.00008}, { -0.00000, 0.00000} },
      {{ 0.00004, -0.00004}, { -0.00001, 0.00001}, { 0.00000, -0.00000}, { -0.00021, 0.00021}, { 0.00001, -0.00001}, { -0.00000, 0.00000}, { -0.00006, 0.00006}, { -0.00001, 0.00001}, { 0.00002, -0.00002}, { 0.00000, -0.00000} },
    };


    // ########## BLR parameters for D** ##############
    // From https://arxiv.org/abs/1711.03110, Table 5
    // For the D^{3/2+} (narrow) D_1 and D2*
    const vector<string> parNameDststN_BLR = {"t1", "tp", "tau1", "tau2", "eta1", "eta2", "eta3"};
    const double centralValDststN_BLR[7] = {0.7, -1.6, -0.5, 2.9, 0., 0., 0.};
    const vector<string> varNameDststN_BLR = {"eig1", "eig2", "eig3", "eig4"};
    const double eigVarDststN_BLR[4][7][2] = {
      {{ 0.0347, -0.0347}, { -0.0185, 0.0185}, { 0.2695, -0.2695}, { -1.3997, 1.3997},  {0, 0}, {0, 0}, {0, 0}},
      {{ -0.0569, 0.0569}, { 0.1972, -0.1972}, { -0.0437, 0.0437}, { -0.0124, 0.0124},  {0, 0}, {0, 0}, {0, 0}},
      {{ -0.0205, 0.0205}, { -0.0059, 0.0059}, { 0.0004, -0.0004}, { -0.0004, 0.0004},  {0, 0}, {0, 0}, {0, 0}},
      {{ -0.0057, 0.0057}, { 0.0274, -0.0274}, { 0.1243, -0.1243}, { 0.0234, -0.0234},  {0, 0}, {0, 0}, {0, 0}},
    };

    // For the D^{1/2+} (wide) D0* and D_1*
    const vector<string> parNameDststW_BLR = {"zt1", "ztp", "zeta1", "chi1", "chi2"};
    const double centralValDststW_BLR[5] = {0.70, 0.2, 0.6, 0., 0.};
    const vector<string> varNameDststW_BLR = {"eig1", "eig2", "eig3"};
    const double eigVarDststW_BLR[3][5][2] = {
      {{ 0.1991, -0.1991}, { -1.3997, 1.3997}, { -0.1873, 0.1873},  {0, 0}, {0, 0}},
      {{ -0.0514, 0.0514}, { -0.0084, 0.0084}, { 0.0085, -0.0085},  {0, 0}, {0, 0}},
      {{ 0.0427, -0.0427}, { -0.0253, 0.0253}, { 0.2342, -0.2342},  {0, 0}, {0, 0}},
    };


    // For the D(2S) and D(2S)*, fitted by Michele on ISGW2
    const vector<string> parNameD2S_BLOP = {"RhoSq", "Cur",  "chi11", "chi21", "chi2p", "chi31", "chi3p", "eta1", "etap"};
    const double centralValD2S_BLOP[9] = {  -0.266,  -0.465, -0.120,  0.046,   0.144,   -0.077,  0.037,   -0.465,  0.168};
    const vector<string> varNameD2S_BLOP = {"RhoSq", "chi11", "chi21", "chi31", "eta1"};
    const double eigVarD2S_BLOP[5][9][2] = {
      {{0.1, -0.1}, {0, 0},      {0, 0},        {0, 0},        {0, 0},      {0, 0},        {0, 0},        {0, 0},      {0, 0}     },
      {{0, 0},      {0, 0},      {0.05, -0.05}, {0, 0},        {0, 0},      {0, 0},        {0, 0},        {0, 0},      {0, 0}     },
      {{0, 0},      {0, 0},      {0, 0},        {0.02, -0.02}, {0, 0},      {0, 0},        {0, 0},        {0, 0},      {0, 0}     },
      {{0, 0},      {0, 0},      {0, 0},        {0, 0},        {0, 0},      {0.04, -0.04}, {0, 0},        {0, 0},      {0, 0}     },
      {{0, 0},      {0, 0},      {0, 0},        {0, 0},        {0, 0},      {0, 0},        {0, 0},        {0.2, -0.2}, {0, 0}     },
    };



    int N_evets_weights_produced = 0;
    int N_evets_analyzed = 0;
};



HammerWeightsProducer::HammerWeightsProducer(const edm::ParameterSet &iConfig)
{
    PrunedParticlesSrc_ = consumes<vector<reco::GenParticle>>(edm::InputTag("prunedGenParticles"));
    indexBmcSrc_ = consumes<int> (edm::InputTag("MCpart", "indexBmc"));

    verbose = iConfig.getParameter<int>( "verbose" );

    hammer.setUnits("GeV");
    auto decayOfInterest = iConfig.getParameter<vector<string>>( "decayOfInterest" );
    for(auto s : decayOfInterest) {
      if(verbose) {cout << "[Hammer]: Including decay " << s << endl;}
      hammer.includeDecay(s);
    }

    auto inputFFScheme_ = iConfig.getParameter<vector<string>>("inputFFScheme");
    if(verbose) {cout << "[Hammer]: Input scheme" << endl;}
    map<string, string> inputFFScheme;
    for(uint i = 0; i < inputFFScheme_.size(); i++) {
      if(i%2 == 1) continue;
      inputFFScheme[inputFFScheme_[i]] = inputFFScheme_[i+1];
      if(verbose){cout << "\t" << inputFFScheme_[i] << ": " << inputFFScheme_[i+1] << endl;}
    }
    hammer.setFFInputScheme(inputFFScheme);
    hammer.addFFScheme("SchmeCLN",       {{"BD*",  "CLNVar"},  {"BD**", "ISGW2"}, {"BD2sall", "ISGW2"}});
    hammer.addFFScheme("SchmeBLPR",      {{"BD*",  "BLPRVar"}, {"BD**", "ISGW2"}, {"BD2sall", "ISGW2"}});
    hammer.addFFScheme("SchmeBGL",       {{"BD*",  "BGLVar"},  {"BD**", "ISGW2"}, {"BD2sall", "ISGW2"}});
    hammer.addFFScheme("SchmeBLR_Dstst", {{"BD*",  "ISGW2"}, {"BD**1", "BLRVar"}, {"BD**2*", "BLRVar"}, {"BD**0*","BLRVar"}, {"BD**1*", "BLRVar"}, {"BD2sall", "ISGW2"}});
    hammer.addFFScheme("SchmeBLOP_D2s",  {{"BD*",  "ISGW2"}, {"BD**", "ISGW2"}, {"BD2sall", "BLOPVar"}});
    hammer.initRun();

    // ################# CLN for D* ##############################
    string centralValuesOpt = "BtoD*CLNVar: {";
    for(auto i=0; i<4; i++) {
      centralValuesOpt += Form("%s: %f, ", parNameCLN[i].c_str(), centralValCLN[i]);
    }
    centralValuesOpt += "}";
    if (verbose) {cout << "[Hammer]: CLN central values\n\t" << centralValuesOpt << endl;}
    hammer.setOptions(centralValuesOpt);
    hammer.specializeWCInWeights("BtoCTauNu",{{"SM", 1.0}});
    hammer.specializeWCInWeights("BtoCMuNu",{{"SM", 1.0}});

    // ################# BLPR for D* ##############################
    centralValuesOpt = "BtoD*BLPRVar: {";
    for(auto i=0; i<7; i++) {
      centralValuesOpt += Form("%s: %f, ", parNameBLPR[i].c_str(), centralValBLPR[i]);
    }
    centralValuesOpt += "}";
    if (verbose) {cout << "[Hammer]: BLPR central values\n\t" << centralValuesOpt << endl;}
    hammer.setOptions(centralValuesOpt);
    hammer.specializeWCInWeights("BtoD*BLPRVar",{{"SM", 1.0}});

    // ################# BGL for D* ##############################
    centralValuesOpt = "BtoD*BGLVar: {";
    centralValuesOpt += Form("avec: [%f, %f, %f], ", centralValBGL[0], centralValBGL[1], centralValBGL[2]);
    centralValuesOpt += Form("bvec: [%f, %f, %f], ", centralValBGL[3], centralValBGL[4], centralValBGL[5]);
    centralValuesOpt += Form("cvec: [%f, %f], ", centralValBGL[6], centralValBGL[7]);
    centralValuesOpt += Form("dvec: [%f, %f]", centralValBGL[8], centralValBGL[9]);
    centralValuesOpt += "}";
    if (verbose) {cout << "[Hammer]: BGL central values\n\t" << centralValuesOpt << endl;}
    hammer.setOptions(centralValuesOpt);

    // Other options still from https://arxiv.org/pdf/2105.14019.pdf
    centralValuesOpt = "BtoD*BGLVar: {";
    //  Tab 9 line 2: f,F1 (1+)
    centralValuesOpt += Form("BcStatesf: [6.739, 6.750, 7.145, 7.150], ");
    // Hammer default                     6.730, 6.736, 7.135, 7.142}; //GeV
    //  Tab 9 line 1: g (1-)
    centralValuesOpt += Form("BcStatesg: [6.329, 6.920, 7.020, 7.280], ");
    // Hammer default                     6.337, 6.899, 7.012, 7.280}; //GeV
    //  Tab 9 line 3: F2 (0-)
    centralValuesOpt += Form("BcStatesP1: [6.275, 6.842, 7.250], ");
    // Hammer default                      6.275, 6.842, 7.250}; //GeV
    // Vcb from abstract rest from Tab 10 (chi+ and - inverted)
    centralValuesOpt += Form("Vcb: 38.4e-3, Chim: 3.894e-4, Chip: 5.131e-4, ChimL: 19.42e-3");
    // Hammer defaults        Vcb: 41.5e-3, Chim: 3.068e-4, Chip: 5.280e-4, ChimL:  2.466e-3
    centralValuesOpt += "}";
    if (verbose) {cout << "[Hammer]: BGL additional settings\n\t" << centralValuesOpt << endl;}
    hammer.setOptions(centralValuesOpt);
    hammer.specializeWCInWeights("BtoD*BGLVar",{{"SM", 1.0}});


    // ################# BLR for D** ##############################
    centralValuesOpt = "BtoD**nBLRVar: {";
    for(auto i=0; i<7; i++) {
      centralValuesOpt += Form("%s: %f, ", parNameDststN_BLR[i].c_str(), centralValDststN_BLR[i]);
    }
    centralValuesOpt += "}";
    if (verbose) {cout << "[Hammer]: BLR for D** central values\n\t" << centralValuesOpt << endl;}
    hammer.setOptions(centralValuesOpt);
    hammer.specializeWCInWeights("BtoD**nBLRVar",{{"SM", 1.0}});

    centralValuesOpt = "BtoD**wBLRVar: {";
    for(auto i=0; i<5; i++) {
      centralValuesOpt += Form("%s: %f, ", parNameDststW_BLR[i].c_str(), centralValDststW_BLR[i]);
    }
    centralValuesOpt += "}";
    if (verbose) {cout << "[Hammer]: BLR for D** central values\n\t" << centralValuesOpt << endl;}
    hammer.setOptions(centralValuesOpt);
    hammer.specializeWCInWeights("BtoD**wBLRVar",{{"SM", 1.0}});


    // ################# BLOP for D(2S) ##############################
    centralValuesOpt = "BtoD2sallBLOPVar: {";
    for(auto i=0; i<9; i++) {
      centralValuesOpt += Form("%s: %f, ", parNameD2S_BLOP[i].c_str(), centralValD2S_BLOP[i]);
    }
    centralValuesOpt += "}";
    if (verbose) {cout << "[Hammer]: BLOP for D(2S) central values\n\t" << centralValuesOpt << endl;}
    hammer.setOptions(centralValuesOpt);
    hammer.specializeWCInWeights("BtoD2sallBLOPVar",{{"SM", 1.0}});


    // ######## Products declaration ####################
    produces<map<string, float>>("outputNtuplizer");
}


void HammerWeightsProducer::produce(edm::Event& iEvent, const edm::EventSetup& iSetup) {
    if (verbose) {cout << "-----------  Hammer weights ----------\n";}
    N_evets_analyzed++;
    // Get prunedGenParticles
    edm::Handle<std::vector<reco::GenParticle>> PrunedGenParticlesHandle;
    iEvent.getByToken(PrunedParticlesSrc_, PrunedGenParticlesHandle);

    // Output collection
    unique_ptr<map<string, float>> outputNtuplizer(new map<string, float>);

    // Get the B index
    edm::Handle<int> indexBmcHandle;
    iEvent.getByToken(indexBmcSrc_, indexBmcHandle);
    int i_B = (*indexBmcHandle);
    if(i_B == -1){
      cout << "[ERROR]: Invalid B idx (i.e. no B MC set)" << endl;
      cerr << "[ERROR]: Invalid B idx (i.e. no B MC set)" << endl;
      (*outputNtuplizer)["wh_CLNCentral"] = 1e-9;
      for(int i=0; i<4; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "CLN" + varNameCLN[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }

      (*outputNtuplizer)["wh_BLPRCentral"] = 1e-9;
      for(int i=0; i<7; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "BLPR" + varNameBLPR[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }

      (*outputNtuplizer)["wh_BGLCentral"] = 1e-9;
      for(int i=0; i<10; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "BGL" + varNameBLPR[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }

      (*outputNtuplizer)["wh_Dstst_BLRCentral"] = 1e-9;
      for(int i=0; i<4; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "DststN_BLR" + varNameDststN_BLR[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }
      for(int i=0; i<3; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "DststW_BLR" + varNameDststW_BLR[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }

      (*outputNtuplizer)["wh_D2S_BLOPCentral"] = 1e-9;
      for(int i=0; i<5; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "D2S_BLOP" + varNameD2S_BLOP[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }

      iEvent.put(move(outputNtuplizer), "outputNtuplizer");
      cout << N_evets_weights_produced << "/" << N_evets_analyzed << endl;
      bool lessThanHalfProduced = 2*N_evets_weights_produced < N_evets_analyzed;
      bool veryFewMissing = 1 - ((float)N_evets_weights_produced)/N_evets_analyzed < 0.05;
      bool moreThanOne = N_evets_analyzed - N_evets_weights_produced > 1;
      if (lessThanHalfProduced) return;
      else if (veryFewMissing || !moreThanOne ) return;
      else exit(1);
    }
    // cout << "i_B retieved: " << i_B << endl;

    // Initialize the Hammer event
    hammer.initEvent();
    Hammer::Process B2DstLNu_Dst2DPi;
    vector<size_t> Bvtx_idxs;
    int idxTau = -1;
    vector<size_t> Tauvtx_idxs;
    int idxDst = -1;
    vector<size_t> Dstvtx_idxs;

    TLorentzVector p4B;
    vector<TLorentzVector> pDauB;
    vector<TLorentzVector> pDauDst;
    bool DstFound = false;
    bool Dstst_indecay = false;
    bool Dst_indecay = false;
    int idxDst_v = -1;

    auto p = (*PrunedGenParticlesHandle)[i_B];
    Hammer::Particle pB({p.energy(), p.px(), p.py(), p.pz()}, p.pdgId());
    if(verbose) {
      p4B.SetPxPyPzE(p.px(), p.py(), p.pz(), p.energy());
      cout << Form("B --> E:%.5f, Px:%.5f, Py:%.5f, Pz:%.5f PDG:%d", p.energy(), p.px(), p.py(), p.pz(), p.pdgId()) << endl;
    }
    auto idxB = B2DstLNu_Dst2DPi.addParticle(pB);
    for(auto d : p.daughterRefVector()) {
      Hammer::Particle B_dau({d->energy(), d->px(), d->py(), d->pz()}, d->pdgId());
      if(verbose) {
        TLorentzVector v;
        v.SetPxPyPzE(d->px(), d->py(), d->pz(), d->energy());
        pDauB.push_back(v);
        if(!DstFound){
          idxDst_v++;
          if (abs(d->pdgId()) == 413) DstFound = true;
        }
        cout << Form("%d --> E:%.5f, Px:%.5f, Py:%.5f, Pz:%.5f", d->pdgId(), d->energy(), d->px(), d->py(), d->pz()) << endl;
      }
      auto idx_d = B2DstLNu_Dst2DPi.addParticle(B_dau);
      Bvtx_idxs.push_back(idx_d);

      auto d_absId = abs(d->pdgId());
      bool isTau = d_absId == 15;
      bool isDst = d_absId == 413;
      bool isD1 = (d_absId == 10413 || d_absId == 10423);
      bool isD1st = (d_absId == 20413 || d_absId == 20423);
      bool isD2st = (d_absId == 415 || d_absId == 425);
      bool isD2S = (d_absId == 100411 || d_absId == 100421);
      bool isDst2S = (d_absId == 100413 || d_absId == 100423);
      bool isDstst = (isD1 || isD1st || isD2st || isD2S || isDst2S);
      if (isDstst) Dstst_indecay = true;
      if (isDst) Dst_indecay = true;
      if(isTau) {
        idxTau = idx_d;
        for (auto dd : d->daughterRefVector()) {
          if(verbose) { cout << Form("\t%d --> E:%.5f, Px:%.5f, Py:%.5f, Pz:%.5f", dd->pdgId(), dd->energy(), dd->px(), dd->py(), dd->pz()) << endl;}
          Hammer::Particle Tau_dau({dd->energy(), dd->px(), dd->py(), dd->pz()}, dd->pdgId());
          auto idx_dd = B2DstLNu_Dst2DPi.addParticle(Tau_dau);
          Tauvtx_idxs.push_back(idx_dd);
        }
      }
      else if(isDst || isDstst) {
        idxDst = idx_d;
        for (auto dd : d->daughterRefVector()) {
          if(verbose) {
            TLorentzVector v;
            v.SetPxPyPzE(dd->px(), dd->py(), dd->pz(), dd->energy());
            pDauDst.push_back(v);
            cout << Form("\t%d --> E:%.5f, Px:%.5f, Py:%.5f, Pz:%.5f", dd->pdgId(), dd->energy(), dd->px(), dd->py(), dd->pz()) << endl;
          }
          Hammer::Particle Dst_dau({dd->energy(), dd->px(), dd->py(), dd->pz()}, dd->pdgId());
          auto idx_dd = B2DstLNu_Dst2DPi.addParticle(Dst_dau);
          Dstvtx_idxs.push_back(idx_dd);
        }
      }
    }

    if(verbose) {
      TLorentzVector vtxB;
      vtxB = p4B;
      for(auto v : pDauB) {
        cout << "Mass: " << v.M() << endl;
        vtxB -= v;
      }
      cout << "B vertex coservation " << endl;
      vtxB.Print();

      TLorentzVector vtxDst;
      vtxDst = pDauB[idxDst_v];
      for(auto v : pDauDst) {
        cout << "Mass: " << v.M() << endl;
        vtxDst -= v;
      }
      cout << "Dst vertex coservation " << endl;
      vtxDst.Print();
    }

    B2DstLNu_Dst2DPi.addVertex(idxB, Bvtx_idxs);
    if(idxTau != -1) {
      B2DstLNu_Dst2DPi.addVertex(idxTau, Tauvtx_idxs);
    }
    if(idxDst != -1) {
      B2DstLNu_Dst2DPi.addVertex(idxDst, Dstvtx_idxs);
    }

    try {
      hammer.addProcess(B2DstLNu_Dst2DPi);
      hammer.processEvent();
    }
    catch (const std::exception& e) {
      cout << "[ERROR] In processing Hammer event with message" << e.what() << endl;
      return;
    }


    if (Dst_indecay) {
      if (verbose) {cout << "Computing Hammer weights for a D* decay" << endl;}
      map<string, double> settingsCLN;
      for(auto n: parNameCLN) settingsCLN["delta_" + n] = 0;
      hammer.setFFEigenvectors("BtoD*", "CLNVar", settingsCLN);
      auto weightsCLN = hammer.getWeights("SchmeCLN");
      if(!weightsCLN.empty()) {
        N_evets_weights_produced++;
        if(verbose) {cout << "CLNCentral: " << flush;}
        for(auto elem: weightsCLN) {
          if(isnan(elem.second)) {
            cout << "[ERROR]: CLNCentral nan weight: " << elem.second << endl;
            cerr << "[ERROR]: CLNCentral nan weight: " << elem.second << endl;
            assert(false);
          }
          (*outputNtuplizer)["wh_CLNCentral"] = elem.second;
          if(verbose) {cout << elem.second << endl;}
        }

        for(int i=0; i<4; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<4; k++) { //Loop over parameters
              settings["delta_" + parNameCLN[k]] = eigVarCLN[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD*", "CLNVar", settings);
            auto weights = hammer.getWeights("SchmeCLN");
            string var_name = "CLN" + varNameCLN[i];
            var_name += j==0? "Up" : "Down";

            if(verbose) {cout << var_name << ": " << flush;}
            for(auto elem: weights) {
              (*outputNtuplizer)["wh_" + var_name] = elem.second;
              if(verbose) {cout << elem.second << Form(" (Ratio to central = %.3f)", (*outputNtuplizer)["wh_" + var_name]/(*outputNtuplizer)["wh_CLNCentral"])<< endl;}
            }
          }
        }
        if(verbose) {cout << endl;}
      }

      map<string, double> settingsBLPR;
      for(auto n: parNameBLPR) settingsBLPR["delta_" + n] = 0;
      hammer.setFFEigenvectors("BtoD*", "BLPRVar", settingsBLPR);
      auto weightsBLPR = hammer.getWeights("SchmeBLPR");
      if(!weightsBLPR.empty()) {
        if(verbose) {cout << "BLPRCentral: " << flush;}
        for(auto elem: weightsBLPR) {
          if(isnan(elem.second)) {
            cout << "[ERROR]: BLPRCentral nan weight: " << elem.second << endl;
            cerr << "[ERROR]: BLPRCentral nan weight: " << elem.second << endl;
            assert(false);
          }
          (*outputNtuplizer)["wh_BLPRCentral"] = elem.second;
          if(verbose) {cout << elem.second << endl;}
        }

        for(int i=0; i<7; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<7; k++) { //Loop over parameters
              settings["delta_" + parNameBLPR[k]] = eigVarBLPR[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD*", "BLPRVar", settings);
            auto weights = hammer.getWeights("SchmeBLPR");
            string var_name = "BLPR" + varNameBLPR[i];
            var_name += j==0? "Up" : "Down";

            if(verbose) {cout << var_name << ": " << flush;}
            for(auto elem: weights) {
              (*outputNtuplizer)["wh_" + var_name] = elem.second;
              if(verbose) {cout << elem.second << endl;}
            }
          }
        }
        if(verbose) {cout << endl;}
      }


      map<string, double> settingsBGL;
      for(auto n: parNameBGL) settingsBGL["delta_" + n] = 0;
      hammer.setFFEigenvectors("BtoD*", "BGLVar", settingsBGL);
      auto weightsBGL = hammer.getWeights("SchmeBGL");
      if(!weightsBGL.empty()) {
        if(verbose) {cout << "BGLCentral: " << flush;}
        for(auto elem: weightsBGL) {
          if(isnan(elem.second)) {
            cout << "[ERROR]: BGLCentral nan weight: " << elem.second << endl;
            cerr << "[ERROR]: BGLCentral nan weight: " << elem.second << endl;
            assert(false);
          }
          (*outputNtuplizer)["wh_BGLCentral"] = elem.second;
          if(verbose) {cout << elem.second << endl;}
        }

        for(int i=0; i<10; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<10; k++) { //Loop over parameters
              settings["delta_" + parNameBGL[k]] = eigVarBGL[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD*", "BGLVar", settings);
            auto weights = hammer.getWeights("SchmeBGL");
            string var_name = "BGL" + varNameBGL[i];
            var_name += j==0? "Up" : "Down";

            if(verbose) {cout << var_name << ": " << flush;}
            for(auto elem: weights) {
              (*outputNtuplizer)["wh_" + var_name] = elem.second;
              if(verbose) {cout << elem.second << endl;}
            }
          }
        }
        if(verbose) {cout << endl;}
      }

      (*outputNtuplizer)["wh_Dstst_BLRCentral"] = 0;
      for(int i=0; i<4; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "DststN_BLR" + varNameDststN_BLR[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }
      for(int i=0; i<3; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "DststW_BLR" + varNameDststW_BLR[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }

      (*outputNtuplizer)["wh_D2S_BLOPCentral"] = 0;
      for(int i=0; i<5; i++) { //Loop over eigenVar
        for (int j=0; j<2; j++) { //Loop over pos/neg direction
          string var_name = "D2S_BLOP" + varNameD2S_BLOP[i];
          var_name += j==0? "Up" : "Down";
          (*outputNtuplizer)["wh_" + var_name] = 0;
        }
      }
    }


    if (Dstst_indecay) {
      if (verbose) {cout << "Computing Hammer weights for a D** decay" << endl;}
      map<string, double> settings_central_N;
      map<string, double> settings_central_W;
      for(auto n: parNameDststN_BLR) settings_central_N["delta_" + n] = 0;
      for(auto n: parNameDststW_BLR) settings_central_W["delta_" + n] = 0;
      hammer.setFFEigenvectors("BtoD**1", "BLRVar", settings_central_N);
      hammer.setFFEigenvectors("BtoD**2*", "BLRVar", settings_central_N);
      hammer.setFFEigenvectors("BtoD**0*", "BLRVar", settings_central_W);
      hammer.setFFEigenvectors("BtoD**1*", "BLRVar", settings_central_W);
      // hammer.setFFEigenvectors("BtoD**", "BLRVar", settings_central);
      auto weightsDstst_BLR = hammer.getWeights("SchmeBLR_Dstst");
      if(!weightsDstst_BLR.empty()) {
        N_evets_weights_produced++;
        if(verbose) {cout << "Dstst_BLRCentral: " << flush;}
        for(auto elem: weightsDstst_BLR) {
          if(isnan(elem.second)) {
            cout << "[ERROR]: Dstst_BLRCentral nan weight: " << elem.second << endl;
            cerr << "[ERROR]: Dstst_BLRCentral nan weight: " << elem.second << endl;
            assert(false);
          }
          (*outputNtuplizer)["wh_Dstst_BLRCentral"] = elem.second;
          if(verbose) {cout << elem.second << endl;}
        }

        for(int i=0; i<4; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<7; k++) { //Loop over parameters
              settings["delta_" + parNameDststN_BLR[k]] = eigVarDststN_BLR[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD**1", "BLRVar", settings);
            hammer.setFFEigenvectors("BtoD**2*", "BLRVar", settings);
            auto weights = hammer.getWeights("SchmeBLR_Dstst");
            string var_name = "DststN_BLR" + varNameDststN_BLR[i];
            var_name += j==0? "Up" : "Down";

            if(verbose) {cout << var_name << ": " << flush;}
            for(auto elem: weights) {
              (*outputNtuplizer)["wh_" + var_name] = elem.second;
              if(verbose) {cout << elem.second << Form(" (Ratio to central = %.3f)", (*outputNtuplizer)["wh_" + var_name]/(*outputNtuplizer)["wh_Dstst_BLRCentral"])<< endl;}
            }
          }
        }

        hammer.setFFEigenvectors("BtoD**1", "BLRVar", settings_central_N);
        hammer.setFFEigenvectors("BtoD**2*", "BLRVar", settings_central_N);
        hammer.setFFEigenvectors("BtoD**0*", "BLRVar", settings_central_W);
        hammer.setFFEigenvectors("BtoD**1*", "BLRVar", settings_central_W);
        // hammer.setFFEigenvectors("BtoD**", "BLRVar", settings_central);
        for(int i=0; i<3; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<5; k++) { //Loop over parameters
              settings["delta_" + parNameDststW_BLR[k]] = eigVarDststW_BLR[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD**0*", "BLRVar", settings);
            hammer.setFFEigenvectors("BtoD**1*", "BLRVar", settings);
            // hammer.setFFEigenvectors("BtoD**", "BLRVar", settings);
            auto weights = hammer.getWeights("SchmeBLR_Dstst");
            string var_name = "DststW_BLR" + varNameDststW_BLR[i];
            var_name += j==0? "Up" : "Down";

            if(verbose) {cout << var_name << ": " << flush;}
            for(auto elem: weights) {
              (*outputNtuplizer)["wh_" + var_name] = elem.second;
              if(verbose) {cout << elem.second << Form(" (Ratio to central = %.3f)", (*outputNtuplizer)["wh_" + var_name]/(*outputNtuplizer)["wh_Dstst_BLRCentral"])<< endl;}
            }
          }
        }

        if(verbose) {cout << endl;}

      }
      else {
        (*outputNtuplizer)["wh_Dstst_BLRCentral"] = 1;
        for(int i=0; i<4; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            string var_name = "DststN_BLR" + varNameDststN_BLR[i];
            var_name += j==0? "Up" : "Down";
            (*outputNtuplizer)["wh_" + var_name] = 1;
          }
        }

        for(int i=0; i<3; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            string var_name = "DststW_BLR" + varNameDststW_BLR[i];
            var_name += j==0? "Up" : "Down";
            (*outputNtuplizer)["wh_" + var_name] = 1.;
          }
        }
      }


      if (verbose) {cout << "Computing Hammer weights for D2S decay" << endl;}
      map<string, double> settings_central;
      for(auto n: parNameD2S_BLOP) settings_central["delta_" + n] = 0;
      hammer.setFFEigenvectors("BtoD(2s)", "BLOPVar", settings_central);
      hammer.setFFEigenvectors("BtoD(2s)*", "BLOPVar", settings_central);
      auto weightsD2S_BLOP = hammer.getWeights("SchmeBLOP_D2s");
      if(!weightsD2S_BLOP.empty()) {
        N_evets_weights_produced++;
        if(verbose) {cout << "D2S_BLOPCentral: " << flush;}
        for(auto elem: weightsD2S_BLOP) {
          if(isnan(elem.second)) {
            cout << "[ERROR]: D2S_BLOPCentral nan weight: " << elem.second << endl;
            cerr << "[ERROR]: D2S_BLOPCentral nan weight: " << elem.second << endl;
            assert(false);
          }
          (*outputNtuplizer)["wh_D2S_BLOPCentral"] = elem.second;
          if(verbose) {cout << elem.second << endl;}
        }

        for(int i=0; i<5; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            map<string, double> settings;
            for (int k=0; k<9; k++) { //Loop over parameters
              settings["delta_" + parNameD2S_BLOP[k]] = eigVarD2S_BLOP[i][k][j];
            }

            hammer.setFFEigenvectors("BtoD(2s)", "BLOPVar", settings);
            hammer.setFFEigenvectors("BtoD(2s)*", "BLOPVar", settings);
            auto weights = hammer.getWeights("SchmeBLOP_D2s");
            string var_name = "D2S_BLOP" + varNameD2S_BLOP[i];
            var_name += j==0? "Up" : "Down";

            if(verbose) {cout << var_name << ": " << flush;}
            for(auto elem: weights) {
              (*outputNtuplizer)["wh_" + var_name] = elem.second;
              if(verbose) {cout << elem.second << Form(" (Ratio to central = %.3f)", (*outputNtuplizer)["wh_" + var_name]/(*outputNtuplizer)["wh_D2S_BLOPCentral"])<< endl;}
            }
          }
        }
        if(verbose) {cout << endl;}

      }
      else {
        (*outputNtuplizer)["wh_D2S_BLOPCentral"] = 1;
        for(int i=0; i<5; i++) { //Loop over eigenVar
          for (int j=0; j<2; j++) { //Loop over pos/neg direction
            string var_name = "D2S_BLOP" + varNameD2S_BLOP[i];
            var_name += j==0? "Up" : "Down";
            (*outputNtuplizer)["wh_" + var_name] = 1;
          }
        }
      }

    }

    // cout << "[DEBUG] Check this part on how to handle empty weights" << endl;
    if (!Dst_indecay && ! Dstst_indecay) return;

    iEvent.put(move(outputNtuplizer), "outputNtuplizer");
    return;
}


DEFINE_FWK_MODULE(HammerWeightsProducer);
