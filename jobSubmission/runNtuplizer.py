#!/usr/bin/env python
from __future__ import print_function, division
from time import sleep
import os

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

FILE_PATH = os.path.dirname(os.path.abspath(__file__))

OUT_LOC_MC = '/storage/af/group/rdst_analysis/BPhysics/data/cmsMC'
OUT_LOC_RD = '/storage/af/group/rdst_analysis/BPhysics/data/cmsRD'

PROCESSES = [
    # Ancillary measurments samples --> Should be run N = 3
    ("CP_General_BdToJpsiKstar_BMuonFilter_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    # Central production --> Should be run N = 3
    ("CP_BdToDstarMuNu_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BdToDstarTauNu_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BuToMuNuDstPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BdToMuNuDstPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BuToTauNuDstPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BdToTauNuDstPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BsToMuNuDstK_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BsToTauNuDstK_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BdToDstDu_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BdToDstDd_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BdToDstDs_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BuToDstDu_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BuToDstDd_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    ("CP_BsToDstDs_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",8,'48h'),
    #
    # Private production --> Should be run N = 100
    ("CP_BdToMuNuDstPiPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen_v3",100,'120m'),
    ("CP_BuToMuNuDstPiPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen_v3",100,'120m'),
    ("CP_BdToTauNuDstPiPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",100,'120m'),
    ("CP_BuToTauNuDstPiPi_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen",100,'120m'),
    ("BParking_Tag_Bd_DDs1_SoftQCDnonD_TuneCP5_13TeV-pythia8",100,'120m'),
    ("BParking_Tag_Bu_DDs1_SoftQCDnonD_TuneCP5_13TeV-pythia8",100,'120m'),
    ("BParking_Tag_B_DstDXX_SoftQCDnonD_TuneCP5_13TeV-pythia8",100,'120m')
]

DATASETS = {
    #'Tag_MC': ('ntuples_TagAndProbeTrigger','cmssw_centralMC_TagAndProbeTrigger.py'),
    'Tag_RD': ('TagAndProbeTrigger','cmssw_cmsRD2018_TagAndProbeTrigger.py'),
    'Bd2JpsiKst_MC': ('ntuples_Bd2JpsiKst','cmssw_centralMC_Bd_JpsiKst-mumuKpi.py'),
    'Bd2JpsiKst_RD': ('Bd2JpsiKst','cmssw_cmsRD2018_Bd_JpsiKst-mumuKpi.py'),
    'B2DstMu_MC': ('ntuples_B2DstMu','cmssw_centralMC_Tag_Bd_MuDst-PiPiK.py'),
    'B2DstMu_RD': ('B2DstMu','cmssw_cmsRD2018_Tag_Bd_MuDst-PiPiK.py'),
}

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser("Run ntuplizer")
    parser.add_argument("-v", "--ntuplesName", default=None, type=str, help="ntuple tag")
    args = parser.parse_args()

    if args.ntuplesName is None:
        args.ntuplesName = raw_input("Enter ntuple tag (Example: 220412): ")

    for dataset, (ntuples_prefix, config) in DATASETS.items():
        answer = None
        while answer not in ('y','n'):
            answer = raw_input("Process %s [y/n]: " % dataset)
        if answer == 'n':
            continue

        ntuplesName = "%s_%s" % (ntuples_prefix,args.ntuplesName)

        if 'MC' in dataset:
            for process, n_files_per_job, max_time in PROCESSES:
                if 'Jpsi' in dataset and 'Jpsi' not in process:
                    continue
                if 'Jpsi' not in dataset and 'Jpsi' in process:
                    continue
                output_dir = os.path.join(OUT_LOC_MC,process,ntuplesName)
                if os.path.exists(output_dir):
                    print(bcolors.FAIL + "Warning: output directory '%s' already exists!" % output_dir + bcolors.ENDC)
                os.system("mkdir -p %s" % output_dir)
                cmd = "python %s/create-condor-jobs -i production/inputFiles_%s.txt -o %s/out_CAND.root -c %s/../config/%s -t %s -N %s --maxtime %s" % (FILE_PATH, process,output_dir,FILE_PATH,config,ntuplesName,n_files_per_job,max_time)
                print(cmd)
                os.system(cmd)
                sleep(1)
        else:
            for i in range(1,6):
                output_dir = "%s/ParkingBPH%i/Run2018D-05May2019promptD-v1_RDntuplizer_%s" % (OUT_LOC_RD,i,ntuplesName)
                if os.path.exists(output_dir):
                    print(bcolors.FAIL + "Warning: output directory '%s' already exists!" % output_dir + bcolors.ENDC)
                os.system("mkdir -p %s" % output_dir)
                n_files_per_job = 40
                max_run_time = '48h'
                nice = 1
                cmd = "python %s/create-condor-jobs -i production/inputFiles_ParkingBPH%i_Run2018D-05May2019promptD-v1_MINIAOD.txt -o %s/out_CAND.root -c %s/../config/%s -t %s -N %i --maxtime %s --nice %i" % (FILE_PATH, i,output_dir,FILE_PATH,config,ntuplesName,n_files_per_job,max_run_time,nice)
                print(cmd)
                os.system(cmd)
                sleep(1)
