import os, sys
import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing

from Configuration.StandardSequences.Eras import eras
process = cms.Process('BPHRDntuplizer', eras.Run2_2018)
# import of standard configurations
process.load('FWCore.MessageService.MessageLogger_cfi')


process.load("TrackingTools/TransientTrack/TransientTrackBuilder_cfi")
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')

from Configuration.AlCa.GlobalTag import GlobalTag
process.GlobalTag = GlobalTag(process.GlobalTag, '102X_dataRun2_v11', '')

'''
############ Command line args ################
'''

args = VarParsing.VarParsing('analysis')
args.register('inputFile', '', args.multiplicity.list, args.varType.string, "Input file or template for glob")
args.register('useLocalLumiList', 1, args.multiplicity.singleton, args.varType.int, "Flag to use local lumi list")
args.outputFile = ''
args.parseArguments()


'''
#####################   Input    ###################
'''
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(args.maxEvents)
)

from glob import glob
if args.inputFile:
    flist = args.inputFile
elif args.inputFiles:
    if len(args.inputFiles) == 1 and args.inputFiles[0].endswith('.txt'):
        with open(args.inputFiles[0]) as f:
            flist = [l[:-1] for l in f.readlines()]
    else:
        flist = args.inputFiles
else:
    fdefault = os.environ['CMSSW_BASE'] + '/src/ntuplizer/BPH_RDntuplizer/production/'
    fdefault += 'inputFiles_ParkingBPH1_Run2018D-05May2019promptD-v1_MINIAOD.txt'
    with open(fdefault) as f:
        flist = [l[:-1] for l in f.readlines()]
    nTot = len(flist)
    i0 = min(5, nTot)
    i1 = int(nTot/3.0)
    i2 = int(nTot*2.0/3.0)
    flist = [flist[i0], flist[i1], flist[i2], flist[-5]]

# print 'Trying to get a local copy'
for i in range(len(flist)):
    if os.path.isfile(flist[i]):
        flist[i] = 'file:' + flist[i]
    elif flist[i].startswith('/store/data/'):
        if os.path.isfile('/storage/cms' + flist[i]):
            flist[i] = 'file:' + '/storage/cms' + flist[i]

process.source = cms.Source("PoolSource",
                            fileNames = cms.untracked.vstring(tuple(flist))
                           )

if args.useLocalLumiList:
    lumiListFile = os.environ['CMSSW_BASE'] + '/src/ntuplizer/BPH_RDntuplizer/production/Cert_314472-325175_13TeV_17SeptEarlyReReco2018ABC_PromptEraD_Collisions18_JSON.txt'
    if os.path.isfile(lumiListFile):
        import FWCore.PythonUtilities.LumiList as LumiList
        process.source.lumisToProcess = LumiList.LumiList(filename = lumiListFile).getVLuminosityBlockRange()

'''
#####################   Output   ###################
'''
if args.outputFile == '.root':
    outname = 'TagAndProbeTrigger_CAND.root'
elif args.outputFile.startswith('_numEvent'):
    outname = 'TagAndProbeTrigger_CAND' + args.outputFile
else:
    outname = args.outputFile

process.TFileService = cms.Service("TFileService",
      fileName = cms.string(outname),
      closeFileFast = cms.untracked.bool(True)
      )



'''
#################   Sequence    ####################
'''
process.l1bits=cms.EDProducer("L1TriggerResultsConverter",
                              src=cms.InputTag("gtStage2Digis"),
                              # src_ext=cms.InputTag("gtStage2Digis"),
                              # storeUnprefireableBit=cms.bool(True),
                              legacyL1=cms.bool(False),
)

process.TnP = cms.EDFilter("TagAndProbeTriggerMuonFilter",
        muonIDScaleFactors = cms.int32(0),
        requireTag = cms.int32(1),
        isMC = cms.int32(0),
        verbose = cms.int32(0),
)


process.p = cms.Path(
                    process.l1bits +
                    process.TnP
                    )


# DEBUG -- dump the event content
# process.output = cms.OutputModule(
#                 "PoolOutputModule",
#                       fileName = cms.untracked.string('edm_output.root'),
#                       )
# process.output_step = cms.EndPath(process.output)
#
# process.schedule = cms.Schedule(
# 		process.p,
# 		process.output_step)


'''
#############   Overall settings    ################
'''

process.MessageLogger.cerr.FwkReport.reportEvery = 10000
