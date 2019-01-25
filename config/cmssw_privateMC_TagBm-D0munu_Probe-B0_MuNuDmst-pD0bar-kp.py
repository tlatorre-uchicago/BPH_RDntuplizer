import FWCore.ParameterSet.Config as cms
import os

process = cms.Process("BPHRDntuplizer")
cmssw_version = os.environ['CMSSW_VERSION']
# import of standard configurations
process.load('FWCore.MessageService.MessageLogger_cfi')




from Configuration.AlCa.GlobalTag import GlobalTag
process.GlobalTag = GlobalTag(process.GlobalTag, '102X_upgrade2018_realistic_v12', '')

'''
#####################   Input    ###################
'''
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(100)
    # input = cms.untracked.int32(-1)
)

# from glob import glob
# flist = glob('/eos/user/o/ocerri/BPhysics/data/cmsMC_private/BPH_Tag-Bm_D0kpmunu_Probe-B0_MuNuDmst-pD0bar-kp-_NoPU_10-2-3_v1/jobs_out/*MINIAODSIM*.root')
# for i in range(len(flist)):
#     flist[i] = 'file:' + flist[i]
flist =[ '/eos/user/o/ocerri/BPhysics/data/cmsMC_private/BPH_Tag-Bm_D0kpmunu_Probe-B0_MuNuDmst-pD0bar-kp-_NoPU_10-2-3_v1/jobs_out/BPH_Tag-Bm_D0kpmunu_Probe-B0_MuNuDmst-pD0bar-kp-_MINIAODSIM_merged_1-300.root']
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring(tuple(flist)) )

process.source.duplicateCheckMode = cms.untracked.string('noDuplicateCheck')


'''
#####################   Output   ###################
'''

process.TFileService = cms.Service("TFileService",
      fileName = cms.string("flat_tree.root"),
      closeFileFast = cms.untracked.bool(True)
      )



'''
#################   Sequence    ####################
'''

BPHTriggerPath = cms.EDFilter("BPHTriggerPathProducer",
        muonCollection = cms.InputTag("slimmedMuons","", "PAT"),
        triggerObjects = cms.InputTag("slimmedPatTrigger"),
        triggerBits = cms.InputTag("TriggerResults","","HLT"),
        muon_charge = cms.int32(-1),
        verbose = cms.int32(1)
)


FlatTreeWriter = cms.EDAnalyzer("FlatTreeWriter",
                        cmssw = cms.string(cmssw_version)
)


process.p = cms.Path(
                    BPHTriggerPath +
                    FlatTreeWriter
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

process.MessageLogger.cerr.FwkReport.reportEvery = 1000
