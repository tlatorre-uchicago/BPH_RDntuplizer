#include "FWCore/Framework/interface/EventSetup.h"
#include "DataFormats/PatCandidates/interface/PackedCandidate.h"
#include "RecoVertex/KinematicFitPrimitives/interface/RefCountedKinematicTree.h"
#include "RecoVertex/KinematicFitPrimitives/interface/KinematicParticle.h"
#include <DataFormats/TrackReco/interface/Track.h>


// Pure ROOT import
#include <TLorentzVector.h>

namespace vtxu {
  RefCountedKinematicTree FitD0(const edm::EventSetup&, pat::PackedCandidate, pat::PackedCandidate, bool, int);
  RefCountedKinematicTree FitDst(const edm::EventSetup&, pat::PackedCandidate, pat::PackedCandidate, pat::PackedCandidate, bool, int);
  TLorentzVector getTLVfromTrack(reco::Track, double);
  TLorentzVector getTLVfromKinPart(ReferenceCountingPointer<KinematicParticle>);
}
