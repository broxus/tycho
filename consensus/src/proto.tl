/**
* Representation of PointData
*/
consensus.pointData
    author:int256
    includes:(vector consensus.peerDigestPair)
    witness:(vector consensus.peerDigestPair)
    anchor_trigger:point.Link
    anchor_proof:point.Link
    time:int
    anchor_time:int
    = consensus.PointData;

consensus.pointBody
    round:int
    payload:(vector bytes)
    pointData:consensus.pointData
    evidence:(vector consensus.peerSignaturePair)
    = consensus.PointBody;

consensus.pointId
    author:int256
    round:int
    digest:int256
    = consensus.PointId;

consensus.pointInner
    digest:int256
    signature:bytes
    body:consensus.pointBody
    = consensus.PointInner;

consensus.pointInfoInner
    round:int
    digest:int256
    data:consensus.pointData
    = consensus.PointInfoInner;

/**
* Representation of BTreeMap<PeerId, Digest>
*/
consensus.peerDigestPair
    peer:int256
    digest:int256
    = consensus.PeerDigestPair;

/**
* Representation of BTreeMap<PeerId, Signature>
*/
consensus.peerSignaturePair
    peer:int256
    signature:bytes
    = consensus.PeerSignaturePair;

/**
* Representation of Link enum
*/
point.link.to_self                                                  = point.Link;
point.link.direct       x:link.through                              = point.Link;
point.link.indirect     to:consensus.pointId path:link.through      = point.Link;

/**
* Representation of Through enum
*/
link.through.witness    x:int256 = link.Through;
link.through.includes   x:int256 = link.Through;


core.broadcastQuery       x:consensus.pointInner        = core.BroadcastQuery;
core.pointQuery           x:consensus.pointId           = core.PointQuery;
core.signatureQuery       x:int32                       = core.SignatureQuery;


/*
* Representation of PointByIdResponse
*/
intercom.pointByIdResponse.defined          x:bytes      = intercom.PointByIdResponse;
intercom.pointByIdResponse.definedNone                   = intercom.PointByIdResponse;
intercom.pointByIdResponse.tryLater                     = intercom.PointByIdResponse;


/*
* Representation of SignatureResponse
*/
intercom.signatureResponse.signature        x:bytes     = intercom.SignatureResponse;
intercom.signatureResponse.noPoint                      = intercom.SignatureResponse;
intercom.signatureResponse.tryLater                     = intercom.SignatureResponse;
intercom.signatureResponse.rejected                     = intercom.SignatureResponse;

/*
*
*/
intercom.signatureRejectedReason.tooOldRound            = intercom.SignatureRejectedReason;
intercom.signatureRejectedReason.cannotSign             = intercom.SignatureRejectedReason;
intercom.signatureRejectedReason.unknownPeer            = intercom.SignatureRejectedReason;

/*
*
*/
core.mpresponse.broadcast                                           = core.MPResponse;
core.mpresponse.point           x:intercom.PointByIdResponse        = core.MPResponse;
core.mpresponse.signature       x:intercom.SignatureResponse        = core.MPResponse;


