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
    pointData:consensus.pointData
    evidence:(vector consensus.peerSignaturePair)
    payload:(vector bytes)
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


core.mpquery.broadcast       consensus.pointInner        = core.MPQuery;
core.mpquery.pointById       consensus.pointId           = core.MPQuery;
core.mpquery.signature       int32                       = core.MPQuery;


/*
* Representation of PointByIdResponse
*/
intercom.pointByIdResponse.defined          x:consensus.pointInner       = intercom.PointByIdResponse;
intercom.pointByIdResponse.definedNone                                   = intercom.PointByIdResponse;
intercom.pointByIdResponse.tryLater                                     = intercom.PointByIdResponse;


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
intercom.signatureRejectedReason.noDagRound             = intercom.SignatureRejectedReason;
intercom.signatureRejectedReason.cannotSign             = intercom.SignatureRejectedReason;

/*
*
*/
core.mpresponse.broadcast                                           = core.MPResponse;
core.mpresponse.pointById       x:intercom.PointByIdResponse        = core.MPResponse;
core.mpresponse.signature       x:intercom.SignatureResponse        = core.MPResponse;


