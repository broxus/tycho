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

/*
* Representation of PointByIdResponse
*/
intercom.pointByIdResponse.defined          x:bytes     = intercom.PointByIdResponse;
intercom.pointByIdResponse.definedNone                  = intercom.PointByIdResponse;
intercom.pointByIdResponse.tryLater                     = intercom.PointByIdResponse;


/*
* Representation of SignatureResponse
*/
intercom.signatureResponse.signature        x:bytes     = intercom.SignatureResponse;
intercom.signatureResponse.noPoint                      = intercom.SignatureResponse;
intercom.signatureResponse.tryLater                     = intercom.SignatureResponse;
intercom.signatureResponse.rejected                     = intercom.SignatureResponse;

/*
* Representation of SignatureRejectedReason
*/
intercom.signatureRejectedReason.tooOldRound            = intercom.SignatureRejectedReason;
intercom.signatureRejectedReason.cannotSign             = intercom.SignatureRejectedReason;
intercom.signatureRejectedReason.unknownPeer            = intercom.SignatureRejectedReason;


/*
* Representation of BroadcastResponse
*/
intercom.broadcastResponse                              = intercom.BroadcastResponse;

/*
* Representation of generic query wrapper
*/
core.queryWrapper  {X:Type}  tag:core.QueryTag  body:X  = core.QueryWrapper X;

/*
* Representation of query wrapper type tag
*/
core.queryTag.broadcast                                 = core.QueryTag;
core.queryTag.pointById                                 = core.QueryTag;
core.queryTag.signature                                 = core.QueryTag;