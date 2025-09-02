/**
* Representation of PointData
*/
consensus.pointData
    includes:(vector consensus.peerDigestPair)
    witness:(vector consensus.peerDigestPair)
    evidence:(vector consensus.peerSignaturePair)
    anchor_trigger:point.Link
    anchor_proof:point.Link
    time:int
    anchor_time:int
    = consensus.PointData;

consensus.pointBody
    author:int256
    round:int
    payload:(vector bytes)
    pointData:consensus.pointData
    = consensus.PointBody;

consensus.pointId
    author:int256
    round:int
    digest:int256
    = consensus.PointId;

consensus.point
    digest:int256
    signature:bytes
    body:consensus.pointBody
    = consensus.PointInner;

consensus.pointInfo
    digest:int256
    signature:bytes
    author:int256
    round:int
    payloadLen:int
    payloadBytes:int
    data:consensus.pointData
    = consensus.PointInfo;

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
intercom.pointByIdResponse.defined          x:bytes         = intercom.PointByIdResponse;
intercom.pointByIdResponse.definedNone                      = intercom.PointByIdResponse;
intercom.pointByIdResponse.tryLater                         = intercom.PointByIdResponse;

/*
* Representation of SignatureResponse
*/
intercom.signatureResponse.signature        x:bytes         = intercom.SignatureResponse;
intercom.signatureResponse.noPoint                          = intercom.SignatureResponse;
intercom.signatureResponse.tryLater                         = intercom.SignatureResponse;
intercom.signatureResponse.rejected                         = intercom.SignatureResponse;

/*
* Representation of SignatureRejectedReason
*/
intercom.signatureRejectedReason.tooOldRound                = intercom.SignatureRejectedReason;
intercom.signatureRejectedReason.cannotSign                 = intercom.SignatureRejectedReason;
intercom.signatureRejectedReason.unknownPeer                = intercom.SignatureRejectedReason;


/*
* Representation of BroadcastResponse
*/
intercom.broadcastResponse                                  = intercom.BroadcastResponse;

/*
* Representation of generic query wrapper
*/
intercom.queryRequest {X:Type} tag:intercom.QueryTag body:X = intercom.QueryRequest X;

/*
* Representation of query wrapper type tag
*/
intercom.queryTag.broadcast                                 = intercom.QueryTag;
intercom.queryTag.pointById                                 = intercom.QueryTag;
intercom.queryTag.signature                                 = intercom.QueryTag;

/*
* Stored event
*/
consensus.storedEvent
    tag:consensus.EventTag
    severity:consensus.EventSeverity
    peer_id:int256
    point_keys:bytes
    message:bytes
    = consensus.StoredEvent;

/*
* Stored event severity
*/
consensus.eventSeverity.info                                = consensus.EventSeverity;
consensus.eventSeverity.warn                                = consensus.EventSeverity;

/*
* Stored event type tag
*/
consensus.eventTag.nodeStarted                              = consensus.EventTag;
consensus.eventTag.banned                                   = consensus.EventTag;
consensus.eventTag.unbanned                                 = consensus.EventTag;
consensus.eventTag.unknownQuery                             = consensus.EventTag;
consensus.eventTag.queryLimitReached                        = consensus.EventTag;
consensus.eventTag.badRequest                               = consensus.EventTag;
consensus.eventTag.badResponse                              = consensus.EventTag;
consensus.eventTag.badPoint                                 = consensus.EventTag;
consensus.eventTag.senderNotAuthor                          = consensus.EventTag;
consensus.eventTag.equivocated                              = consensus.EventTag;
consensus.eventTag.evidenceNoInclusion                      = consensus.EventTag;
consensus.eventTag.illFormed                                = consensus.EventTag;
consensus.eventTag.invalid                                  = consensus.EventTag;
