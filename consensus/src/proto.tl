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
intercom.signatureResponse.banned                           = intercom.SignatureResponse;
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
intercom.broadcastResponse.ok                               = intercom.BroadcastResponse;
intercom.broadcastResponse.tryLater                         = intercom.BroadcastResponse;
intercom.broadcastResponse.banned                           = intercom.BroadcastResponse;

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
    kind:consensus.EventKind
    severity:consensus.EventSeverity
    peer_id:int256
    point_keys:bytes
    message:bytes
    = consensus.StoredEvent;

/*
* Stored event severity
*/
consensus.eventSeverity.debug                               = consensus.EventSeverity;
consensus.eventSeverity.warn                                = consensus.EventSeverity;

/*
* Stored event kind
*/
consensus.eventKind.nodeStarted                             = consensus.EventKind;
consensus.eventKind.unknownQuery                            = consensus.EventKind;
consensus.eventKind.broadcastLimitReached                   = consensus.EventKind;
consensus.eventKind.sigRequestLimitReached                  = consensus.EventKind;
consensus.eventKind.uploadLimitReached                      = consensus.EventKind;
consensus.eventKind.illegalPoint                            = consensus.EventKind;
consensus.eventKind.senderNotAuthor                         = consensus.EventKind;
consensus.eventKind.equivocated                             = consensus.EventKind;
consensus.eventKind.evidenceNoInclusion                     = consensus.EventKind;
consensus.eventKind.illFormed                               = consensus.EventKind;
consensus.eventKind.invalid                                 = consensus.EventKind;
