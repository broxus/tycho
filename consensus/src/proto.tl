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
* Stored journal record key
*/
journal.recordKey
   time:int
   seqNo:int
   = journal.RecordKey;

/*
* Stored journal record value
*/
journal.recordValue
    kind:journal.RecordKind
    action:journal.RecordAction
    peer_id:int256
    point_keys:bytes
    message:bytes
    = journal.RecordValue;

/*
* Stored journal record kind
*/
journal.recordKind.nodeStarted                              = journal.RecordKind;
journal.recordKind.banned
    until:int
    origin:journal.recordOrigin
    = journal.RecordKind;
journal.recordKind.unbanned   origin:journal.RecordOrigin   = journal.RecordKind;
journal.recordKind.event      tag:journal.EventTag          = journal.RecordKind;

/*
* Stored journal record action
*/
journal.recordAction.store                                  = journal.RecordAction;
journal.recordAction.countPenalty                           = journal.RecordAction;
journal.recordAction.storeAndCountPenalty                   = journal.RecordAction;

/*
* Stored journal record origin
*/
journal.recordOrigin.manual                                 = journal.RecordOrigin;
journal.recordOrigin.parentKey     key:journal.RecordKey    = journal.RecordOrigin;
journal.recordOrigin.parentKeyTag
   key:journal.RecordKey
   tag:journal.EventTag
   = journal.RecordOrigin;

/*
* Stored journal event type tag
*/
journal.eventTag.unknownQuery                               = journal.EventTag;
journal.eventTag.queryLimitReached                          = journal.EventTag;
journal.eventTag.badRequest                                 = journal.EventTag;
journal.eventTag.badResponse                                = journal.EventTag;
journal.eventTag.badPoint                                   = journal.EventTag;
journal.eventTag.senderNotAuthor                            = journal.EventTag;
journal.eventTag.equivocated                                = journal.EventTag;
journal.eventTag.evidenceNoInclusion                        = journal.EventTag;
journal.eventTag.illFormed                                  = journal.EventTag;
journal.eventTag.invalid                                    = journal.EventTag;
