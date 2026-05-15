/**
* Representation of PointData
*/
consensus.pointData
    includes:(vector consensus.peerDigestPair)
    witness:(vector consensus.peerDigestPair)
    evidence:(vector consensus.peerSignaturePair)
    role:consensus.pointRole
    time:int
    anchor_time:int
    = consensus.PointData;

consensus.pointBody
    point_bytes:int
    author:transport.PeerId
    round:int
    payload:(vector bytes)
    data:consensus.pointData
    = consensus.PointBody;

consensus.pointId
    round:int
    digest:int256
    author:transport.PeerId
    = consensus.PointId;

consensus.point
    digest:int256
    signature:bytes
    body:consensus.pointBody
    = consensus.PointInner;

consensus.pointInfo
    id:consensus.pointId
    signature:bytes
    payload_len:int
    payload_bytes:int
    data:consensus.pointData
    = consensus.PointInfo;

/**
* Representation of BTreeMap<PeerId, Digest>
*/
consensus.peerDigestPair
    peer:transport.PeerId
    digest:int256
    = consensus.PeerDigestPair;

/**
* Representation of BTreeMap<PeerId, Signature>
*/
consensus.peerSignaturePair
    peer:transport.PeerId
    signature:bytes
    = consensus.PeerSignaturePair;

/**
* Representation of Point Role enum
*/
consensus.pointRole.regular
    anchor_proof:consensus.anchorLink
    anchor_trigger:consensus.anchorLink
    = consensus.PointRole;
consensus.pointRole.proof
    anchor_proof:consensus.link.indirect
    anchor_trigger:consensus.anchorLink
    = consensus.PointRole;
consensus.pointRole.trigger                                           = consensus.PointRole;
consensus.pointRole.sticky                 seq_no:int                 = consensus.PointRole;
consensus.pointRole.genesis                                           = consensus.PointRole;

/**
* Representation of Anchor Link enum
*/
consensus.anchorLink.direct       x:consensus.link.through              = consensus.AnchorLink;
consensus.anchorLink.indirect     x:consensus.link.indirect             = consensus.AnchorLink;

consensus.link.through.includes   x:transport.PeerId                    = consensus.link.Through;
consensus.link.through.witness    x:transport.PeerId                    = consensus.link.Through;

consensus.link.indirect
    to:consensus.pointId
    path:consensus.link.through
    = consensus.link.Indirect;

/*
* Representation of DownloadResponse
*/
intercom.downloadResponse.defined          x:bytes         = intercom.DownloadResponse;
intercom.downloadResponse.definedNone                      = intercom.DownloadResponse;
intercom.downloadResponse.tryLater                         = intercom.DownloadResponse;

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
intercom.queryTag.download                                  = intercom.QueryTag;
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
    is_ban_related:bool
    kind:journal.RecordKind
    peer_id:int256
    point_keys:(vector journal.PointRef)
    message:bytes
    = journal.RecordValue;

/*
* Stored journal record kind
*/
journal.recordKind.nodeStarted                              = journal.RecordKind;
journal.recordKind.banned         until:int                 = journal.RecordKind;
journal.recordKind.unbanned                                 = journal.RecordKind;
journal.recordKind.event          tag:journal.EventTag      = journal.RecordKind;

/*
* Stored journal event type tag
*/
journal.eventTag.badQuery                                   = journal.EventTag;
journal.eventTag.queryLimitReached                          = journal.EventTag;
journal.eventTag.pointIntegrityError                        = journal.EventTag;
journal.eventTag.replacedPoint                              = journal.EventTag;
journal.eventTag.illFormedPoint                             = journal.EventTag;
journal.eventTag.invalidPoint                               = journal.EventTag;
journal.eventTag.forkedPoint                                = journal.EventTag;
journal.eventTag.evidenceNoInclusion                        = journal.EventTag;

/*
* Journal point reference
*/
journal.pointRef            bytes           bool            = journal.PointRef;

/*
* Ref counted journal point representation
*/
journal.point   ref_count:int32   journal.pointStoredData   = journal.Point;
journal.pointStoredData.sub                                 = journal.PointStoredData;
journal.pointStoredData.data       consensus.point          = journal.PointStoredData;
