/**
* Representation of PointData
*/
consensus.pointData
    includes:(vector consensus.peerDigestPair)
    witness:(vector consensus.peerDigestPair)
    evidence:(vector consensus.peerSignaturePair)
    anchor_trigger:consensus.anchorLink
    anchor_proof:consensus.anchorLink
    time:int
    anchor_time:int
    = consensus.PointData;

consensus.pointBody
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
* Representation of Anchor Link enum
*/
consensus.anchorLink.to_self                                            = consensus.AnchorLink;
consensus.anchorLink.direct       x:consensus.link.through              = consensus.AnchorLink;
consensus.anchorLink.indirect     x:consensus.link.indirect             = consensus.AnchorLink;

/**
* Representation of Indirect Link
*/
consensus.link.indirect
    to:consensus.pointId
    path:consensus.link.through
    = consensus.link.Indirect;

/**
* Representation of Through enum
*/
consensus.link.through.includes   x:transport.PeerId                    = consensus.link.Through;
consensus.link.through.witness    x:transport.PeerId                    = consensus.link.Through;

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