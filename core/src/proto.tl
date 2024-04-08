// Overlay
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* @param key   compressed ed25519 verifying key
*/
overlay.peerId key:int256 = overlay.PeerId;

/**
* Public overlay ping model
* @param value     unix timestamp in millis when ping was sent
*/
overlay.ping
    value:long
    = overlay.Ping

/**
* Public overlay pong model. Sending pong back to sender should follow receiving ping model
* @param value     unix timestamp in millis when ping was sent
*/
overlay.pong
    value:long
    = overlay.Pong


---functions---


/**
* Send Ping
* @param ping    ping request
*/
overlay.sendPing
