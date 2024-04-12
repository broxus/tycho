// Overlay
////////////////////////////////////////////////////////////////////////////////

---types---

/**
* Public overlay ping model
* @param value     unix timestamp in millis when ping was sent
*/
overlay.ping
    = overlay.Ping;

/**
* Public overlay pong model. Sending pong back to sender should follow receiving ping model
* @param value     unix timestamp in millis when ping was sent
*/
overlay.pong
    = overlay.Pong;

/**
* A successful response for the overlay query
*
* @param value  an existing value
*/
publicOverlay.response.ok value:publicOverlay.Value = publicOverlay.Response;
/**
* An unsuccessul response for the overlay query
*/
publicOverlay.response.error = publicOverlay.Response;

/**
* A response for the `publicOverlay.getNextKeyBlockIds` query
* @param blocks         list of key blocks
* @param incomplete     flag points to finishinig query
*/
publicOverlay.keyBlockIdsResponse blocks:(vector publicOverlay.blockId) incomplete:Bool = publicOverlay.KeyBlockIdsResponse;

---functions---

/**
* Get list of next key block ids.
*
* @param block     block to start with
*/
publicOverlay.getNextKeyBlockIds block:publicOverlay.blockId max_size:int = publicOverlay.KeyBlockIdsResponse;
