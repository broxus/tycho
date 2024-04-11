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


---functions---

