/* Track active viewing time separately from the media timeline. */
function trackPlayback(video, url, part, onReported) {
  const listeners = new AbortController();
  const listen = (name, handler) => video.addEventListener(name, handler, { signal: listeners.signal });
  let session, sample, pending;

  function start() {
    if (!session) {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      bytes[6] = (bytes[6] & 15) | 64;
      bytes[8] = (bytes[8] & 63) | 128;
      const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
      const id = [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join('-');
      session = { id, started_at: Date.now(), last_played_at: 0, watched_ms: 0, sequence: 0 };
    }
    sample = { clock: performance.now(), position: video.currentTime, rate: video.playbackRate };
  }

  function update() {
    if (!sample || video.seeking) return;
    const clock = performance.now();
    const elapsed = clock - sample.clock;
    const position = video.currentTime;
    const played = (position - sample.position) * 1000 / sample.rate;
    if (played > 0 && played <= elapsed + 300) {
      session.watched_ms += Math.min(played, elapsed);
      session.last_played_at = Date.now();
    }
    sample = { clock, position, rate: video.playbackRate };
  }

  function report() {
    if (!session || session.watched_ms < 1) return;
    const payload = {
      part, started_at: session.started_at, last_played_at: session.last_played_at,
      watched_ms: Math.floor(session.watched_ms), position_ms: Math.max(0, Math.round(video.currentTime * 1000)),
      sequence: ++session.sequence
    };
    pending = { url: url + '/' + session.id, body: JSON.stringify(payload) };
    send(pending);
  }

  function send(reportData) {
    fetch(reportData.url, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: reportData.body, keepalive: true
    }).then(response => {
      if (response.ok && pending === reportData) {
        pending = null;
        onReported();
      }
    }).catch(() => { /* Keep the cumulative report for the next heartbeat. */ });
  }

  function stop() {
    update();
    sample = null;
    report();
  }

  listen('playing', start);
  listen('timeupdate', () => {
    update();
    if (session?.sequence === 0) report();
  });
  listen('ratechange', update);
  listen('pause', stop);
  listen('waiting', stop);
  listen('seeking', () => { sample = null; });
  listen('seeked', () => {
    if (!video.paused && video.readyState >= 3) start();
  });
  listen('ended', () => { stop(); session = null; });
  window.addEventListener('pagehide', stop, { signal: listeners.signal });
  window.addEventListener('pageshow', () => {
    if (!video.paused && video.readyState >= 3) start();
  }, { signal: listeners.signal });
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      update();
      report();
    }
  }, { signal: listeners.signal });
  const reportTimer = setInterval(() => {
    if (sample) { update(); report(); }
    else if (pending) send(pending);
  }, 10_000);
  if (!video.paused && video.readyState >= 3) start();

  return () => {
    stop();
    clearInterval(reportTimer);
    listeners.abort();
  };
}
