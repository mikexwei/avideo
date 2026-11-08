const { event, mpv, http, preferences, console: log } = iina;
let session, previous;

function send() {
  if (!session || session.watched < 1 || session.watched === session.sent) return;
  const current = session;
  const watched = Math.floor(current.watched);
  current.lastReport = Date.now();
  current.sent = current.watched;
  const data = {
    source: 'iina', started_at: current.started, last_played_at: current.lastPlayed,
    watched_ms: watched, position_ms: current.position, sequence: ++current.sequence
  };
  if (current.part) data.part = current.part;
  http.post(current.url, { data }).catch(() => {
    if (current === session && Math.floor(current.sent) === watched) current.sent = 0;
    log.log('AVideo 播放记录上报失败');
  });
}

function sample() {
  if (!session) return;
  const now = Date.now(), position = mpv.getNumber('time-pos'), rate = mpv.getNumber('speed') || 1;
  if (!Number.isFinite(position)) return;
  session.position = Math.max(0, Math.round(position * 1000));
  if (mpv.getFlag('pause') || mpv.getFlag('seeking') || mpv.getFlag('paused-for-cache')) {
    previous = null;
    return;
  }
  if (previous) {
    const elapsed = now - previous.now;
    const played = (position - previous.position) * 1000 / previous.rate;
    if (elapsed > 0 && elapsed < 2000 && played > 0 && played <= elapsed + 250) {
      session.watched += Math.min(played, elapsed);
      session.lastPlayed = now;
    }
  }
  previous = { now, position, rate };
}

function close() {
  send();
  session = previous = null;
}

event.on('iina.file-loaded', url => {
  close();
  const server = String(preferences.get('server') || '').trim().replace(/\/$/, '');
  if (!/^https?:\/\/[a-z0-9.:[\]-]+$/i.test(server) || typeof url !== 'string' ||
      !url.startsWith(server + '/')) return;
  const match = /^\/api\/videos\/([\w%-]+)\/stream(?:\.\w+)?(?:\?([^#]*))?$/.exec(url.slice(server.length));
  if (!match) return;
  const query = (match[2] || '').split('&');
  if (query.includes('download=1')) return;
  const part = query.map(item => /^part=(part0*[1-9][0-9]*)$/.exec(item)).find(Boolean)?.[1];
  const id = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, char => {
    const random = Math.floor(Math.random() * 16);
    return (char === 'x' ? random : (random & 3) | 8).toString(16);
  });
  session = { url: server + '/api/videos/' + match[1] + '/playback/' + id, part,
    started: Date.now(), lastPlayed: 0, watched: 0, position: 0, sequence: 0, sent: 0, lastReport: 0 };
  sample();
});
event.on('mpv.seek', () => { previous = null; });
event.on('mpv.pause.changed', () => { sample(); send(); });
event.on('mpv.end-file', close);
event.on('iina.window-will-close', close);
setInterval(() => {
  if (!session) return;
  sample();
  if (Date.now() - session.lastReport >= 10_000) send();
}, 500);
