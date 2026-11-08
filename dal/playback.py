"""Durable, idempotent playback sessions for clients that report progress."""
import re
import time
import uuid


# Match the base-code routing used by the video player, including version changes.
VIDEO_KEY_SQL = """CASE
    WHEN UPPER(v.code) LIKE '%-UC' THEN SUBSTR(UPPER(v.code), 1, LENGTH(v.code)-3)
    WHEN UPPER(v.code) LIKE '%-U' OR UPPER(v.code) LIKE '%-C' THEN SUBSTR(UPPER(v.code), 1, LENGTH(v.code)-2)
    ELSE UPPER(v.code) END"""


def video_key(code):
    return re.sub(r'-(?:UC|U|C)$', '', code.upper())


def ensure_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS playback_sessions (
        id TEXT PRIMARY KEY,
        video_key TEXT NOT NULL,
        source TEXT NOT NULL,
        part TEXT,
        started_at INTEGER NOT NULL,
        last_played_at INTEGER NOT NULL,
        watched_ms INTEGER NOT NULL,
        position_ms INTEGER NOT NULL,
        sequence INTEGER NOT NULL
    )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_playback_video
        ON playback_sessions(video_key,last_played_at DESC)""")


def report(conn, code, session_id, payload):
    try:
        session_id = str(uuid.UUID(session_id))
    except ValueError as exc:
        raise ValueError('无效的播放会话') from exc
    if not isinstance(payload, dict):
        raise ValueError('无效的播放数据')
    source = payload.get('source', 'web')
    part = payload.get('part')
    if source not in ('web', 'iina') or (part is not None and (
            not isinstance(part, str) or not re.fullmatch(r'part0*[1-9][0-9]*', part))):
        raise ValueError('无效的播放器来源或分集')
    fields = ('started_at', 'last_played_at', 'watched_ms', 'position_ms', 'sequence')
    values = [payload.get(name) for name in fields]
    if any(type(value) is not int or not 0 <= value < 2**53 for value in values):
        raise ValueError('无效的播放数据')
    started, last_played, watched, position, sequence = values
    if not (0 < started <= last_played <= int(time.time() * 1000) + 60_000) or not 0 < watched <= last_played - started + 1000:
        raise ValueError('无效的播放时长')
    ensure_schema(conn)
    key = video_key(code)
    conn.execute("""INSERT INTO playback_sessions
        (id,video_key,source,part,started_at,last_played_at,watched_ms,position_ms,sequence)
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            part=excluded.part,
            last_played_at=MAX(playback_sessions.last_played_at,excluded.last_played_at),
            watched_ms=MAX(playback_sessions.watched_ms,excluded.watched_ms),
            position_ms=excluded.position_ms, sequence=excluded.sequence
        WHERE playback_sessions.video_key=excluded.video_key
          AND playback_sessions.source=excluded.source
          AND playback_sessions.started_at=excluded.started_at
          AND playback_sessions.sequence<excluded.sequence""",
                 (session_id, key, source, part, started, last_played, watched, position, sequence))


def history(conn, code):
    ensure_schema(conn)
    key = video_key(code)
    summary = conn.execute("""SELECT COUNT(*) AS play_count,
        MAX(last_played_at) AS last_played_at, COALESCE(SUM(watched_ms),0) AS watched_ms
        FROM playback_sessions WHERE video_key=?""", (key,)).fetchone()
    sessions = conn.execute("""SELECT id,source,part,started_at,last_played_at,watched_ms,position_ms
        FROM playback_sessions WHERE video_key=? ORDER BY last_played_at DESC,id DESC LIMIT 30""", (key,))
    return {'play_count': summary[0], 'last_played_at': summary[1], 'watched_ms': summary[2],
            'sessions': [dict(row) for row in sessions]}
