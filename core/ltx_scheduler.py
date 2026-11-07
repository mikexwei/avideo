import math
import json
import sqlite3
from datetime import datetime, timedelta
from statistics import NormalDist
from zoneinfo import ZoneInfo


TIMEZONE = ZoneInfo('America/Los_Angeles')
DAILY_LIMIT = 200


def hourly_limits(daily_limit=DAILY_LIMIT, peak_hour=17, spread=4):
    curve = NormalDist(mu=peak_hour + 0.5, sigma=spread)
    weights = [curve.cdf(hour + 1) - curve.cdf(hour) for hour in range(24)]
    scaled = [weight / sum(weights) * daily_limit for weight in weights]
    limits = [math.floor(value) for value in scaled]
    for hour in sorted(range(24), key=lambda h: scaled[h] - limits[h], reverse=True)[:daily_limit - sum(limits)]:
        limits[hour] += 1
    return limits


HOURLY_LIMITS = hourly_limits()


def _ensure_attempts(conn):
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ltx_scrape_attempts'").fetchone()
    if exists:
        return
    conn.execute('CREATE TABLE ltx_scrape_attempts (id INTEGER PRIMARY KEY, code TEXT NOT NULL, started_at REAL NOT NULL)')
    conn.execute('CREATE INDEX idx_ltx_attempts_started ON ltx_scrape_attempts(started_at)')
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ltx_scrape_results'").fetchone():
        conn.execute("INSERT INTO ltx_scrape_attempts(code, started_at) "
                     "SELECT code, CAST(strftime('%s', scraped_at) AS REAL) FROM ltx_scrape_results "
                     "WHERE strftime('%s', scraped_at) IS NOT NULL")


def quota(db_path, code=None, now=None, force=False):
    """Read quota, or atomically reserve one attempt when a code is supplied."""
    now = (now or datetime.now(TIMEZONE)).astimezone(TIMEZONE)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = datetime.fromtimestamp(hour_start.timestamp() + 3600, TIMEZONE)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        with conn:
            conn.execute('BEGIN IMMEDIATE')
            _ensure_attempts(conn)
            config = {'daily_limit': DAILY_LIMIT, 'peak_hour': 17, 'spread': 4}
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scrape_settings'").fetchone():
                row = conn.execute("SELECT value_json FROM scrape_settings WHERE key='quota'").fetchone()
                if row:
                    config.update(json.loads(row[0]))
            daily_limit = config['daily_limit']
            limits = hourly_limits(**config)
            daily_used = conn.execute('SELECT COUNT(*) FROM ltx_scrape_attempts WHERE started_at >= ? AND started_at < ?',
                                      (day_start.timestamp(), day_end.timestamp())).fetchone()[0]
            hour_used = conn.execute('SELECT COUNT(*) FROM ltx_scrape_attempts WHERE started_at >= ? AND started_at < ?',
                                     (hour_start.timestamp(), hour_end.timestamp())).fetchone()[0]
            limit = limits[now.hour]
            allowed = force or (daily_used < daily_limit and hour_used < limit)
            reason = None if allowed else 'daily_limit' if daily_used >= daily_limit else 'hourly_limit'
            next_at = None
            if not allowed:
                candidate = day_end if daily_used >= daily_limit else hour_end
                while limits[candidate.hour] == 0:
                    candidate = datetime.fromtimestamp(candidate.timestamp() + 3600, TIMEZONE)
                next_at = candidate.isoformat()
            if code is not None and allowed:
                conn.execute('INSERT INTO ltx_scrape_attempts(code, started_at) VALUES (?, ?)', (code, now.timestamp()))
                daily_used += 1
                hour_used += 1
            return {'allowed': allowed, 'reason': reason, 'daily_used': daily_used, 'daily_limit': daily_limit,
                    'hour_used': hour_used, 'hour_limit': limit, 'next_at': next_at,
                    'timezone': str(TIMEZONE), 'hourly_limits': limits, **config}
    finally:
        conn.close()
