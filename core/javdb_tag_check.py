"""Candidate selection and results for the read-only JavDB tag check."""
import re
from urllib.parse import urlsplit

KIND = 'javdb_ltx_check'
ACTIVE = ('PREPARING', 'PENDING', 'RUNNING', 'WAITING')


def tag_key(name):
    return re.sub(r'\s+', '', name).casefold().replace('無碼', '无码')


EXCLUDED_TAGS = {'ltx破解', 'ltx无码破解', '原装无码', '原裝无码', '无码流出', 'vr'}


def candidates(conn):
    tags_by_code = {}
    rows = conn.execute("""
        SELECT v.code, t.name FROM videos v
        LEFT JOIN video_tag_link l ON l.video_id=v.id
        LEFT JOIN tags t ON t.id=l.tag_id
        WHERE v.deleted=0 ORDER BY v.code
    """)
    for code, name in rows:
        tags = tags_by_code.setdefault(code, set())
        if name:
            tags.add(tag_key(name))
    return [code for code, tags in tags_by_code.items()
            if '硬字幕' in tags and tags.isdisjoint(EXCLUDED_TAGS)]


def result_for_tags(tags, url):
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.netloc not in ('javdb.com', 'www.javdb.com') or not parsed.path.startswith('/v/'):
        raise ValueError('JavDB 详情页地址无效')
    matched = next((tag for tag in tags if tag_key(tag) == 'ltx破解'), None)
    return {'status': 'found' if matched else 'absent', 'url': url, 'tag': matched}
