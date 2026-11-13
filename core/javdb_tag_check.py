"""Candidate selection and results for the read-only JavDB tag check."""
import json
import re
from urllib.parse import urlsplit

KIND = 'javdb_ltx_check'
ACTIVE = ('PREPARING', 'PENDING', 'RUNNING', 'WAITING')
TERMINAL = ('DONE', 'FAILED', 'SKIPPED', 'CANCELLED', 'SUSPENDED')


def tag_key(name):
    return re.sub(r'\s+', '', name).casefold().replace('無碼', '无码')


EXCLUDED_TAGS = {'ltx破解', 'ltx无码破解', '原装无码', '原裝无码', '无码流出', 'vr'}


def filters(source):
    result = {}
    for key in ('prefix', 'actor', 'q'):
        value = source.get(key, '')
        if not isinstance(value, str) or len(value) > 200:
            raise ValueError('筛选条件无效')
        result[key] = value.strip()
    checked = source.get('checked', 'unchecked')
    if checked not in ('unchecked', 'checked', 'all'):
        raise ValueError('检查历史筛选无效')
    return dict(result, checked=checked)


def candidates(conn, selection):
    # Apply tag exclusions across all active parts of the code.
    tags_by_code = {}
    for code, name in conn.execute("""
        SELECT v.code,t.name FROM videos v
        LEFT JOIN video_tag_link l ON l.video_id=v.id
        LEFT JOIN tags t ON t.id=l.tag_id WHERE v.deleted=0 ORDER BY v.code
    """):
        tags = tags_by_code.setdefault(code, set())
        if name:
            tags.add(tag_key(name))
    matches = {code for code, tags in tags_by_code.items()
               if '硬字幕' in tags and tags.isdisjoint(EXCLUDED_TAGS)
               and code.casefold().startswith(selection['prefix'].casefold())}

    checked = set()
    for row in conn.execute('SELECT checkpoint_json FROM scrape_tasks WHERE kind=?', (KIND,)):
        checked.update(item['code'] for item in json.loads(row[0]).get('items', [])
                       if item['status'] in ('found', 'absent', 'not_found') and not item.get('error'))
    actors = {}
    for code, name, name_zh in conn.execute("""
        SELECT DISTINCT v.code,a.name,a.name_zh FROM videos v
        JOIN video_actor_link l ON l.video_id=v.id JOIN actors a ON a.id=l.actor_id
        WHERE v.deleted=0 AND a.is_ignored=0 ORDER BY a.id
    """):
        if code in matches:
            actors.setdefault(code, []).append((name, name_zh))
    rows = []
    query = selection['q'].casefold()
    actor = selection['actor'].casefold()
    for code, title_zh, title_jp in conn.execute("""
        SELECT code,title_zh,title_jp FROM videos WHERE id IN
        (SELECT MIN(id) FROM videos WHERE deleted=0 GROUP BY code) ORDER BY code
    """):
        if code not in matches or (selection['checked'] != 'all'
                                    and (code in checked) != (selection['checked'] == 'checked')):
            continue
        if query and not any(query in (value or '').casefold() for value in (code, title_zh, title_jp)):
            continue
        names = actors.get(code, [])
        if actor and not any(actor in (value or '').casefold() for pair in names for value in pair):
            continue
        rows.append({'code': code, 'title': title_zh or title_jp or '',
                     'actors': list(dict.fromkeys(zh or name for name, zh in names)),
                     'checked': code in checked})
    return rows


def result_for_tags(tags, url):
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.netloc not in ('javdb.com', 'www.javdb.com') or not parsed.path.startswith('/v/'):
        raise ValueError('JavDB 详情页地址无效')
    matched = next((tag for tag in tags if tag_key(tag) == 'ltx破解'), None)
    return {'status': 'found' if matched else 'absent', 'url': url, 'tag': matched}
