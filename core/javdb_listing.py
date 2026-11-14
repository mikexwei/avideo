"""Durable JavDB list collection and local-library comparison."""
import json
import re
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlsplit

from core.browser_queue import BROWSER_TIMEZONE
from core.subtitle_policy import VERSION_SUFFIX, stored_subtitle

KIND = 'javdb_listing'
ACTIVE = ('PREPARING', 'PENDING', 'RUNNING', 'WAITING')
TERMINAL = ('DONE', 'FAILED', 'SKIPPED', 'CANCELLED', 'SUSPENDED')
SCHEDULE = 'javdb_listing_schedule'


def active_task(conn):
    return conn.execute("""SELECT id FROM scrape_tasks WHERE kind=? AND deleted=0 AND cancel_requested=0
        AND status IN (?, ?, ?, ?) LIMIT 1""", (KIND, *ACTIVE)).fetchone()


def submit(runtime, conn, trigger):
    return runtime.submit(KIND, payload={'trigger': trigger}, priority=-100, connection=conn)


def schedule_due(runtime, now=None):
    now = now or datetime.now(BROWSER_TIMEZONE)
    day = now.date().isoformat()

    def due(settings):
        if not settings['enabled'] or settings.get('last_day') == day:
            return False
        hour, minute = map(int, settings['time'].split(':'))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return now >= target and target.timestamp() >= settings.get('saved_at', 0)

    if not due(runtime.settings(SCHEDULE)):
        return False
    with runtime.connect(write=True) as conn:
        settings = json.loads(conn.execute('SELECT value_json FROM scrape_settings WHERE key=?',
                                           (SCHEDULE,)).fetchone()[0])
        if not due(settings) or active_task(conn):
            return False
        submit(runtime, conn, 'scheduled')
        conn.execute("UPDATE scrape_settings SET value_json=json_set(value_json, '$.last_day', ?) WHERE key=?",
                     (day, SCHEDULE))
        return True


def list_url(number):
    return 'https://javdb.com/?' + urlencode({'page': number, 'vft': 6, 'vst': 2})


def base_code(code):
    return VERSION_SUFFIX.sub('', code.strip()).upper()


def valid_url(url, *, detail=False):
    parsed = urlsplit(url)
    return (parsed.scheme == 'https' and parsed.netloc in ('javdb.com', 'www.javdb.com')
            and (parsed.path.startswith('/v/') if detail else parsed.path == '/'))


def read_page(page, number):
    """Read a loaded listing. Ambiguous layouts fail rather than ending the run."""
    if not valid_url(page.url) or any(parse_qs(urlsplit(page.url).query).get(key) != [value]
                                       for key, value in {'vft': '6', 'vst': '2'}.items()):
        raise ValueError('JavDB 跳转到了其他页面')
    if parse_qs(urlsplit(page.url).query).get('page', ['1']) != [str(number)]:
        raise ValueError('JavDB 返回了不同的页码')
    parsed = page.evaluate("""() => {
      const lists = [...document.querySelectorAll('.movie-list')].filter(list =>
        list.querySelector('.video-title strong'));
      const weekly = /每[週周]推薦|每[週周]推荐|本[週周](?:推薦|推荐)|weekly/i;
      const eligible = lists.filter(list => {
        if (list.closest('[class*="weekly"], [id*="weekly"], .recommendations, .weekly-recommend')) return false;
        for (let node = list; node && node !== document.body; node = node.parentElement) {
          // Shared containers can have a recommendation heading above the main grid.
          if (node !== list && (node.querySelector('.toolbar') ||
              lists.some(other => other !== list && node.contains(other)))) break;
          const heading = node.previousElementSibling;
          if (heading?.matches('h1, h2, h3, h4, .title, .section-title')
              && weekly.test(heading.textContent.trim())) return false;
          if (weekly.test(node.querySelector(':scope > h2, :scope > h3, :scope > .title')?.textContent || '')) return false;
        }
        return true;
      });
      const navigation = [...document.querySelectorAll('.pagination, nav[aria-label="pagination"]')];
      const paginated = new Set();
      for (const nav of navigation) {
        for (let node = nav; node && node !== document.body; node = node.parentElement) {
          const candidates = eligible.filter(list => node.contains(list));
          if (!candidates.length) continue;
          if (candidates.length === 1) paginated.add(candidates[0]);
          break;
        }
      }
      const grids = eligible.filter(list => list.matches('.movie-list.h.cols-4.vcols-8'));
      const toolbarLists = grids.filter(list => list.previousElementSibling?.matches('.toolbar'));
      const main = toolbarLists.length === 1 ? toolbarLists : grids.length === 1 ? grids :
        eligible.length === 1 ? eligible : [...paginated];
      const cards = main.length === 1 ? [...main[0].querySelectorAll('.video-title strong')] : [];
      const entries = cards.map(code => {
        const item = code.closest('.item') || code.closest('a[href]');
        if (!item || code.closest('.movie-list') !== main[0]) return null;
        const title = item.querySelector('.video-title')?.cloneNode(true);
        title?.querySelector('strong')?.remove();
        const link = item.matches('a[href]') ? item : item.querySelector('a[href*="/v/"]');
        return {code: code.textContent.trim(), title: link?.getAttribute('title')?.trim() || title?.textContent.trim() || '',
          actors: [...item.querySelectorAll('a[href^="/actors/"]')].map(a => a.textContent.trim()).filter(Boolean),
          url: link?.href};
      }).filter(Boolean);
      const next = [...document.querySelectorAll('.pagination-next, a[rel="next"]')];
      const enabled = next.filter(el => !el.matches('[disabled], [aria-disabled="true"], .is-disabled') && el.href);
      const current = navigation.map(nav => nav.querySelector('.is-current, [aria-current="page"]')?.textContent.trim());
      return {lists: main.length, grids: lists.length, candidates: eligible.length, cards: cards.length, entries, next: enabled[0]?.href || null,
        ambiguousNext: next.some(el => !el.href && !el.matches('[disabled], [aria-disabled="true"], .is-disabled')),
        pagination: navigation.length, current};
    }""")
    if parsed['lists'] != 1 or not parsed['entries'] or any(
        not entry.get('code') or not valid_url(entry.get('url') or '', detail=True)
        for entry in parsed['entries']
    ):
        raise ValueError(f"无法识别 JavDB 主列表（第 {number} 页，含番号的列表 {parsed.get('grids', 0)}，候选列表 {parsed.get('candidates', 0)}，"
                         f"番号卡片 {parsed.get('cards', 0)}，分页区 {parsed['pagination']}）")
    next_url = parsed['next']
    if next_url:
        query = parse_qs(urlsplit(next_url).query)
        if not valid_url(next_url) or query.get('page') != [str(number + 1)] or any(
            query.get(key) != [value] for key, value in {'vft': '6', 'vst': '2'}.items()
        ):
            raise ValueError('JavDB 下一页链接与当前筛选不一致')
    elif not parsed['pagination'] or parsed['ambiguousNext'] or str(number) not in parsed['current']:
        raise ValueError('无法确认 JavDB 已到末页')
    return {'items': parsed['entries'], 'has_next': bool(next_url)}


def save_page(conn, task_id, number, result):
    before = conn.total_changes
    history = False
    for entry in result['items']:
        code = base_code(entry['code'])
        previous = conn.execute("""SELECT 1 FROM javdb_listing_items i JOIN scrape_tasks t ON t.id=i.task_id
            WHERE i.code=? AND t.kind=? AND t.status='DONE' AND t.id<>? LIMIT 1""", (code, KIND, task_id)).fetchone()
        if previous:
            history = True
            break
        metadata = conn.execute("""SELECT actors_json, tags_json FROM javdb_listing_items
            WHERE code=? AND tags_json IS NOT NULL LIMIT 1""", (code,)).fetchone()
        actors, tags = tuple(metadata) if metadata else (json.dumps(entry['actors'], ensure_ascii=False), None)
        conn.execute("""INSERT OR IGNORE INTO javdb_listing_items(task_id,code,page,title,actors_json,url,tags_json)
            VALUES (?,?,?,?,?,?,?)""", (task_id, code, number, entry['title'], actors, entry['url'], tags))
    return conn.total_changes - before, 'history' if history else None if result['has_next'] else 'last_page'


def read_detail(page, url):
    from core.scraper.video_scraper import _female_actor_names_from_block

    if not valid_url(page.url, detail=True) or urlsplit(page.url).path != urlsplit(url).path:
        raise ValueError('JavDB 跳转到了其他详情页')
    actors, tags = [], []
    has_identity = False
    for block in page.locator('.movie-panel-info .panel-block').all():
        label = re.split(r'[:：]', block.inner_text(), maxsplit=1)[0].strip()
        if label in ('番號', '番号'):
            has_identity = True
        elif label in ('類別', '类别', '標籤', '标签'):
            tags.extend(name.strip() for name in block.locator('.value a').all_inner_texts())
        elif label in ('演員', '演员'):
            actors.extend(_female_actor_names_from_block(block))
    if not has_identity:
        raise ValueError('无法识别 JavDB 详情页')
    return {'actors': list(dict.fromkeys(filter(None, actors))), 'tags': list(dict.fromkeys(filter(None, tags)))}


def local_codes(conn):
    return {base_code(row[0]) for row in conn.execute('SELECT DISTINCT code FROM videos WHERE deleted=0')}


def pending_detail(conn, task_id):
    local = local_codes(conn)
    rows = conn.execute("""SELECT i.code, i.url FROM javdb_listing_items i
        WHERE i.tags_json IS NULL ORDER BY i.task_id=? DESC, i.rowid""", (task_id,))
    return next((row for row in rows if row['code'] not in local), None)


def save_detail(conn, code, result):
    conn.execute("""UPDATE javdb_listing_items SET actors_json=?, tags_json=? WHERE code=?""",
                 (json.dumps(result['actors'], ensure_ascii=False), json.dumps(result['tags'], ensure_ascii=False), code))


def local_versions(conn, codes):
    versions = {}
    for row in conn.execute('SELECT id,code,title_zh,title_jp FROM videos WHERE deleted=0 ORDER BY id'):
        key = base_code(row['code'])
        if key not in codes:
            continue
        version = versions.setdefault(key, {}).setdefault(row['code'], {
            'code': row['code'], 'title_zh': row['title_zh'], 'title_jp': row['title_jp'], 'tags': set(), 'actors': set(), 'names': set(),
        })
        version['title_zh'] = version['title_zh'] or row['title_zh']
        version['title_jp'] = version['title_jp'] or row['title_jp']
    for code, tag in conn.execute("""SELECT DISTINCT v.code,t.name FROM videos v
        JOIN video_tag_link l ON l.video_id=v.id JOIN tags t ON t.id=l.tag_id WHERE v.deleted=0"""):
        key = base_code(code)
        if key in versions:
            versions[key][code]['tags'].add(tag)
    for code, name, zh in conn.execute("""SELECT DISTINCT v.code,a.name,a.name_zh FROM videos v
        JOIN video_actor_link l ON l.video_id=v.id JOIN actors a ON a.id=l.actor_id WHERE v.deleted=0 AND a.is_ignored=0"""):
        key = base_code(code)
        if key in versions:
            versions[key][code]['actors'].add(zh or name)
            versions[key][code]['names'].update(n for n in (name, zh) if n)
    for group in versions.values():
        for item in group.values():
            tags = item['tags']
            normalized_tags = {t.replace('無碼', '无码').replace('原裝', '原装') for t in tags}
            item['subtitle'] = stored_subtitle(tags) or '未分类'
            item['uncensored'] = sorted(t for t in normalized_tags if any(marker in t for marker in ('无码破解', '无码流出', '原装无码'))) or (['仅番号带 U'] if item['code'].upper().endswith(('-U', '-UC')) else ['无无码标记'])
            item['actors'] = sorted(item['actors'])
            item['names'] = sorted(item['names'])
            item['tags'] = sorted(tags)
    return versions


def results(conn, task_id, filters):
    if task_id is None:
        items = list(conn.execute("""SELECT i.* FROM javdb_listing_items i JOIN scrape_tasks t ON t.id=i.task_id
            WHERE i.rowid IN (SELECT MAX(rowid) FROM javdb_listing_items GROUP BY code)
            ORDER BY t.created_at DESC, i.rowid"""))
    else:
        items = list(conn.execute('SELECT * FROM javdb_listing_items WHERE task_id=? ORDER BY rowid', (task_id,)))
    local = local_versions(conn, {item['code'] for item in items}) if items else {}
    from dal.db_manager import _actor_name_key

    translations = {}
    if items:
        for name, zh in conn.execute("""SELECT name,name_zh FROM actors
            WHERE is_ignored=0 AND name_zh IS NOT NULL AND name_zh<>'' ORDER BY id"""):
            translations.setdefault(_actor_name_key(name), zh)

    def matches(key, values, *, exact=False):
        term = filters.get(key, '').strip().casefold()
        if not term:
            return True
        found = any(term == value.casefold() if exact else term in value.casefold() for value in values)
        return found != (filters.get('exclude_' + key) == '1')

    in_db, subtitle, uncensored = (filters.get(key, '') for key in ('in_db', 'subtitle', 'uncensored'))
    output = []
    for item in items:
        versions = list(local.get(item['code'], {}).values())
        if in_db in ('yes', 'no') and not matches('in_db', ['yes' if versions else 'no'], exact=True):
            continue
        if any(not matches(key, values, exact=True) for key, values in (
            ('subtitle', [v['subtitle'] for v in versions]),
            ('uncensored', [tag for v in versions for tag in v['uncensored']]),
        )):
            continue
        if subtitle and uncensored and all(filters.get('exclude_' + key) != '1' for key in ('subtitle', 'uncensored')) and not any(
            subtitle == v['subtitle'] and uncensored in v['uncensored']
            for v in versions
        ):
            continue
        if versions:
            originals = [name for v in versions for name in v['names']]
            names = sorted({name for v in versions for name in v['actors']})
            tags = sorted({tag for v in versions for tag in v['tags']})
        else:
            originals = json.loads(item['actors_json'])
            names = list(dict.fromkeys(translations.get(_actor_name_key(name), name) for name in originals))
            tags = json.loads(item['tags_json'] or '[]')
        if not matches('actor', names + originals):
            continue
        title = next((v['title_zh'] for v in versions if v['title_zh']), None) or (
            next((v['title_jp'] for v in versions if v['title_jp']), None) or item['title']
        )
        if not (matches('q', [item['code'], title, item['title']]) and matches('code', [item['code']])
                and matches('title', [title, item['title']]) and matches('tag', tags)):
            continue
        output.append(dict(code=item['code'], url=item['url'], page=item['page'], in_db=bool(versions), title=title,
                           actors=names, tags=tags, metadata_pending=not versions and item['tags_json'] is None,
                           versions=[{k: v for k, v in version.items() if k not in ('names', 'tags', 'title_zh', 'title_jp')} for version in versions]))
    return output
