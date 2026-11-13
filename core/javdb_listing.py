"""Durable JavDB list collection and local-library comparison."""
import json
from urllib.parse import parse_qs, urlencode, urlsplit

from core.subtitle_policy import VERSION_SUFFIX, stored_subtitle

KIND = 'javdb_listing'
ACTIVE = ('PREPARING', 'PENDING', 'RUNNING', 'WAITING')
TERMINAL = ('DONE', 'FAILED', 'SKIPPED', 'CANCELLED', 'SUSPENDED')


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
      const toolbarLists = eligible.filter(list =>
        list.matches('.movie-list.h.cols-4.vcols-8') && list.previousElementSibling?.matches('.toolbar'));
      const main = toolbarLists.length === 1 ? toolbarLists : eligible.length === 1 ? eligible : [...paginated];
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
            WHERE i.code=? AND t.kind=? AND t.status='DONE' AND t.deleted=0 LIMIT 1""", (code, KIND)).fetchone()
        if previous:
            history = True
            break
        conn.execute("""INSERT OR IGNORE INTO javdb_listing_items(task_id,code,page,title,actors_json,url)
            VALUES (?,?,?,?,?,?)""", (task_id, code, number, entry['title'], json.dumps(entry['actors'], ensure_ascii=False), entry['url']))
    return conn.total_changes - before, 'history' if history else None if result['has_next'] else 'last_page'


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
            tags = item.pop('tags')
            normalized_tags = {t.replace('無碼', '无码').replace('原裝', '原装') for t in tags}
            item['subtitle'] = stored_subtitle(tags) or '未分类'
            item['uncensored'] = sorted(t for t in normalized_tags if any(marker in t for marker in ('无码破解', '无码流出', '原装无码'))) or (['仅番号带 U'] if item['code'].upper().endswith(('-U', '-UC')) else ['无无码标记'])
            item['actors'] = sorted(item['actors'])
            item['names'] = sorted(item['names'])
    return versions


def results(conn, task_id, filters):
    items = list(conn.execute('SELECT * FROM javdb_listing_items WHERE task_id=? ORDER BY rowid', (task_id,)))
    local = local_versions(conn, {item['code'] for item in items}) if items else {}
    query, actor = (filters.get(key, '').strip().casefold() for key in ('q', 'actor'))
    in_db, subtitle, uncensored = (filters.get(key, '') for key in ('in_db', 'subtitle', 'uncensored'))
    output = []
    for item in items:
        versions = list(local.get(item['code'], {}).values())
        if (in_db == 'yes' and not versions) or (in_db == 'no' and versions):
            continue
        if (subtitle or uncensored) and not any(
            (not subtitle or subtitle == v['subtitle']) and (not uncensored or uncensored in v['uncensored'])
            for v in versions
        ):
            continue
        names = sorted({name for v in versions for name in v['actors']}) or json.loads(item['actors_json'])
        if actor and not any(actor in name.casefold() for name in names + [n for v in versions for n in v['names']]):
            continue
        title = next((v['title_zh'] for v in versions if v['title_zh']), None) or (
            next((v['title_jp'] for v in versions if v['title_jp']), None) or item['title']
        )
        if query and not any(query in value.casefold() for value in (item['code'], title, item['title'])):
            continue
        output.append(dict(code=item['code'], url=item['url'], page=item['page'], in_db=bool(versions), title=title,
                           actors=names, versions=[{k: v for k, v in version.items() if k not in ('names', 'title_zh', 'title_jp')} for version in versions]))
    return output
