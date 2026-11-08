"""Raw series identities, durable translations and reviewed final groups."""
import json
import time
import unicodedata

from zhconv import convert


SCHEMA = """
CREATE TABLE IF NOT EXISTS series_jp (
    id INTEGER PRIMARY KEY, raw_name TEXT NOT NULL, scope TEXT NOT NULL,
    maker TEXT NOT NULL DEFAULT '', source_ref TEXT NOT NULL DEFAULT '',
    UNIQUE(raw_name, scope)
);
CREATE TABLE IF NOT EXISTS series_final (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS series_cn (
    id INTEGER PRIMARY KEY, id_jp INTEGER NOT NULL UNIQUE REFERENCES series_jp(id),
    name TEXT, status TEXT NOT NULL, task_id TEXT,
    id_final INTEGER REFERENCES series_final(id)
);
CREATE INDEX IF NOT EXISTS idx_series_cn_final ON series_cn(id_final);
CREATE INDEX IF NOT EXISTS idx_series_cn_task ON series_cn(task_id);
CREATE TABLE IF NOT EXISTS series_publications (
    report_id TEXT PRIMARY KEY, groups_json TEXT NOT NULL, published_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TRIGGER IF NOT EXISTS sync_series_translation
AFTER UPDATE OF status ON scrape_tasks
WHEN NEW.kind='series_translate' AND NEW.status != OLD.status
 AND NEW.status IN ('PENDING','DONE','FAILED','SKIPPED','CANCELLED')
BEGIN
    UPDATE series_cn
    SET status=CASE NEW.status WHEN 'DONE' THEN 'DONE' WHEN 'PENDING' THEN 'TRANSLATING' ELSE 'FAILED' END,
        name=CASE WHEN NEW.status='DONE' THEN json_extract(NEW.result_json,'$') ELSE NULL END
    WHERE task_id=NEW.id;
END;
"""


def migrate(conn):
    conn.executescript(SCHEMA)
    columns = {row[1] for row in conn.execute('PRAGMA table_info(videos)')}
    if columns and 'series_jp_id' not in columns:
        # Recheck under the write lock in case another service just migrated.
        conn.execute('BEGIN IMMEDIATE')
        if 'series_jp_id' not in {row[1] for row in conn.execute('PRAGMA table_info(videos)')}:
            conn.execute('ALTER TABLE videos ADD COLUMN series_jp_id INTEGER REFERENCES series_jp(id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_series_jp ON videos(series_jp_id)')
        conn.commit()


def normalize_input(name):
    return ''.join(char for char in unicodedata.normalize('NFKC', name) if char.isalnum())


def translated_name(name):
    return convert(name.strip(), 'zh-cn')


def attach(conn, store, code, raw, maker='', source_ref=''):
    """Called inside the same write transaction that saved the film metadata."""
    raw = (raw or '').strip()
    if not raw:
        conn.execute('UPDATE videos SET series_jp_id=NULL WHERE code=?', (code,))
        return
    maker = translated_name(maker or '')
    maker_scope = 'maker:' + maker.casefold()
    scope = 'javdb:' + source_ref if source_ref else maker_scope
    previous = conn.execute('''SELECT j.* FROM videos v JOIN series_jp j ON j.id=v.series_jp_id
        WHERE v.code=? AND j.raw_name=? AND j.maker=? LIMIT 1''', (code, raw, maker)).fetchone()
    if previous and (not source_ref or not previous['source_ref'] or previous['source_ref'] == source_ref):
        id_jp = previous['id']
        if source_ref and not previous['source_ref']:
            conn.execute('UPDATE series_jp SET source_ref=? WHERE id=?', (source_ref, id_jp))
    else:
        existing = conn.execute("SELECT id FROM series_jp WHERE raw_name=? AND source_ref=? AND source_ref!='' ORDER BY id LIMIT 1",
                                (raw, source_ref)).fetchone()
        if existing is None:
            conn.execute('INSERT OR IGNORE INTO series_jp(raw_name,scope,maker,source_ref) VALUES (?,?,?,?)',
                         (raw, scope, maker, source_ref))
        id_jp = existing[0] if existing else conn.execute('SELECT id FROM series_jp WHERE raw_name=? AND scope=?', (raw, scope)).fetchone()[0]
    conn.execute('UPDATE videos SET series_jp_id=? WHERE code=?', (id_jp, code))
    if conn.execute('SELECT 1 FROM series_cn WHERE id_jp=?', (id_jp,)).fetchone():
        return
    known = conn.execute('''SELECT c.name FROM series_cn c JOIN series_jp j ON j.id=c.id_jp
        WHERE j.raw_name=? AND c.status='DONE' ORDER BY c.id LIMIT 1''', (raw,)).fetchone()
    if not known and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='series_clusters'").fetchone():
        from core.translator.service import is_likely_japanese
        legacy = conn.execute("SELECT canonical_name_zh FROM series_clusters WHERE canonical_name=? AND trim(COALESCE(canonical_name_zh,''))!=''", (raw,)).fetchone()
        if legacy and not is_likely_japanese(legacy[0]):
            known = (translated_name(legacy[0]),)
    if known:
        conn.execute("INSERT INTO series_cn(id_jp,name,status) VALUES (?,?,'DONE')", (id_jp, known[0]))
        return
    pending = conn.execute('''SELECT c.task_id FROM series_cn c JOIN series_jp j ON j.id=c.id_jp
        WHERE j.raw_name=? AND c.status='TRANSLATING' LIMIT 1''', (raw,)).fetchone()
    task_id = pending[0] if pending else store.submit('series_translate', raw, {'raw_name': raw}, priority=-2000, connection=conn)
    conn.execute("INSERT INTO series_cn(id_jp,status,task_id) VALUES (?,'TRANSLATING',?)", (id_jp, task_id))


def backfill(store):
    with store.connect(write=True) as conn:
        rows = conn.execute('''SELECT code,series,maker FROM videos
            WHERE id IN (SELECT MIN(id) FROM videos WHERE deleted=0 AND series_jp_id IS NULL
                AND trim(COALESCE(series,''))!='' GROUP BY code)''').fetchall()
        for row in rows:
            attach(conn, store, row['code'], row['series'], row['maker'])


def report_snapshot(conn):
    return [dict(row) for row in conn.execute('''SELECT c.id, j.raw_name, j.maker, j.source_ref, c.name AS name_zh,
        c.id_final, f.name AS final_name, f.revision,
        COUNT(DISTINCT v.code) AS films, MIN(v.code) AS sample
        FROM series_cn c JOIN series_jp j ON j.id=c.id_jp
        JOIN videos v ON v.series_jp_id=j.id AND v.deleted=0
        LEFT JOIN series_final f ON f.id=c.id_final
        GROUP BY c.id ORDER BY c.id''')]


class ReviewConflict(ValueError):
    pass


def publish(conn, report_id, groups):
    if not isinstance(groups, list) or not groups:
        raise ValueError('请选择至少一组待发布的系列')
    payload = json.dumps(groups, ensure_ascii=False, sort_keys=True)
    existing = conn.execute('SELECT groups_json FROM series_publications WHERE report_id=?', (report_id,)).fetchone()
    if existing:
        if existing[0] == payload:
            return
        raise ReviewConflict('报告已发布，请生成新报告')
    row = conn.execute("SELECT result_json FROM scrape_tasks WHERE id=? AND kind='series_report' AND status='DONE'", (report_id,)).fetchone()
    if not row:
        raise ValueError('报告尚未生成完成')
    snapshot = {item['id']: item for item in json.loads(row[0])['members']}
    current = {item['id']: item for item in report_snapshot(conn)}
    changes = []
    reviewed_ids, revised_groups = set(), set()
    for group in groups:
        if not isinstance(group, dict):
            raise ValueError('审核分组格式无效')
        name, ids, target = group.get('name'), group.get('member_ids'), group.get('final_id')
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 300 or not isinstance(ids, list) or not ids:
            raise ValueError('请填写最终名称并选择成员')
        if any(type(i) is not int or i not in snapshot or i in reviewed_ids for i in ids) or len(set(ids)) != len(ids):
            raise ValueError('成员无效，或同一个成员出现在多个组中')
        if target is not None and (type(target) is not int or not any(snapshot[i]['id_final'] == target for i in ids) or target in revised_groups):
            raise ValueError('目标系列须来自所选成员，且每次只能修改一次')
        if target is None and len(ids) < 2 and snapshot[ids[0]]['id_final'] is None:
            raise ValueError('新建聚合系列至少需要两个成员')
        for member_id in ids:
            before, after = snapshot[member_id], current.get(member_id)
            if after is None or any(after[key] != before[key] for key in (
                    'id_final', 'revision', 'name_zh', 'films', 'sample', 'maker', 'source_ref')):
                raise ReviewConflict('系列成员或译名已变化，请重新生成报告')
        reviewed_ids.update(ids)
        if target is not None:
            revised_groups.add(target)
        changes.append((translated_name(name), ids, target))
    touched = set()
    for name, ids, target in changes:
        touched.update(snapshot[i]['id_final'] for i in ids if snapshot[i]['id_final'] is not None)
        if target is None:
            target = conn.execute('INSERT INTO series_final(name) VALUES (?)', (name,)).lastrowid
        else:
            conn.execute('UPDATE series_final SET name=? WHERE id=?', (name, target))
        touched.add(target)
        conn.executemany('UPDATE series_cn SET id_final=? WHERE id=?', ((target, i) for i in ids))
    conn.executemany('UPDATE series_final SET revision=revision+1 WHERE id=?', ((i,) for i in touched))
    conn.execute('INSERT INTO series_publications(report_id,groups_json) VALUES (?,?)', (report_id, payload))
    conn.execute('INSERT INTO admin_history(created_at,action,target,details_json) VALUES (?,?,?,?)',
                 (time.time(), 'series_publish', report_id, json.dumps({'groups': len(changes)})))


def generate_report(members):
    """Use BGE-M3 to propose pairs. The report cannot change any membership."""
    from collections import defaultdict
    from heapq import heappush, heapreplace
    from math import sqrt
    from operator import mul

    from ollama import Client
    from core.translator.service import DEFAULT_HOST

    texts = {row['id']: ' '.join(dict.fromkeys(filter(None, (
        normalize_input(row['raw_name']), normalize_input(row['name_zh'] or ''))))) for row in members}
    inputs = list(dict.fromkeys(filter(None, texts.values())))
    vectors = {}
    client = Client(host=DEFAULT_HOST, timeout=120)
    for offset in range(0, len(inputs), 64):
        batch = inputs[offset:offset + 64]
        embeddings = client.embed(model='bge-m3', input=batch)['embeddings']
        for text, vector in zip(batch, embeddings, strict=True):
            length = sqrt(sum(value * value for value in vector))
            vectors[text] = tuple(value / length for value in vector)
    candidates = defaultdict(list)
    for index, left in enumerate(members):
        a = vectors.get(texts[left['id']])
        if a is None:
            continue
        for right in members[index + 1:]:
            if left['id_final'] is not None and left['id_final'] == right['id_final']:
                continue
            b = vectors.get(texts[right['id']])
            if b is None:
                continue
            score = max(-1.0, min(1.0, sum(map(mul, a, b))))
            for member_id, neighbor in ((left['id'], right['id']), (right['id'], left['id'])):
                heap = candidates[member_id]
                item = (score, neighbor)
                if len(heap) < 3:
                    heappush(heap, item)
                elif item > heap[0]:
                    heapreplace(heap, item)
    pairs = {(min(a, b), max(a, b)): score for a, heap in candidates.items() for score, b in heap}
    by_id = {row['id']: row for row in members}
    groups = []
    for (a, b), score in sorted(pairs.items(), key=lambda item: (-item[1], item[0]))[:150]:
        left, right = by_id[a], by_id[b]
        preferred = max((left, right), key=lambda row: (bool(row['final_name']), bool(row['name_zh']), row['films']))
        reason = '来源系列相同' if left['source_ref'] and left['source_ref'] == right['source_ref'] else (
            '片商相同' if left['maker'] and left['maker'] == right['maker'] else '名称语义接近')
        groups.append({'member_ids': [a, b], 'name': preferred['final_name'] or preferred['name_zh'] or preferred['raw_name'],
                       'score': round(score, 4), 'reason': reason})
    return {'members': members, 'groups': groups}
