"""Raw series identities, durable translations and reviewed final groups."""
import json
import time
import unicodedata

from zhconv import convert


REPORT_VERSION = 3
AUTO_GROUP_THRESHOLD = 0.95

SCHEMA = """
CREATE TABLE IF NOT EXISTS series_jp (
    id INTEGER PRIMARY KEY, raw_name TEXT NOT NULL UNIQUE
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
    if 'scope' in {row[1] for row in conn.execute('PRAGMA table_info(series_jp)')}:
        conn.execute('BEGIN IMMEDIATE')
        if 'scope' in {row[1] for row in conn.execute('PRAGMA table_info(series_jp)')}:
            merge_same_names(conn)
        conn.commit()


def merge_same_names(conn):
    duplicates = conn.execute('SELECT raw_name FROM series_jp GROUP BY raw_name HAVING COUNT(*)>1').fetchall()
    for (raw,) in duplicates:
        rows = conn.execute('''SELECT j.id AS jp_id,c.id AS cn_id,c.id_final,c.name,c.status,c.task_id
            FROM series_jp j LEFT JOIN series_cn c ON c.id_jp=j.id WHERE j.raw_name=?
            ORDER BY c.id_final IS NULL, CASE c.status WHEN 'DONE' THEN 0 WHEN 'TRANSLATING' THEN 1
                WHEN 'FAILED' THEN 2 ELSE 3 END, j.id''', (raw,)).fetchall()
        finals = {row['id_final'] for row in rows if row['id_final'] is not None}
        if len(finals) > 1:
            raise ReviewConflict(f'同名系列「{raw}」已发布到不同分组，请先解决归属冲突')
        keep = rows[0]
        if keep['status'] != 'DONE':
            translated = next((row for row in rows if row['status'] == 'DONE'), None)
            if translated:
                conn.execute("UPDATE series_cn SET name=?, status='DONE', task_id=? WHERE id=?",
                             (translated['name'], translated['task_id'], keep['cn_id']))
        for discard in rows[1:]:
            conn.execute('UPDATE videos SET series_jp_id=? WHERE series_jp_id=?', (keep['jp_id'], discard['jp_id']))
            conn.execute('DELETE FROM series_cn WHERE id_jp=?', (discard['jp_id'],))
            conn.execute('DELETE FROM series_jp WHERE id=?', (discard['jp_id'],))
    conn.execute('CREATE TABLE series_jp_new (id INTEGER PRIMARY KEY, raw_name TEXT NOT NULL UNIQUE)')
    conn.execute('INSERT INTO series_jp_new(id,raw_name) SELECT id,raw_name FROM series_jp')
    conn.execute('DROP TABLE series_jp')
    conn.execute('ALTER TABLE series_jp_new RENAME TO series_jp')


def normalize_input(name):
    return ''.join(char for char in unicodedata.normalize('NFKC', name) if char.isalnum())


def translated_name(name):
    return convert(name.strip(), 'zh-cn')


def attach(conn, store, code, raw):
    """Called inside the same write transaction that saved the film metadata."""
    raw = (raw or '').strip()
    if not raw:
        conn.execute('UPDATE videos SET series_jp_id=NULL WHERE code=?', (code,))
        return
    conn.execute('INSERT OR IGNORE INTO series_jp(raw_name) VALUES (?)', (raw,))
    id_jp = conn.execute('SELECT id FROM series_jp WHERE raw_name=?', (raw,)).fetchone()[0]
    conn.execute('UPDATE videos SET series_jp_id=? WHERE code=?', (id_jp, code))
    if conn.execute('SELECT 1 FROM series_cn WHERE id_jp=?', (id_jp,)).fetchone():
        return
    task_id = store.submit('series_translate', raw, {'raw_name': raw}, priority=-2000, connection=conn)
    conn.execute("INSERT INTO series_cn(id_jp,status,task_id) VALUES (?,'TRANSLATING',?)", (id_jp, task_id))


def report_snapshot(conn):
    return [dict(row) for row in conn.execute('''SELECT c.id, j.raw_name, c.name AS name_zh,
        c.id_final, f.name AS final_name, f.revision,
        COUNT(DISTINCT v.code) AS films, MIN(v.code) AS sample
        FROM series_cn c JOIN series_jp j ON j.id=c.id_jp
        JOIN videos v ON v.series_jp_id=j.id AND v.deleted=0
        LEFT JOIN series_final f ON f.id=c.id_final
        WHERE c.status='DONE' AND trim(COALESCE(c.name,''))!=''
        GROUP BY c.id ORDER BY c.id''')]


class ReviewConflict(ValueError):
    pass


def combine_review_groups(groups):
    """Combine overlapping groups that the reviewer selected, preserving the first name."""
    combined = []
    for group in groups:
        if not isinstance(group, dict):
            raise ValueError('审核分组格式无效')
        name, ids, target = group.get('name'), group.get('member_ids'), group.get('final_id')
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 300 or not isinstance(ids, list) or not ids:
            raise ValueError('请填写最终名称并选择成员')
        if any(type(i) is not int for i in ids) or len(set(ids)) != len(ids):
            raise ValueError('成员无效')
        if target is not None and type(target) is not int:
            raise ValueError('目标系列无效')
        matching = [item for item in combined if set(ids).intersection(item['member_ids'])]
        if not matching:
            combined.append(dict(name=name, member_ids=ids, final_id=target))
            continue
        targets = {value for value in [target, *(item['final_id'] for item in matching)] if value is not None}
        if len(targets) > 1:
            raise ValueError('重叠组选择了不同的发布目标，请只保留一个')
        first = matching[0]
        existing_ids = [member_id for item in matching for member_id in item['member_ids']]
        first['member_ids'] = list(dict.fromkeys(existing_ids + ids))
        first['final_id'] = next(iter(targets), None)
        for item in matching[1:]:
            combined.remove(item)
    return combined


def publish(conn, report_id, groups):
    if not isinstance(groups, list) or not groups:
        raise ValueError('请选择至少一组待发布的系列')
    submitted = json.dumps(groups, ensure_ascii=False, sort_keys=True)
    groups = combine_review_groups(groups)
    payload = json.dumps(groups, ensure_ascii=False, sort_keys=True)
    existing = conn.execute('SELECT groups_json FROM series_publications WHERE report_id=?', (report_id,)).fetchone()
    if existing:
        if existing[0] in (payload, submitted):
            return
        raise ReviewConflict('报告已发布，请生成新报告')
    report = completed_report(conn, report_id)
    snapshot = validate_review_groups(conn, report, groups)
    touched = set()
    for group in groups:
        name, ids, target = translated_name(group['name']), group['member_ids'], group['final_id']
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
                 (time.time(), 'series_publish', report_id, json.dumps({'groups': len(groups)})))


def completed_report(conn, report_id):
    row = conn.execute("""SELECT result_json FROM scrape_tasks
        WHERE id=? AND kind='series_report' AND status='DONE' AND deleted=0""", (report_id,)).fetchone()
    if not row:
        raise ValueError('报告尚未生成完成')
    report = json.loads(row[0])
    if report.get('version') != REPORT_VERSION:
        raise ReviewConflict('报告使用旧的系列规则，请重新生成报告')
    return report


def validate_review_groups(conn, report, groups):
    snapshot = {item['id']: item for item in report['members']}
    current = {item['id']: item for item in report_snapshot(conn)}
    destinations = {member_id: index for index, group in enumerate(groups) for member_id in group['member_ids']}
    for carried in report.get('draft_groups', ()):
        targets = {destinations.get(member_id) for member_id in carried['member_ids']}
        if None in targets or len(targets) != 1:
            raise ValueError('上轮审核组须作为整体保留或合并')
    revised_groups = set()
    for group in groups:
        ids, target = group['member_ids'], group['final_id']
        if any(i not in snapshot for i in ids):
            raise ValueError('成员无效')
        if target is not None and (not any(snapshot[i]['id_final'] == target for i in ids) or target in revised_groups):
            raise ValueError('目标系列须来自所选成员，且每次只能修改一次')
        if target is None and len(ids) < 2 and snapshot[ids[0]]['id_final'] is None:
            raise ValueError('新建聚合系列至少需要两个成员')
        for member_id in ids:
            before, after = snapshot[member_id], current.get(member_id)
            if after is None or any(after[key] != before[key] for key in (
                    'id_final', 'revision', 'raw_name', 'name_zh', 'films', 'sample')):
                raise ReviewConflict('系列成员或译名已变化，请重新生成报告')
        if target is not None:
            revised_groups.add(target)
    return snapshot


def report_units(members, draft_groups):
    represented = {member_id for group in draft_groups for member_id in group['member_ids']}
    return [
        *(dict(member_ids=group['member_ids'], name=group['name'], final_id=group['final_id'], draft=True)
          for group in draft_groups),
        *(dict(member_ids=[row['id']], name=row['name_zh'], final_id=row['id_final'], draft=False)
          for row in members if row['id'] not in represented and normalize_input(row['name_zh'] or '')),
    ]


def generate_report(members, draft_groups=(), round_number=1):
    """Use BGE-M3 to propose pairs. The report cannot change any membership."""
    from collections import defaultdict
    from heapq import heappush, heapreplace
    from math import sqrt
    from operator import mul

    from ollama import Client
    from core.translator.service import DEFAULT_HOST

    represented = {member_id for group in draft_groups for member_id in group['member_ids']}
    members = [row for row in members if row['id'] in represented or normalize_input(row['name_zh'] or '')]
    units = report_units(members, draft_groups)
    texts = [normalize_input(translated_name(unit['name'])) for unit in units]
    inputs = list(dict.fromkeys(filter(None, texts)))
    vectors = {}
    client = Client(host=DEFAULT_HOST, timeout=120)
    for offset in range(0, len(inputs), 64):
        batch = inputs[offset:offset + 64]
        embeddings = client.embed(model='bge-m3', input=batch)['embeddings']
        for text, vector in zip(batch, embeddings, strict=True):
            length = sqrt(sum(value * value for value in vector))
            vectors[text] = tuple(value / length for value in vector)
    candidates = defaultdict(list)
    parents = list(range(len(units)))
    automatic = {}

    def root(index):
        while index != parents[index]:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    for index, left in enumerate(units):
        a = vectors.get(texts[index])
        if a is None:
            continue
        for right_index in range(index + 1, len(units)):
            right = units[right_index]
            if left['final_id'] is not None and left['final_id'] == right['final_id']:
                continue
            b = vectors.get(texts[right_index])
            if b is None:
                continue
            score = max(-1.0, min(1.0, sum(map(mul, a, b))))
            if score > AUTO_GROUP_THRESHOLD:
                left_root, right_root = root(index), root(right_index)
                if left_root != right_root:
                    parents[right_root] = left_root
                    automatic[index, right_index] = score
                continue
            for member_id, neighbor in ((index, right_index), (right_index, index)):
                heap = candidates[member_id]
                item = (score, neighbor)
                if len(heap) < 3:
                    heappush(heap, item)
                elif item > heap[0]:
                    heapreplace(heap, item)
    pairs = {(min(a, b), max(a, b)): score for a, heap in candidates.items() for score, b in heap if root(a) != root(b)}
    groups = []
    final_names = {row['id_final']: row['final_name'] or row['name_zh']
                   for row in members if row['id_final'] is not None}
    def ranked(item):
        return -item[1], item[0]
    edges = sorted(automatic.items(), key=ranked) + sorted(pairs.items(), key=ranked)[:150]
    for (a, b), score in edges:
        left, right = units[a], units[b]
        preferred = max((left, right), key=lambda unit: (unit['draft'], unit['final_id'] is not None))
        name = preferred['name']
        if round_number == 1 and preferred['final_id'] is not None:
            name = final_names[preferred['final_id']]
        groups.append({'member_ids': left['member_ids'] + right['member_ids'], 'name': name,
                       'comparison_names': [left['name'], right['name']],
                       'score': round(score, 4), 'automatic': (a, b) in automatic, 'reason': '名称语义接近'})
    return {'version': REPORT_VERSION, 'round': round_number, 'members': members,
            'draft_groups': list(draft_groups), 'groups': groups}
