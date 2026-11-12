"""Read-only validity checks shared with the browser immediately before access."""
import json
from contextlib import nullcontext


SPECIAL_CODE_PREFIXES = ('CN', 'DUMMY', 'WEST')


def is_special_code(code):
    return bool(code) and code.upper().partition('-')[0] in SPECIAL_CODE_PREFIXES


class TargetChanged(ValueError):
    pass


def snapshot(store, code, connection=None):
    with (nullcontext(connection) if connection is not None else store.connect()) as conn:
        rows = conn.execute('SELECT * FROM videos WHERE UPPER(code)=UPPER(?) AND deleted=0 ORDER BY id', (code,)).fetchall()
        if not rows:
            raise TargetChanged('影片已删除或番号已改变')
        result = []
        for row in rows:
            item = dict(row)
            item['_tags'] = [dict(tag) for tag in conn.execute('SELECT t.* FROM tags t JOIN video_tag_link l ON l.tag_id=t.id WHERE l.video_id=? ORDER BY t.id', (row['id'],))]
            item['_actors'] = [dict(actor) for actor in conn.execute('SELECT a.* FROM actors a JOIN video_actor_link l ON l.actor_id=a.id WHERE l.video_id=? ORDER BY a.id', (row['id'],))]
            result.append(item)
        return result


def matches_video_snapshot(store, code, expected, connection=None):
    def without_actor_avatars(rows):
        return [
            {**row, '_actors': [
                {key: value for key, value in actor.items() if key != 'avatar_path'}
                for actor in row['_actors']
            ]}
            for row in rows
        ]

    # Actor jobs can update avatars independently. Names and links still matter.
    current = snapshot(store, code, connection=connection)
    return without_actor_avatars(current) == without_actor_avatars(expected)


def validate_command(store, command):
    current = store.command(command['id'])
    task = store.get(command['task_id'])
    if current['cancel_requested'] or not task or task['cancel_requested'] or task['deleted'] or task['execution_id'] != command['execution_id']:
        raise TargetChanged('任务已取消或执行版本已改变')
    payload = json.loads(command['payload_json'])
    if payload.get('actor_snapshot') is not None:
        with store.connect() as conn:
            actor = conn.execute('SELECT * FROM actors WHERE id=?', (payload['actor_snapshot']['id'],)).fetchone()
        if actor is None or dict(actor) != payload['actor_snapshot']:
            raise TargetChanged('演员在等待期间已改变')
    if command['kind'] == 'video' and is_special_code(payload.get('code')):
        raise TargetChanged('特殊番号不参与刮削')
    expected = payload.get('snapshot')
    if expected is not None and not matches_video_snapshot(store, payload['code'], expected):
        raise TargetChanged('影片在等待期间已改变，结果需重新请求')
    return payload
