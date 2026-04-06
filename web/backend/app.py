import os
from pathlib import Path
from flask import Flask, jsonify, request, send_file, send_from_directory
from werkzeug.utils import secure_filename

from dal.db_manager import (
    get_actor_with_videos,
    get_tag_with_videos,
    get_video_by_code,
    get_videos_by_prefix,
    get_videos_by_series,
    list_all_actors_with_count,
    list_all_prefixes_with_count,
    list_all_series_with_count,
    list_all_tags_with_count,
    list_videos,
    assign_series_cluster,
    delete_video,
    find_actor_by_name,
    get_video_file_path,
    merge_actors,
    patch_actor,
    patch_actor_avatar,
    patch_video,
    patch_video_cover,
    patch_video_relations,
    rename_video_code,
    search_actors,
    search_series,
    search_videos,
)

app = Flask(__name__, static_folder='../static', static_url_path='/static', template_folder='../templates')
app.json.ensure_ascii = False


@app.get('/api/videos')
def api_videos():
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    sort = request.args.get('sort', 'date')
    return jsonify(list_videos(page=page, limit=limit, sort=sort))


@app.get('/api/videos/<string:code>')
def api_video_detail(code: str):
    data = get_video_by_code(code)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


@app.patch('/api/videos/<string:code>')
def api_video_patch(code: str):
    body = request.get_json(silent=True) or {}
    actor_names = body.pop('actors', None)
    tag_names = body.pop('tags', None)
    new_code = body.pop('code', None)
    cluster_id = body.pop('series_cluster_id', None)
    changed = False
    # If user picked a cluster, assign via cluster logic (merge old series in)
    if cluster_id:
        changed = assign_series_cluster(code, int(cluster_id))
        body.pop('series', None)  # don't double-write series
    if body:
        changed = patch_video(code, body) or changed
    if actor_names is not None or tag_names is not None:
        changed = patch_video_relations(code, actor_names, tag_names) or changed
    if new_code and new_code != code:
        if not rename_video_code(code, new_code):
            return jsonify({'error': 'code already exists or not found'}), 400
        changed = True
        code = new_code
    if not changed:
        return jsonify({'error': 'no valid fields or not found'}), 400
    return jsonify({'new_code': code, **get_video_by_code(code)})


_MIME_MAP = {
    '.mp4': 'video/mp4',
    '.m4v': 'video/mp4',
    '.mov': 'video/quicktime',
    '.avi': 'video/x-msvideo',
    '.wmv': 'video/x-ms-wmv',
    '.mkv': 'video/x-matroska',
    '.webm': 'video/webm',
    '.flv': 'video/x-flv',
}

def _do_stream(code: str, part: str = None):
    path = get_video_file_path(code, part)
    if not path or not os.path.exists(path):
        return jsonify({'error': 'file not found'}), 404
    ext = Path(path).suffix.lower()
    mime = _MIME_MAP.get(ext, 'video/mp4')
    resp = send_file(path, mimetype=mime, conditional=True)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Range'
    resp.headers['Access-Control-Expose-Headers'] = 'Content-Range, Accept-Ranges, Content-Length'
    resp.headers['Accept-Ranges'] = 'bytes'
    return resp


@app.get('/api/videos/<string:code>/stream')
def api_video_stream(code: str):
    return _do_stream(code, request.args.get('part') or None)


# 带扩展名的别名路由，供 VRPlayer 等依赖 URL 后缀判断格式的播放器使用
@app.get('/api/videos/<string:code>/stream.<string:ext>')
def api_video_stream_ext(code: str, ext: str):
    return _do_stream(code, request.args.get('part') or None)


@app.route('/api/videos/<string:code>/stream', methods=['OPTIONS'])
@app.route('/api/videos/<string:code>/stream.<string:ext>', methods=['OPTIONS'])
def api_video_stream_options(code: str, **kwargs):
    resp = app.make_default_options_response()
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Range'
    return resp


@app.delete('/api/videos/<string:code>')
def api_video_delete(code: str):
    result = delete_video(code)
    if not result['ok']:
        return jsonify({'error': result.get('error', 'failed')}), 404
    return jsonify(result)


_COVERS_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'covers')

@app.post('/api/videos/<string:code>/cover')
def api_video_upload_cover(code: str):
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'no file'}), 400
    ext = Path(secure_filename(f.filename)).suffix.lower()
    if ext not in {'.jpg', '.jpeg', '.png', '.webp'}:
        return jsonify({'error': 'unsupported file type'}), 400
    os.makedirs(_COVERS_DIR, exist_ok=True)
    filename = f'{code}{ext}'
    f.save(os.path.join(_COVERS_DIR, filename))
    rel_path = f'covers/{filename}'
    if not patch_video_cover(code, rel_path):
        return jsonify({'error': 'video not found'}), 404
    return jsonify({'cover_path': rel_path})


@app.get('/api/search')
def api_search():
    q = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    return jsonify(search_videos(q, page=page, limit=limit))


@app.get('/api/actors')
def api_actors_search():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    return jsonify(search_actors(q))


@app.get('/api/series')
def api_series_search():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    return jsonify(search_series(q))


@app.get('/api/tags/all')
def api_tags_all():
    return jsonify(list_all_tags_with_count())


@app.get('/api/actors/all')
def api_actors_all():
    return jsonify(list_all_actors_with_count())


@app.get('/api/series/all')
def api_series_all():
    return jsonify(list_all_series_with_count())


@app.get('/api/series/videos')
def api_series_videos():
    cid = request.args.get('cid', type=int)
    s = request.args.get('s', '').strip() or None
    if not cid and not s:
        return jsonify({'error': 'missing cid or s'}), 400
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    sort = request.args.get('sort', 'date')
    return jsonify(get_videos_by_series(series_name=s, cluster_id=cid, page=page, limit=limit, sort=sort))


@app.get('/api/prefixes/all')
def api_prefixes_all():
    return jsonify(list_all_prefixes_with_count())


@app.get('/api/prefixes/videos')
def api_prefix_videos():
    prefix = request.args.get('prefix', '').strip()
    if not prefix:
        return jsonify({'error': 'missing prefix'}), 400
    page = request.args.get('page', 1, type=int) or 1
    limit = request.args.get('limit', 24, type=int) or 24
    sort = request.args.get('sort', 'date')
    return jsonify(get_videos_by_prefix(prefix, page=page, limit=limit, sort=sort))


@app.get('/api/actors/<int:actor_id>')
def api_actor(actor_id: int):
    data = get_actor_with_videos(actor_id)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


@app.patch('/api/actors/<int:actor_id>')
def api_actor_patch(actor_id: int):
    body = request.get_json(silent=True) or {}
    new_name = body.get('name', '').strip()

    # Check if renaming `name` would collide with an existing actor → merge
    if new_name:
        conflict = find_actor_by_name(new_name, exclude_id=actor_id)
        if conflict:
            merge_actors(source_id=actor_id, target_id=conflict['id'])
            data = get_actor_with_videos(conflict['id'])
            return jsonify({'merged': True, 'actor': data['actor'], 'redirect_id': conflict['id']})

    if not patch_actor(actor_id, body):
        return jsonify({'error': 'no valid fields or not found'}), 400
    data = get_actor_with_videos(actor_id)
    return jsonify({'merged': False, 'actor': data['actor']})


_AVATARS_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'avatars')
_ALLOWED_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}

@app.post('/api/actors/<int:actor_id>/avatar')
def api_actor_upload_avatar(actor_id: int):
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'no file'}), 400
    ext = Path(secure_filename(f.filename)).suffix.lower()
    if ext not in _ALLOWED_EXT:
        return jsonify({'error': 'unsupported file type'}), 400
    os.makedirs(_AVATARS_DIR, exist_ok=True)
    filename = f'actor_{actor_id}{ext}'
    save_path = os.path.join(_AVATARS_DIR, filename)
    f.save(save_path)
    rel_path = f'avatars/{filename}'
    if not patch_actor_avatar(actor_id, rel_path):
        return jsonify({'error': 'actor not found'}), 404
    return jsonify({'avatar_path': rel_path})


@app.get('/api/tags/<int:tag_id>')
def api_tag(tag_id: int):
    data = get_tag_with_videos(tag_id)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), '..', 'templates')

@app.get('/', defaults={'path': ''})
@app.get('/<path:path>')
def index(path):
    return send_from_directory(_TEMPLATES_DIR, 'index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False)
