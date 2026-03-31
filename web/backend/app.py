from flask import Flask, jsonify, request

from dal.db_manager import (
    get_actor_with_videos,
    get_tag_with_videos,
    get_video_by_code,
    list_videos,
    search_videos,
)

app = Flask(__name__, static_folder='../static', static_url_path='/static')


@app.get('/api/videos')
def api_videos():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 24))
    return jsonify(list_videos(page=page, limit=limit))


@app.get('/api/videos/<string:code>')
def api_video_detail(code: str):
    data = get_video_by_code(code)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


@app.get('/api/search')
def api_search():
    q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 24))
    return jsonify(search_videos(q, page=page, limit=limit))


@app.get('/api/actors/<int:actor_id>')
def api_actor(actor_id: int):
    data = get_actor_with_videos(actor_id)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


@app.get('/api/tags/<int:tag_id>')
def api_tag(tag_id: int):
    data = get_tag_with_videos(tag_id)
    if not data:
        return jsonify({'error': 'not found'}), 404
    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False)
