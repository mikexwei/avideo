"""Read MKV text subtitle tracks without modifying the original media."""
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path

from config import DATA_DIR

CACHE_ROOT = DATA_DIR / 'subtitle_cache'
_TEXT_CODECS = {'subrip', 'ass', 'ssa', 'webvtt', 'text'}
_LANGUAGES = {'chi': 'zh', 'zho': 'zh', 'cmn': 'zh', 'jpn': 'ja', 'eng': 'en', 'kor': 'ko'}
_LABELS = {'zh': '中文', 'ja': '日语', 'en': '英语', 'ko': '韩语'}


def fingerprint(path):
    path = Path(path)
    info = path.stat()
    identity = [str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def manifest(key):
    path = CACHE_ROOT / key / 'tracks.json'
    try:
        tracks = json.loads(path.read_text())
        if all((path.parent / f"{track['id']}.vtt").is_file() for track in tracks):
            return tracks
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def extract(path, key):
    path = Path(path)
    cached = manifest(key)
    if cached is not None:
        return cached
    if fingerprint(path) != key:
        raise ValueError('影片已替换，请重新加载字幕')
    destination = CACHE_ROOT / key
    destination.mkdir(parents=True, exist_ok=True)
    try:
        probe = subprocess.run([
            'ffprobe', '-v', 'error', '-select_streams', 's',
            '-show_entries', 'stream=index,codec_name:stream_tags=language,title:stream_disposition=default',
            '-of', 'json', str(path),
        ], check=True, capture_output=True, timeout=30)
        streams = [stream for stream in json.loads(probe.stdout)['streams']
                   if stream['codec_name'] in _TEXT_CODECS]
        tracks = []
        with tempfile.TemporaryDirectory(dir=destination) as directory:
            temporary = Path(directory)
            if streams:
                command = ['ffmpeg', '-nostdin', '-v', 'error', '-i', str(path)]
                for stream in streams:
                    track_id = stream['index']
                    tags = stream.get('tags', {})
                    raw_language = tags.get('language', 'und').lower()
                    language = _LANGUAGES.get(raw_language, raw_language)
                    if not re.fullmatch(r'[a-z]{2,3}(?:-[a-z0-9]{2,8})*', language):
                        language = 'und'
                    tracks.append({'id': track_id, 'language': language,
                                   'label': tags.get('title') or _LABELS.get(language) or f'字幕 {len(tracks) + 1}',
                                   'default': bool(stream.get('disposition', {}).get('default'))})
                    command.extend(['-map', f'0:{track_id}', '-c:s', 'webvtt', str(temporary / f'{track_id}.vtt')])
                subprocess.run(command, check=True, capture_output=True, timeout=900)
            if fingerprint(path) != key:
                raise ValueError('影片已替换，请重新加载字幕')
            for track in tracks:
                name = f"{track['id']}.vtt"
                (temporary / name).replace(destination / name)
            (temporary / 'tracks.json').write_text(json.dumps(tracks, ensure_ascii=False))
            (temporary / 'tracks.json').replace(destination / 'tracks.json')
        return tracks
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError('字幕提取超时') from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError('无法提取影片字幕') from exc
    except FileNotFoundError as exc:
        raise RuntimeError('影片文件或 FFmpeg 不可用') from exc
