# Replace Video Feature Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "替换视频" modal to the movie detail page that lets users upload a replacement video file, configuring Chinese subtitle / uncensored suffixes and part numbering, with atomic file swap and DB sync.

**Architecture:** Four layers — DB function (`replace_video_file`), Flask endpoint (`POST /api/videos/<code>/replace`), modal HTML template in `tpl-video-detail`, and Vue 3 Composition API JS wired into `VideoDetail.setup()`. File operations follow a safe write-verify-swap pattern: upload to `.temp`, verify byte count, then atomically delete original and rename temp.

**Tech Stack:** Python 3, Flask, SQLite via `dal/db_manager.py`, Vue 3 (CDN, Composition API), Tailwind CSS 2, vanilla XHR for upload progress.

---

## File Map

| File | Change | Responsibility |
|------|--------|----------------|
| `dal/db_manager.py` | Modify | Add `replace_video_file()` |
| `web/backend/app.py` | Modify | Add `_compute_final_filename()` helper + endpoint, set `MAX_CONTENT_LENGTH` |
| `tests/test_replace_video.py` | Create | All tests: DB function, filename helper, endpoint |
| `web/templates/index.html` | Modify | Modal template HTML + Vue JS logic in `VideoDetail.setup()` |

---

## Task 1: DB function `replace_video_file()`

**Files:**
- Create: `tests/test_replace_video.py`
- Modify: `dal/db_manager.py` (add after `patch_video_file_path`, ~line 1419)

- [ ] **Step 1: Write failing tests**

Create `tests/test_replace_video.py`:

```python
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


def _init_schema(conn):
    conn.executescript("""
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            part TEXT,
            original_file_path TEXT,
            updated_at TIMESTAMP
        );
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE video_tag_link (
            video_id INTEGER,
            tag_id INTEGER,
            PRIMARY KEY (video_id, tag_id)
        );
    """)
    conn.commit()


class ReplaceVideoFileTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        _init_schema(conn)
        conn.execute("INSERT INTO videos (code, part, original_file_path) VALUES ('SSIS-001', NULL, '/vol/SSIS-001-UC.mp4')")
        conn.execute("INSERT INTO tags (name) VALUES ('中文字幕')")
        conn.execute("INSERT INTO video_tag_link (video_id, tag_id) VALUES (1, 1)")
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def _call(self, code='SSIS-001', part=None, dest='/vol/SSIS-001-C.mp4',
              sub_zh=False, leaked=False, native=False):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            from dal.db_manager import replace_video_file
            return replace_video_file(code, part, dest, sub_zh, leaked, native)

    def test_updates_file_path(self):
        result = self._call(dest='/vol/SSIS-001-C.mp4', sub_zh=True)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT original_file_path FROM videos WHERE code='SSIS-001'").fetchone()
        conn.close()
        self.assertEqual(row[0], '/vol/SSIS-001-C.mp4')

    def test_adds_missing_tag(self):
        result = self._call(native=True)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        tags = {r[0] for r in conn.execute(
            "SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id=t.id WHERE vtl.video_id=1"
        ).fetchall()}
        conn.close()
        self.assertIn('原装无码', tags)

    def test_removes_tag_when_off(self):
        result = self._call(sub_zh=False)
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        tags = {r[0] for r in conn.execute(
            "SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id=t.id WHERE vtl.video_id=1"
        ).fetchall()}
        conn.close()
        self.assertNotIn('中文字幕', tags)

    def test_unknown_code_returns_error(self):
        result = self._call(code='NOSUCH')
        self.assertFalse(result['ok'])
```

- [ ] **Step 2: Run to confirm failure**

```
python -m pytest tests/test_replace_video.py -v
```
Expected: `FAILED` — `replace_video_file` not yet defined.

- [ ] **Step 3: Implement `replace_video_file()` in `dal/db_manager.py`**

Add after `patch_video_file_path` (after line 1419):

```python
def replace_video_file(
    code: str,
    original_part: Optional[str],
    dest_path: str,
    subtitle_zh: bool,
    uncensored_leaked: bool,
    uncensored_native: bool,
) -> Dict[str, Any]:
    """Update file path and sync 中文字幕/无码破解/原装无码 tags after a file replacement."""
    tag_desired = {'中文字幕': subtitle_zh, '无码破解': uncensored_leaked, '原装无码': uncensored_native}
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return {'ok': False, 'error': 'not found'}

        if original_part:
            cursor.execute(
                'SELECT id FROM videos WHERE code = ? AND part = ? ORDER BY id ASC LIMIT 1',
                (resolved_code, original_part),
            )
        else:
            cursor.execute(
                'SELECT id FROM videos WHERE code = ? AND (part IS NULL OR part = "") ORDER BY id ASC LIMIT 1',
                (resolved_code,),
            )
        row = cursor.fetchone()
        if not row:
            return {'ok': False, 'error': 'part not found' if original_part else 'not found'}
        video_id = row['id']

        cursor.execute(
            'UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
            (dest_path, video_id),
        )

        cursor.execute('SELECT id FROM videos WHERE code = ?', (resolved_code,))
        all_ids = [r['id'] for r in cursor.fetchall()]
        ph = ','.join('?' * len(all_ids))

        cursor.execute(
            f'SELECT t.name FROM tags t JOIN video_tag_link vtl ON vtl.tag_id = t.id WHERE vtl.video_id IN ({ph})',
            all_ids,
        )
        current_tags = {r['name'] for r in cursor.fetchall()}

        for tag_name, should_have in tag_desired.items():
            if should_have and tag_name not in current_tags:
                cursor.execute('INSERT OR IGNORE INTO tags (name) VALUES (?)', (tag_name,))
                cursor.execute('SELECT id FROM tags WHERE name = ?', (tag_name,))
                tag_row = cursor.fetchone()
                if tag_row:
                    for vid in all_ids:
                        cursor.execute(
                            'INSERT OR IGNORE INTO video_tag_link (video_id, tag_id) VALUES (?, ?)',
                            (vid, tag_row['id']),
                        )
            elif not should_have and tag_name in current_tags:
                cursor.execute('SELECT id FROM tags WHERE name = ?', (tag_name,))
                tag_row = cursor.fetchone()
                if tag_row:
                    cursor.execute(
                        f'DELETE FROM video_tag_link WHERE video_id IN ({ph}) AND tag_id = ?',
                        all_ids + [tag_row['id']],
                    )

        conn.commit()
        logger.info(f'🔄 replace_video_file [{code}]: {dest_path}')
        return {'ok': True, 'original_file_path': dest_path}
    except sqlite3.Error as e:
        logger.error(f'❌ replace_video_file 失败 [{code}]: {e}')
        return {'ok': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()
```

- [ ] **Step 4: Run to confirm pass**

```
python -m pytest tests/test_replace_video.py::ReplaceVideoFileTests -v
```
Expected: 4 PASSED.

- [ ] **Step 5: Commit**

```bash
git add dal/db_manager.py tests/test_replace_video.py
git commit -m "feat: add replace_video_file DB function"
```

---

## Task 2: Backend endpoint

**Files:**
- Modify: `web/backend/app.py`
- Modify: `tests/test_replace_video.py` (append new test classes)

- [ ] **Step 1: Append failing tests to `tests/test_replace_video.py`**

```python
import io
from pathlib import Path
from unittest.mock import patch

from web.backend.app import app, _compute_final_filename


class ComputeFinalFilenameTests(unittest.TestCase):
    def test_no_toggles(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', False, False, False, None), 'SSIS-001.mp4')

    def test_subtitle_zh(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', True, False, False, None), 'SSIS-001-C.mp4')

    def test_uncensored_leaked(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', False, True, False, None), 'SSIS-001-U.mp4')

    def test_uncensored_native(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', False, False, True, None), 'SSIS-001-U.mp4')

    def test_both_on(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', True, True, False, None), 'SSIS-001-UC.mp4')

    def test_with_part(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mp4', True, False, False, '2'), 'SSIS-001-C.part2.mp4')

    def test_mkv_extension(self):
        self.assertEqual(_compute_final_filename('SSIS-001.mkv', False, False, True, None), 'SSIS-001-U.mkv')


class ReplaceEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_missing_file_returns_400(self):
        resp = self.client.post(
            '/api/videos/SSIS-001/replace',
            data={'file_size': '4', 'dest_name': 'SSIS-001.mp4'},
            content_type='multipart/form-data',
        )
        self.assertEqual(resp.status_code, 400)

    def test_missing_dest_name_returns_400(self):
        resp = self.client.post(
            '/api/videos/SSIS-001/replace',
            data={'file': (io.BytesIO(b'data'), 'upload.mp4'), 'file_size': '4'},
            content_type='multipart/form-data',
        )
        self.assertEqual(resp.status_code, 400)

    def test_video_not_found_returns_404(self):
        with patch('web.backend.app.get_video_file_path', return_value=None):
            resp = self.client.post(
                '/api/videos/NOSUCH/replace',
                data={'file': (io.BytesIO(b'x'), 'u.mp4'), 'file_size': '1', 'dest_name': 'x.mp4'},
                content_type='multipart/form-data',
            )
        self.assertEqual(resp.status_code, 404)

    def test_size_mismatch_deletes_temp_returns_400(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig = Path(tmpdir) / 'SSIS-001.mp4'
            orig.write_bytes(b'original')
            with patch('web.backend.app.get_video_file_path', return_value=str(orig)):
                resp = self.client.post(
                    '/api/videos/SSIS-001/replace',
                    data={'file': (io.BytesIO(b'data'), 'u.mp4'), 'file_size': '9999', 'dest_name': 'SSIS-001.mp4'},
                    content_type='multipart/form-data',
                )
        self.assertEqual(resp.status_code, 400)
        self.assertTrue(orig.exists())
        self.assertFalse(list(Path(tmpdir).glob('*.temp')))

    def test_happy_path_returns_200(self):
        video_bytes = b'new video content'
        with tempfile.TemporaryDirectory() as tmpdir:
            orig = Path(tmpdir) / 'SSIS-001.mp4'
            orig.write_bytes(b'original content')
            with patch('web.backend.app.replace_video_file', return_value={'ok': True}), \
                 patch('web.backend.app.get_video_by_code', return_value={'code': 'SSIS-001', 'tags': [], 'actors': []}), \
                 patch('web.backend.app.get_video_file_path', return_value=str(orig)):
                resp = self.client.post(
                    '/api/videos/SSIS-001/replace',
                    data={
                        'file': (io.BytesIO(video_bytes), 'upload.mp4'),
                        'file_size': str(len(video_bytes)),
                        'dest_name': 'SSIS-001.mp4',
                        'subtitle_zh': '1',
                    },
                    content_type='multipart/form-data',
                )
            self.assertEqual(resp.status_code, 200)
            self.assertFalse(orig.exists())
            self.assertTrue((Path(tmpdir) / 'SSIS-001-C.mp4').exists())
            self.assertEqual((Path(tmpdir) / 'SSIS-001-C.mp4').read_bytes(), video_bytes)
```

- [ ] **Step 2: Run to confirm failure**

```
python -m pytest tests/test_replace_video.py::ComputeFinalFilenameTests tests/test_replace_video.py::ReplaceEndpointTests -v
```
Expected: `ImportError: cannot import name '_compute_final_filename'` and/or 404/405 on the endpoint.

- [ ] **Step 3: Implement in `web/backend/app.py`**

**3a.** Add `Optional` to existing imports at top:
```python
from typing import Optional
```

**3b.** Add `replace_video_file` to the `dal.db_manager` import block (join the existing list):
```python
    replace_video_file,
```

**3c.** After the `app = Flask(...)` block, add:
```python
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024 * 1024  # 50 GB
```

**3d.** Add the helper function before the first `@app.get` route:
```python
def _compute_final_filename(
    dest_name: str,
    subtitle_zh: bool,
    uncensored_leaked: bool,
    uncensored_native: bool,
    part: Optional[str],
) -> str:
    p = Path(dest_name)
    stem, ext = p.stem, p.suffix
    has_c = subtitle_zh
    has_u = uncensored_leaked or uncensored_native
    if has_c and has_u:
        suffix = '-UC'
    elif has_c:
        suffix = '-C'
    elif has_u:
        suffix = '-U'
    else:
        suffix = ''
    part_seg = f'.part{part}' if part else ''
    return f'{stem}{suffix}{part_seg}{ext}'
```

**3e.** Add the endpoint after `api_video_file_path_patch`:
```python
@app.post('/api/videos/<string:code>/replace')
def api_video_replace(code: str):
    original_part = request.args.get('part') or None
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'no file'}), 400
    dest_name = (request.form.get('dest_name') or '').strip()
    if not dest_name:
        return jsonify({'error': 'missing dest_name'}), 400
    try:
        expected_size = int(request.form.get('file_size') or 0)
    except ValueError:
        return jsonify({'error': 'invalid file_size'}), 400
    if not expected_size:
        return jsonify({'error': 'missing file_size'}), 400

    subtitle_zh = request.form.get('subtitle_zh') == '1'
    uncensored_leaked = request.form.get('uncensored_leaked') == '1'
    uncensored_native = request.form.get('uncensored_native') == '1'
    dest_part = (request.form.get('part') or '').strip() or None

    original_path = get_video_file_path(code, original_part)
    if not original_path:
        return jsonify({'error': 'video not found'}), 404

    final_name = _compute_final_filename(dest_name, subtitle_zh, uncensored_leaked, uncensored_native, dest_part)
    dest_path = Path(original_path).parent / final_name
    temp_path = Path(str(dest_path) + '.temp')

    try:
        f.save(str(temp_path))
        actual_size = os.path.getsize(str(temp_path))
        if actual_size != expected_size:
            temp_path.unlink(missing_ok=True)
            return jsonify({'error': f'size mismatch: expected {expected_size}, got {actual_size}'}), 400
        Path(original_path).unlink(missing_ok=True)
        temp_path.rename(dest_path)
    except Exception as e:
        temp_path.unlink(missing_ok=True)
        return jsonify({'error': str(e)}), 500

    result = replace_video_file(code, original_part, str(dest_path), subtitle_zh, uncensored_leaked, uncensored_native)
    if not result['ok']:
        return jsonify({'error': result.get('error', 'db update failed')}), 500

    return jsonify({'ok': True, **get_video_by_code(code)})
```

- [ ] **Step 4: Run all tests to confirm pass**

```
python -m pytest tests/test_replace_video.py -v
```
Expected: all 15 PASSED.

- [ ] **Step 5: Commit**

```bash
git add web/backend/app.py tests/test_replace_video.py
git commit -m "feat: add replace video endpoint"
```

---

## Task 3: Frontend modal template

**Files:**
- Modify: `web/templates/index.html` (template block `tpl-video-detail`, lines 426–634)

- [ ] **Step 1: Add "替换视频" button to the playback row**

Find the playback buttons `<div>` in `tpl-video-detail` (after "复制 URL" button, ~line 496). Add a third button:

```html
            <button @click="openReplaceModal"
              class="px-3 py-2 rounded bg-gray-700 hover:bg-gray-600 text-gray-200 text-sm">替换视频</button>
```

- [ ] **Step 2: Add modal HTML**

Insert before the closing `</div>` of the `v-else` block in `tpl-video-detail` (~line 632):

```html
      <!-- Replace Video Modal -->
      <teleport to="body">
        <div v-if="showReplaceModal" class="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-70" @click.self="closeReplaceModal">
          <div class="bg-gray-900 border border-gray-700 rounded-lg w-full max-w-md mx-4 p-5">
            <div class="flex items-center justify-between mb-4">
              <h2 class="text-base font-semibold text-gray-100">替换视频</h2>
              <button @click="closeReplaceModal" :disabled="replaceUploading" class="text-gray-500 hover:text-gray-300 text-lg leading-none">×</button>
            </div>

            <!-- Original part selector (multi-part videos only) -->
            <div v-if="fileParts.length > 1" class="mb-3">
              <label class="block text-xs text-gray-500 mb-1">替换哪个 Part</label>
              <select v-model="replaceOriginalPart" :disabled="replaceUploading"
                class="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1.5 text-sm text-gray-200 focus:outline-none">
                <option v-for="f in fileParts" :key="f.part || ''" :value="f.part || ''">{{ f.part || '完整' }}</option>
              </select>
            </div>

            <!-- File picker -->
            <div class="mb-3">
              <button @click="$refs.replaceFileInput.click()" :disabled="replaceUploading"
                class="px-3 py-1.5 text-sm rounded bg-gray-700 hover:bg-gray-600 text-gray-200 disabled:opacity-50">选择文件</button>
              <span class="ml-2 text-sm text-gray-400">{{ replaceFile ? replaceFile.name : '未选择' }}</span>
              <input ref="replaceFileInput" type="file" class="hidden" @change="onReplaceFileChange" :disabled="replaceUploading" />
            </div>

            <!-- Toggles -->
            <div class="grid grid-cols-2 gap-x-4 gap-y-2 items-center mb-3">
              <span class="text-sm text-gray-400">中文字幕</span>
              <button @click="replaceSubtitleZh = !replaceSubtitleZh" :disabled="replaceUploading"
                :class="['px-3 py-1 rounded text-xs font-medium w-16', replaceSubtitleZh ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400']">
                {{ replaceSubtitleZh ? 'YES' : 'NO' }}
              </button>
              <span class="text-sm text-gray-400">无码破解</span>
              <button @click="replaceUncensoredLeaked = !replaceUncensoredLeaked" :disabled="replaceUploading"
                :class="['px-3 py-1 rounded text-xs font-medium w-16', replaceUncensoredLeaked ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400']">
                {{ replaceUncensoredLeaked ? 'YES' : 'NO' }}
              </button>
              <span class="text-sm text-gray-400">原装无码</span>
              <button @click="replaceUncensoredNative = !replaceUncensoredNative" :disabled="replaceUploading"
                :class="['px-3 py-1 rounded text-xs font-medium w-16', replaceUncensoredNative ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400']">
                {{ replaceUncensoredNative ? 'YES' : 'NO' }}
              </button>
              <span class="text-sm text-gray-400">Part</span>
              <button @click="replacePartOn = !replacePartOn" :disabled="replaceUploading"
                :class="['px-3 py-1 rounded text-xs font-medium w-16', replacePartOn ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400']">
                {{ replacePartOn ? 'ON' : 'OFF' }}
              </button>
              <template v-if="replacePartOn">
                <span class="text-sm text-gray-400">Part 编号</span>
                <div class="flex gap-1">
                  <button v-for="n in [1,2,3,4]" :key="n" @click="replacePartNum = n" :disabled="replaceUploading"
                    :class="['px-2 py-1 rounded text-xs', replacePartNum === n ? 'bg-indigo-600 text-white' : 'bg-gray-700 text-gray-400 hover:bg-gray-600']">{{ n }}</button>
                </div>
              </template>
            </div>

            <!-- Filename preview -->
            <div v-if="replaceFile" class="mb-3 space-y-2">
              <div class="flex items-center gap-2">
                <span class="text-xs text-gray-500 w-20 flex-shrink-0">目标文件名</span>
                <input v-model="replaceDestName" :disabled="replaceUploading"
                  class="flex-1 bg-gray-800 border border-gray-600 rounded px-2 py-1 text-xs text-gray-200 font-mono focus:outline-none focus:border-indigo-500" />
              </div>
              <div class="flex items-center gap-2">
                <span class="text-xs text-gray-500 w-20 flex-shrink-0">最终文件名</span>
                <span class="text-xs text-gray-300 font-mono break-all">{{ replaceFinalName }}</span>
              </div>
            </div>

            <!-- Progress -->
            <div v-if="replaceUploading" class="mb-3">
              <div class="flex items-center gap-2 mb-1">
                <div class="flex-1 bg-gray-700 rounded-full h-1.5">
                  <div class="bg-indigo-500 h-1.5 rounded-full transition-all" :style="{ width: replaceProgress + '%' }"></div>
                </div>
                <span class="text-xs text-gray-400 w-8 text-right flex-shrink-0">{{ replaceProgress }}%</span>
              </div>
              <p class="text-xs text-gray-500">{{ replaceSpeed }} · 已用 {{ replaceElapsed }} · 剩余 {{ replaceRemaining }}</p>
            </div>

            <!-- Error -->
            <p v-if="replaceError" class="text-red-400 text-sm mb-3">⚠ {{ replaceError }}</p>

            <!-- Actions -->
            <div class="flex justify-end gap-2 mt-4">
              <button @click="closeReplaceModal" :disabled="replaceUploading"
                class="px-4 py-1.5 text-sm rounded bg-gray-700 hover:bg-gray-600 text-gray-200 disabled:opacity-50">取消</button>
              <button @click="startReplaceUpload" :disabled="!replaceFile || replaceUploading"
                class="px-4 py-1.5 text-sm rounded bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-50">
                {{ replaceUploading ? '上传中…' : '上传' }}
              </button>
            </div>
          </div>
        </div>
      </teleport>
```

- [ ] **Step 3: Commit**

```bash
git add web/templates/index.html
git commit -m "feat: add replace video modal template"
```

---

## Task 4: Frontend JS logic

**Files:**
- Modify: `web/templates/index.html` (JS section inside `VideoDetail.setup()` and its `return`, ~lines 1140–1410)

- [ ] **Step 1: Add refs, computed, and helpers to `VideoDetail.setup()`**

After the `// Cover upload` block (~line 1204), insert:

```js
        // Replace video modal
        const showReplaceModal = ref(false);
        const replaceOriginalPart = ref('');
        const replaceFile = ref(null);
        const replaceDestName = ref('');
        const replaceSubtitleZh = ref(false);
        const replaceUncensoredLeaked = ref(false);
        const replaceUncensoredNative = ref(false);
        const replacePartOn = ref(false);
        const replacePartNum = ref(1);
        const replaceUploading = ref(false);
        const replaceProgress = ref(0);
        const replaceSpeed = ref('');
        const replaceElapsed = ref('');
        const replaceRemaining = ref('');
        const replaceError = ref('');

        const replaceFinalName = computed(() => {
          if (!replaceDestName.value) return '';
          const name = replaceDestName.value;
          const dot = name.lastIndexOf('.');
          const stem = dot > 0 ? name.slice(0, dot) : name;
          const ext  = dot > 0 ? name.slice(dot) : '';
          const hasC = replaceSubtitleZh.value;
          const hasU = replaceUncensoredLeaked.value || replaceUncensoredNative.value;
          const suffix = (hasC && hasU) ? '-UC' : hasC ? '-C' : hasU ? '-U' : '';
          const partSeg = replacePartOn.value && replacePartNum.value ? `.part${replacePartNum.value}` : '';
          return `${stem}${suffix}${partSeg}${ext}`;
        });

        function _fmtTime(secs) {
          const s = Math.max(0, Math.floor(secs));
          return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
        }

        function _getOriginalPathForPart(part) {
          const v = video.value;
          if (v.parts && v.parts.length > 1) {
            const found = v.parts.find(p => (p.part || '') === part);
            return found ? found.original_file_path : v.original_file_path;
          }
          return v.original_file_path;
        }

        function openReplaceModal() {
          replaceFile.value = null;
          replaceDestName.value = '';
          replaceSubtitleZh.value = false;
          replaceUncensoredLeaked.value = false;
          replaceUncensoredNative.value = false;
          replaceUploading.value = false;
          replaceProgress.value = 0;
          replaceSpeed.value = '';
          replaceElapsed.value = '';
          replaceRemaining.value = '';
          replaceError.value = '';
          replaceOriginalPart.value = selectedPart.value || '';
          const m = replaceOriginalPart.value.match(/(\d+)$/);
          if (m) { replacePartOn.value = true; replacePartNum.value = parseInt(m[1]); }
          else    { replacePartOn.value = false; replacePartNum.value = 1; }
          showReplaceModal.value = true;
        }

        function closeReplaceModal() {
          if (replaceUploading.value) return;
          showReplaceModal.value = false;
        }

        function onReplaceFileChange(e) {
          const file = e.target.files[0];
          if (!file) return;
          replaceFile.value = file;
          replaceError.value = '';
          const origPath = _getOriginalPathForPart(replaceOriginalPart.value);
          if (origPath) {
            const origFilename = origPath.split('/').pop();
            const dot = origFilename.lastIndexOf('.');
            const origStem = dot > 0 ? origFilename.slice(0, dot) : origFilename;
            const baseStem = origStem.replace(/-(UC|C|U)$/i, '');
            const uploadDot = file.name.lastIndexOf('.');
            const uploadExt = uploadDot > 0 ? file.name.slice(uploadDot) : '';
            replaceDestName.value = baseStem + uploadExt;
          } else {
            replaceDestName.value = file.name;
          }
        }

        function startReplaceUpload() {
          if (!replaceFile.value || replaceUploading.value) return;
          replaceUploading.value = true;
          replaceError.value = '';
          replaceProgress.value = 0;
          replaceSpeed.value = '–';
          replaceElapsed.value = '0:00';
          replaceRemaining.value = '–';

          const fd = new FormData();
          fd.append('file', replaceFile.value);
          fd.append('file_size', String(replaceFile.value.size));
          fd.append('dest_name', replaceDestName.value);
          fd.append('subtitle_zh', replaceSubtitleZh.value ? '1' : '0');
          fd.append('uncensored_leaked', replaceUncensoredLeaked.value ? '1' : '0');
          fd.append('uncensored_native', replaceUncensoredNative.value ? '1' : '0');
          if (replacePartOn.value && replacePartNum.value) fd.append('part', String(replacePartNum.value));

          const qs = replaceOriginalPart.value ? '?part=' + encodeURIComponent(replaceOriginalPart.value) : '';
          const xhr = new XMLHttpRequest();
          const startTime = Date.now();
          let lastLoaded = 0, lastTime = startTime;
          const speedSamples = [];

          xhr.upload.addEventListener('progress', e => {
            if (!e.lengthComputable) return;
            const now = Date.now();
            replaceProgress.value = Math.round(e.loaded / e.total * 100);
            replaceElapsed.value = _fmtTime((now - startTime) / 1000);
            const dt = (now - lastTime) / 1000;
            if (dt >= 1) {
              const bps = (e.loaded - lastLoaded) / dt;
              speedSamples.push(bps);
              if (speedSamples.length > 5) speedSamples.shift();
              const avg = speedSamples.reduce((a, b) => a + b, 0) / speedSamples.length;
              replaceSpeed.value = avg >= 1e6 ? (avg / 1e6).toFixed(1) + ' MB/s' : (avg / 1e3).toFixed(0) + ' KB/s';
              replaceRemaining.value = avg > 0 ? _fmtTime((e.total - e.loaded) / avg) : '–';
              lastLoaded = e.loaded;
              lastTime = now;
            }
          });

          xhr.addEventListener('load', () => {
            replaceUploading.value = false;
            if (xhr.status === 200) {
              try { video.value = JSON.parse(xhr.responseText); } catch (_) {}
              showReplaceModal.value = false;
            } else {
              let msg = '上传失败';
              try { msg = JSON.parse(xhr.responseText).error || msg; } catch (_) {}
              replaceError.value = msg;
            }
          });

          xhr.addEventListener('error', () => {
            replaceUploading.value = false;
            replaceError.value = '网络错误，上传失败';
          });

          xhr.open('POST', '/api/videos/' + video.value.code + '/replace' + qs);
          xhr.send(fd);
        }
```

- [ ] **Step 2: Add new identifiers to the `return` statement in `VideoDetail.setup()`**

Find the `return {` block (~line 1382) and add before the closing `}`:

```js
          showReplaceModal, replaceOriginalPart,
          replaceFile, replaceDestName, replaceFinalName,
          replaceSubtitleZh, replaceUncensoredLeaked, replaceUncensoredNative,
          replacePartOn, replacePartNum,
          replaceUploading, replaceProgress, replaceSpeed, replaceElapsed, replaceRemaining,
          replaceError,
          openReplaceModal, closeReplaceModal, onReplaceFileChange, startReplaceUpload,
```

- [ ] **Step 3: Verify in browser**

Start the server:
```bash
python -m flask --app web.backend.app run --host 0.0.0.0 --port 8000
```

Open any movie detail page and check:
1. "替换视频" button appears in the playback row
2. Clicking it opens the modal
3. "选择文件" opens the OS file picker
4. After picking a file, "目标文件名" auto-fills with the stripped original stem + upload extension
5. Toggling 中文字幕 / 无码 / Part updates "最终文件名" in real time
6. Part toggle shows/hides the 1/2/3/4 selector
7. "取消" closes the modal; clicking the backdrop also closes it
8. "上传" is disabled until a file is selected
9. For a multi-part video, "替换哪个 Part" selector appears at the top

- [ ] **Step 4: Commit**

```bash
git add web/templates/index.html
git commit -m "feat: add replace video modal JS logic"
```
