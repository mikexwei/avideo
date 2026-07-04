# 网页分字段改名 + 按文件修改时间排序 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给网页加两个功能——列表按文件修改时间排序，以及详情弹窗里分字段（番号/女优/标题）重命名视频文件。

**Architecture:** 后端 Flask + SQLite（`dal/db_manager.py` 为 DAL 层，`web/backend/app.py` 为路由层）。前端是单文件 Vue 3 应用 `web/templates/index.html`（内联 x-template + Composition API）。排序走服务端 `_order_clause`；改名新增一个端点做「组装文件名 → 物理改名 → 更新 DB」。

**Tech Stack:** Python 3.12、Flask、sqlite3、Vue 3（CDN）、Tailwind（CDN）、pytest。

---

## 文件结构

- `dal/db_manager.py`
  - 修改 `_order_clause`：加 `mtime` 分支。
  - 修改 `get_actor_with_videos` / `get_tag_with_videos` 的视频 SELECT：加 `v.file_mtime` 字段（供前端本地 mtime 排序）。
  - 新增 `rename_video_file(code, part, new_path, new_code)`：定位记录并更新 `original_file_path` + `code`。
- `web/backend/app.py`
  - import 加 `sanitize_component`。
  - 新增端点 `POST /api/videos/<code>/rename-file`。
- `web/templates/index.html`
  - `SortBar` 模板加「文件时间」按钮。
  - 两处 `sortedVideos` computed 加 `mtime` 分支。
  - `tpl-video-detail` 的「本地文件」区块加分字段改名 UI，`VideoDetail` setup 加解析/提交逻辑。
- `tests/test_replace_video.py`：追加 `_order_clause` 与 `rename_video_file` 的测试。

## 测试策略说明

- **后端逻辑**（`_order_clause`、`rename_video_file`、rename 端点）走 TDD，用 `tests/test_replace_video.py` 既有的「临时 sqlite + 临时文件」模式。
- **前端**（SortBar 按钮、sortedVideos 分支、改名 UI）与 **SQL 字段补充**（Task 2）无 pytest 覆盖，用明确的手工验证步骤确认。

---

## Task 1: 后端按文件修改时间排序

**Files:**
- Modify: `dal/db_manager.py:32-36`（`_order_clause`）
- Test: `tests/test_replace_video.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_replace_video.py` 末尾追加：

```python
from dal.db_manager import _order_clause


class OrderClauseTests(unittest.TestCase):
    def test_mtime_sorts_by_file_mtime_desc(self):
        self.assertEqual(_order_clause('mtime'), "COALESCE(file_mtime, '') DESC, id DESC")

    def test_score_unchanged(self):
        self.assertEqual(
            _order_clause('score'),
            "COALESCE(score, 0) DESC, COALESCE(release_date, '') DESC, id DESC",
        )

    def test_default_is_release_date(self):
        self.assertEqual(_order_clause('date'), "COALESCE(release_date, '') DESC, id DESC")
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_replace_video.py::OrderClauseTests -v`
Expected: FAIL —`test_mtime_sorts_by_file_mtime_desc` 断言不符（当前 `mtime` 落到 default 分支返回 release_date 的子句）。

- [ ] **Step 3: 实现**

把 `dal/db_manager.py` 的 `_order_clause` 改为：

```python
def _order_clause(sort: str) -> str:
    """Return the ORDER BY body for video queries."""
    if sort == 'score':
        return "COALESCE(score, 0) DESC, COALESCE(release_date, '') DESC, id DESC"
    if sort == 'mtime':
        return "COALESCE(file_mtime, '') DESC, id DESC"
    return "COALESCE(release_date, '') DESC, id DESC"
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python -m pytest tests/test_replace_video.py::OrderClauseTests -v`
Expected: PASS（3 个）。

- [ ] **Step 5: 提交**

```bash
git add dal/db_manager.py tests/test_replace_video.py
git commit -m "feat: add file-mtime sort option to _order_clause"
```

---

## Task 2: 演员/标签详情视频接口补 file_mtime 字段

前端在演员页、标签页用客户端本地排序，视频对象需要带 `file_mtime`，否则 mtime 排序无依据。

**Files:**
- Modify: `dal/db_manager.py`（`get_actor_with_videos` 约 674 行、`get_tag_with_videos` 约 722 行的视频 SELECT）

- [ ] **Step 1: 改 `get_actor_with_videos` 的视频 SELECT**

把该函数里这行：

```python
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted
```

改为：

```python
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted, v.file_mtime
```

- [ ] **Step 2: 改 `get_tag_with_videos` 的视频 SELECT**

把该函数里这行：

```python
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted
```

改为：

```python
            SELECT v.id, v.code, v.title_jp, v.title_zh, v.release_date, v.cover_path, v.score, v.deleted, v.file_mtime
```

- [ ] **Step 3: 验证函数返回含字段**（直接调 DAL，不依赖 HTTP 服务/端口）

Run: `python3 -c "from dal.db_manager import get_actor_with_videos as f; d=f(1); vs=(d or {}).get('videos'); print(bool(vs) and 'file_mtime' in vs[0])"`
Expected: 打印 `True`（若 id=1 无视频，换一个有视频的 actor id）。

- [ ] **Step 4: 提交**

```bash
git add dal/db_manager.py
git commit -m "feat: include file_mtime in actor/tag detail video payloads"
```

---

## Task 3: 前端 SortBar 按钮 + sortedVideos mtime 分支

**Files:**
- Modify: `web/templates/index.html`（`#tpl-sort-bar` 约 1695 行；两处 `sortedVideos` 约 3116、3206 行）

- [ ] **Step 1: SortBar 模板加「文件时间」按钮**

在 `#tpl-sort-bar` 里「评分」按钮之后（`</button>` 与 `</div>` 之间）插入：

```html
    <button @click="$emit('change', 'mtime')"
      :class="['px-2.5 py-1 rounded text-xs transition', sort === 'mtime' ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700']">
      文件时间
    </button>
```

- [ ] **Step 2: 第一处 sortedVideos 加 mtime 分支（带 selectedTags 的那处，约 3116）**

把：

```javascript
          if (sort.value === 'score') return arr.sort((a, b) => (b.score || 0) - (a.score || 0));
          return arr.sort((a, b) => (b.release_date || '').localeCompare(a.release_date || ''));
```

改为：

```javascript
          if (sort.value === 'score') return arr.sort((a, b) => (b.score || 0) - (a.score || 0));
          if (sort.value === 'mtime') return arr.sort((a, b) => (b.file_mtime || '').localeCompare(a.file_mtime || ''));
          return arr.sort((a, b) => (b.release_date || '').localeCompare(a.release_date || ''));
```

- [ ] **Step 3: 第二处 sortedVideos 加 mtime 分支（约 3206）**

把该处相同的两行：

```javascript
          if (sort.value === 'score') return arr.sort((a, b) => (b.score || 0) - (a.score || 0));
          return arr.sort((a, b) => (b.release_date || '').localeCompare(a.release_date || ''));
```

改为：

```javascript
          if (sort.value === 'score') return arr.sort((a, b) => (b.score || 0) - (a.score || 0));
          if (sort.value === 'mtime') return arr.sort((a, b) => (b.file_mtime || '').localeCompare(a.file_mtime || ''));
          return arr.sort((a, b) => (b.release_date || '').localeCompare(a.release_date || ''));
```

> 注意：Step 2 和 Step 3 的两行原文完全相同，Edit 时用各自函数上下文（selectedTags 的那处 vs `/api/tags/` 那处）区分，或用 `replace_all` 一次替换两处（两处目标一致，可行）。

- [ ] **Step 4: 手工验证**

在浏览器打开主列表（`#/videos`），点排序栏的「文件时间」，确认：
1. 按钮高亮切换正常。
2. 列表顺序变化（服务端 `sort=mtime` 生效，最新修改在前）。
3. URL 带 `?sort=mtime`。
再打开任一演员/标签详情页，点「文件时间」，确认本地排序也按修改时间。

- [ ] **Step 5: 提交**

```bash
git add web/templates/index.html
git commit -m "feat: add file-mtime sort button and client-side sort branch"
```

---

## Task 4: DAL `rename_video_file`

**Files:**
- Modify: `dal/db_manager.py`（在 `replace_video_file` 之后新增函数）
- Test: `tests/test_replace_video.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_replace_video.py` 追加（`ReplaceVideoFileTests` 已建好含 `code`/`original_file_path` 列的临时表，这里复用同样的建表模式，新开一个 TestCase）：

```python
from dal.db_manager import rename_video_file


class RenameVideoFileTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                part TEXT,
                original_file_path TEXT,
                updated_at TIMESTAMP
            );
        """)
        conn.execute(
            "INSERT INTO videos (code, part, original_file_path) "
            "VALUES ('ABP-119-C', NULL, '/vol/ABP-119-C.old.mp4')"
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_updates_path_and_code(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            result = rename_video_file('ABP-119-C', None, '/vol/ABP-119-UC.桃谷绘里香.[标题].mp4', 'ABP-119-UC')
        self.assertTrue(result['ok'])
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT code, original_file_path FROM videos WHERE id=1").fetchone()
        conn.close()
        self.assertEqual(row[0], 'ABP-119-UC')
        self.assertEqual(row[1], '/vol/ABP-119-UC.桃谷绘里香.[标题].mp4')

    def test_unknown_code_returns_error(self):
        with patch('dal.db_manager.DB_PATH', self.db_path):
            result = rename_video_file('NOSUCH', None, '/vol/x.mp4', 'NOSUCH')
        self.assertFalse(result['ok'])
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_replace_video.py::RenameVideoFileTests -v`
Expected: FAIL —`ImportError`/`AttributeError`（`rename_video_file` 未定义）。

- [ ] **Step 3: 实现**

在 `dal/db_manager.py` 的 `replace_video_file` 函数之后新增：

```python
def rename_video_file(
    code: str,
    part: Optional[str],
    new_path: str,
    new_code: str,
) -> Dict[str, Any]:
    """Point a video record at a renamed file: update original_file_path and code.

    Physical file rename and filename assembly happen in the app layer; this only
    touches the DB row identified by (code, part).
    """
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        resolved_code = _resolve_video_code(cursor, code)
        if not resolved_code:
            return {'ok': False, 'error': 'not found'}

        if part:
            cursor.execute(
                'SELECT id FROM videos WHERE code = ? AND part = ? ORDER BY id ASC LIMIT 1',
                (resolved_code, part),
            )
        else:
            cursor.execute(
                'SELECT id FROM videos WHERE code = ? AND (part IS NULL OR part = "") ORDER BY id ASC LIMIT 1',
                (resolved_code,),
            )
        row = cursor.fetchone()
        if not row:
            return {'ok': False, 'error': 'part not found' if part else 'not found'}

        cursor.execute(
            'UPDATE videos SET original_file_path = ?, code = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
            (new_path, new_code, row['id']),
        )
        conn.commit()
        logger.info(f'✏️  rename_video_file [{code} -> {new_code}]: {new_path}')
        return {'ok': True}
    except sqlite3.Error as e:
        logger.error(f'❌ rename_video_file 失败 [{code}]: {e}')
        return {'ok': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python -m pytest tests/test_replace_video.py::RenameVideoFileTests -v`
Expected: PASS（2 个）。

- [ ] **Step 5: 提交**

```bash
git add dal/db_manager.py tests/test_replace_video.py
git commit -m "feat: add rename_video_file DAL to update path and code"
```

---

## Task 5: app.py rename-file 端点

**Files:**
- Modify: `web/backend/app.py`（import 段第 20-24 行；在 replace 端点之后新增路由）
- Test: `tests/test_replace_video.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_replace_video.py` 追加一个集成测试。它用 Flask test client，patch 掉 DB 层和 `get_video_by_code`，在真实临时目录里放一个假文件验证物理改名：

```python
import tempfile as _tf
from unittest.mock import patch as _patch
from web.backend.app import app as flask_app


class RenameFileEndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = flask_app.test_client()
        self.dir = _tf.mkdtemp()
        self.old = Path(self.dir) / 'ABP-119-C.old.mp4'
        self.old.write_bytes(b'x')

    def tearDown(self):
        import shutil
        shutil.rmtree(self.dir, ignore_errors=True)

    def _post(self, body):
        return self.client.post('/api/videos/ABP-119-C/rename-file', json=body)

    def test_renames_file_and_updates_db(self):
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)), \
             _patch('web.backend.app.rename_video_file', return_value={'ok': True}) as m_ren, \
             _patch('web.backend.app.get_video_by_code', return_value={'code': 'ABP-119-UC'}):
            resp = self._post({'part': None, 'code': 'ABP-119-UC', 'actors': ['桃谷绘里香'], 'title': '标题'})
        self.assertEqual(resp.status_code, 200)
        new = Path(self.dir) / 'ABP-119-UC.桃谷绘里香.[标题].mp4'
        self.assertTrue(new.exists())
        self.assertFalse(self.old.exists())
        m_ren.assert_called_once()

    def test_target_exists_returns_409(self):
        (Path(self.dir) / 'ABP-119-UC.[标题].mp4').write_bytes(b'y')
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)):
            resp = self._post({'part': None, 'code': 'ABP-119-UC', 'actors': [], 'title': '标题'})
        self.assertEqual(resp.status_code, 409)
        self.assertTrue(self.old.exists())

    def test_empty_code_returns_400(self):
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)):
            resp = self._post({'part': None, 'code': '   ', 'actors': [], 'title': 't'})
        self.assertEqual(resp.status_code, 400)

    def test_source_missing_returns_400(self):
        self.old.unlink()
        with _patch('web.backend.app.get_video_file_path', return_value=str(self.old)):
            resp = self._post({'part': None, 'code': 'ABP-119-UC', 'actors': [], 'title': 't'})
        self.assertEqual(resp.status_code, 400)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_replace_video.py::RenameFileEndpointTests -v`
Expected: FAIL —端点不存在，返回 404。

- [ ] **Step 3: 加 import**

把 `web/backend/app.py` 第 20-24 行的 import 块：

```python
from utils.rename_and_move_videos import (
    VideoRecord,
    build_destination,
    build_filename,
)
```

改为：

```python
from utils.rename_and_move_videos import (
    VideoRecord,
    build_destination,
    build_filename,
    sanitize_component,
)
```

并在 `from dal.db_manager import (` 块里加入 `rename_video_file,`（与 `replace_video_file,` 相邻）。

- [ ] **Step 4: 实现端点**

在 `web/backend/app.py` 的 replace 端点（`api_video_replace`，约 931 行结束）之后新增：

```python
@app.post('/api/videos/<string:code>/rename-file')
def api_video_rename_file(code: str):
    body = request.get_json(silent=True) or {}
    part = (body.get('part') or '').strip() or None
    new_code = sanitize_component(body.get('code') or '')
    if not new_code:
        return jsonify({'error': 'invalid code'}), 400
    actors = [a for a in (sanitize_component(x) for x in (body.get('actors') or [])) if a]
    title = sanitize_component(body.get('title') or '')

    orig = get_video_file_path(code, part)
    if not orig:
        return jsonify({'error': 'not found'}), 404
    old_path = Path(orig)
    ext = old_path.suffix

    components = [new_code, *actors]
    if title:
        components.append(f'[{title}]')
    if part:
        components.append(part)
    new_name = '.'.join(c for c in components if c) + ext
    new_path = old_path.parent / new_name

    if new_path == old_path:
        return jsonify({'ok': True, **get_video_by_code(code)})
    if not old_path.exists():
        return jsonify({'error': 'source file missing'}), 400
    if new_path.exists():
        return jsonify({'error': 'target exists'}), 409

    try:
        old_path.rename(new_path)
    except OSError as e:
        return jsonify({'error': str(e)}), 500

    result = rename_video_file(code, part, str(new_path), new_code)
    if not result['ok']:
        try:
            new_path.rename(old_path)
        except OSError:
            pass
        return jsonify({'error': result.get('error', 'db update failed')}), 500

    return jsonify({'ok': True, **get_video_by_code(new_code)})
```

- [ ] **Step 5: 跑测试确认通过**

Run: `python -m pytest tests/test_replace_video.py::RenameFileEndpointTests -v`
Expected: PASS（4 个）。

- [ ] **Step 6: 跑整个测试文件确保无回归**

Run: `python -m pytest tests/test_replace_video.py -q`
Expected: 全部 PASS。

- [ ] **Step 7: 提交**

```bash
git add web/backend/app.py tests/test_replace_video.py
git commit -m "feat: add rename-file endpoint (compose filename, rename, sync db)"
```

---

## Task 6: 前端详情弹窗分字段改名 UI

**Files:**
- Modify: `web/templates/index.html`（`tpl-video-detail` 的「本地文件」区块约 1259-1273；`VideoDetail` setup 约 2646-3070 与其 `return {...}`）

- [ ] **Step 1: 在 VideoDetail setup 里加状态与方法**

在 `VideoDetail` 组件 setup 内（`const fileParts = computed(...)` 附近、`return {` 之前）加入：

```javascript
        const renamingPath = ref(null);
        const renameFields = ref({ code: '', actors: '', title: '' });

        function parseFilename(path) {
          const name = path.split('/').pop();
          const dot = name.lastIndexOf('.');
          const ext = dot > 0 ? name.slice(dot) : '';
          const segs = (dot > 0 ? name.slice(0, dot) : name).split('.');
          let part = '';
          if (segs.length && /^part\w+$/i.test(segs[segs.length - 1])) part = segs.pop();
          const code = segs.shift() || '';
          let title = '';
          if (segs.length && /^\[.*\]$/.test(segs[segs.length - 1])) {
            title = segs.pop().replace(/^\[|\]$/g, '');
          }
          return { code, actors: segs.join('.'), title, part, ext };
        }

        function startRename(f) {
          renamingPath.value = f.path;
          const parsed = parseFilename(f.path);
          renameFields.value = { code: parsed.code, actors: parsed.actors, title: parsed.title };
        }

        function cancelRename() { renamingPath.value = null; }

        async function submitRename(f) {
          const fields = renameFields.value;
          const actors = (fields.actors || '').split('.').map(s => s.trim()).filter(Boolean);
          const body = { part: f.part || null, code: fields.code, actors, title: fields.title };
          const res = await apiFetch('/api/videos/' + video.value.code + '/rename-file', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          });
          renamingPath.value = null;
          const newCode = res.code || video.value.code;
          if (newCode !== route.params.code) {
            router.replace('/video/' + encodeURIComponent(newCode));
          } else {
            video.value = res;
          }
        }
```

- [ ] **Step 2: 把这些暴露到模板**

在 `VideoDetail` 的 `return { ... }` 里追加：

```javascript
          renamingPath, renameFields, startRename, cancelRename, submitRename,
```

- [ ] **Step 3: 在「本地文件」区块加改名入口与内联表单**

把 `tpl-video-detail` 里的文件行（约 1261-1272）：

```html
            <div v-for="(f, i) in fileParts" :key="i"
              class="bg-gray-800 rounded px-3 py-2 mb-1">
              <div class="flex items-center justify-between gap-2 mb-0.5">
                <span v-if="f.part" class="text-xs text-indigo-400 font-mono flex-shrink-0">{{ f.part }}</span>
                <button @click="copyPath(f.path)"
                  class="text-xs flex-shrink-0 px-2 py-0.5 rounded transition"
                  :class="copiedPaths[f.path] ? 'bg-green-700 text-white' : 'bg-gray-700 hover:bg-gray-600 text-gray-300'">
                  {{ copiedPaths[f.path] ? '已复制 ✓' : '复制' }}
                </button>
              </div>
              <span class="text-xs text-gray-400 font-mono break-all leading-relaxed">{{ f.path }}</span>
            </div>
```

改为：

```html
            <div v-for="(f, i) in fileParts" :key="i"
              class="bg-gray-800 rounded px-3 py-2 mb-1">
              <div class="flex items-center justify-between gap-2 mb-0.5">
                <span v-if="f.part" class="text-xs text-indigo-400 font-mono flex-shrink-0">{{ f.part }}</span>
                <div class="flex items-center gap-1 ml-auto">
                  <button @click="startRename(f)"
                    class="text-xs flex-shrink-0 px-2 py-0.5 rounded transition bg-gray-700 hover:bg-gray-600 text-gray-300">
                    改名
                  </button>
                  <button @click="copyPath(f.path)"
                    class="text-xs flex-shrink-0 px-2 py-0.5 rounded transition"
                    :class="copiedPaths[f.path] ? 'bg-green-700 text-white' : 'bg-gray-700 hover:bg-gray-600 text-gray-300'">
                    {{ copiedPaths[f.path] ? '已复制 ✓' : '复制' }}
                  </button>
                </div>
              </div>
              <span class="text-xs text-gray-400 font-mono break-all leading-relaxed">{{ f.path }}</span>

              <div v-if="renamingPath === f.path" class="mt-2 space-y-1.5 border-t border-gray-700 pt-2">
                <div class="flex items-center gap-2">
                  <span class="text-xs text-gray-500 w-10 flex-shrink-0">番号</span>
                  <input v-model="renameFields.code" placeholder="ABP-119-UC"
                    class="flex-1 bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-indigo-500" />
                </div>
                <div class="flex items-center gap-2">
                  <span class="text-xs text-gray-500 w-10 flex-shrink-0">女优</span>
                  <input v-model="renameFields.actors" placeholder="多个用 . 分隔"
                    class="flex-1 bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-indigo-500" />
                </div>
                <div class="flex items-center gap-2">
                  <span class="text-xs text-gray-500 w-10 flex-shrink-0">标题</span>
                  <input v-model="renameFields.title" placeholder="标题（不含方括号）"
                    class="flex-1 bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:outline-none focus:border-indigo-500" />
                </div>
                <div class="flex justify-end gap-2 pt-1">
                  <button @click="cancelRename"
                    class="text-xs px-2 py-1 rounded bg-gray-700 hover:bg-gray-600 text-gray-300">取消</button>
                  <button @click="submitRename(f)"
                    class="text-xs px-2 py-1 rounded bg-indigo-600 hover:bg-indigo-500 text-white">重命名</button>
                </div>
              </div>
            </div>
```

- [ ] **Step 4: 手工验证**

确保本地服务在跑。浏览器打开任一视频详情：
1. 「本地文件」每个文件行出现「改名」按钮。
2. 点「改名」，三字段被当前文件名正确预填（番号=第一段、女优=中间段点连、标题=方括号内）。
3. 改标题后点「重命名」：弹窗刷新，`本地文件`路径变成新文件名；到磁盘确认文件已被重命名（`ls` 对应目录）。
4. 改番号后确认列表/详情用新番号（URL 更新到 `#/video/新番号`）。
5. 改成一个已存在的文件名时，请求返回 409、原文件不动（可在 Network 面板看状态码）。

- [ ] **Step 5: 提交**

```bash
git add web/templates/index.html
git commit -m "feat: field-based file rename UI in video detail modal"
```

---

## 收尾

- [ ] **全量测试**

Run: `python -m pytest tests/ -q`
Expected: 全部 PASS。

- [ ] **确认两个功能端到端可用**（按 Task 3 Step 4 与 Task 6 Step 4 的手工清单）。
