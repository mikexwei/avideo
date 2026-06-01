# 批量添加/替换 + 手动添加 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two new buttons to the home page — "⚡ 批量添加/替换" (smart batch that auto-detects add vs replace per filename) and "手动添加" (shortcut to existing manual flow) — without modifying any existing endpoints or modals.

**Architecture:** A single new backend endpoint `POST /api/videos/batch-check` queries the DB by code and returns existence info for a list of filenames. A new `BatchModal` Vue component in `index.html` drives a 3-step flow: select files → review (table with new/replace auto-detected, tag toggles for replace rows, volume picker for new rows) → sequential progress (new groups via existing `add-auto`+SSE pipeline, replace items via existing `/replace` XHR). The `AddVideoModal` gets an `initialStep` prop so "手动添加" can jump straight to `manual-code`.

**Tech Stack:** Flask (Python), SQLite, Vue 3 Composition API (CDN, no build step), Tailwind CSS (CDN), `EventSource` for SSE, `fetch` for XHR uploads.

---

## File Structure

| File | Change |
|------|--------|
| `web/backend/app.py` | Add `POST /api/videos/batch-check` endpoint (≈40 lines) |
| `web/templates/index.html` | Add `tpl-batch-modal` template; add `BatchModal` component; promote `AUTO_STEPS` + `autoStepLabel` to module level; add `initialStep` prop to `AddVideoModal`; add two buttons and wiring to `HomeView` |
| `tests/test_batch_api.py` | New test file for the batch-check endpoint |

---

## Task 1: Backend — `POST /api/videos/batch-check`

**Files:**
- Modify: `web/backend/app.py` (after the `api_media_libraries` endpoint, around line 328)
- Create: `tests/test_batch_api.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_batch_api.py`:

```python
import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from web.backend.app import app


def _make_db(rows=None):
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            part TEXT,
            original_file_path TEXT UNIQUE
        );
    """)
    for row in (rows or []):
        conn.execute(
            'INSERT INTO videos (code, part, original_file_path) VALUES (?, ?, ?)',
            row,
        )
    conn.commit()
    conn.close()
    return path


class BatchCheckEndpointTests(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def _post(self, filenames, db):
        with patch('web.backend.app.DB_PATH', db):
            return self.client.post(
                '/api/videos/batch-check',
                data=json.dumps({'filenames': filenames}),
                content_type='application/json',
            )

    def test_existing_code_returns_exists_true(self):
        db = _make_db([('ABW-123-C', None, '/vol/ABW-123-C/ABW-123-C.mp4')])
        resp = self._post(['ABW-123-C.mp4'], db)
        os.unlink(db)
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(len(data), 1)
        self.assertTrue(data[0]['exists'])
        self.assertEqual(data[0]['code'], 'ABW-123-C')
        self.assertIsNone(data[0]['part'])
        self.assertEqual(data[0]['original_path'], '/vol/ABW-123-C/ABW-123-C.mp4')

    def test_non_existing_code_returns_exists_false(self):
        db = _make_db()
        resp = self._post(['SSIS-999.mkv'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertFalse(data[0]['exists'])
        self.assertEqual(data[0]['code'], 'SSIS-999')
        self.assertIsNone(data[0]['original_path'])
        self.assertEqual(data[0]['db_parts'], [])

    def test_part_extracted_from_filename(self):
        db = _make_db()
        resp = self._post(['WEST-042-part1.mp4'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertEqual(data[0]['code'], 'WEST-042')
        self.assertEqual(data[0]['part'], 'part1')
        self.assertFalse(data[0]['exists'])

    def test_db_parts_returned_for_multipart_video(self):
        db = _make_db([
            ('WEST-010', 'part1', '/vol/WEST-010/a.mp4'),
            ('WEST-010', 'part2', '/vol/WEST-010/b.mp4'),
        ])
        resp = self._post(['WEST-010.mkv'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertTrue(data[0]['exists'])
        self.assertIn('part1', data[0]['db_parts'])
        self.assertIn('part2', data[0]['db_parts'])

    def test_unparseable_filename_returns_null_exists_and_error(self):
        db = _make_db()
        resp = self._post(['random_file_no_code.mp4'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertIsNone(data[0]['exists'])
        self.assertIsNone(data[0]['code'])
        self.assertIn('error', data[0])

    def test_empty_list_returns_empty_array(self):
        db = _make_db()
        resp = self._post([], db)
        os.unlink(db)
        self.assertEqual(resp.get_json(), [])

    def test_multiple_filenames_processed_correctly(self):
        db = _make_db([('ABW-123', None, '/vol/ABW-123/ABW-123.mp4')])
        resp = self._post(['ABW-123.mp4', 'NEW-999.mp4'], db)
        os.unlink(db)
        data = resp.get_json()
        self.assertEqual(len(data), 2)
        self.assertTrue(data[0]['exists'])
        self.assertFalse(data[1]['exists'])

    def test_non_list_filenames_returns_400(self):
        resp = self.client.post(
            '/api/videos/batch-check',
            data=json.dumps({'filenames': 'not-a-list'}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 400)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_batch_api.py -v 2>&1 | head -30
```

Expected: all 8 tests FAIL with `404` or `AttributeError` (endpoint not yet defined).

- [ ] **Step 3: Implement the endpoint**

In `web/backend/app.py`, insert after the `api_media_libraries` function (after line 328):

```python
@app.post('/api/videos/batch-check')
def api_batch_check():
    data = request.get_json(silent=True) or {}
    filenames = data.get('filenames', [])
    if not isinstance(filenames, list):
        return jsonify({'error': 'filenames must be a list'}), 400

    results = []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        for filename in filenames:
            stem = Path(filename).stem
            code, part = extract_video_code(stem)
            if not code:
                results.append({
                    'filename': filename, 'code': None, 'part': part,
                    'exists': None, 'original_path': None, 'db_parts': [],
                    'error': '无法解析番号',
                })
                continue
            rows = conn.execute(
                'SELECT original_file_path, part FROM videos WHERE code = ?', (code,)
            ).fetchall()
            if rows:
                results.append({
                    'filename': filename, 'code': code, 'part': part,
                    'exists': True,
                    'original_path': rows[0]['original_file_path'],
                    'db_parts': [r['part'] for r in rows],
                })
            else:
                results.append({
                    'filename': filename, 'code': code, 'part': part,
                    'exists': False, 'original_path': None, 'db_parts': [],
                })
    finally:
        conn.close()

    return jsonify(results)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_batch_api.py -v
```

Expected: 8/8 PASS.

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
python -m pytest tests/ -v 2>&1 | tail -20
```

Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add tests/test_batch_api.py web/backend/app.py
git commit -m "feat: add POST /api/videos/batch-check endpoint"
```

---

## Task 2: AddVideoModal `initialStep` prop + 手动添加 button

**Files:**
- Modify: `web/templates/index.html`

**Context:** `AddVideoModal` is defined starting at line ~1416. `HomeView` is at line ~1638. The `tpl-home` template button area is around line 273–282.

- [ ] **Step 1: Add `initialStep` prop to `AddVideoModal`**

Find:
```javascript
    const AddVideoModal = {
      template: '#tpl-add-video',
      emits: ['close'],
      setup(props, { emit }) {
```

Replace with:
```javascript
    const AddVideoModal = {
      template: '#tpl-add-video',
      props: { initialStep: { type: String, default: 'file-select' } },
      emits: ['close'],
      setup(props, { emit }) {
```

Then find inside `AddVideoModal.setup()`:
```javascript
        const addStep    = ref('file-select');
```

Replace with:
```javascript
        const addStep    = ref(props.initialStep);
```

- [ ] **Step 2: Add `showManualModal` state to `HomeView.setup()`**

Find in `HomeView.setup()`:
```javascript
        const showAddModal = ref(false);
        function openAddModal() { showAddModal.value = true; }
        function closeAddModal() { showAddModal.value = false; }
```

Replace with:
```javascript
        const showAddModal = ref(false);
        function openAddModal() { showAddModal.value = true; }
        function closeAddModal() { showAddModal.value = false; }

        const showManualModal = ref(false);
        function openManualModal() { showManualModal.value = true; }
        function closeManualModal() { showManualModal.value = false; }
```

- [ ] **Step 3: Add to HomeView return**

Find:
```javascript
          showAddModal, openAddModal, closeAddModal,
```

Replace with:
```javascript
          showAddModal, openAddModal, closeAddModal,
          showManualModal, openManualModal, closeManualModal,
```

- [ ] **Step 4: Add 手动添加 button and modal instance to `tpl-home`**

Find in `tpl-home`:
```html
          <button @click="openAddModal"
            class="px-3 py-1.5 text-sm rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white font-medium flex-shrink-0">
            + 添加影片
          </button>
```

Replace with:
```html
          <button @click="openManualModal"
            class="px-3 py-1.5 text-sm rounded-lg bg-gray-700 hover:bg-gray-600 text-gray-200 font-medium flex-shrink-0">
            手动添加
          </button>
          <button @click="openAddModal"
            class="px-3 py-1.5 text-sm rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white font-medium flex-shrink-0">
            + 添加影片
          </button>
```

Find in `tpl-home`:
```html
      <add-video-modal v-if="showAddModal" @close="closeAddModal" />
```

Replace with:
```html
      <add-video-modal v-if="showAddModal" @close="closeAddModal" />
      <add-video-modal v-if="showManualModal" :initial-step="'manual-code'" @close="closeManualModal" />
```

- [ ] **Step 5: Verify in browser**

Start server (`python web/backend/app.py` or however the project runs), open `http://10.0.0.30:8000/#/videos`. Confirm:
- "手动添加" button appears to the left of "添加影片"
- Clicking "手动添加" opens the modal directly at the manual-code step (番号设置), not the file-select step
- Clicking "添加影片" still opens the normal full modal flow

- [ ] **Step 6: Commit**

```bash
git add web/templates/index.html
git commit -m "feat: add 手动添加 button with direct manual-code entry"
```

---

## Task 3: BatchModal skeleton + HomeView wiring

**Files:**
- Modify: `web/templates/index.html`

**Context:** Module-level constants `AUTO_STEPS` and `autoStepLabel` are currently defined inside `AddVideoModal.setup()` (lines ~1434–1455). Promote them to module level so `BatchModal` can also use them. The new `tpl-batch-modal` template block goes before `<!-- ========== AddVideoModal ==========-->` (line ~290). The `BatchModal` JS component definition goes between the `queryTags` function and the `AddVideoModal` definition (around line 1414).

- [ ] **Step 1: Promote `AUTO_STEPS` and `autoStepLabel` to module level**

Find in `AddVideoModal.setup()`:
```javascript
        // ── Auto mode state ─────────────────────────────────────
        const AUTO_STEPS = [
          'parse_code','calc_dir','upload','db_insert','tags',
          'scrape','actors','translate','short_title',
          'calc_filename','rename','db_update',
        ];
        const autoProgressSteps = ref([]);
        const autoFinished = ref(false);

        const autoProgressPct = computed(() => {
          const done = autoProgressSteps.value.filter(s => s.status === 'done').length;
          return Math.round((done / AUTO_STEPS.length) * 100);
        });

        function autoStepLabel(step) {
          const map = {
            parse_code: '解析番号', calc_dir: '计算目标目录', upload: '上传文件',
            db_insert: '写入数据库', tags: '同步标签', scrape: '刮削影片信息',
            actors: '获取演员信息', translate: '翻译标题', short_title: '计算短标题',
            calc_filename: '计算文件名', rename: '重命名文件', db_update: '更新数据库',
          };
          return map[step] || step;
        }
```

Replace with (removing `AUTO_STEPS` and `autoStepLabel` from inside setup, they move to module level):
```javascript
        // ── Auto mode state ─────────────────────────────────────
        const autoProgressSteps = ref([]);
        const autoFinished = ref(false);

        const autoProgressPct = computed(() => {
          const done = autoProgressSteps.value.filter(s => s.status === 'done').length;
          return Math.round((done / AUTO_STEPS.length) * 100);
        });
```

Also find in `AddVideoModal.setup()` return:
```javascript
          autoProgressSteps, autoProgressPct, autoFinished, autoStepLabel,
```

Replace with:
```javascript
          autoProgressSteps, autoProgressPct, autoFinished,
          autoStepLabel,
```

(Note: `autoStepLabel` is now a module-level function, but it still needs to be in the return so the template can call it. Keep it in the return but it now references the module-level function.)

Then find (in the main script, before `AddVideoModal` definition, i.e., after `function queryTags` at line ~1413):

```javascript
    // ── AddVideoModal ─────────────────────────────────────────
```

Insert before this line:
```javascript
    // ── Shared auto-pipeline constants ───────────────────────
    const AUTO_STEPS = [
      'parse_code','calc_dir','upload','db_insert','tags',
      'scrape','actors','translate','short_title',
      'calc_filename','rename','db_update',
    ];
    function autoStepLabel(step) {
      const map = {
        parse_code: '解析番号', calc_dir: '计算目标目录', upload: '上传文件',
        db_insert: '写入数据库', tags: '同步标签', scrape: '刮削影片信息',
        actors: '获取演员信息', translate: '翻译标题', short_title: '计算短标题',
        calc_filename: '计算文件名', rename: '重命名文件', db_update: '更新数据库',
      };
      return map[step] || step;
    }

```

- [ ] **Step 2: Add `tpl-batch-modal` template**

Find in `index.html`:
```html
  <!-- ========== AddVideoModal ========== -->
  <script type="text/x-template" id="tpl-add-video">
```

Insert before it:
```html
  <!-- ========== BatchModal ========== -->
  <script type="text/x-template" id="tpl-batch-modal">
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-80" @click.self="!batchProcessing && $emit('close')">
    <div class="bg-gray-900 rounded-xl w-full max-w-xl mx-4 shadow-2xl" style="max-height:90vh;overflow-y:auto">
      <div class="flex items-center justify-between px-5 py-4 border-b border-gray-800">
        <span class="font-semibold text-white">⚡ 批量添加/替换</span>
        <div class="flex items-center gap-3">
          <span class="text-xs text-gray-500" v-if="batchStep !== 'batch-progress' || batchProcessing">
            步骤 {{ batchStepNum }} / 3
          </span>
          <button @click="$emit('close')" :disabled="batchProcessing"
            class="text-gray-500 hover:text-gray-300 disabled:opacity-30 text-lg leading-none">✕</button>
        </div>
      </div>

      <!-- Step 1: batch-select -->
      <template v-if="batchStep === 'batch-select'">
        <!-- content added in Task 4 -->
      </template>

      <!-- Step 2: batch-review -->
      <template v-if="batchStep === 'batch-review'">
        <!-- content added in Task 4 -->
      </template>

      <!-- Step 3: batch-progress -->
      <template v-if="batchStep === 'batch-progress'">
        <!-- content added in Tasks 5 & 6 -->
      </template>

    </div>
  </div>
  </script>

```

- [ ] **Step 3: Add `BatchModal` JS component definition**

Find:
```javascript
    // ── AddVideoModal ─────────────────────────────────────────
    const AddVideoModal = {
```

Insert before it:
```javascript
    // ── BatchModal ────────────────────────────────────────────
    const BatchModal = {
      template: '#tpl-batch-modal',
      emits: ['close'],
      setup(props, { emit }) {
        const batchStep       = ref('batch-select');
        const batchFiles      = ref([]);
        const batchChecking   = ref(false);
        const batchCheckError = ref('');
        const batchGroups     = ref([]);
        const batchVolume     = ref('');
        const mediaLibraries  = ref([]);
        const batchProcessing = ref(false);
        const batchResults    = ref([]);

        const batchStepNum = computed(() =>
          ({ 'batch-select': 1, 'batch-review': 2, 'batch-progress': 3 }[batchStep.value] || 1)
        );
        const hasNewGroups  = computed(() => batchGroups.value.some(g => !g.exists && !g.warning));
        const hasWarnings   = computed(() => batchGroups.value.some(g => g.warning));
        const canStartUpload = computed(() =>
          !hasWarnings.value && batchGroups.value.length > 0 &&
          (!hasNewGroups.value || batchVolume.value)
        );

        onMounted(async () => {
          const libs = await apiFetch('/api/config/media-libraries');
          if (libs) mediaLibraries.value = libs;
        });

        return {
          batchStep, batchFiles, batchChecking, batchCheckError,
          batchGroups, batchVolume, mediaLibraries,
          batchProcessing, batchResults,
          batchStepNum, hasNewGroups, hasWarnings, canStartUpload,
          autoStepLabel,
        };
      }
    };

```

- [ ] **Step 4: Register `BatchModal` in `HomeView.components` and add button + state**

Find:
```javascript
      components: { VideoCard, Pagination, SortBar, AddVideoModal },
```

Replace with:
```javascript
      components: { VideoCard, Pagination, SortBar, AddVideoModal, BatchModal },
```

Find in `HomeView.setup()`:
```javascript
        const showManualModal = ref(false);
        function openManualModal() { showManualModal.value = true; }
        function closeManualModal() { showManualModal.value = false; }
```

Replace with:
```javascript
        const showManualModal = ref(false);
        function openManualModal() { showManualModal.value = true; }
        function closeManualModal() { showManualModal.value = false; }

        const showBatchModal = ref(false);
        function openBatchModal() { showBatchModal.value = true; }
        function closeBatchModal() { showBatchModal.value = false; }
```

Find in HomeView return:
```javascript
          showManualModal, openManualModal, closeManualModal,
```

Replace with:
```javascript
          showManualModal, openManualModal, closeManualModal,
          showBatchModal, openBatchModal, closeBatchModal,
```

- [ ] **Step 5: Add "批量添加/替换" button and modal instance to `tpl-home`**

Find in `tpl-home`:
```html
          <button @click="openManualModal"
            class="px-3 py-1.5 text-sm rounded-lg bg-gray-700 hover:bg-gray-600 text-gray-200 font-medium flex-shrink-0">
            手动添加
          </button>
```

Replace with:
```html
          <button @click="openManualModal"
            class="px-3 py-1.5 text-sm rounded-lg bg-gray-700 hover:bg-gray-600 text-gray-200 font-medium flex-shrink-0">
            手动添加
          </button>
          <button @click="openBatchModal"
            class="px-3 py-1.5 text-sm rounded-lg bg-indigo-700 hover:bg-indigo-600 text-white font-medium flex-shrink-0">
            ⚡ 批量添加/替换
          </button>
```

Find in `tpl-home`:
```html
      <add-video-modal v-if="showManualModal" :initial-step="'manual-code'" @close="closeManualModal" />
```

Replace with:
```html
      <add-video-modal v-if="showManualModal" :initial-step="'manual-code'" @close="closeManualModal" />
      <batch-modal v-if="showBatchModal" @close="closeBatchModal" />
```

- [ ] **Step 6: Verify in browser**

Open `http://10.0.0.30:8000/#/videos`. Confirm:
- Three buttons visible: "手动添加", "⚡ 批量添加/替换", "+ 添加影片"
- Clicking "批量添加/替换" opens a modal with header "⚡ 批量添加/替换" and "步骤 1 / 3" indicator
- Modal body shows empty placeholder steps (no content yet — that's OK, added in Task 4)
- "手动添加" and "+ 添加影片" still work correctly

- [ ] **Step 7: Commit**

```bash
git add web/templates/index.html
git commit -m "feat: add BatchModal skeleton and 批量添加/替换 button"
```

---

## Task 4: BatchModal Steps 1 & 2 — UI + batch-check call

**Files:**
- Modify: `web/templates/index.html`

**Context:** Fill in the placeholder templates for `batch-select` and `batch-review` in `tpl-batch-modal`, and add the JS functions `onBatchFilesChange`, `runBatchCheck`, and `buildGroups` to `BatchModal.setup()`.

- [ ] **Step 1: Add Step 1 template (file-select)**

Find in `tpl-batch-modal`:
```html
      <!-- Step 1: batch-select -->
      <template v-if="batchStep === 'batch-select'">
        <!-- content added in Task 4 -->
      </template>
```

Replace with:
```html
      <!-- Step 1: batch-select -->
      <template v-if="batchStep === 'batch-select'">
        <div class="p-5 flex flex-col gap-4">
          <div>
            <div class="text-xs text-gray-500 uppercase tracking-wider mb-2">选择视频文件</div>
            <div class="flex flex-col gap-2">
              <div v-if="batchFiles.length" class="flex flex-col gap-1">
                <div v-for="(f, i) in batchFiles" :key="i"
                  class="flex items-center gap-2 px-3 py-1.5 bg-gray-800 rounded text-sm">
                  <span class="text-indigo-400 text-xs">🎬</span>
                  <span class="flex-1 truncate text-gray-300 font-mono text-xs">{{ f.name }}</span>
                </div>
              </div>
              <button @click="$refs.batchFileInput.click()"
                class="px-4 py-2.5 border border-dashed border-gray-700 rounded-lg text-sm text-gray-400 hover:border-gray-500 hover:text-gray-300 text-center transition-colors">
                {{ batchFiles.length ? '+ 添加更多文件' : '点击选择视频文件…' }}
              </button>
              <input ref="batchFileInput" type="file" class="hidden" multiple
                accept=".mp4,.mkv,.avi,.mov,.wmv,.m4v"
                @change="onBatchFilesChange" />
            </div>
          </div>
          <div v-if="batchCheckError" class="text-sm text-red-400 px-3 py-2 bg-red-900 bg-opacity-20 rounded-lg">
            {{ batchCheckError }}
          </div>
        </div>
        <div class="flex justify-end gap-3 px-5 py-4 border-t border-gray-800">
          <button @click="$emit('close')"
            class="px-4 py-2 text-sm rounded-lg bg-gray-800 hover:bg-gray-700 text-gray-300">取消</button>
          <button @click="runBatchCheck" :disabled="!batchFiles.length || batchChecking"
            class="px-4 py-2 text-sm rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-40">
            <span v-if="batchChecking">检测中…</span>
            <span v-else>下一步 →</span>
          </button>
        </div>
      </template>
```

- [ ] **Step 2: Add Step 2 template (review)**

Find in `tpl-batch-modal`:
```html
      <!-- Step 2: batch-review -->
      <template v-if="batchStep === 'batch-review'">
        <!-- content added in Task 4 -->
      </template>
```

Replace with:
```html
      <!-- Step 2: batch-review -->
      <template v-if="batchStep === 'batch-review'">
        <div class="p-5 flex flex-col gap-4">
          <div class="text-sm text-gray-400">
            检测到 {{ batchGroups.length }} 个番号 —
            <span class="text-green-400">{{ batchGroups.filter(g => !g.exists && !g.warning).length }} 新增</span>，
            <span class="text-yellow-400">{{ batchGroups.filter(g => g.exists && !g.warning).length }} 替换</span>
            <span v-if="batchGroups.filter(g => g.warning).length" class="text-orange-400">
              ，{{ batchGroups.filter(g => g.warning).length }} 需要处理
            </span>
          </div>

          <div class="overflow-x-auto">
            <table class="w-full text-sm border-collapse">
              <thead>
                <tr class="text-xs text-gray-500 uppercase tracking-wider border-b border-gray-800">
                  <th class="text-left py-2 px-2">番号</th>
                  <th class="text-left py-2 px-2">文件</th>
                  <th class="text-left py-2 px-2">操作</th>
                  <th class="text-left py-2 px-2">标签确认</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="g in batchGroups" :key="g.code || g.files[0].filename"
                  :class="['border-b border-gray-800', g.warning ? 'bg-orange-900 bg-opacity-10' : g.exists ? 'bg-yellow-900 bg-opacity-5' : '']">
                  <td class="py-2 px-2 font-mono text-indigo-400 text-xs whitespace-nowrap">{{ g.code || '?' }}</td>
                  <td class="py-2 px-2 max-w-xs">
                    <div v-for="f in g.files" :key="f.filename"
                      class="text-xs text-gray-400 truncate font-mono">{{ f.filename }}</div>
                    <div v-if="g.exists && g.original_path"
                      class="text-xs text-gray-600 truncate mt-0.5">→ {{ g.original_path }}</div>
                  </td>
                  <td class="py-2 px-2 whitespace-nowrap">
                    <span v-if="g.warning"
                      class="px-1.5 py-0.5 rounded text-xs font-bold bg-orange-900 bg-opacity-40 text-orange-400">⚠ 警告</span>
                    <template v-else-if="g.exists">
                      <span class="px-1.5 py-0.5 rounded text-xs font-bold bg-yellow-900 bg-opacity-40 text-yellow-400">替换</span>
                    </template>
                    <template v-else>
                      <span class="px-1.5 py-0.5 rounded text-xs font-bold bg-green-900 bg-opacity-40 text-green-400">新增</span>
                      <span v-if="g.files.length > 1"
                        class="ml-1 px-1.5 py-0.5 rounded text-xs font-bold bg-indigo-900 bg-opacity-40 text-indigo-400">
                        {{ g.files.length }} parts
                      </span>
                    </template>
                  </td>
                  <td class="py-2 px-2">
                    <div v-if="g.warning" class="text-xs text-orange-400 max-w-xs leading-snug">{{ g.warning }}</div>
                    <div v-else-if="!g.exists" class="text-xs text-gray-500">自动刮削</div>
                    <div v-else class="flex flex-col gap-1.5">
                      <label v-for="[key, label] in [['subtitle_zh','中文字幕'],['uncensored_leaked','无码破解'],['uncensored_native','原装无码']]"
                        :key="key" class="flex items-center gap-1.5 cursor-pointer">
                        <button @click="g[key] = !g[key]"
                          class="w-7 h-4 rounded-full transition-colors flex-shrink-0 relative"
                          :class="g[key] ? 'bg-indigo-600' : 'bg-gray-700'">
                          <span class="absolute top-0.5 w-3 h-3 bg-white rounded-full transition-all"
                            :class="g[key] ? 'left-3.5' : 'left-0.5'"></span>
                        </button>
                        <span class="text-xs text-gray-400">{{ label }}</span>
                      </label>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <!-- Volume picker — only for new files -->
          <div v-if="hasNewGroups" class="border-t border-gray-800 pt-4">
            <div class="text-xs text-gray-500 uppercase tracking-wider mb-2">新增文件存放位置</div>
            <div class="flex flex-col gap-2">
              <label v-for="v in mediaLibraries" :key="v.path"
                class="flex items-center gap-3 px-3 py-2.5 rounded-lg border cursor-pointer transition-colors"
                :class="batchVolume === v.path ? 'border-indigo-500 bg-indigo-950' : 'border-gray-700 bg-gray-800 hover:border-gray-600'">
                <input type="radio" :value="v.path" v-model="batchVolume" class="accent-indigo-500" />
                <span class="flex-1 text-sm text-gray-200 truncate">{{ v.path }}</span>
                <span v-if="v.free_gb !== null" class="text-xs flex-shrink-0"
                  :class="v.free_gb < 10 ? 'text-red-400' : 'text-gray-400'">
                  {{ v.free_gb }} GB 可用
                </span>
              </label>
            </div>
          </div>
          <div v-else class="text-xs text-gray-500 text-center py-1">
            替换文件将上传至原视频所在目录，无需选择 Volume
          </div>
        </div>
        <div class="flex justify-end gap-3 px-5 py-4 border-t border-gray-800">
          <button @click="batchStep = 'batch-select'"
            class="px-4 py-2 text-sm rounded-lg bg-gray-800 hover:bg-gray-700 text-gray-300">← 返回</button>
          <button @click="startBatchProcess" :disabled="!canStartUpload"
            class="px-4 py-2 text-sm rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-40">
            开始上传
          </button>
        </div>
      </template>
```

- [ ] **Step 3: Add JS functions to `BatchModal.setup()` return**

In `BatchModal.setup()`, find the `return {` block and add the new functions to it. First insert the function definitions before the `return`:

```javascript
        function onBatchFilesChange(e) {
          const newFiles = Array.from(e.target.files || []);
          const existingNames = new Set(batchFiles.value.map(f => f.name));
          batchFiles.value = [...batchFiles.value, ...newFiles.filter(f => !existingNames.has(f.name))];
        }

        async function runBatchCheck() {
          batchChecking.value = true;
          batchCheckError.value = '';
          try {
            const resp = await fetch('/api/videos/batch-check', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ filenames: batchFiles.value.map(f => f.name) }),
            });
            const data = await resp.json();
            if (!resp.ok) throw new Error(data.error || '检测失败');
            batchGroups.value = buildGroups(data);
            batchStep.value = 'batch-review';
          } catch (e) {
            batchCheckError.value = e.message;
          } finally {
            batchChecking.value = false;
          }
        }

        function buildGroups(items) {
          const map = {};
          for (const item of items) {
            const key = item.code || ('__err__' + item.filename);
            if (!map[key]) {
              map[key] = {
                code: item.code,
                files: [],
                exists: item.exists,
                original_path: item.original_path,
                db_parts: item.db_parts || [],
                subtitle_zh: /(?:-UC|-C)$/i.test(item.code || ''),
                uncensored_leaked: /(?:-UC|-U)$/i.test(item.code || ''),
                uncensored_native: false,
                warning: item.error || null,
              };
            }
            map[key].files.push(item);
            // mixed new+replace in same code → warn
            if (map[key].exists !== null && item.exists !== null && map[key].exists !== item.exists) {
              map[key].warning = '同一番号下新增与替换混合，请检查文件';
            }
          }
          // ambiguous part: replace row but can't tell which part to replace
          for (const g of Object.values(map)) {
            if (!g.warning && g.exists && g.db_parts.length > 1) {
              const allHavePart = g.files.every(f => f.part);
              if (!allHavePart) {
                g.warning = '无法确定 Part，请在文件名中加上 -partN（如 SSIS-456-part1.mp4）';
              }
            }
          }
          return Object.values(map);
        }
```

Then add these functions to the `return` statement:
```javascript
          onBatchFilesChange, runBatchCheck, buildGroups,
```

- [ ] **Step 4: Verify in browser**

Open the batch modal. In Step 1:
- Click file picker, select 2–3 video files with recognizable codes (e.g., `ABW-123.mp4`, `SSIS-456.mkv`)
- File list appears below the button
- Click "下一步" — shows "检测中…" briefly then transitions to Step 2
- Step 2 shows a table with each filename, detected code, and new/replace badge

Also test edge cases in Step 2:
- If a code exists in DB → row shows yellow "替换" badge with 3 tag toggles
- If no code exists → row shows green "新增" badge with "自动刮削" text
- Volume picker appears below table only when there are new files

- [ ] **Step 5: Commit**

```bash
git add web/templates/index.html
git commit -m "feat: BatchModal Steps 1 & 2 — file select and review table"
```

---

## Task 5: BatchModal Step 3 — new files (add-auto + SSE)

**Files:**
- Modify: `web/templates/index.html`

**Context:** Add the Step 3 template and the `startBatchProcess` / `processNewGroup` JS functions to `BatchModal.setup()`. Processing is sequential: each group is processed one at a time. New groups call `POST /api/videos/add-auto` then open an `EventSource` to stream progress.

- [ ] **Step 1: Add Step 3 template**

Find in `tpl-batch-modal`:
```html
      <!-- Step 3: batch-progress -->
      <template v-if="batchStep === 'batch-progress'">
        <!-- content added in Tasks 5 & 6 -->
      </template>
```

Replace with:
```html
      <!-- Step 3: batch-progress -->
      <template v-if="batchStep === 'batch-progress'">
        <div class="p-5 flex flex-col gap-3" style="max-height:65vh;overflow-y:auto">
          <div v-for="(r, i) in batchResults" :key="r.code || i"
            :class="['rounded-lg p-3 transition-colors', r.status === 'running' ? 'border border-indigo-600 bg-indigo-950 bg-opacity-50' : 'bg-gray-800']">
            <!-- Row header -->
            <div class="flex items-center gap-2 mb-1">
              <span class="font-mono text-indigo-400 text-xs">{{ r.code }}</span>
              <span :class="['text-xs px-1.5 py-0.5 rounded font-bold',
                r.type === 'new' ? 'bg-green-900 bg-opacity-40 text-green-400' : 'bg-yellow-900 bg-opacity-40 text-yellow-400']">
                {{ r.type === 'new' ? '新增' : '替换' }}
              </span>
              <span class="ml-auto text-xs" :class="{
                'text-gray-500': r.status === 'pending',
                'text-indigo-300': r.status === 'running',
                'text-green-400': r.status === 'done',
                'text-red-400': r.status === 'error',
              }">
                {{ {pending:'等待中', running:'处理中…', done:'✓ 完成', error:'✕ 失败'}[r.status] }}
              </span>
            </div>
            <!-- Replace: single message line -->
            <div v-if="r.type === 'replace' && r.message && r.status !== 'error'"
              class="text-xs text-gray-400 mt-1">{{ r.message }}</div>
            <!-- New: SSE step list -->
            <div v-if="r.type === 'new' && (r.status === 'running' || r.status === 'done')"
              class="mt-2 flex flex-col gap-0.5 pl-1">
              <div v-for="step in r.progressSteps" :key="step.step"
                class="flex items-start gap-2 text-xs">
                <span class="flex-shrink-0 mt-0.5 w-3 text-center">
                  <span v-if="step.status === 'done'" class="text-green-500">✓</span>
                  <span v-else-if="step.status === 'running'" class="text-indigo-400">⊙</span>
                  <span v-else-if="step.status === 'error'" class="text-red-400">✕</span>
                  <span v-else class="text-gray-700">○</span>
                </span>
                <span :class="step.status === 'done' ? 'text-gray-600 line-through' : step.status === 'running' ? 'text-gray-200' : step.status === 'error' ? 'text-red-400' : 'text-gray-700'">
                  {{ autoStepLabel(step.step) }}
                  <span v-if="step.message && step.status !== 'pending'"
                    class="ml-1 font-normal" style="text-decoration:none"
                    :class="step.status === 'done' ? 'text-gray-500' : step.status === 'running' ? 'text-indigo-300' : 'text-red-400'">
                    {{ step.message }}
                  </span>
                </span>
              </div>
            </div>
            <!-- Error -->
            <div v-if="r.status === 'error' && r.message"
              class="text-xs text-red-400 mt-1">{{ r.message }}</div>
          </div>
          <div class="text-xs text-gray-600 text-center pt-1">
            {{ batchResults.filter(r => r.status === 'done' || r.status === 'error').length }}
            / {{ batchResults.length }} 已处理
          </div>
        </div>
        <div class="flex justify-end gap-3 px-5 py-4 border-t border-gray-800">
          <button @click="$emit('close')" :disabled="batchProcessing"
            class="px-4 py-2 text-sm rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-40">
            完成
          </button>
        </div>
      </template>
```

- [ ] **Step 2: Add `startBatchProcess` and `processNewGroup` to `BatchModal.setup()`**

In `BatchModal.setup()`, before the `return`, add:

```javascript
        async function startBatchProcess() {
          batchStep.value = 'batch-progress';
          batchProcessing.value = true;
          batchResults.value = batchGroups.value.map(g => ({
            code: g.code,
            type: g.exists ? 'replace' : 'new',
            status: 'pending',
            message: '',
            progressSteps: g.exists ? [] : AUTO_STEPS.map(s => ({ step: s, status: 'pending', message: '' })),
          }));

          for (let i = 0; i < batchGroups.value.length; i++) {
            const group = batchGroups.value[i];
            const result = batchResults.value[i];
            result.status = 'running';
            try {
              if (!group.exists) {
                await processNewGroup(group, result);
              } else {
                await processReplaceGroup(group, result);
              }
              result.status = 'done';
            } catch (e) {
              result.status = 'error';
              result.message = e.message;
            }
          }
          batchProcessing.value = false;
        }

        async function processNewGroup(group, result) {
          const fd = new FormData();
          for (const fi of group.files) {
            const f = batchFiles.value.find(bf => bf.name === fi.filename);
            if (f) fd.append('files[]', f);
          }
          fd.append('volume_path', batchVolume.value);

          const res = await fetch('/api/videos/add-auto', { method: 'POST', body: fd });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || '上传失败');
          const jobId = data.job_id;

          await new Promise((resolve, reject) => {
            const es = new EventSource(`/api/jobs/${jobId}/stream`);
            es.onmessage = (e) => {
              const event = JSON.parse(e.data);
              if (event.step === 'ping') return;
              const idx = result.progressSteps.findIndex(s => s.step === event.step);
              if (idx >= 0) {
                result.progressSteps[idx] = { ...result.progressSteps[idx], ...event };
              }
              if (event.step === 'done') {
                es.close();
                resolve();
              } else if (event.step === 'error') {
                es.close();
                reject(new Error(event.message));
              }
            };
            es.onerror = () => { es.close(); reject(new Error('连接中断')); };
          });
        }
```

Also add a placeholder `processReplaceGroup` (to be implemented in Task 6) so the code compiles now:

```javascript
        async function processReplaceGroup(group, result) {
          result.message = '上传中…';
          // implemented in Task 6
          throw new Error('replace not yet implemented');
        }
```

Add all three functions to the `return`:
```javascript
          startBatchProcess, processNewGroup, processReplaceGroup,
```

- [ ] **Step 3: Verify new-file path in browser**

Select 1–2 video files with codes that do NOT exist in the DB. Click through Steps 1–2, confirm they show as "新增". Click "开始上传". Verify:
- Modal transitions to Step 3 "batch-progress"
- The new file group shows SSE steps appearing one by one
- Steps tick off: 解析番号 ✓, 写入DB ✓, 刮削 ⊙ (then ✓), etc.
- On completion, row shows "✓ 完成"
- "完成" button activates after all items processed

- [ ] **Step 4: Commit**

```bash
git add web/templates/index.html
git commit -m "feat: BatchModal Step 3 — new file path via add-auto SSE"
```

---

## Task 6: BatchModal Step 3 — replace path + verify end-to-end

**Files:**
- Modify: `web/templates/index.html`

**Context:** Replace the placeholder `processReplaceGroup` with the real implementation. The existing `POST /api/videos/<code>/replace` endpoint expects: `file` (multipart), `dest_name` (base filename to write on disk), `file_size` (integer, for integrity check), `subtitle_zh`, `uncensored_leaked`, `uncensored_native` (all '1'/'0'), optional `part` (new part label). The `?part=` query param specifies which DB row to replace.

- [ ] **Step 1: Replace `processReplaceGroup` with real implementation**

Find in `BatchModal.setup()`:
```javascript
        async function processReplaceGroup(group, result) {
          result.message = '上传中…';
          // implemented in Task 6
          throw new Error('replace not yet implemented');
        }
```

Replace with:
```javascript
        async function processReplaceGroup(group, result) {
          // For each file in the group (usually one, unless replacing multiple parts)
          for (const fi of group.files) {
            const f = batchFiles.value.find(bf => bf.name === fi.filename);
            if (!f) throw new Error(`文件未找到: ${fi.filename}`);

            const originalPart = fi.part;
            const qp = originalPart ? `?part=${encodeURIComponent(originalPart)}` : '';
            const url = `/api/videos/${encodeURIComponent(group.code)}/replace${qp}`;

            // Use the original filename on disk as dest_name so the path stays in the same directory.
            // Backend's _compute_final_filename will add/remove suffix flags based on the toggles.
            const destName = group.original_path
              ? group.original_path.split('/').pop()
              : f.name;

            const fd = new FormData();
            fd.append('file', f);
            fd.append('dest_name', destName);
            fd.append('file_size', String(f.size));
            fd.append('subtitle_zh', group.subtitle_zh ? '1' : '0');
            fd.append('uncensored_leaked', group.uncensored_leaked ? '1' : '0');
            fd.append('uncensored_native', group.uncensored_native ? '1' : '0');
            if (originalPart) fd.append('part', originalPart);

            result.message = group.files.length > 1 ? `上传中… (${fi.filename})` : '上传中…';
            const res = await fetch(url, { method: 'POST', body: fd });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || '替换失败');
          }
          result.message = '替换完成';
        }
```

- [ ] **Step 2: Verify replace path in browser**

Find a video that already exists in the DB (check `http://10.0.0.30:8000/#/videos` for a known code). Prepare a test file named with that exact code (e.g., if `ABW-123-C` exists, create `ABW-123-C.mp4` as a test file). Then:

1. Open "⚡ 批量添加/替换"
2. Select the test file
3. Click "下一步" — verify the row shows "替换" badge and the original path appears
4. Toggle "中文字幕" on/off to verify toggles work
5. Click "开始上传"
6. Verify Step 3 shows the replace row going from "处理中…" to "✓ 完成"
7. Verify "完成" button activates and clicking it closes the modal

- [ ] **Step 3: Verify mixed batch (new + replace) in browser**

Select two files: one with a code that exists in DB (replace) and one that doesn't (new add). Verify:
- Step 2 shows both rows correctly typed
- Step 3 processes them in order (replace first completes as single line, then new file shows full SSE pipeline)
- Both show "✓ 完成"

- [ ] **Step 4: Run full test suite**

```bash
python -m pytest tests/ -v 2>&1 | tail -20
```

Expected: all tests pass including `tests/test_batch_api.py`.

- [ ] **Step 5: Commit**

```bash
git add web/templates/index.html
git commit -m "feat: BatchModal Step 3 — replace path; batch add/replace complete"
```
