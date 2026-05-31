# 添加影片功能设计文档

**日期**: 2026-05-30  
**状态**: 已批准，待实现

---

## 概述

在 `/videos` 页面右上角添加"+ 添加影片"按钮，点击后弹出多步骤 modal，支持**自动模式**（从文件名解析番号，全自动刮削/翻译/重命名）和**手动模式**（手动指定 WEST/CN/DUMMY 番号，填写元数据）。

---

## 1. 用户流程

### 1.1 共用步骤

**Step 1 — 文件选择**
- 用户选择一个或多个视频文件（支持 .mp4/.mkv/.avi 等）
- 多选时按文件名字母排序，自动分配 part1, part2…
- 选择目标 Volume（下拉列表展示 `config.MEDIA_LIBRARIES`）
- 点击"下一步"

**Step 2 — 模式选择**
- 展示已选文件列表（含 part 分配）
- 切换：⚡ 自动模式 / ✏️ 手动模式
- 点击"开始"

### 1.2 自动模式

**Step 3A — 实时进度（SSE 流）**

Modal 保持打开，显示每个步骤的实时状态（✓ 完成 / ⊙ 进行中 / ○ 待执行）：

1. 解析番号（`extract_video_code`）
2. 计算目标目录（`build_destination`）
3. 上传文件到目标 volume 临时目录
4. 写入数据库（PENDING 状态）
5. 添加标签（根据文件名 -C/-U/-UC）
6. 刮削影片信息（JavDB，Playwright headless）
7. 获取演员信息（`update_video_metadata` 处理）
8. 翻译标题（`translate_title`）
9. 计算短标题（`shorten_chinese_title_with_candidates`）
10. 计算文件名（`build_filename`）
11. 重命名文件
12. 更新数据库

全部完成后进入 Step 5（完成页）。步骤失败时显示错误信息，允许手动关闭。

### 1.3 手动模式

**Step 3M — 番号设置**
- 选择番号类型：WEST / CN / DUMMY
- 查询 DB 最大番号（`GET /api/videos/max-code/<type>`），展示推荐下一个数字
- 用户可修改数字
- 是否分 Part：如选择多文件则默认开启，显示 "N 个文件 → part1…partN"
- 点击"上传"：文件上传到目标 volume，入库，返回 code

**Step 4M — 元数据编辑**
- 字段：原始标题、中文标题、短标题、演员（逗号分隔）、Tags（可多选/手动添加）
- 点击"完成"：调用 `PATCH /api/videos/<code>` 更新 DB，触发文件重命名
- 点击"跳过"：直接进入 Step 5

### 1.4 完成页

- 显示番号和已添加的标签
- 两个按钮：关闭 / 查看影片

---

## 2. 后端 API

### 2.1 新增端点

#### `GET /api/videos/max-code/<string:code_type>`

- `code_type`：`WEST` / `CN` / `DUMMY`（大小写不敏感）
- 查询 `SELECT MAX(code) FROM videos WHERE code LIKE '<TYPE>-%'`，解析尾部数字
- 返回：`{"code_type": "WEST", "max_number": 41, "suggested": 42}`
- DB 无记录时：`{"max_number": 0, "suggested": 1}`

#### `POST /api/videos/add-auto`

- Content-Type: `multipart/form-data`
- 字段：`files[]`（多文件）、`volume_path`（目标 volume 根路径）
- 处理：
  1. 验证 `volume_path` 在 `MEDIA_LIBRARIES` 中
  2. 创建 `job_id = uuid4()`
  3. 文件保存到 `<volume_path>/.upload_tmp/<job_id>/`（保留原始文件名）
  4. 启动 background thread（见 §2.3）
  5. 返回：`{"job_id": "xxx"}`

#### `GET /api/jobs/<string:job_id>/stream`

- SSE 端点（`text/event-stream`）
- 从 `_jobs[job_id].queue` 读取事件并 yield
- 事件格式（每行 `data: <json>\n\n`）：
  ```json
  {"step": "parse_code",   "status": "done",    "message": "番号: ABW-123-C"}
  {"step": "scrape",       "status": "running",  "message": "刮削中…"}
  {"step": "scrape",       "status": "done",     "message": "刮削完成"}
  {"step": "error",        "status": "error",    "message": "刮削失败: timeout"}
  {"step": "done",         "status": "done",     "message": "完成", "code": "ABW-123-C"}
  ```
- 步骤名称列表：`parse_code | calc_dir | upload | db_insert | tags | scrape | actors | translate | short_title | calc_filename | rename | db_update`
- Job 不存在时返回 404

#### `POST /api/videos/add-manual`

- Content-Type: `multipart/form-data`
- 字段：`files[]`、`volume_path`、`code_type`、`code_number`（整数）、`is_multipart`（"1"/"0"）
- 处理：
  1. 构造 code：`f"{code_type.upper()}-{int(code_number):03d}"`
  2. 文件上传到 `<volume_path>/.upload_tmp/<job_id>/`
  3. 计算每个文件的目标路径（`code_prefix` 子目录），无 title 时文件名为 `<code>[.partN]<ext>`
  4. 移动文件到目标目录
  5. `batch_insert_scanned_videos` 入库
  6. 同步 derived tags（`sync_derived_video_tags_for_code`）
- 返回：`{"codes": ["WEST-042"], "job_id": "xxx"}`

手动模式的元数据更新和文件重命名复用现有端点：
- 元数据：`PATCH /api/videos/<code>`（现有）
- 文件路径更新：`PATCH /api/videos/<code>/file-path`（现有）

### 2.2 Job 管理（内存）

```python
# app.py 模块级
import threading
import queue
from dataclasses import dataclass, field

@dataclass
class _Job:
    job_id: str
    queue: queue.Queue = field(default_factory=queue.Queue)
    status: str = 'running'   # running | done | error
    created_at: float = field(default_factory=time.time)

_jobs: dict[str, _Job] = {}
_jobs_lock = threading.Lock()
```

- 每次请求 `add-auto` / `add-manual` 创建一个 `_Job`
- SSE handler 消费 `job.queue`，遇到 `step="done"` 或 `step="error"` 后退出
- 定时清理：启动时启一个 daemon thread，每 10 分钟清理 1 小时前的 job

### 2.3 自动模式 Pipeline Thread

```python
def _run_auto_pipeline(job: _Job, file_paths: list[Path], volume_path: Path):
    def emit(step, status, message, **extra):
        job.queue.put({"step": step, "status": status, "message": message, **extra})

    try:
        # Step 1: 解析番号
        codes_and_parts = []  # [(code, part, tmp_path)]
        for i, fpath in enumerate(sorted(file_paths)):
            code, part = extract_video_code(fpath.stem)
            if not code:
                raise ValueError(f"无法解析番号: {fpath.name}")
            if len(file_paths) > 1 and not part:
                part = f"part{i+1}"
            codes_and_parts.append((code, part, fpath))
        emit("parse_code", "done", f"番号: {codes_and_parts[0][0]}")

        # Step 2-12: 按顺序执行，每步 emit 事件
        # ...（详见实现计划）

        emit("done", "done", "完成", code=codes_and_parts[0][0])
    except Exception as e:
        emit("error", "error", str(e))
    finally:
        job.status = 'done'
```

**Playwright 使用方式**（在 thread 内）：
```python
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(viewport={'width': 1280, 'height': 800})
    load_javdb_cookies(context)
    page = context.new_page()
    Stealth().apply_stealth_sync(page)
    result_data = scrape_video_info(page, base_code)
    browser.close()
```

---

## 3. 前端设计

### 3.1 按钮位置

在 `tpl-home` 的 filter 行（`div.flex.items-center.justify-between`）右侧添加按钮：

```html
<button @click="openAddModal" class="px-3 py-1.5 text-sm rounded bg-indigo-600 hover:bg-indigo-500 text-white font-medium">
  + 添加影片
</button>
```

### 3.2 Modal 状态机

`addStep` ref 的状态转移：

```
closed → file-select → mode-select
                              ↓ auto         ↓ manual
                       auto-progress    manual-code
                              ↓               ↓
                            done        manual-meta
                                              ↓
                                            done
```

### 3.3 核心 Vue Refs

```javascript
const showAddModal  = ref(false);
const addStep       = ref('file-select');  // 当前步骤
const addFiles      = ref([]);             // File[] 已选文件
const addVolume     = ref('');             // 目标 volume 路径
const addMode       = ref('auto');         // 'auto' | 'manual'

// 自动模式
const addJobId      = ref('');
const addProgress   = ref([]);             // [{step, status, message}]
const addEventSrc   = ref(null);           // EventSource

// 手动模式
const manualCodeType   = ref('WEST');
const manualCodeNumber = ref(1);
const manualSuggestedNumber = ref(1);
const manualIsMultipart = ref(false);
const manualCodes   = ref([]);             // 入库后的 codes
const manualTitle   = ref('');
const manualTitleZh = ref('');
const manualShortTitle = ref('');
const manualActors  = ref('');
const manualTags    = ref([]);
```

### 3.4 SSE 客户端

```javascript
async function startAutoUpload() {
    const fd = new FormData();
    addFiles.value.forEach(f => fd.append('files[]', f));
    fd.append('volume_path', addVolume.value);
    
    const res = await fetch('/api/videos/add-auto', { method: 'POST', body: fd });
    const { job_id } = await res.json();
    addJobId.value = job_id;
    addStep.value = 'auto-progress';
    
    const es = new EventSource(`/api/jobs/${job_id}/stream`);
    addEventSrc.value = es;
    es.onmessage = (e) => {
        const event = JSON.parse(e.data);
        // 更新对应 step 的状态
        const idx = addProgress.value.findIndex(s => s.step === event.step);
        if (idx >= 0) addProgress.value[idx] = event;
        else addProgress.value.push(event);
        
        if (event.step === 'done' || event.step === 'error') {
            es.close();
            if (event.step === 'done') addStep.value = 'done';
        }
    };
}
```

### 3.5 手动模式文件重命名触发

手动模式 Step 4M 提交后：
1. `PATCH /api/videos/<code>` 更新 title/actors/tags
2. 前端调用计算文件名逻辑（或由后端在 `add-manual` 返回时已完成初步命名）
3. 若有 short_title，可再次调用 `PATCH /api/videos/<code>/file-path` 更新路径

---

## 4. 文件路径约定

- 上传临时目录：`<volume_path>/.upload_tmp/<job_id>/`（保留原始文件名）
- 初始目标路径：`<volume_path>/avideo/<code_prefix>/<original_filename>`
- 最终目标路径：`<volume_path>/avideo/<code_prefix>/<build_filename() 结果>`
- 临时目录在 pipeline 完成（或失败）后由 job 清理

---

## 5. 标签推导规则（自动模式 Step 5）

文件名中检测（与现有 `extract_video_code` 逻辑一致）：

| 文件名特征 | 添加 Tag |
|-----------|---------|
| 含 `-C` 后缀 | 中文字幕 |
| 含 `-U` 后缀 | 无码破解 |
| 含 `-UC` 或 `-U-C` | 中文字幕 + 无码破解 |

通过 `sync_derived_video_tags_for_code(code)` 统一处理（已有逻辑）。

---

## 6. 错误处理

| 场景 | 处理方式 |
|------|---------|
| 文件名无法解析番号 | 自动模式：SSE error，提示用改手动模式 |
| 刮削超时/失败 | SSE error，DB 记录状态为 FAILED，文件已在目标目录（保留） |
| 翻译/短标题失败 | 跳过该步骤，继续后续步骤（非阻塞） |
| Volume 不在白名单 | 400 错误 |
| 番号已存在 | `batch_insert_scanned_videos` 的 INSERT OR IGNORE 跳过，但文件已上传 |

---

## 7. 文件改动范围

| 文件 | 改动 |
|------|------|
| `web/backend/app.py` | 新增 5 个端点，job 管理，pipeline thread |
| `web/templates/index.html` | HomeView 按钮 + AddVideoModal 组件 |
| `config.py` | 无需改动 |
| `core/scanner.py` | 无需改动（直接调用） |
| `core/translator/service.py` | 无需改动（直接调用） |
| `utils/rename_and_move_videos.py` | 无需改动（直接调用 `build_filename`、`build_destination`） |

---

## 8. 不在本次范围内

- 断点续传（大文件上传中断恢复）
- 上传进度百分比（XHR progress 事件可后续加）
- 自动模式失败后重试 UI
- 已有 daily_pipeline 与新增入库的冲突处理（INSERT OR IGNORE 已覆盖）
