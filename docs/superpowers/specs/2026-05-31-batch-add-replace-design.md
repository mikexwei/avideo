# 批量添加/替换 + 手动添加 功能设计文档

**日期**: 2026-05-31
**状态**: 已批准，待实现

---

## 概述

在首页新增两个按钮：

1. **⚡ 批量添加/替换** — 用户选择多个视频文件，系统自动从文件名解析番号，检查 DB 判断每个番号是新增还是替换，统一在 Review 界面确认后批量处理。
2. **手动添加** — 直接进入现有手动添加流程（`AddVideoModal` 的 `manual-code` 步骤）。

**保持不变：**
- 现有"添加影片"按钮及 modal（自动 + 手动两种模式）
- 详情页"替换视频"按钮

---

## 1. 首页 UI

首页右上角按钮区，从左到右：

```
[手动添加]  [⚡ 批量添加/替换]  [添加影片]
```

- "手动添加"：次要样式（灰色）
- "批量添加/替换"：主要样式（蓝色）
- "添加影片"：保持现有样式不变

---

## 2. 功能一：批量添加/替换

### 2.1 Modal 状态机

```
batch-select → (loading: batch-check API) → batch-review → batch-progress → batch-done
```

### 2.2 Step 1 — 选文件（`batch-select`）

- 用户选择一个或多个视频文件（`.mp4` / `.mkv` / `.avi` 等）
- 展示已选文件列表
- 点击"下一步"后：
  1. 前端调用 `POST /api/videos/batch-check`，传入文件名列表
  2. 显示 loading 状态
  3. 收到响应后进入 Step 2

### 2.3 Step 2 — 确认（`batch-review`）

展示 Review 表格，顶部显示摘要（"检测到 N 个番号 — X 新增，Y 替换"）。

**表格列：** 番号 | 文件 | 操作 | 标签确认

**新增行：**
- 操作列：绿色 `新增` badge
- 标签列：显示"自动刮削"（不可编辑）

**替换行：**
- 操作列：黄色 `替换` badge
- 文件列下方：显示原始文件路径（灰色、只读）
- 标签列：3 个 toggle 开关（中文字幕 / 无码破解 / 原装无码），初始值从文件名推导（`-C` → 中文字幕开，`-U` → 无码破解开，`-UC` 或 `-U-C` → 两者都开）

**多 Part 分组（前端处理）：**
- `batch-check` 响应是按文件逐条返回的；前端收到响应后，按 `code` 字段分组，相同 code 的多条合并为一行
- 分组后若所有文件均为新增（`exists: false`）→ 整组走新增逻辑；若均为替换（`exists: true`）→ 走替换逻辑
- 若同一 code 下部分文件 exists=true、部分 exists=false（即 DB 中已有 part1 但没有 part2），该行标注橙色警告"同一番号下新增与替换混合，请检查文件"，禁止上传
- 操作列额外显示 `N parts` badge

**边缘情况 — 替换 Part 不明确：**
- 替换行对应的文件名中没有 part 信息（`extract_video_code` 返回 `part=None`），但该番号在 DB 中有多个 part
- 该行显示橙色警告："无法确定 Part，请在文件名中加上 -partN（如 SSIS-456-part1.mp4）"
- "开始上传"按钮禁用，直到所有警告消除

**Volume 选择（仅当有新增文件时出现）：**
- 位于表格下方
- 单选卡片，每张显示路径和剩余空间（GB）
- 全部为替换时，此区域不显示

### 2.4 Step 3 — 批量进度（`batch-progress`）

- 按 Step 2 表格顺序，每个番号顺序处理（先一项完成再开始下一项）
- 每个番号显示为一个卡片：

**新增卡片：**
- 展开 SSE 步骤（与现有 `auto-progress` 步骤一致）：解析番号 → 计算目标目录 → 上传 → 写入 DB → 标签 → 刮削 → 演员 → 翻译 → 短标题 → 计算文件名 → 重命名 → 更新 DB
- 调用现有 `POST /api/videos/add-auto` + `GET /api/jobs/<id>/stream`

**替换卡片：**
- 单行，显示"上传中… → 替换完成"
- 调用现有 `POST /api/videos/<code>/replace`（XHR，带进度）

**错误处理：**
- 某项失败显示红色错误信息，继续处理下一项
- 全部完成后"完成"按钮可点

### 2.5 完成（`batch-done`）

- 显示处理摘要（N 成功，M 失败）
- 失败项显示错误原因
- "完成"按钮关闭 modal

---

## 3. 功能二：手动添加

**前端改动（极小）：**
- 首页新增"手动添加"按钮
- 点击后打开现有 `AddVideoModal`，通过 prop `initialStep="manual-code"` 跳过 `file-select` 和 `mode-select`，直接进入手动流程
- Modal 标题在 `initialStep="manual-code"` 时显示"手动添加"

**后端：无任何改动。**

---

## 4. 新增后端端点

### `POST /api/videos/batch-check`

唯一的新增端点。

**请求：**
```json
{"filenames": ["ABW-123-C.mp4", "SSIS-456.mkv", "WEST-042-part1.mp4", "WEST-042-part2.mp4"]}
```

**处理：**
1. 对每个 filename，调用 `extract_video_code(Path(filename).stem)` 得到 `(code, part)`
2. 若 code 为 None，返回该项 `{exists: null, error: "无法解析番号"}`
3. 精确查询：`SELECT id, original_file_path, part FROM videos WHERE code = ?`（code 为 step 1 得到的完整 code，含 -C/-U/-UC 后缀）
4. 若存在 → `exists: true`，附上 `original_path`（DB 中的路径）和 `db_parts`（该番号所有 part 列表）
5. 若不存在 → `exists: false`

**响应示例：**
```json
[
  {"filename": "ABW-123-C.mp4",     "code": "ABW-123-C", "part": null,    "exists": false, "original_path": null, "db_parts": []},
  {"filename": "SSIS-456.mkv",      "code": "SSIS-456",  "part": null,    "exists": true,  "original_path": "/Volumes/Media1/.../SSIS-456.mkv", "db_parts": [null]},
  {"filename": "WEST-042-part1.mp4","code": "WEST-042",  "part": "part1", "exists": false, "original_path": null, "db_parts": []},
  {"filename": "WEST-042-part2.mp4","code": "WEST-042",  "part": "part2", "exists": false, "original_path": null, "db_parts": []}
]
```

---

## 5. 复用的现有端点

| 操作 | 端点 | 备注 |
|------|------|------|
| 新增文件上传 | `POST /api/videos/add-auto` | 每个新增 code group 调用一次 |
| 新增进度流 | `GET /api/jobs/<id>/stream` | SSE，复用现有逻辑 |
| 替换文件上传 | `POST /api/videos/<code>/replace` | 每个替换文件调用一次 |
| Volume 列表 | `GET /api/config/media-libraries` | 含剩余空间，已实现 |

---

## 6. 文件改动范围

| 文件 | 改动 |
|------|------|
| `web/backend/app.py` | 新增 `POST /api/videos/batch-check` 端点（约 40 行） |
| `web/templates/index.html` | 首页加 2 个按钮；新增 `BatchModal` 组件；`AddVideoModal` 支持 `initialStep` prop |
| `tests/test_batch_api.py` | 新增 `batch-check` 端点的单元测试 |

---

## 7. 不在本次范围内

- 新增文件上传后的"查看影片"快捷跳转（已有 `batch-done` 界面可后续加）
- 替换路径的刮削/重新翻译（替换只更新文件，不重新刮削元数据）
- 断点续传
