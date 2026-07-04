# 设计文档：网页分字段改名 + 按文件修改时间排序

日期：2026-07-03

## 背景

两个独立的小功能：

1. **分字段改名**：在网页详情弹窗里，让用户按「番号 / 女优 / 标题」三个字段编辑，重命名磁盘上的视频文件。
2. **按文件修改时间排序**：视频列表新增一个「文件时间」排序选项。

现状参考：

- 文件名规约由 [`build_filename()`](../../../utils/rename_and_move_videos.py) 生成：`番号.女优1.女优2.[标题].partN.ext`（点分隔，`sanitize_component` 清洗每段）。
- 已有 [`finalize-rename`](../../../web/backend/app.py) 端点：从元数据自动重生成规范文件名。本功能与之不同——由用户手工分字段编辑。
- 排序由 [`_order_clause(sort)`](../../../dal/db_manager.py) 决定，现支持 `date`（发布日期，默认）和 `score`（评分）。
- 前端是单文件 Vue 应用 [`index.html`](../../../web/templates/index.html)，`SortBar` 组件目前只有 `date` / `score` 两个按钮。
- `videos` 表已有 `file_mtime`（TEXT，ISO 格式）列，且上一轮已做过全量重同步。

## 范围与非目标

**明确范围（功能1）**：只改「文件名 + `original_file_path` + `code` 列」。`code` 列被设为清洗后的番号字段（含 `-C/-U/-UC` 后缀）——用户没动番号则 `code` 不变，动了番号主干则整段跟着变。

**非目标（功能1）**：不改 actor 关联表、不改 `title_zh/title_jp` 列、不改封面文件。已知副作用（不在本次处理，仅记录）：

- 改番号主干后，按 `code` 关联的封面（`web/static/covers/CODE.jpg`）会对不上。
- exFAT 卷上会残留旧文件名的 `._旧名` 边车文件（不影响功能）。

---

## 功能2：按文件修改时间排序

### 后端

[`_order_clause(sort)`](../../../dal/db_manager.py) 增加分支：

```python
if sort == 'mtime':
    return "COALESCE(file_mtime, '') DESC, id DESC"
```

`file_mtime` 是 ISO 8601 字符串，字典序即时间序，`DESC` 即最新在前。空值用 `COALESCE` 兜底排到最后。

作用于所有把 `sort` 透传到服务端的列表：`/api/videos`、`/api/series/videos`、`/api/prefixes/videos`（分别经 `list_videos`、`get_videos_by_series`、`get_videos_by_prefix`，它们都调用 `_order_clause`）。

### 前端

- `SortBar` 模板（`#tpl-sort-bar`）在 `date`/`score` 后加第三个按钮，值 `mtime`，文案「文件时间」，样式复用现有 active/inactive class。
- 客户端本地排序的视图（`index.html` 中两处 `sortedVideos` computed，各带 `date`/`score` 分支）增加 `mtime` 分支：按 `v.file_mtime` 字符串降序。为此需确认这些视图的视频对象里带 `file_mtime`；若列表接口 payload 未含该字段，则在对应查询的 SELECT 里补上 `file_mtime`。

---

## 功能1：分字段改名

### 数据流

```
详情弹窗
  └─ 「文件名」区块（每个文件/part 一行）
       ├─ 显示当前文件名
       ├─ 输入框：番号 / 女优（. 分隔）/ 标题
       └─ [重命名] → POST /api/videos/<code>/rename-file
                          body: { part, code, actors: [...], title }
                     ← 返回更新后的详情（get_video_by_code）
```

### 前端：解析当前文件名以预填

给定文件名 `stem.ext`，按下述规则拆成三字段（纯字符串操作，不查元数据）：

1. 去掉扩展名 `ext`。
2. 从尾部识别 `.partN` 段 → `part`（不可编辑，原样保留）。
3. 按 `.` 分段。第 0 段 → **番号**。
4. 若最后一段形如 `[...]` → **标题**（去掉方括号）。
5. 番号与标题之间的所有段 → **女优**列表（输入框里用 `.` 连接展示）。

不完全符合规约的旧文件名：能拆出番号（第 0 段）即可，女优/标题可能为空，用户可自行补。

### 前端：提交

- 女优输入框按 `.` 拆成数组（去空白、去空段）。
- 发送 `{ part, code, actors, title }`；`part` 取自该行文件（单文件时为该记录的 part，可能为 null）。

### 后端：`POST /api/videos/<code>/rename-file`

1. 解析 body：`part`、`code`、`actors`（list）、`title`（str）。
2. 用 `code`（解析后）+ `part` 定位唯一记录（沿用 `replace_video_file` 里按 part 定位的写法：`part` 为空时匹配 `part IS NULL OR part = ''`）。找不到 → 404。
3. 取旧文件 `old_path`，扩展名 `ext = old_path.suffix`。
4. 组装新文件名，复用 `build_filename` 的 join 逻辑（`sanitize_component` 清洗每段）：
   ```
   components = [sanitize(code)] + [sanitize(a) for a in actors if 非空]
   if title: components += [f'[{sanitize(title)}]']
   if part:  components += [sanitize(part)]
   new_name = '.'.join(非空 components) + ext
   ```
5. 校验：`sanitize(code)` 非空（否则 400 `invalid code`）；`new_name` 不含路径分隔符/控制符（`sanitize_component` 已负责清洗）。
6. `new_path = old_path.parent / new_name`。
   - 若 `new_path == old_path`：无变化，直接返回当前详情（不算错误）。
   - 若 `new_path` 已存在（且不是自己）→ 409 `target exists`。
7. `old_path` 不存在 → 400 `source file missing`。
8. `os.rename(old_path, new_path)` 物理改名。
9. DB 更新：`UPDATE videos SET original_file_path = ?, code = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`，其中 `code = sanitize(code)`（后缀已含在番号里）。
   - 若 DB 更新抛错：尝试把文件改回 `old_path`，返回 500。
10. 返回 `{ ok: True, **get_video_by_code(new_code) }`。

顺序说明：先物理改名再更新 DB，失败时回滚文件名，和 `replace` 端点的思路一致，保证两边最终一致。

### DAL

新增 `rename_video_file(code, part, new_path, new_code) -> dict`，只做「定位记录 + 更新 path/code」这一步（物理改名和文件名组装放在 app 层，因为涉及文件系统与 `build_filename`）。返回 `{'ok': bool, 'error'?: str}`，风格对齐 `replace_video_file`。

> 备选：直接在 app 端点里用 sqlite 更新，不新增 DAL 函数。倾向新增 DAL 函数，和 `replace_video_file` 对称、便于单测。

### 错误处理汇总

| 情况 | HTTP | 说明 |
|---|---|---|
| 番号清洗后为空 | 400 | `invalid code` |
| 目标文件名已存在 | 409 | `target exists` |
| 源文件不存在 | 400 | `source file missing` |
| 记录不存在 | 404 | `not found` |
| `os.rename`/DB 异常 | 500 | 带异常信息，尽力回滚文件名 |
| 新旧文件名相同 | 200 | 无操作，返回当前详情 |

---

## 测试

仿 [`tests/test_replace_video.py`](../../../tests/test_replace_video.py) 的风格（临时 sqlite + 临时文件）。

**功能1（后端）**：

- 改名成功：物理文件被重命名，`original_file_path` 与 `code` 同步更新。
- 番号带后缀（如 `ABP-119-UC`）：`code` 列等于清洗后的番号，含后缀。
- 目标已存在 → 409，且原文件不动、DB 不变。
- 源文件不存在 → 400。
- 番号清洗后为空 → 400。
- 女优/标题为空：生成 `番号.partN.ext` 形式，正常。
- 新旧同名 → 200 无操作。

**功能2（后端）**：

- `list_videos(sort='mtime')` 的 ORDER BY 生效：构造 3 条不同 `file_mtime` 的记录，断言返回顺序为 mtime 降序。

**前端**：文件名解析/组装是 JS 逻辑，无 pytest 覆盖；靠手工验证（详情弹窗改名后文件与列表刷新正确）。

---

## 涉及文件

- `dal/db_manager.py`：`_order_clause` 加 `mtime`；新增 `rename_video_file`。
- `web/backend/app.py`：新增 `POST /api/videos/<code>/rename-file` 端点。
- `web/templates/index.html`：`SortBar` 加「文件时间」按钮；两处 `sortedVideos` 加 `mtime` 分支；详情弹窗加「文件名」分字段改名区块及提交逻辑。
- `tests/test_replace_video.py`（或新建 `tests/test_rename_file.py`）：补充上述测试。
