# AVideo 第三方 HTTP API 接入指南

## 接入约定

- 示例服务地址为 `http://127.0.0.1:8000`。远程客户端应替换为实际部署地址。
- 当前应用未内置 API Key 或用户鉴权。请在可信网络或受访问控制的反向代理后使用。管理和 Worker 写接口会拒绝带有不匹配 `Origin` 的请求。浏览器跨域播放接口单独支持 CORS。
- 路径中的 `{code}`、`{actor_id}` 等需要替换为实际值。路径参数和查询参数使用 URL 编码。番号建议使用响应里的完整 `code`，包括 `-C`、`-U` 或 `-UC` 后缀。
- JSON 请求发送 `Content-Type: application/json`。表单、二进制上传、视频流、字幕和 SSE 的格式见对应章节。多数错误以非 2xx 状态和 `{"error":"..."}` 返回。客户端应先检查 HTTP 状态和响应类型，再解析正文。
- 返回 `202` 表示任务已受理。带 `job_id` 的响应可查任务状态。文件上传请求的应用层大小上限为 50 GiB。

## 影片、演员和标签查询

| 方法和路径 | 请求 | 响应 |
| --- | --- | --- |
| `GET /api/videos` | 筛选和分页参数见下表 | `{page, limit, total, items}`，每个番号一条 |
| `GET /api/videos/{code}` | 影片番号 | 影片详情，含演员、标签和文件路径 |
| `GET /api/search` | `q` 完整番号忽略末尾的 `-C`、`-U`、`-UC`，返回同一基础番号的全部在库版本。其他查询匹配番号、原文标题、中文标题或片商。支持 `page`、`limit` | `{page, limit, total, items}` |
| `GET /api/actors` | `q` 模糊匹配原名或中文名 | 最多 10 条 `{id, name, name_zh, avatar_path}`。空查询返回 `[]` |
| `GET /api/actors/all` | 无 | 有在库影片且未忽略的演员，另含去重作品数 `count`、搜索用名称 `search_names` |
| `GET /api/actors/{actor_id}` | 整数演员 ID | `{actor, videos}`。作品项含 `code`、`tags` 和 `deleted`。无该演员时返回 `404` |
| `GET /api/tags/all` | 默认只返回在库影片使用的标签。传 `include_unused=1` 也返回未使用的标签 | `[{id, name, count}]`，`count` 为关联的在库番号数 |
| `GET /api/tags/{tag_id}` | 整数标签 ID | `{tag, videos}`。作品项含 `code` 和 `deleted`。无该标签时返回 `404` |
| `GET /api/series` | `q` 匹配原系列名、译名或归并后的名称 | 最多 10 条 `{name, name_zh}`。旧数据库可能只返回 `name` |
| `GET /api/series/all` | 无 | 系列名 `series`、原名 `raw_name`、作品数 `count`。启用系列目录时还含 `jp_id`、`final_id`、`translation_status` |
| `GET /api/series/videos` | `jp` 原系列 ID、`final` 归并系列 ID 或 `s` 原系列名，三选一。支持 `page`、`limit`、`sort` | `{page, limit, total, items, name}` |
| `GET /api/prefixes/all` | 无 | `[{prefix, count}]`，按番号首段分组 |
| `GET /api/prefixes/videos` | 必填 `prefix`，如 `DEMO`。支持 `page`、`limit`、`sort` | `{page, limit, total, items}` |
| `GET /api/recommendations` | `tag`、`tags`、`exclude` 同影片列表 | 最多 8 条随机推荐摘要 |
| `GET /api/stats` | 无 | 总数 `total_videos`、`deleted_videos`、`total_actors`、`total_tags`、`total_series`，以及 `scrape_status`、`translated`、`has_cover`、`by_year`、`score_dist`、`top_makers`、`top_actors`、`actor_disk_usage`、`top_tags` |

### 影片列表筛选

| 参数 | 说明 |
| --- | --- |
| `page`、`limit` | 从第 1 页开始，默认每页 24 条，最大 200 条。搜索、系列作品和番号前缀作品使用相同的分页参数 |
| `sort` | `date` 按发行日期（默认）、`score` 按评分、`mtime` 按文件修改时间、`last_played` 按最近播放、`watched` 按累计观看时长，均为降序。系列和前缀作品支持前三种 |
| `year` | 发行年份，如 `2024` |
| `score_min`、`score_max` | 最低分（含）、最高分（不含） |
| `has_cover=1`、`has_translation=1` | 只保留有封面、已有中文标题的记录 |
| `scrape_status` | 按影片刮削状态精确匹配，例如 `PENDING`、`SUCCESS`、`FAILED` |
| `tag`、`tags` | 用标签名称精确匹配。可重复传 `tag`，也可传逗号分隔的 `tags`。默认要求同一个在库文件记录命中所有选中标签 |
| `exclude=1` | 某番号的任一在库分段命中任一选中标签时，排除整个番号。未提供标签时不应用此筛选 |

列表只筛选未删除记录。响应中的 `total` 是符合条件的不同番号数，`items` 包含 `id`、`code`、`title_jp`、`title_zh`、`release_date`、`score`、`cover_path`、`scrape_status`、`deleted`。按播放排序时还返回 `last_played_at` 和 `watched_ms`。搜索、演员作品、标签作品和前缀作品的摘要可能含已删除记录，取在库影片时请检查 `deleted`。

### 影片详情和导出

| 详情字段 | 含义 |
| --- | --- |
| `code` | 数据库实际使用的番号 |
| `original_file_path`、`part`、`file_size`、`file_mtime`、`file_birthtime` | 首个文件的服务器路径、分段名、字节数及时间字段。文件名取路径最后一段 |
| `parts` | 同番号有多条文件记录时返回。每项含 `id` 以及对应分段的文件字段。导出时遍历该数组一次 |
| `total_file_size` | 文件大小合计，单位字节。没有已知大小时可为 `null` |
| `actors` | 此番号关联的未忽略演员，元素含 `id`、`name`、`name_zh`、`avatar_path` |
| `tags` | 此番号关联的去重标签，元素为 `{id, name}` |
| `subtitle_classification`、`subtitle_zh` | 字幕分类和是否有字幕。未分类的旧记录返回 `null` 和 `false` |
| `record_exists`、`exists`、`status` | 在库为 `true, true, ACTIVE`。软删除为 `true, false, DELETED`，仍返回 `200`。无记录为 `false, false, NOT_FOUND`，返回 `404` |
| 其他元数据 | `id`、`deleted`、`scrape_status`、标题、发行日期、片商、发行商、系列、时长、评分、封面。部分字段可为 `null` |

`original_file_path` 是数据库记录的服务端路径。远程客户端下载时使用下载接口。封面和头像的相对路径可拼接在服务地址的 `/static/` 下。

下面的 Python 3 示例分页导出番号、文件名、演员、标签和路径。每行输出一个文件。按演员筛选时，先获取 `/api/actors/{actor_id}` 的 `videos[].code`，再与筛选列表的番号取交集。详情按番号汇总，文件级筛选使用详情中的各分段路径或文件清单。

```python
import json
from urllib.parse import quote, urlencode
from urllib.request import urlopen

base_url = "http://127.0.0.1:8000"


def get_json(path):
    with urlopen(base_url + path, timeout=30) as response:
        return json.load(response)


page = 1
while True:
    query = urlencode({"year": 2024, "tag": "硬字幕", "limit": 100, "page": page})
    listing = get_json("/api/videos?" + query)
    for item in listing["items"]:
        video = get_json("/api/videos/" + quote(item["code"], safe=""))
        if not video["exists"]:
            continue
        for file in video.get("parts", [video]):
            path = file["original_file_path"]
            if path:
                print(json.dumps({
                    "code": video["code"],
                    "filename": path.rsplit("/", 1)[-1],
                    "actors": [a["name_zh"] or a["name"] for a in video["actors"]],
                    "tags": [t["name"] for t in video["tags"]],
                    "path": path,
                }, ensure_ascii=False))
    if not listing["items"] or page * listing["limit"] >= listing["total"]:
        break
    page += 1
```

## 文件清单、下载和播放

| 方法和路径 | 请求 | 响应 |
| --- | --- | --- |
| `GET /api/videos/files` | `extension` 默认为 `mkv`，可传 `mp4` 或 `.MP4` 等 | `{extension, total, missing, files}`。每个实际存在的在库文件一条，`missing` 统计缺失文件 |
| `GET /api/videos/{code}/download-url` | 影片番号 | `{input_code, code, base_code, download_url, files}`。每个文件含 `part`、`filename`、`file_size`、`download_url`。多文件时顶层链接为 `null` |
| `GET /api/videos/{code}/stream` | 可传 `part=part1`。加 `download=1` 作为附件下载 | 视频字节流。支持 `Range: bytes=...`，有效范围请求返回 `206`，文件缺失返回 `404` |
| `GET /api/videos/{code}/stream.{ext}` | 同上，`ext` 用于需要 URL 扩展名的播放器 | 同上。实际媒体类型由文件决定 |
| `OPTIONS /api/videos/{code}/stream` | 查询允许的方法 | 当前 Flask 自动响应优先，返回 `Allow`，未附带 CORS 预检头 |
| `OPTIONS /api/videos/{code}/stream.{ext}` | 同上 | 同上 |
| `GET /api/videos/{code}/subtitles` | 可传 `part` | 缓存就绪返回 `{tracks}`，未就绪返回 `202` 和 `{"status":"pending"}` |
| `POST /api/videos/{code}/subtitles` | 同上，请求体可空 | 必要时排队提取字幕。返回 `202` 后用 GET 轮询。缓存任务失败时返回 `503` |
| `GET /api/videos/{code}/subtitles/{key}/{track_id}.vtt` | 直接请求字幕轨的 `url`，保留其中的 `part` | `text/vtt`，失效或未知字幕轨返回 `404` |
| `GET /api/videos/{code}/playback` | 影片番号 | `{play_count, last_played_at, watched_ms, sessions}`，最多带最近 30 次会话 |
| `POST /api/videos/{code}/playback/{session_id}` | UUID 会话 ID 和播放进度 JSON，见下文 | `204`，空响应 |

文件清单的 `files[]` 含 `id`、`code`、`part`、`filename`、`file_size`、`subtitle_classification`、`subtitle_zh`、`uncensored_leaked`、`uncensored_native`、`download_url`、`replace_url`。扩展名去掉前导点并转小写后需为 1–10 位字母或数字，非法值返回 `400`。清单按扩展名筛选且不分页，演员、完整标签和路径请查影片详情。

字幕轨含 `id`、`language`、`label`、`default`、`url`。无法识别源文件时返回空轨道数组。

播放进度字段为 `source`（`web`、`iina`、`direct`，默认 `web`）、可选 `part`（如 `part1`）、`started_at`、`last_played_at`、`watched_ms`、`position_ms`、`sequence`。时间戳使用 Unix 毫秒，观看时长和播放位置使用毫秒。`direct` 要求时长和位置均为零，其他来源要求正的累计观看时长。每次播放使用新的 UUID，同一次播放固定开始时间和来源，并逐次递增 `sequence`。重复或乱序上报不会覆盖较新的进度。IINA 也可用同名表单字段上报，需显式传 `source=iina`。

## 上传、替换和文件维护

| 方法和路径 | 请求 | 响应 |
| --- | --- | --- |
| `GET /api/config/media-libraries` | 无 | 可选媒体库列表，含 `path`、`name`、`available`、`free_gb`、`used_gb`、`total_gb`、`used_percent`、`free_percent`。容量以 GiB 计 |
| `POST /api/videos/batch-check` | JSON `{"filenames":["DEMO-001.mp4"]}` | 每个文件名一条检测结果，含 `code`、`db_code`、`part`、`status`、`record_exists`、`exists`、`scrape_status`、`original_path`、`db_parts` 和字幕、无码标识。解析失败的条目含 `error` |
| `GET /api/videos/max-code/{code_type}` | `WEST`、`CN` 或 `DUMMY` | `{code_type, max_number, suggested}`。建议编号为历史最大值加一 |
| `POST /api/videos/upload-part` | 原始二进制请求体和上传头，见下文 | `{ok, name}`，`name` 为实际暂存文件名 |
| `POST /api/videos/add-auto` | 暂存批次 JSON，或直接传 multipart 表单 | `202` 和 `{job_id}` |
| `POST /api/videos/add-manual` | 选定番号的暂存批次 JSON，或 multipart 表单 | `{codes}`，同步完成 |
| `POST /api/videos/{code}/finalize-rename` | 请求体可空，按当前元数据同步生成文件名 | `{ok, new_paths}`。目标路径冲突返回 `409` |
| `POST /api/videos/{code}/replace-preview` | JSON `{"files":[{"filename":"new.mp4","part":"part1"}],"subtitle_classification":"硬字幕","uncensored":false}`，`part` 可省略 | `{names}`，按输入顺序返回生成的文件名 |
| `POST /api/videos/{code}/replace` | 查询参数 `part` 指定原分段。文件和元数据支持二进制或 multipart，见下文 | `{ok, ...影片详情}`，用响应的 `code` 获取替换后的番号 |
| `POST /api/videos/{code}/rename-file` | JSON 必填目标 `code`，可选 `actors`（姓名数组）、`title`、`part` | `{ok, rename_job_ids, ...影片详情}`。使用原文件扩展名，实际改名由任务完成 |
| `PATCH /api/videos/{code}/file-path` | JSON `original_file_path`（兼容 `origin_file_path`），有多个分段时传 `part` | `{ok, id, code, part, original_file_path}`。只更新数据库路径 |
| `DELETE /api/videos/{code}` | 数据库中的完整番号 | `{ok, deleted_files}`。删除磁盘文件并将对应记录标记为已删除 |

### 上传请求格式

1. 用 `GET /api/config/media-libraries` 选择可用的 `path`。自动新增前可用 `batch-check` 判断番号是否已在库。
2. 每个上传批次生成一个 UUID，逐个调用 `upload-part`。请求体为文件字节，头部传 `X-Upload-Id`、`X-Volume-Path`、`X-Dest-Name`，可传 `X-File-Size`（字节）校验长度。对路径和文件名做百分号编码。手动新增和自动新增使用同一个文件名模板。
3. 提交 `add-auto` 或 `add-manual`，JSON 传同一批次的 `upload_id` 和 `volume_path`。直接传 multipart 时，用重复的 `files[]` 字段代替 `upload_id`。两种新增方式均可用 `tags` 附加已存在的标签。
   - `add-auto` 每批一个番号。可选 `subtitle_classification`（见下文）和 `uncensored_crack_tag`（`JAV 无码破解`、`LADA 无码破解`、`LTX 无码破解` 或空串）。省略破解标签默认为 `LTX 无码破解`，有码影片应显式传空串。文件准备入库后返回任务 ID，随后异步刮削。同一暂存批次已提交时可返回已有任务 ID。
   - `add-manual` 还需 `code_type`（`WEST`、`CN`、`DUMMY`）和正整数 `code_number`。可用 `is_multipart=1` 指定分段。

替换请求使用 `Content-Type: application/octet-stream` 和以下头部，或用 `multipart/form-data` 上传 `file`，同时传对应表单字段。

| 二进制请求头 | 表单字段 | 用途 |
| --- | --- | --- |
| `X-Dest-Name` | `dest_name` | 必填目标名称，二进制头部值需百分号编码 |
| `X-File-Size` | `file_size` | 必填文件大小，单位字节 |
| `X-Part` | `part` | 新文件分段名。原分段使用 URL 查询参数 |
| `X-Replace-Whole` | `replace_whole` | `1` 表示用单文件替换整部影片 |
| `X-Subtitle-Classification` | `subtitle_classification` | 可选字幕分类，头部值需百分号编码。此分类决定字幕后缀 |
| `X-Uncensored-Leaked` | `uncensored_leaked` | `1` 表示流出或破解 |
| `X-Uncensored-Native` | `uncensored_native` | `1` 表示原装无码 |
| `X-Uncensored-Crack-Tag` | `uncensored_crack_tag` | 可选破解标签，取值同自动新增，头部值需百分号编码 |
| `X-Tags` | `tags` | 可选附加标签，传 JSON 数组字符串，头部值需百分号编码 |

### 字幕分类

上传、替换、替换预览、元数据修改均使用 `subtitle_classification`。审核接口使用 `classification`。允许值为 `硬字幕`、`外挂 OCR 字幕`、`外挂音频字幕` 和 `无字幕`。显式传空串、`null`、旧标签或其他值返回 `400`。通用 `tags` 字段保留普通标签，字幕分类通过专用字段提交。

上传或替换省略分类时，上传文件番号带 `-C` 或 `-UC` 默认选 `硬字幕`，其余默认 `无字幕`。显式分类优先。VR 默认并且只允许 `无字幕`。同一番号各分段共用分类。选 `无字幕` 时去掉字幕后缀 `C`，无码后缀 `U` 继续按原有规则处理。三种有字幕分类都会保留或添加 `C`。原先的替换输入字段 `subtitle_zh` 和 `X-Subtitle-Zh` 不再决定字幕分类。

`CN`、`DUMMY`、`WEST` 使用相同的字幕和文件命名规则，跳过刮削。刮削保留已保存的字幕分类。

上传和替换的完整请求示例见[上传客户端接入手册](api-upload-client-guide.md)。

## 修改元数据和字幕分类

| 方法和路径 | 请求 | 响应 |
| --- | --- | --- |
| `PATCH /api/videos/{code}` | JSON 可编辑 `code`、`title_jp`、`title_zh`、`title_zh_short`、`release_date`、`duration`、`maker`、`publisher`、`series`、`score`、`actors`、`tags`、`subtitle_classification` | `{new_code, rename_job_ids, ...影片详情}` |
| `POST /api/videos/{code}/cover` | multipart 字段 `file`。支持 JPG、JPEG、PNG、WebP | `{cover_path}` |
| `PATCH /api/actors/{actor_id}` | JSON `name` 和/或 `name_zh` | `{merged, actor, rename_job_ids}`。合并时另有 `redirect_id` |
| `POST /api/actors/{actor_id}/avatar` | multipart 字段 `file`。支持 JPG、JPEG、PNG、WebP、GIF | `{avatar_path}` |
| `GET /api/videos/subtitle-review` | 无 | 待审核的 MKV 番号数组，每项含 `code`、`cover_path`、`title`、`file_count`、`size_gib`、`classification` |
| `POST /api/videos/{code}/subtitle-classification` | JSON `classification` 取四种合法分类之一 | `{ok, code, classification}`，更新标签并记为已审核 |

元数据 PATCH 的 `actors` 接受姓名字符串或 `{name, name_zh}` 对象组成的数组，`tags` 接受标签名数组。传入数组会替换整组关联，传空数组会清空，省略对应字段则保留。改番号、标题或演员后，如果响应含 `rename_job_ids`，可逐个查询文件重命名任务。

## 刮削和异步任务

| 方法和路径 | 请求 | 响应 |
| --- | --- | --- |
| `POST /api/videos/rescrape` | JSON `scrape_status` 为 `FAILED` 或 `PENDING` | `202` 和 `{job_id, codes}`，没有符合条件的普通番号时返回 `400` |
| `POST /api/videos/{code}/rescrape` | 完整在库番号，请求体可空。特殊番号不参与刮削 | `202` 和 `{job_id, code}` |
| `GET /api/jobs/{task_id}` | 提交接口返回的 `job_id` | 任务记录，含 `id`、`kind`、`status`、`error` 和解码后的 `result`。无任务时返回 `404` |
| `GET /api/jobs/{job_id}/stream` | 可用 `Last-Event-ID` 或 `after` 从上次事件续读 | SSE，`data` 为 JSON。处理 `step: "ping"` 心跳，收到 `done` 或 `error` 后停止等待并查询任务结果 |
| `GET /api/scrape-progress` | 无 | 最近 24 小时的影片刮削进度数组，含 `id`、`kind`、`code`、`status`、`phase`、`message`、`created_at`、`files`、`video` |

轮询任务详情时，`DONE`、`FAILED`、`SKIPPED`、`CANCELLED` 为终态。批量任务需再看 `result` 中各番号的结果。等待状态可能与后台服务暂停或浏览器访问间隔有关。队列行为详见[刮削队列文档](scrape-queue.md)。

```bash
BASE=http://127.0.0.1:8000
curl -fsS "$BASE/api/jobs/$JOB_ID"
curl -fsSN "$BASE/api/jobs/$JOB_ID/stream"
```

## 管理 API

这些接口供受信任的第三方运维程序调用。

| 方法和路径 | 请求 | 响应 |
| --- | --- | --- |
| `GET /api/admin/services` | 无 | `{services, browser}`，含 Consumer、Browser Service 和浏览器设置 |
| `POST /api/admin/services/{name}/control` | `name` 为 `consumer` 或 `browser`。JSON `action` 为 `start`、`stop`、`pause`、`resume`、`restart` | 服务状态对象。用状态接口确认进程实际状态 |
| `GET /api/admin/browser/settings` | 无 | 浏览器设置对象 |
| `PATCH /api/admin/browser/settings` | JSON 可改 `interval_min`、`interval_max`、`backoff_base`、`backoff_max`。单位秒，范围 1–86400，每组最小值不得超过最大值 | 更新后的设置 |
| `GET /api/admin/browser/usage` | 可选 `date=YYYY-MM-DD`，以 `America/Los_Angeles` 计，默认当天 | `date`、`today`、`timezone`、`tracking_since`、`quota`、`effective_at` 和逐小时 `hours` |
| `PUT /api/admin/browser/quota` | JSON 必须包含 `peak_hour`（整数 0–23）、`peak_limit`（整数 1–3600）、`spread`（0.5–12 小时） | `{quota, effective_at}`，含 24 小时额度，下一整点生效 |
| `POST /api/admin/browser/{action}` | `action` 为 `login-status` 或 `open-login`，请求体可空 | `202` 和 `{job_id}` |
| `GET /api/admin/{resource}` | `resource` 为 `workflows`、`commands`、`events`、`history`、`audit`。查询参数见下文 | `{items, total, offset, limit}` |
| `GET /api/admin/workflows/{task_id}` | 任务 ID | 与任务详情接口相同 |
| `POST /api/admin/workflows/{task_id}/{action}` | `cancel`、`retry` 或 `priority`。修改优先级传 JSON `priority`，范围为 -10000 至 10000 的整数 | `{ok, job_id}` |
| `POST /api/admin/events/{event_id}/redeliver` | 整数结果事件 ID，请求体可空 | 待消费事件返回 `{ok, message}`，可重试的已确认失败事件返回 `202` 和 `{ok, job_id}`，其他情形返回 `400` |
| `GET /api/admin/series/reports` | 无 | 最近 20 份系列聚合报告，含 `id`、`status`、`created_at`、`error`、`published_at`、`version`、`round` |
| `POST /api/admin/series/reports` | 请求体可空，生成或复用正在执行的报告 | `202` 和 `{id}` |
| `GET /api/admin/series/reports/{report_id}` | 报告 ID | `{status, error, result, published_at, published_groups}` |
| `POST /api/admin/series/reports/{report_id}/next` | 用已完成报告和审核分组生成下一轮。传下文的 JSON | `202` 和 `{id}`，报告冲突返回 `409` |
| `POST /api/admin/series/reports/{report_id}/publish` | 对已完成报告发布审核分组。传下文的 JSON | `{ok}`，报告或成员冲突返回 `409` |

管理列表用 `limit`（默认 50，最大 200）和 `offset`（默认 0）分页。`status` 适用于 `workflows` 和 `commands`。`task_id` 适用于除 `audit` 以外的列表。列表项保留任务和事件记录的原始字段，部分 `*_json` 字段为序列化字符串。

系列报告的下一轮和发布接口均传 `{"groups":[{"name":"系列名","member_ids":[12,34],"final_id":null}]}`。成员 ID 从报告取得，`final_id` 可省略或指向所选成员已有的归并系列。单个尚未归并的成员无法单独创建新组。上一轮的审核组须整体保留或合并。报告生成后若成员或译名已变更，需重新生成报告再提交。

### 兼容和停用接口

| 方法和路径 | 请求及响应 |
| --- | --- |
| `GET /api/worker/status` | 返回 `{worker}`，对应 Consumer 状态 |
| `POST /api/worker/control` | JSON `action` 同服务控制接口，返回 `{worker}` |
| `POST /api/javdb/session/close` | 停止 Browser Service，请求体可空，返回服务状态 |
| `GET /api/ltx`、`POST /api/ltx`、`PATCH /api/ltx`、`DELETE /api/ltx` | 已停用，返回 `410` 和 `{error, enabled:false}` |
| `GET /api/ltx/{path}`、`POST /api/ltx/{path}`、`PATCH /api/ltx/{path}`、`DELETE /api/ltx/{path}` | 已停用的 LTX 子路径，返回同样的 `410` |

带 `GET` 的路由也支持 Flask 自动提供的 `HEAD`。框架还会为路由生成默认的 `OPTIONS` 响应。
