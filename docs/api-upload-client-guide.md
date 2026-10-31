# AVideo 上传客户端接入手册

本文档供外部客户端通过 HTTP API 向 AVideo 新增或替换视频文件。

## 1. 基本信息

- 服务地址示例：`http://10.0.0.30:8000`
- API 前缀：`/api`
- 当前接口没有身份认证。只应在可信内网中使用。
- 单个上传请求最大为 50 GiB。
- 文件内容使用原始 HTTP body 上传，不使用 Base64。
- 新增影片是异步任务；替换影片是同步请求。
- 客户端应设置较长或无限的上传超时。

本文中的 `BASE_URL`：

```text
http://10.0.0.30:8000
```

## 2. 推荐工作流

```text
选择文件
  -> batch-check 检测番号和数据库状态
  -> 按影片分组
  -> NOT_FOUND: upload-part -> add-auto -> SSE
  -> ACTIVE:    POST /replace
  -> DELETED:   停止并交由用户处理
```

不要仅根据上传文件名决定新增或替换，必须先调用 `batch-check`。

## 3. 文件名要求

推荐使用以下格式：

```text
ABW-123.mp4
ABW-123-C.mp4
IPVR-179-U.part1.mp4
IPVR-179-U.part2.mp4
```

后缀含义：

| 后缀 | 含义 |
|---|---|
| 无后缀 | 有码、无中文字幕 |
| `-C` | 有码中文 |
| `-U` | 无码 |
| `-UC` | 无码中文 |

多 part 文件必须明确包含 `.partN`、`-partN` 或其他可识别的 part 标记。替换已有多 part 影片时，不允许省略 part。

## 4. 检测文件

### 请求

```http
POST /api/videos/batch-check
Content-Type: application/json
```

```json
{
  "filenames": [
    "IPVR-179-U.part2.mp4",
    "ABW-123-C.mp4"
  ]
}
```

### 响应示例

```json
[
  {
    "filename": "IPVR-179-U.part2.mp4",
    "code": "IPVR-179-U",
    "part": "part2",
    "db_code": "IPVR-179-U",
    "record_exists": true,
    "exists": true,
    "status": "ACTIVE",
    "scrape_status": "SUCCESS",
    "original_path": "/Volumes/T71/avideo/IPVR/IPVR-179-U....part1.mp4",
    "db_parts": ["part1", "part2"]
  }
]
```

### 字段说明

| 字段 | 说明 |
|---|---|
| `filename` | 客户端提交的原始文件名 |
| `code` | 从文件名解析出的番号 |
| `part` | 解析出的 part；单文件通常为 `null` |
| `db_code` | 数据库中的真实番号；替换时必须使用此值 |
| `record_exists` | 数据库是否存在记录，包括已删除记录 |
| `exists` | 是否存在有效记录 |
| `status` | `ACTIVE`、`DELETED` 或 `NOT_FOUND` |
| `scrape_status` | 刮削状态 |
| `original_path` | 当前文件路径 |
| `db_parts` | 数据库中已有的 part 列表 |
| `error` | 文件名无法解析时的错误 |

`ABW-123`、`ABW-123-C`、`ABW-123-U` 和 `ABW-123-UC` 在检测阶段会按同一基础番号匹配。因此，替换请求必须使用响应中的 `db_code`，不能直接使用 `code`。

## 5. 获取可用 Volume

```http
GET /api/config/media-libraries
```

```bash
curl -s "$BASE_URL/api/config/media-libraries"
```

新增影片时，`volume_path` 必须是该接口返回的完整路径之一。替换影片不需要选择 Volume，文件会写入原文件所在目录。

## 6. 新增影片

新增分为三步：创建上传组、上传文件、启动自动处理。

### 6.1 创建 upload_id

客户端生成标准 UUID。一个 `upload_id` 只能包含同一个影片番号的文件。

```text
550e8400-e29b-41d4-a716-446655440000
```

不同番号必须使用不同的 `upload_id`。

### 6.2 上传文件

```http
POST /api/videos/upload-part
X-Upload-Id: <UUID>
X-Volume-Path: /Volumes/T71/avideo
X-Dest-Name: IPVR-179-U.part1.mp4
X-File-Size: 6136402186

<raw file bytes>
```

成功响应：

```json
{"ok": true, "name": "IPVR-179-U.part1.mp4"}
```

`X-File-Size` 必须与实际收到的字节数一致。否则服务器删除临时文件并返回 `400`。

包含中文或特殊字符的 `X-Dest-Name`、`X-Volume-Path` 应使用 URL percent-encoding。服务端会执行 URL decode。

### 6.3 多 part 上传

多 part 使用相同 `upload_id`，每个文件分别调用一次 `upload-part`：

```text
upload_id=A
  IPVR-179-U.part1.mp4
  IPVR-179-U.part2.mp4

upload_id=B
  ABW-123-C.mp4
```

只有所有 part 上传成功后，才能调用 `add-auto`。

### 6.4 启动自动处理

```http
POST /api/videos/add-auto
Content-Type: application/json
```

```json
{
  "upload_id": "550e8400-e29b-41d4-a716-446655440000",
  "volume_path": "/Volumes/T71/avideo",
  "uncensored_crack_tag": "LTX 无码破解",
  "subtitle_classification": ""
}
```

响应：

```json
{"job_id": "550e8400-e29b-41d4-a716-446655440000"}
```

标签允许值：

| 字段 | 允许值 |
|---|---|
| `uncensored_crack_tag` | `""`、`"JAV 无码破解"`、`"LADA 无码破解"`、`"LTX 无码破解"` |
| `subtitle_classification` | `""`、`"机翻中文"`、`"中文字幕"` |

重要：有码影片必须显式发送：

```json
"uncensored_crack_tag": ""
```

如果完全省略 `uncensored_crack_tag`，服务端默认使用 `LTX 无码破解`。

### 6.5 监听处理进度

```http
GET /api/jobs/<job_id>/stream
Accept: text/event-stream
```

这是 SSE 流。每条消息格式如下：

```text
data: {"step":"scrape","status":"running","message":"刮削影片信息（JavDB）…"}

data: {"step":"done","status":"done","message":"全部完成","code":"IPVR-179-U"}
```

终止事件：

- `step == "done"`：成功
- `step == "error"`：失败

`step == "ping"` 是保活消息，应忽略。

客户端应在收到 `add-auto` 响应后立即连接 SSE。任务保存在服务进程内存中，服务重启后无法恢复任务或进度流。

## 7. 替换已有影片

替换前必须先执行 `batch-check`，并使用返回的：

- `db_code` 作为 URL 中的番号；
- `part` 作为替换目标；
- 已有影片属性作为标签默认值。

不要因为上传文件名缺少 `-U`、`-C` 或 `-UC`，自动清除数据库中已有的影片属性。

### 7.1 替换单文件或指定 part

```http
POST /api/videos/IPVR-179-U/replace?part=part2
X-Dest-Name: IPVR-179-U.神菜美舞.%5B标题%5D.mp4
X-File-Size: 8880789106
X-Part: part2
X-Subtitle-Zh: 0
X-Subtitle-Classification:
X-Uncensored-Leaked: 1
X-Uncensored-Crack-Tag: LTX%20无码破解
X-Uncensored-Native: 0

<raw file bytes>
```

后端会根据 `X-Part` 自动在最终文件名末尾加入 `.part2`。`X-Dest-Name` 可以不包含 `.part2`。

成功响应包含更新后的影片详情：

```json
{
  "ok": true,
  "code": "IPVR-179-U",
  "parts": []
}
```

### 7.2 替换请求字段

| Header | 必填 | 说明 |
|---|---:|---|
| `X-Dest-Name` | 是 | 目标基础文件名；特殊字符需要 URL encode |
| `X-File-Size` | 是 | 文件字节数，必须大于 0 |
| `X-Part` | 多 part 必填 | 例如 `part2` |
| `X-Subtitle-Zh` | 是 | `1` 或 `0` |
| `X-Subtitle-Classification` | 建议 | 空、`机翻中文` 或 `中文字幕` |
| `X-Uncensored-Leaked` | 是 | 破解无码为 `1` |
| `X-Uncensored-Crack-Tag` | 建议 | 空或具体破解类型 |
| `X-Uncensored-Native` | 是 | 原装无码为 `1` |
| `X-Replace-Whole` | 否 | `1` 表示用单文件替换整个 multipart 影片 |

URL 查询参数 `part` 表示数据库中要被替换的原 part；`X-Part` 表示新文件保存后的 part。通常两者相同。

### 7.3 用单文件替换整个 multipart 影片

```http
POST /api/videos/ABW-123-C/replace?part=part1
X-Replace-Whole: 1
```

该操作会删除同番号的其他 part 文件和数据库记录，属于破坏性操作。客户端必须要求用户明确确认。

## 8. 批量处理规则

服务端没有单一的“批量上传并全部处理”端点。客户端负责调度。

推荐算法：

1. 将所有文件名提交给 `batch-check`。
2. 拒绝所有带 `error` 的项目。
3. `DELETED` 项目停止处理，提示人工确认。
4. 替换项目按 `db_code` 分组；新增项目按 `code` 分组。
5. 同一个影片的多个 part 必须放在同一组。
6. 新增组各自创建一个 `upload_id`，依次上传各 part，然后调用一次 `add-auto`。
7. 替换组对每个 part 分别调用一次 `/replace`。
8. 同一番号的替换请求顺序执行，不要并发修改同一影片。
9. 不同番号可以有限并发，建议并发数不超过 2，避免磁盘和刮削浏览器过载。
10. 单项失败不应阻止其他番号继续处理，但必须保留失败原因。

伪代码：

```text
results = batchCheck(files)
groups = groupResults(results)

for group in groups:
    if group.status == DELETED or group.hasError:
        reportAndSkip(group)
    else if group.exists:
        for file in group.files sequentially:
            replace(db_code=group.db_code, part=file.part, file=file)
    else:
        upload_id = uuid()
        for file in group.files sequentially:
            uploadPart(upload_id, file)
        job_id = addAuto(upload_id, group.options)
        waitForSSE(job_id)
```

## 9. curl 示例

### 新增单文件

以下命令适用于 macOS：

```bash
BASE_URL='http://10.0.0.30:8000'
VOLUME='/Volumes/12TB/avideo'
FILE='ABW-123-C.mp4'
UPLOAD_ID=$(uuidgen | tr 'A-F' 'a-f')
SIZE=$(stat -f%z "$FILE")

curl --fail-with-body \
  -X POST "$BASE_URL/api/videos/upload-part" \
  -H "X-Upload-Id: $UPLOAD_ID" \
  -H "X-Volume-Path: $VOLUME" \
  -H "X-Dest-Name: $FILE" \
  -H "X-File-Size: $SIZE" \
  --data-binary "@$FILE"

RESPONSE=$(curl --fail-with-body -s \
  -X POST "$BASE_URL/api/videos/add-auto" \
  -H 'Content-Type: application/json' \
  -d "{
    \"upload_id\": \"$UPLOAD_ID\",
    \"volume_path\": \"$VOLUME\",
    \"uncensored_crack_tag\": \"\",
    \"subtitle_classification\": \"中文字幕\"
  }")

JOB_ID=$(printf '%s' "$RESPONSE" | jq -r '.job_id')
curl -N "$BASE_URL/api/jobs/$JOB_ID/stream"
```

Linux 获取文件大小应使用：

```bash
SIZE=$(stat -c%s "$FILE")
```

### 新增 multipart

```bash
UPLOAD_ID=$(uuidgen | tr 'A-F' 'a-f')

for FILE in IPVR-179-U.part1.mp4 IPVR-179-U.part2.mp4; do
  SIZE=$(stat -f%z "$FILE")
  curl --fail-with-body \
    -X POST "$BASE_URL/api/videos/upload-part" \
    -H "X-Upload-Id: $UPLOAD_ID" \
    -H "X-Volume-Path: /Volumes/T71/avideo" \
    -H "X-Dest-Name: $FILE" \
    -H "X-File-Size: $SIZE" \
    --data-binary "@$FILE" || exit 1
done
```

上传完成后对该 `UPLOAD_ID` 调用一次 `add-auto`。

## 10. HTTP 状态与重试

| 状态 | 含义 | 客户端处理 |
|---|---|---|
| `200` | 成功 | 继续下一步 |
| `400` | 参数、文件名、大小或标签错误 | 修正请求，不自动重试 |
| `404` | 影片、任务或 part 不存在 | 重新执行检测 |
| `409` | 路径或数据库目标冲突 | 停止并要求人工处理 |
| `413` | 请求超过 50 GiB | 不可通过当前接口上传该文件 |
| `500` | 服务端或文件操作失败 | 记录响应；确认状态后再决定是否重试 |

上传连接中断后，可使用相同 `upload_id` 和文件名重新调用 `upload-part`。服务端会从头覆盖该临时文件，不支持断点续传。

不要在不知道 `add-auto` 是否已启动的情况下盲目重复提交。应先检查影片详情：

```http
GET /api/videos/<code>
Accept: application/json
```

## 11. 客户端完成条件

新增影片只有在 SSE 收到 `step: done` 后才算完成。响应中的 `code` 是最终番号，客户端应使用它生成详情页链接：

```text
http://10.0.0.30:8000/#/video/<code>
```

替换影片在 `/replace` 返回 HTTP `200` 且 JSON 中 `ok: true` 后完成。响应中的 `code` 可能因标签选择发生变化，后续操作必须使用响应中的新番号。

## 12. 已知限制

- 不支持断点续传。
- 不支持查询上传临时文件的进度或列表。
- 异步任务状态不持久化，服务重启后丢失。
- `add-auto` 一次只能处理一个番号组。
- JavDB 刮削依赖服务端常驻浏览器状态。
- 当前接口无认证、无 API key、无访问频率限制。
