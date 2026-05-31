# Replace Video Feature — Design Spec
Date: 2026-05-30

## Overview

Add a "替换视频" button to the movie detail page. Clicking it opens a modal that lets the user upload a new video file to replace the existing one, with control over filename suffixes (中文字幕/无码) and part numbering. After a verified upload, the original file is deleted atomically and the DB is updated.

## Architecture

**Files changed:**
- `web/templates/index.html` — modal UI + JS logic (inline in VideoDetail component)
- `web/backend/app.py` — new `POST /api/videos/<code>/replace` endpoint + `MAX_CONTENT_LENGTH`
- `dal/db_manager.py` — new `replace_video_file()` function

## End-to-End Flow

```
User clicks "替换视频"
  → Modal opens
  → User selects original_part (only if video has multiple parts)
  → User picks file → editable dest_name shown
  → User adjusts toggles → final filename previewed in real time
  → User clicks "上传"
  → XHR multipart POST /api/videos/<code>/replace?part=<original_part>
  → Server saves as <dest_path>.temp
  → Server: getsize(.temp) == file_size?
      ✗ → delete .temp → return 400 → frontend shows red error
      ✓ → delete original file → rename .temp → dest_path
          → update DB (original_file_path + tags)
          → return 200
  → Frontend closes modal, refreshes video data
```

## Filename Computation Logic

### Step 1 — Extract base stem from original path

Strip any existing `-C`, `-U`, `-UC` suffix from the original filename stem:

```python
re.sub(r'-(UC|C|U)$', '', stem, flags=re.IGNORECASE)
```

Examples:
- `/vol/folder/SSIS-001-UC.mp4` → stem = `SSIS-001`
- `/vol/folder/SSIS-001-C.mp4` → stem = `SSIS-001`
- `/vol/folder/SSIS-001.mp4`   → stem = `SSIS-001`

### Step 2 — Show editable dest_name after file selection

Replace extension with the uploaded file's extension:
- Original: `SSIS-001-UC.mp4`, uploaded: `upload.mp4` → show `SSIS-001.mp4`
- Original: `SSIS-001-UC.mp4`, uploaded: `upload.mkv` → show `SSIS-001.mkv`

User may freely edit this field (including the extension).

### Step 3 — Compute suffix from toggles

| 中文字幕 | 无码破解 | 原装无码 | suffix |
|---------|---------|---------|--------|
| off | off | off | (none) |
| on  | off | off | `-C`   |
| off | on  | off | `-U`   |
| off | off | on  | `-U`   |
| on  | on  | off | `-UC`  |
| on  | off | on  | `-UC`  |
| on  | on  | on  | `-UC`  |

无码破解 and 原装无码 both map to `-U`. Both on simultaneously still produces one `-U`.

### Step 4 — Append suffix and part

```
final_name = {stem_without_ext}{suffix}[.part{N}]{ext}
```

Examples:
- dest_name=`SSIS-001.mp4`, suffix=`-C`, part=off  → `SSIS-001-C.mp4`
- dest_name=`SSIS-001.mp4`, suffix=`-UC`, part=2   → `SSIS-001-UC.part2.mp4`
- dest_name=`SSIS-001.mp4`, suffix=(none), part=1  → `SSIS-001.part1.mp4`

**Final file path** = original file's directory + final filename (directory is unchanged).

## UI Layout

### Trigger

"替换视频" button added to the playback buttons row:
```
[ ▶ Play ]  [ 复制 URL ]  [ 替换视频 ]
```

### Modal Structure

```
┌─────────────────────────────────────┐
│  替换视频                        [×] │
├─────────────────────────────────────┤
│  替换 Part  [Part 1 ▾]              │  ← only shown if video has multiple parts
├─────────────────────────────────────┤
│  [选择文件]  upload.mp4              │
├─────────────────────────────────────┤
│  中文字幕    [ NO / YES ]            │
│  无码破解    [ NO / YES ]            │
│  原装无码    [ NO / YES ]            │
│  Part        [ OFF / ON ]           │
│               选择: [1][2][3][4]    │  ← only when Part = ON
├─────────────────────────────────────┤
│  目标文件名  [SSIS-001.mp4_______]  │  ← editable input
│  最终文件名  SSIS-001-C.part2.mp4   │  ← read-only, computed in real time
├─────────────────────────────────────┤
│  ████████░░░░░░░  64%               │  ← shown during upload only
│  12.4 MB/s · 已用 0:32 · 剩余 0:18  │
│  ⚠ 上传失败：文件大小不匹配          │  ← red, shown on failure
├─────────────────────────────────────┤
│            [取消]  [上传]            │
└─────────────────────────────────────┘
```

### Interaction Rules

- "上传" disabled until a file is selected
- During upload: "上传" → "上传中…" (disabled), all inputs locked
- On success: modal closes, video data refreshes
- On failure: red error shown, inputs unlocked, user may retry
- "替换 Part" selector: only visible when `fileParts.length > 1`; defaults to currently selected playback part; pre-fills the dest Part toggle to match

### Progress Bar Computation

All values derived from XHR `upload.onprogress` events:
- **Speed**: sliding average of `loaded_delta / time_delta`, updated every second
- **Elapsed**: `now - upload_start_time`, formatted as `m:ss`
- **Remaining**: `(total - loaded) / avg_speed`, formatted as `m:ss`

## Backend Endpoint

`POST /api/videos/<code>/replace?part=<original_part>`

### Multipart Form Fields

| Field | Type | Description |
|-------|------|-------------|
| `file` | file | the video file |
| `file_size` | int | client-side byte count for verification |
| `dest_name` | str | user-confirmed filename (with extension, without suffix/part) |
| `subtitle_zh` | 0/1 | 中文字幕 toggle |
| `uncensored_leaked` | 0/1 | 无码破解 toggle |
| `uncensored_native` | 0/1 | 原装无码 toggle |
| `part` | str or empty | destination part number ("1", "2", … or empty) |

### Server Execution Order

1. Query original file path from DB via `get_video_file_path(code, original_part)`
2. Compute final filename (suffix + part logic)
3. `dest_path` = original directory / final filename
4. Save uploaded file to `dest_path + ".temp"`
5. `os.path.getsize(temp) == file_size`?
   - No → delete `.temp` → return 400
   - Yes → continue
6. Delete original file
7. `rename(temp, dest_path)`
8. Call `replace_video_file(code, original_part, dest_path, tags_delta)`
9. Return 200 + `{ new_path, tags }`

### Flask Config

```python
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024 * 1024  # 50 GB
```

## DB Update — `replace_video_file()`

```sql
UPDATE videos SET original_file_path = :dest_path
WHERE code = :code AND (part = :original_part OR (part IS NULL AND :original_part IS NULL))
```

Tag delta logic for each of 中文字幕 / 无码破解 / 原装无码:
- toggle=on  AND tag not in current record → `INSERT INTO video_tag_link`
- toggle=off AND tag in current record     → `DELETE FROM video_tag_link`
