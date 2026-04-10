# Usage Guide

所有命令均在项目根目录下执行，使用 `.venv` 虚拟环境。

---

## 初始化（首次运行）

```bash
# 安装依赖
pip install -r requirements.txt
playwright install chromium

# 初始化数据库（建表 + 迁移）
.venv/bin/python dal/schema.py
```

---

## 日常工作流

### 第一步：扫描本地文件

将磁盘上的视频文件扫描入库，提取番号、分集信息和文件元数据（size、mtime、birthtime）。

```bash
# 扫描 config.py 中配置的所有目录
.venv/bin/python core/scanner.py

# 或指定单个目录
.venv/bin/python core/scanner.py /Volumes/12TB
```

- 已入库的文件自动跳过（`INSERT OR IGNORE`）
- 新文件以 `PENDING` 状态入库，等待刮削

---

### 第二步：刮削视频元数据

从 JavDB 抓取标题、封面、演员、标签、评分等元数据。

**前置：注入登录 Cookie（首次 / Cookie 过期时操作）**

1. 在浏览器登录 JavDB，用 Cookie Editor 扩展导出为 JSON（不加密）
2. 将文件保存为项目根目录下的 `javdb_cookie.json`
3. 刮削器启动时会自动注入，无需其他操作

```bash
# 刮削 PENDING 状态（默认）
.venv/bin/python core/auto_scraper.py

# 重试 FAILED 状态
.venv/bin/python core/auto_scraper.py --status=failed
```

- 自动轮询所有指定状态的视频
- 无任务时休眠后再次检查，可长期后台运行
- 内置随机延迟，防止触发 JavDB 封控

---

### 第三步：刮削演员头像

为没有头像的演员抓取头像和中文译名。

```bash
.venv/bin/python core/auto_actor_scraper.py
```

- 自动跳过已有头像的演员和 `is_ignored=1` 的男优
- 发现同一头像时自动合并重名演员记录
- 可与视频刮削同时运行（操作不同表，互不干扰）

---

### 第四步：翻译标题

调用本地 Ollama（gemma4:e4b）将日文标题翻译为中文。

```bash
# 只翻译尚未翻译的（默认）
.venv/bin/python utils/translate_titles.py

# 强制全量重新翻译
.venv/bin/python utils/translate_titles.py --retranslate

# 指定 Ollama 地址或模型
.venv/bin/python utils/translate_titles.py --host http://10.0.0.43:11434 --model gemma4:e4b
```

- 按 `title_jp` 去重，避免同番号多分集重复请求
- 翻译前自动去除番号、颜文字、特殊符号，保留日中英文字
- 不剥离女优名字（保留完整语义）
- prompt 风格：原味、简短、不做道德过滤
- 依赖 Ollama 服务（默认 `http://10.0.0.43:11434`），需提前确认可达

---

## 启动 Web 服务

```bash
.venv/bin/python -m flask --app web.backend.app run --host 0.0.0.0 --port 8000
```

访问 `http://localhost:8000`

---

## 高级工具

### 系列名聚类（去重合并）

将数据库中拼写相近或属于同一系列的 `series` 字段合并为统一名称，分两步走：

**Step 1：发现并生成聚类方案**

```bash
.venv/bin/python utils/cluster_series.py
```

- 使用 NLP 相似度 + Ollama（qwen2.5:32b）双重验证
- 结果写入 `series_clusters` 表（`is_reviewed=0`）和 `data/logs/cluster_series_dryrun_zh.md`
- 需要人工审查表中数据，确认无误的行将 `is_reviewed` 改为 `1`

**Step 2：应用已审核的结果**

```bash
# 先确认（开启 DRY_RUN=True，默认）
.venv/bin/python utils/cluster_series.py --apply

# 在 cluster_series.py 中将 DRY_RUN 改为 False 后再执行
.venv/bin/python utils/cluster_series.py --apply
```

---

### 查找重复视频

找出数据库中 `(code, part)` 相同但有多个物理文件的重复记录。

```bash
.venv/bin/python utils/find_duplicates.py
```

---

### 修复分集标记

重新用扫描器的正则逻辑对数据库中已有记录的 `part` 字段进行校正。

```bash
.venv/bin/python utils/fix_db_parts.py
```

---

### 命令行翻译调试

交互式测试 Ollama 翻译效果。

```bash
.venv/bin/python utils/translate_cli.py
```

---

## 新视频入库标准流程

```
有新视频到达磁盘
    ↓
python core/scanner.py          # 扫描，新文件入库为 PENDING
    ↓
python core/auto_scraper.py     # 刮削元数据（可长期后台运行）
    ↓
python core/auto_actor_scraper.py  # 刮演员头像（可同时跑）
    ↓
python utils/translate_titles.py   # 翻译未翻译的标题
```

---

## 常见问题

**Q: 刮削时遇到 Cloudflare 拦截**
脚本会自动处理 5 秒盾和 18 岁确认弹窗。若长时间卡住，检查网络是否能正常访问 `javdb.com`。

**Q: 翻译脚本报连接错误**
用 `--host` 参数指定正确的 Ollama 地址，或确认默认地址 `http://10.0.0.43:11434` 可达。

**Q: 某部片子刮削失败（FAILED 状态）**
批量重试所有失败记录：
```bash
.venv/bin/python core/auto_scraper.py --status=failed
```
重试单条记录，手动重置状态后运行：
```bash
sqlite3 data/avideo.db "UPDATE videos SET scrape_status='PENDING' WHERE code='XXXXX';"
.venv/bin/python core/auto_scraper.py
```

**Q: 如何只对单个番号测试刮削**
修改 `core/scraper/video_scraper.py` 底部 `__main__` 块中的 `test_code`，直接运行该文件：
```bash
.venv/bin/python core/scraper/video_scraper.py
```
