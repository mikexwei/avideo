import logging
import os
import random
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional, Tuple
import re
import requests
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError

# 动态引入 config
import sys
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import COVERS_DIR, AVATARS_DIR, JAVDB_COOKIES_FILE

logger = logging.getLogger(__name__)

# Sentinel returned by scrape_video_info when the search returned results but none
# of them matched the requested code exactly — caller freezes the record for manual
# handling instead of scraping a wrong match.
FREEZE = 'FREEZE'

_SAMESITE_MAP = {
    "no_restriction": "None",
    "lax": "Lax",
    "strict": "Strict",
    "unspecified": "None",
}

def load_javdb_cookies(context):
    """从 Cookie Editor 导出的 JSON 注入 Cookie 到 Playwright context。"""
    import json
    if not JAVDB_COOKIES_FILE.exists():
        logger.warning(f"⚠️ 未找到 Cookie 文件: {JAVDB_COOKIES_FILE}，将以未登录状态运行。")
        return
    try:
        raw = json.loads(JAVDB_COOKIES_FILE.read_text(encoding="utf-8"))
        cookies = []
        for c in raw:
            same_site_raw = (c.get("sameSite") or "").lower()
            same_site = _SAMESITE_MAP.get(same_site_raw, "None")
            cookie = {
                "name": c["name"],
                "value": c["value"],
                "domain": c["domain"],
                "path": c.get("path", "/"),
                "secure": c.get("secure", False),
                "httpOnly": c.get("httpOnly", False),
                "sameSite": same_site,
            }
            exp = c.get("expirationDate")
            if exp:
                cookie["expires"] = int(exp)
            cookies.append(cookie)
        context.add_cookies(cookies)
        logger.info(f"🍪 成功注入 {len(cookies)} 条 JavDB Cookie，已登录状态就绪。")
    except Exception as e:
        logger.error(f"❌ 注入 Cookie 失败: {e}")

# JavDB 基础域名 (如果国内被墙，后续可配置为镜像站域名如 javdb36.com 等)
BASE_URL = "https://javdb.com"

# 全局刮削计数器，用于触发周期性的深度防封禁等待
scrape_count = 0

JAVDB_PROFILE_DIR = project_root / "data" / "javdb_chrome_profile"
JAVDB_PROFILE_LOCK_FILE = project_root / "data" / "javdb_chrome_profile.lock"

@contextmanager
def javdb_profile_lock(timeout: float = 600.0):
    """Cross-process lock for JavDB's persistent browser profile."""
    import fcntl

    JAVDB_PROFILE_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(JAVDB_PROFILE_LOCK_FILE, "a+")
    start = time.time()
    try:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() - start >= timeout:
                    raise TimeoutError("JavDB browser profile is still in use")
                time.sleep(1.0)
        yield
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()

def launch_javdb_browser(playwright, headless: bool = False):
    """Launch a browser suitable for JavDB/Cloudflare.

    JavDB's newer Cloudflare check blocks Playwright's bundled Chromium more
    aggressively. Prefer the locally installed Chrome browser, then fall back
    to bundled Chromium so machines without Chrome still work.
    """
    args = ['--disable-blink-features=AutomationControlled']
    channel = (os.getenv("JAVDB_BROWSER_CHANNEL") or "chrome").strip()
    if channel:
        try:
            return playwright.chromium.launch(
                channel=channel,
                headless=headless,
                args=args,
            )
        except Exception as e:
            logger.warning(f"⚠️ 启动 Chrome channel [{channel}] 失败，回退 Playwright Chromium: {e}")
    return playwright.chromium.launch(headless=headless, args=args)

def launch_javdb_context(playwright, headless: bool = False):
    """Launch JavDB in a persistent Chrome profile so Cloudflare state survives restarts."""
    args = ['--disable-blink-features=AutomationControlled']
    channel = (os.getenv("JAVDB_BROWSER_CHANNEL") or "chrome").strip()
    JAVDB_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    if channel:
        try:
            return playwright.chromium.launch_persistent_context(
                str(JAVDB_PROFILE_DIR),
                channel=channel,
                headless=headless,
                args=args,
                viewport={'width': 1280, 'height': 800},
            )
        except Exception as e:
            logger.warning(f"⚠️ 启动持久化 Chrome channel [{channel}] 失败，回退 Playwright Chromium: {e}")
    return playwright.chromium.launch_persistent_context(
        str(JAVDB_PROFILE_DIR),
        headless=headless,
        args=args,
        viewport={'width': 1280, 'height': 800},
    )

def simulate_human_behavior(page: Page, min_sec: float, max_sec: float):
    """模拟人类操作：随机移动鼠标、滚动滚轮，最后回到顶部"""
    total_sleep = random.uniform(min_sec, max_sec)
    start_time = time.time()
    
    # 获取视口大小以限制鼠标移动范围
    viewport = page.viewport_size
    width = viewport['width'] if viewport else 1280
    height = viewport['height'] if viewport else 800
    
    # 在 80% 的等待时间内进行动作模拟，留一点时间在顶部静默
    while time.time() - start_time < total_sleep * 0.8:
        if random.random() > 0.5:
            # 随机平滑移动鼠标
            x = random.randint(0, width)
            y = random.randint(0, height)
            try:
                page.mouse.move(x, y, steps=random.randint(5, 15))
            except:
                pass
        else:
            # 随机滚动滚轮 (正数为向下，负数为向上)
            try:
                page.mouse.wheel(0, random.randint(-400, 600))
            except:
                pass
        # 动作间的微小停顿
        time.sleep(random.uniform(0.15, 0.45))
        
    # 强制回到页面顶端
    try:
        page.evaluate("window.scrollTo(0, 0)")
    except:
        pass
        
    # 补齐剩下的等待时间
    elapsed = time.time() - start_time
    remaining = total_sleep - elapsed
    if remaining > 0:
        time.sleep(remaining)

def random_sleep(min_sec: float = 3.0, max_sec: float = 7.5):
    """模拟人类的随机停顿"""
    time.sleep(random.uniform(min_sec, max_sec))

def bypass_javdb_security(page: Page):
    """统一处理 JavDB 的 18岁弹窗 和 Cloudflare 验证"""
    over18_btn = page.locator('.over18-modal a.button.is-success, .over18-modal a:has-text("是"), a[href^="/over18?respond=1"]')
    try:
        over18_btn.first.wait_for(state="attached", timeout=5000)
    except Exception:
        pass
    if over18_btn.count() > 0:
        logger.info("🔞 触发 18 岁验证拦截，正在自动点击通过...")
        over18_btn.first.click(force=True)
        try:
            page.wait_for_selector(".over18-modal", state="detached", timeout=10000)
        except Exception:
            href = over18_btn.first.get_attribute("href")
            if href:
                over18_url = BASE_URL + href if href.startswith("/") else href
                page.goto(over18_url, timeout=30000)
        random_sleep(5.67, 10.75)
        
    try:
        cf_iframe = page.locator('iframe[src*="cloudflare"], iframe[src*="turnstile"]').first
        cf_iframe.wait_for(state="visible", timeout=4000)
        
        logger.info("🛡️ 遇到 Cloudflare 强制点击验证，尝试使用物理坐标模拟人类点击...")
        box = cf_iframe.bounding_box()
        if box:
            target_x = box['x'] + 35 
            target_y = box['y'] + box['height'] / 2
            
            page.mouse.move(target_x, target_y, steps=random.randint(15, 25))
            time.sleep(random.uniform(0.75, 9.0))
            page.mouse.click(target_x, target_y)
            logger.info("👆 已物理点击验证框，等待盾牌放行...")
    except Exception:
        # 没有遇到点击验证（或者被自动放行了），无视异常
        pass

def _is_cloudflare_challenge(page: Page) -> bool:
    """Return True when Cloudflare's managed challenge page is still active."""
    try:
        title = page.title()
        if "Just a moment" in title:
            return True
        body = page.locator("body").inner_text(timeout=1000)
        return (
            "Performing security verification" in body
            or "This website uses a security service" in body
        )
    except Exception:
        return False

def download_cover(image_url: str, code: str) -> Optional[str]:
    """下载封面图并返回本地相对路径"""
    try:
        # JavDB 的图片通常在 jdbimgs.com，防御相对较弱，直接用 requests 下载即可
        # 补全可能缺失的协议头
        if image_url.startswith('//'):
            image_url = 'https:' + image_url
        elif image_url.startswith('/'):
            image_url = BASE_URL + image_url
            
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://javdb.com/'
        }
        response = requests.get(image_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            # 获取图片后缀，如 .jpg (用 urlparse 避免 query string 污染后缀)
            ext = Path(urlparse(image_url).path).suffix
            if not ext:
                ext = ".jpg"
                
            filename = f"{code}{ext}"
            file_path = COVERS_DIR / filename
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            # 返回相对于 web 静态目录的路径，方便后续 Flask 直接调用
            return f"covers/{filename}"
        else:
            logger.warning(f"⚠️ 下载封面失败，HTTP 状态码: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"❌ 下载封面发生异常: {e}")
        return None

def download_avatar(image_url: str, actor_name: str) -> Optional[str]:
    """下载演员头像并返回本地相对路径"""
    # 如果是没有头像的占位图，直接跳过节约空间
    if not image_url or "noavatar" in image_url.lower() or "default" in image_url.lower():
        logger.warning(f"⚠️ {actor_name} 似乎使用了系统默认占位图，跳过下载。")
        return None

    try:
        # 补全可能缺失的协议头
        if image_url.startswith('//'):
            image_url = 'https:' + image_url
        elif image_url.startswith('/'):
            image_url = BASE_URL + image_url
            
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://javdb.com/'
        }
        response = requests.get(image_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            # 核心升级：利用 JavDB 图片 URL 自身的特征路径来生成文件名 (如 avatars/y8/y8A.jpg -> y8_y8A.jpg)
            parsed_url = urlparse(image_url)
            path_obj = Path(parsed_url.path)
            
            if len(path_obj.parts) >= 2:
                filename = f"{path_obj.parts[-2]}_{path_obj.name}"
            else:
                filename = path_obj.name
                
            file_path = AVATARS_DIR / filename
            
            # 物理去重：如果文件已经存在，说明之前另一个马甲已经下载过这张图了，直接跳过写入，减少磁盘损耗
            if not file_path.exists():
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"🖼️ 成功下载头像: {filename}")
            else:
                logger.info(f"⏭️ 头像物理文件已存在，跳过下载: {filename}")
            
            return f"avatars/{filename}"
        else:
            logger.warning(f"⚠️ 下载头像失败，HTTP 状态码: {response.status_code} [{actor_name}]")
        return None
    except Exception as e:
        logger.error(f"❌ 下载头像发生异常 [{actor_name}]: {e}")
        return None


def _female_actor_names_from_block(block) -> list[str]:
    """Extract only female actor links from a JavDB actor panel block."""
    actor_links = block.locator("span.value a")
    actors = actor_links.evaluate_all(
        """
        (links) => {
          const colorCategory = (value) => {
            const text = (value || '').toLowerCase();
            const colors = [];
            for (const match of text.matchAll(/#([0-9a-f]{3,8})\\b/g)) {
              let hex = match[1];
              if (hex.length === 3 || hex.length === 4) hex = [...hex].map(c => c + c).join('');
              if (hex.length === 8 && parseInt(hex.slice(6, 8), 16) === 0) continue;
              if (hex.length === 6 || hex.length === 8) {
                colors.push([0, 2, 4].map(i => parseInt(hex.slice(i, i + 2), 16)));
              }
            }
            for (const match of text.matchAll(/rgba?\\(\\s*([\\d.]+)[,\\s]+([\\d.]+)[,\\s]+([\\d.]+)(?:\\s*[,/]\\s*([\\d.]+%?))?\\s*\\)/g)) {
              if (match[4] && parseFloat(match[4]) === 0) continue;
              colors.push([Number(match[1]), Number(match[2]), Number(match[3])]);
            }
            for (const [r8, g8, b8] of colors) {
              const [r, g, b] = [r8, g8, b8].map(v => Math.max(0, Math.min(255, v)) / 255);
              const max = Math.max(r, g, b), min = Math.min(r, g, b), delta = max - min;
              if (max === 0 || delta / max < 0.25) continue;
              let hue;
              if (max === r) hue = 60 * (((g - b) / delta) % 6);
              else if (max === g) hue = 60 * ((b - r) / delta + 2);
              else hue = 60 * ((r - g) / delta + 4);
              if (hue < 0) hue += 360;
              if (hue <= 30 || hue >= 300) return 'female';
              if (hue >= 175 && hue <= 270) return 'male';
            }
            return null;
          };
          const colorBits = (el) => {
            const bits = [];
            for (const node of [el, el.nextElementSibling, el.previousElementSibling, el.parentElement]) {
              if (!node) continue;
              const style = window.getComputedStyle(node);
              bits.push(node.getAttribute('style'), style.color, style.backgroundColor, style.borderColor);
            }
            return bits;
          };
          return links
            .filter((link) => {
              const bits = colorBits(link);
              if (bits.some(value => colorCategory(value) === 'female')) return true;
              if (bits.some(value => colorCategory(value) === 'male')) return false;
              return !link.matches(':has(+ strong.symbol.male)');
            })
            .map((link) => link.textContent.trim())
            .filter(Boolean);
        }
        """
    )
    return [a.strip() for a in actors if a and a.strip()]


def parse_detail_page(page: Page, code: str) -> Dict:
    """在详情页提取所有元数据"""
    data = {
        'code': code,
        'title_jp': '',
        'release_date': '',
        'duration': '',
        'maker': '',
        'publisher': '',
        'series': '',
        'score': 0.0,
        'categories': '',
        'actors': '',
        'cover_path': ''
    }

    try:
        # 1. 抓取日文原标题 (后续留给大模型翻译)
        title_elem = page.locator(".current-title")
        if title_elem.count() > 0:
            data['title_jp'] = title_elem.first.inner_text().strip()

        
        # 2. 抓取封面图 URL (根据最新的精准 DOM 结构升级)
        cover_elem = page.locator("img.video-cover")
        
        if cover_elem.count() > 0:
            # 拿到 src 属性的值
            cover_url = cover_elem.first.get_attribute("src")
            if cover_url:
                logger.info(f"正在下载封面图: {cover_url}")
                # 调用下载图片的函数（这一步我们之前已经写好了）
                data['cover_path'] = download_cover(cover_url, code)

        # 3. 抓取面板信息 (日期、片商、演员等)
        panel_blocks = page.locator("div.panel-block")
        for i in range(panel_blocks.count()):
            block = panel_blocks.nth(i)
            # block_text 形如: "日期: 2023-01-01" 或 "演員: 三上悠亞, 橋本有菜"
            block_text = block.inner_text().strip()
            
            if "日期:" in block_text:
                data['release_date'] = block.locator("span.value").inner_text().strip()
            elif "時長:" in block_text:
                data['duration'] = block.locator("span.value").inner_text().strip()
            elif "片商:" in block_text:
                data['maker'] = block.locator("span.value").inner_text().strip()
            elif "發行:" in block_text:
                data['publisher'] = block.locator("span.value").inner_text().strip()
            elif "系列:" in block_text:
                data['series'] = block.locator("span.value").inner_text().strip()
            elif "評分:" in block_text:
                score_text = block.locator("span.value").inner_text().strip()
                # 提取评分数字，如 "4.5分, 由..." -> "4.5"
                try:
                    data['score'] = float(score_text.split('分')[0].strip())
                except:
                    pass
            elif "類別:" in block_text:
                tags = block.locator("span.value a").all_inner_texts()
                data['categories'] = ",".join([t.strip() for t in tags])
            elif "演員:" in block_text:
                actors = _female_actor_names_from_block(block)
                
                # 如果过滤后还有演员，再进行拼装
                if actors:
                    data['actors'] = ",".join([a.strip() for a in actors])
                else:
                    data['actors'] = ""
                
                logger.info(f"提取到女演员: {data['actors']}")

        return data

    except Exception as e:
        logger.error(f"❌ 解析详情页失败: {e}")
        return data

def check_login_status(page: Page) -> bool:
    """Navigate to JavDB home and verify if the session is logged in."""
    try:
        page.goto(f"{BASE_URL}/?locale=zh", timeout=20000)
        bypass_javdb_security(page)
        page.wait_for_selector("nav", timeout=10000)
        # Logged in = a "My" / profile link exists in the navbar
        logged_in = page.locator("a.navbar-item[href*='/users/']").count() > 0
        return logged_in
    except Exception:
        return False

def _normalize_search_code(code: str) -> str:
    """剥离本地附加后缀，得到更纯净的搜索词。"""
    return re.sub(r'-[cur]+$', '', code, flags=re.IGNORECASE)

def _search_code_variants(search_code: str) -> list[str]:
    normalized = (search_code or '').strip()
    if not normalized:
        return []
    if normalized.upper().startswith('DSVR-'):
        return [normalized, '3' + normalized]
    if normalized.upper().startswith('3DSVR-'):
        return [normalized, normalized[1:]]
    return [normalized]

def _is_login_required(page: Page) -> bool:
    """判断页面是否被登录墙拦截。"""
    return "/login" in page.url or page.locator("input[type='password']").count() > 0

def _drop_session_and_reload(page: Page, url: str, wait_selector: str, timeout: int = 25000) -> bool:
    """Cookie 失效时清除 session，以访客模式重新加载同一 URL。
    返回 True 表示重载成功且不在登录页。"""
    logger.warning("🔑 检测到 Cookie 已失效，清除 session 后切换为访客模式...")
    try:
        page.context.clear_cookies()
        page.goto(url, timeout=30000)
        bypass_javdb_security(page)
        page.wait_for_selector(wait_selector, state="attached", timeout=timeout)
        if _is_login_required(page):
            logger.warning("🔒 访客模式下该页面仍受限（仅限登录用户），跳过。")
            return False
        logger.info("✅ 已切换至访客模式，继续刮削。")
        return True
    except Exception as e:
        logger.error(f"❌ 访客模式重载失败: {e}")
        return False

def _open_video_search_page(page: Page, search_code: str) -> bool:
    """打开搜索页并完成基础拦截处理。"""
    search_url = f"{BASE_URL}/search?f=all&q={search_code}&locale=zh"
    logger.info(f"正在访问: {search_url}")
    page.goto(search_url, timeout=30000)
    bypass_javdb_security(page)

    try:
        page.wait_for_selector(".movie-list, .empty-message, input[type='password']", state="attached", timeout=25000)
    except PlaywrightTimeoutError:
        if _is_cloudflare_challenge(page):
            logger.error("🛡️ Cloudflare 验证未通过：请使用系统 Chrome channel 或手动更新可用 Cookie。")
        return False
    bypass_javdb_security(page)
    return True

def _search_results_are_empty(page: Page) -> bool:
    """判断搜索页是否没有有效影片结果。"""
    return page.locator(".empty-message").count() > 0 or page.locator(".movie-list").count() == 0

def _apply_search_cooldown(page: Page, current_count: int):
    """根据当前计数应用防封冷却策略。"""
    if current_count % 10 == 0:
        logger.info("⏳ 触发周期性深度防机器验证冷却 (5-10秒)...")
        simulate_human_behavior(page, 22.5, 55.0)
    else:
        simulate_human_behavior(page, 5.5, 22.5)

def _normalize_match_code(code: str) -> str:
    normalized = (code or '').strip().upper()
    if normalized.startswith('3DSVR-'):
        return 'DSVR-' + normalized[len('3DSVR-'):]
    return normalized


def _pick_matching_index(item_codes, search_code: str) -> Optional[int]:
    """返回番号与 search_code 匹配（忽略大小写；DSVR/3DSVR 视为同一番号）的结果下标。"""
    target = _normalize_match_code(search_code)
    if not target:
        return None
    for i, c in enumerate(item_codes):
        if _normalize_match_code(c) == target:
            return i
    return None


def _read_result_codes(page: Page) -> list:
    """读取每个搜索结果项显示的番号 (.video-title strong)。"""
    items = page.locator(".item")
    codes = []
    for i in range(items.count()):
        try:
            codes.append(items.nth(i).locator(".video-title strong").first.inner_text().strip())
        except Exception:
            codes.append('')
    return codes


def _open_result_detail(page: Page, code: str, index: int) -> bool:
    """点击指定下标的搜索结果并等待详情页加载。"""
    bypass_javdb_security(page)
    link = page.locator(".item").nth(index).locator("a").first
    logger.info(f"进入精确匹配的搜索结果 (第 {index + 1} 条)...")
    link.click()

    try:
        page.wait_for_selector(".movie-panel-info, input[type='password']", timeout=15000)
    except PlaywrightTimeoutError:
        logger.error(f"⚠️ {code} 等待详情页加载超时。")
        return False
    return True

def scrape_video_info(page: Page, code: str) -> Optional[Dict]:
    """核心动作：控制浏览器搜索并进入详情页"""
    global scrape_count
    current_count = scrape_count
    scrape_count += 1
    
    # 剥离本地附加的特征后缀 (-C, -U, -UC, -R 等)，避免影响 JavDB 搜索精度
    # 例如将 ABC-123-C 或 ABC-123-UC 统一还原为真正的番号 ABC-123
    search_code = _normalize_search_code(code)
    
    logger.info(f"🎬 开始刮削: DB番号 [{code}] -> 搜索词 [{search_code}] (当前刮削计数: {current_count})")
    
    try:
        result_codes = []
        match_index = None
        matched_search_code = search_code
        for variant in _search_code_variants(search_code):
            # 1. 打开搜索页
            if not _open_video_search_page(page, variant):
                logger.error(f"⚠️ {code} 等待页面加载超时 (网络异常或被强力 Cloudflare 拦截)。")
                return None

            # 2. 判断是否被强制要求登录 → 尝试访客模式降级
            search_url = f"{BASE_URL}/search?f=all&q={variant}&locale=zh"
            if _is_login_required(page):
                if not _drop_session_and_reload(
                    page, search_url,
                    ".movie-list, .empty-message, input[type='password']"
                ):
                    return None

            # 3. 判断搜索结果是否为空
            if _search_results_are_empty(page):
                logger.warning(f"❌ 搜索结果为空: 未在 JavDB 找到番号 {variant}。")
                continue

            # 4. 搜索页防封冷却
            _apply_search_cooldown(page, current_count)

            # 5. 找到与番号精确匹配的搜索结果（第一位不一定是正确的片）
            result_codes = _read_result_codes(page)
            if not result_codes:
                logger.warning(f"❌ 未在 JavDB 搜索列表中提取到有效的影片项目 ({variant})。跳过此搜索词。")
                continue
            match_index = _pick_matching_index(result_codes, search_code)
            if match_index is not None:
                matched_search_code = variant
                break

        if match_index is None:
            if not result_codes:
                logger.warning(f"❌ 搜索结果为空: 未在 JavDB 找到番号 {code}。跳过此片，等待后续手工添加信息。")
                return None
            logger.warning(
                f"🧊 搜索结果中无匹配 [{search_code}] 的番号（候选: {result_codes[:5]}），冻结待人工处理。"
            )
            return FREEZE

        # 6. 进入匹配的详情页
        if matched_search_code != search_code:
            logger.info(f"🔁 使用 JavDB 备用搜索词 [{matched_search_code}] 匹配 [{search_code}]。")
        if not _open_result_detail(page, code, match_index):
            return None

        if _is_login_required(page):
            # 可能是详情页单独受限，再试一次访客降级（cookies 可能在步骤 2 已清）
            current_url = page.url
            if not _drop_session_and_reload(
                page, current_url,
                ".movie-panel-info, input[type='password']",
                timeout=15000,
            ):
                logger.warning(f"🔒 详情受限: {code} 仅限登录用户，跳过。")
                return None

        # 6. 详情页稳定等待
        simulate_human_behavior(page, 4.5, 18.75)

        # 7. 执行页面解析
        scraped_data = parse_detail_page(page, code)
        logger.info(f"✅ 刮削成功: {scraped_data['title_jp'][:60]}...")
        
        return scraped_data

    except Exception as e:
        logger.error(f"❌ 刮削过程发生严重错误 [{code}]: {e}")
        return None

def scrape_actor_info(page: Page, actor_name: str) -> Tuple[Optional[str], Optional[str]]:
    """抓取演员头像与中文译名，返回 (avatar_path, name_zh)"""
    if not actor_name or len(actor_name.strip()) < 2:
        logger.warning(f"⏭️ 演员名字 [{actor_name}] 长度少于 2 个字符，跳过抓取。")
        return None, None
        
    global scrape_count
    current_count = scrape_count
    scrape_count += 1
    
    logger.info(f"💃 开始抓取演员: [{actor_name}] (当前刮削计数: {current_count})")
    
    try:
        # 1. 访问演员搜索接口
        search_url = f"{BASE_URL}/search?f=actor&q={actor_name}&locale=zh"
        page.goto(search_url, timeout=30000)
        
        bypass_javdb_security(page)

        try:
            page.wait_for_selector(".actor-box, .item, .empty-message, input[type='password']", timeout=25000)
        except PlaywrightTimeoutError:
            logger.error(f"⚠️ {actor_name} 等待页面加载超时。")
            return None, None

        actor_search_url = f"{BASE_URL}/search?f=actor&q={actor_name}&locale=zh"
        if _is_login_required(page):
            if not _drop_session_and_reload(
                page, actor_search_url,
                ".actor-box, .item, .empty-message, input[type='password']"
            ):
                return None, None

        if page.locator(".empty-message").count() > 0 or page.locator(".actor-box, .item").count() == 0:
            logger.warning(f"❌ 未在 JavDB 找到演员 {actor_name}。")
            return None, None

        if current_count % 10 == 0:
            simulate_human_behavior(page, 22.5, 55.0)
        else:
            simulate_human_behavior(page, 5.5, 22.5)

        # 2. 获取所有搜索结果链接并遍历验证
        search_result_links = []
        elements = page.locator(".actor-box a, .item a")
        
        if elements.count() > 10:
            logger.warning(f"⏭️ 搜索结果过多 ({elements.count()} 个)，疑似泛匹配，跳过 [{actor_name}] 的抓取。")
            return None, None
            
        for i in range(elements.count()):
            href = elements.nth(i).get_attribute("href")
            if href:
                search_result_links.append(href)
                
        matched = False
        raw_name_text = ""
        
        for i, link in enumerate(search_result_links):
            full_url = BASE_URL + link if link.startswith('/') else link
            logger.info(f"👉 正在查看第 {i+1}/{len(search_result_links)} 个搜索结果...")
            
            if i == 0:
                # 第一次直接点，行为最自然
                elements.nth(i).click()
            else:
                # === 新增：在轮询下一个搜索结果前，加入随机等待时间，防止请求过快触发风控 ===
                logger.info(f"⏳ 正在前往下一个搜索结果，模拟人类停顿缓冲...")
                simulate_human_behavior(page, 5.0, 55.0)
                
                # 后续的由于页面改变了，直接用网络跳转
                page.goto(full_url, timeout=30000)
                
            # 3. 等待详情页加载并验证名字
            try:
                page.wait_for_selector(".actor-section-name, .title.is-4, .avatar, input[type='password']", timeout=15000)
            except PlaywrightTimeoutError:
                logger.warning(f"⚠️ 第 {i+1} 个结果详情页加载超时，跳过。")
                continue
                
            if _is_login_required(page):
                detail_url = page.url
                if not _drop_session_and_reload(
                    page, detail_url,
                    ".actor-section-name, .title.is-4, .avatar, input[type='password']",
                    timeout=15000,
                ):
                    continue

            simulate_human_behavior(page, 3.0, 37.5)
            
            name_elem = page.locator(".actor-section-name, .title.is-4").first
            if name_elem.count() > 0:
                raw_name_text = (name_elem.text_content() or "").strip()
                # 过滤掉详情页可能附带的性别符号(♀)，防止影响后续的正则匹配
                raw_name_text = raw_name_text.replace('♀', '').strip()
                
                # 验证是否包含输入的女优名字（双端去空格+转小写，防止中日英混排误伤）
                safe_raw = re.sub(r'\s+', '', raw_name_text).lower()
                safe_actor = re.sub(r'\s+', '', actor_name).lower()
                
                if safe_actor in safe_raw:
                    logger.info(f"✅ 成功命中！[{actor_name}] 匹配当前页面名称: [{raw_name_text}]")
                    matched = True
                    break
                else:
                    logger.info(f"⏭️ 名字不匹配: 预期=[{actor_name}], 实际=[{raw_name_text}]。尝试下一个结果...")
            else:
                logger.info(f"⏭️ 第 {i+1} 个结果未找到名字元素，跳过...")
                
        if not matched:
            logger.warning(f"❌ 遍历完所有 {len(search_result_links)} 个搜索结果，未找到与 [{actor_name}] 匹配的女优。")
            return None, None

        # ================= 新增：提取并转换中文译名 =================
        name_zh = None
        try:
            name_elem = page.locator(".actor-section-name").first
            if name_elem.count() > 0:
                raw_name_text = (name_elem.text_content() or "").strip()
                logger.info(f"📝 抓取到演员名称原始文本: [{raw_name_text}]")
            if raw_name_text:
                logger.info(f"📝 开始处理演员名称文本: [{raw_name_text}]")

                # 用中英文逗号进行分割
                parts = re.split(r'[,，]', raw_name_text)
                if len(parts) > 1:
                    target_text = parts[0].strip()
                    logger.info(f"✂️ 提取到别名/译名组合: 日文名=[{parts[1].strip()}], 备选译名=[{target_text}]")
                else:
                    target_text = parts[0].strip()
                    logger.info(f"✂️ 未发现别名，进入单名处理逻辑: [{target_text}]")

                # 校验是否全部为汉字 (Kanji)，兼容名字中可能存在的空格
                if re.match(r'^[\u4e00-\u9fa5\s]+$', target_text):
                    try:
                        import zhconv
                        # 转换为简体中文
                        name_zh = zhconv.convert(target_text, 'zh-cn')
                        if name_zh == target_text:
                            logger.info(f"🔤 名称 [{target_text}] 为纯汉字，直接作为译名。")
                        else:
                            logger.info(f"🔤 成功转换中文译名: [{target_text}] -> [{name_zh}]")
                    except ImportError:
                        logger.warning("⚠️ 未安装 zhconv 库，跳过简繁转换。请运行: pip install zhconv")
                        name_zh = target_text
                # 校验是否为全英文或数字组合 (如 "AIKA", "Rio")
                elif re.match(r'^[a-zA-Z0-9\s\.\-]+$', target_text):
                    name_zh = target_text
                    logger.info(f"🔤 名称 [{target_text}] 为纯英文/数字，直接作为译名。")
                else:
                    if len(parts) > 1:
                        name_zh = target_text
                        logger.info(f"🔤 提取到备选译名(含特殊字符): [{name_zh}]")
                    else:
                        logger.info(f"⏭️ 文本 [{target_text}] 包含假名，且无别名，跳过作为译名。")
                    
        except Exception as e:
            logger.debug(f"提取中文名异常: {e}")
        # ============================================================

        avatar_url = None
        
        # 尝试 1: 查找常规的 img 标签
        avatar_img = page.locator(".avatar img, span.avatar img, .actor-avatar img, img.avatar").first
        if avatar_img.count() > 0:
            avatar_url = avatar_img.get_attribute("src")
        else:
            # 尝试 2: 从 span 的 style="background-image: url(...)" 中提取
            avatar_span = page.locator(".actor-avatar span.avatar, span.avatar").first
            if avatar_span.count() > 0:
                style_str = avatar_span.get_attribute("style") or ""
                match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style_str)
                if match:
                    avatar_url = match.group(1)
        
        avatar_path = None
        if avatar_url:
            avatar_path = download_avatar(avatar_url, actor_name)
        else:
            logger.warning(f"❌ 未能从详情页找到 [{actor_name}] 的头像 URL。")
            
        return avatar_path, name_zh

    except Exception as e:
        logger.error(f"❌ 刮削演员发生异常 [{actor_name}]: {e}")
        return None, None

# ----------- 本地独立测试块 -----------
if __name__ == "__main__":
    from playwright_stealth import Stealth
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    # 找一个极其标准的番号进行打样测试
    test_code = "CJOD-503" 
    
    print("="*50)
    print(f"🚀 开始单兵测试刮削器: {test_code}")
    print("="*50)
    
    # 补充：为单兵测试提供启动浏览器的支持
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled'])
        context = browser.new_context(
            viewport={'width': 1280, 'height': 800}
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)
        
        result = scrape_video_info(page, test_code)
    
        if result:
            print("\n🎉 最终提取到的数据结构：")
            for k, v in result.items():
                print(f"  {k:<15}: {v}")
        else:
            print("\n💀 刮削失败。请检查日志。")
