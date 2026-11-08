"""Image downloads performed outside the browser process."""
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import requests
from config import COVERS_DIR, AVATARS_DIR
BASE_URL = 'https://javdb.com'
logger = logging.getLogger(__name__)

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
