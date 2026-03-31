import os
from pathlib import Path

# 项目根目录绝对路径
BASE_DIR = Path(__file__).resolve().parent

# 数据与日志目录
DATA_DIR = BASE_DIR / "data"
LOG_DIR = DATA_DIR / "logs"

# 数据库文件路径
DB_PATH = DATA_DIR / "avideo.db"

# 封面图下载存放路径
COVERS_DIR = BASE_DIR / "web" / "static" / "covers"


# 演员头像下载存放路径
AVATARS_DIR = BASE_DIR / "web" / "static" / "avatars"

# 确保必要的目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
COVERS_DIR.mkdir(parents=True, exist_ok=True)
AVATARS_DIR.mkdir(parents=True, exist_ok=True)



MEDIA_LIBRARIES = [
    Path("/Volumes/12TB"),
    Path("/Volumes/8TB/avideo"),
    Path("/Volumes/T71/avideo"),
    Path("/Volumes/T72/avideo"),
    Path("/Volumes/T73/avideo"),
    Path("/Volumes/2TB/avideo"),
]