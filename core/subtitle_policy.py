import re

HARD_SUBTITLE = '硬字幕'
NO_SUBTITLE = '无字幕'
SUBTITLE_CLASSIFICATIONS = (HARD_SUBTITLE, '外挂 OCR 字幕', '外挂音频字幕', NO_SUBTITLE)
SUBTITLE_TAGS = SUBTITLE_CLASSIFICATIONS + (
    '中文字幕', '机翻中文', '机翻中文 V2', '外挂字幕', '内嵌中文',
)
VERSION_SUFFIX = re.compile(r'-(?:UC|U|C)$', re.IGNORECASE)


def default_subtitle(code):
    return HARD_SUBTITLE if code.upper().endswith(('-C', '-UC')) else NO_SUBTITLE


def validate_subtitle(value, *, vr=False):
    if value not in SUBTITLE_CLASSIFICATIONS:
        raise ValueError('无效的字幕分类')
    if vr and value != NO_SUBTITLE:
        raise ValueError('VR 影片只能选择无字幕')
    return value


def stored_subtitle(tags):
    classifications = set(tags).intersection(SUBTITLE_CLASSIFICATIONS)
    return next(iter(classifications)) if len(classifications) == 1 else None


def subtitle_code(code, classification, *, uncensored=None):
    if uncensored is None:
        uncensored = code.upper().endswith(('-U', '-UC'))
    has_subtitle = classification != NO_SUBTITLE
    suffix = '-UC' if uncensored and has_subtitle else '-U' if uncensored else '-C' if has_subtitle else ''
    return VERSION_SUFFIX.sub('', code) + suffix
