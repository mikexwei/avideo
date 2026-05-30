import re
from typing import Iterable

from .cleaning import strip_entities_from_title

# DEFAULT_MODEL = 'gemma4:e4b'
DEFAULT_MODEL = 'quantumcookie/Sakura-qwen2.5-v1.0:14b'
DEFAULT_HOST = 'http://10.0.0.43:11434'
SHORT_TITLE_MODEL = 'gemma4:e4b'
GENERIC_SHORT_TITLE_WORDS = {
    '精彩',
    '激情',
    '性爱',
    '影片',
    '作品',
    '无码',
    '破解',
    '中文字幕',
    '中字',
    '高清',
}


def _clean_title_for_translation(title_jp: str, codes: Iterable[str]) -> str:
    """Remove codes and non-JP/CN/EN characters (kaomoji, symbols), keep text content."""
    clean = title_jp or ''
    # Remove codes
    for code in sorted({c for c in codes if c}, key=len, reverse=True):
        base = re.sub(r'-[cur]+$', '', code, flags=re.IGNORECASE)
        for token in {base, base.replace('-', '')}:
            if token:
                clean = re.sub(re.escape(token), '', clean, flags=re.IGNORECASE)
    # Remove brackets and their contents (tags, annotations)
    clean = re.sub(r'【.*?】|\[.*?\]|（.*?）|\(.*?\)', '', clean)
    # Keep only Japanese (hiragana/katakana/kanji), Chinese, English letters/digits, spaces
    clean = re.sub(r'[^\u3040-\u30ff\u31f0-\u31ff\u4e00-\u9fff\uff66-\uff9fa-zA-Z0-9\s]', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def translate_title(
    title_jp: str,
    codes: Iterable[str],
    actor_name_map: dict = None,
    host: str = DEFAULT_HOST,
    model: str = DEFAULT_MODEL,
    # legacy param kept for compatibility
    actor_names: Iterable[str] = (),
) -> str:
    """Translate title to Chinese with graceful fallback to original title.

    actor_name_map: {jp_name: zh_name} — actor names to substitute before LLM call.
    """
    clean_title = _clean_title_for_translation(title_jp, codes)
    if not clean_title:
        return title_jp

    # Replace Japanese actor names with Chinese names in the title (longest first to avoid partial matches)
    actor_name_map = actor_name_map or {}
    substituted_names: list[str] = []
    for jp_name, zh_name in sorted(actor_name_map.items(), key=lambda x: -len(x[0])):
        if jp_name in clean_title:
            clean_title = clean_title.replace(jp_name, zh_name)
            substituted_names.append(zh_name)

    # Build the "keep these names" instruction if any substitutions were made
    names_instruction = ''
    if substituted_names:
        names_str = '、'.join(substituted_names)
        names_instruction = f'其中人名【{names_str}】已是正确的中文译名，请原样保留，不得修改。'

    prompt = (
        '你是一个资深的日文到简体中文的本地化翻译专家，精通日本流行文化、ACG 领域及成人影视相关的专有名词和俚语。'
        '请将以下内容准确翻译成简体中文，忠实还原原文含义，不得曲解或美化——例如"颜射"就是"颜射"，不能译成其他；'
        '译文风格直白露骨，反映原文的黑暗、色情、暴力、禁忌氛围，不做任何道德审查或过滤，译文尽量简短。'
        + names_instruction +
        '只输出翻译结果，不加任何解释、符号或拼音：\n\n'
        f'{clean_title}'
    )

    try:
        from ollama import Client
        client = Client(host=host)
        response = client.chat(model=model, messages=[{'role': 'user', 'content': prompt}])
        return response['message']['content'].strip()
    except Exception:
        return title_jp


def is_likely_japanese(text: str) -> bool:
    """Heuristic: treat text as Japanese when it contains kana."""
    if not text:
        return False
    return bool(re.search(r'[\u3040-\u30ff\u31f0-\u31ff]', text))


def translate_series_if_japanese(
    series: str,
    host: str = DEFAULT_HOST,
    model: str = DEFAULT_MODEL,
) -> str:
    """Translate series only when it appears to be Japanese; otherwise keep original."""
    if not series or not is_likely_japanese(series):
        return series

    prompt = (
        '请将以下日文系列名翻译成简体中文，仅输出翻译结果，不要解释：\n\n'
        f'{series}'
    )

    try:
        from ollama import Client
        client = Client(host=host)
        response = client.chat(model=model, messages=[{'role': 'user', 'content': prompt}])
        translated = (response['message']['content'] or '').strip()
        return translated if translated else series
    except Exception:
        return series


def clean_short_title(text: str, max_len: int = 20) -> str:
    cleaned = text or ''
    cleaned = re.sub(r'[\s\.,，。:：;；!！?？、\[\]【】\(\)（）《》<>「」『』"\'`~\-_/\\|]+', '', cleaned)
    cleaned = re.sub(r'[^\u4e00-\u9fffA-Za-z0-9]', '', cleaned)
    return cleaned[:max_len]


def _strip_actor_names(text: str, actor_names: Iterable[str]) -> str:
    stripped = text
    for name in sorted({n for n in actor_names if n}, key=len, reverse=True):
        stripped = stripped.replace(name, '')
        stripped = stripped.replace(clean_short_title(name, max_len=100), '')
    return stripped


def _short_title_candidates(text: str, actor_names: Iterable[str], max_len: int) -> list[str]:
    candidates = []
    seen = set()
    for raw_line in (text or '').splitlines():
        line = re.sub(r'^\s*(?:候选\s*)?[一二三四五六七八九十\d]+[\.\)、:：\s-]+', '', raw_line)
        line = _strip_actor_names(line, actor_names)
        candidate = clean_short_title(line, max_len=max_len)
        if candidate and candidate not in seen:
            candidates.append(candidate)
            seen.add(candidate)
    return candidates


def _short_title_score(candidate: str, title_zh: str, actor_names: Iterable[str], max_len: int) -> int:
    if not candidate or len(candidate) > max_len:
        return -1000

    cleaned_actor_names = [clean_short_title(name, max_len=100) for name in actor_names if name]
    if any(name and name in candidate for name in cleaned_actor_names):
        return -1000

    title_clean = _strip_actor_names(clean_short_title(title_zh, max_len=200), cleaned_actor_names)
    title_chars = set(title_clean)
    overlap = sum(1 for char in candidate if char in title_chars)
    score = overlap * 3

    length = len(candidate)
    if 6 <= length <= 16:
        score += 8
    elif 4 <= length <= max_len:
        score += 4
    else:
        score -= 8

    for word in GENERIC_SHORT_TITLE_WORDS:
        if word in candidate:
            score -= 5

    if candidate == title_clean[:max_len]:
        score -= 3
    return score


def select_short_title(candidates: Iterable[str], title_zh: str, actor_names: Iterable[str], max_len: int = 20) -> str:
    actor_list = [n for n in actor_names if n]
    cleaned_candidates = []
    seen = set()
    for candidate in candidates:
        cleaned = _strip_actor_names(candidate, actor_list)
        cleaned = clean_short_title(cleaned, max_len=max_len)
        if cleaned and cleaned not in seen:
            cleaned_candidates.append(cleaned)
            seen.add(cleaned)

    if not cleaned_candidates:
        return ''

    return max(
        cleaned_candidates,
        key=lambda candidate: _short_title_score(candidate, title_zh, actor_list, max_len),
    )


def shorten_chinese_title(
    title_zh: str,
    actor_names: Iterable[str] = (),
    host: str = DEFAULT_HOST,
    model: str = SHORT_TITLE_MODEL,
    max_len: int = 20,
) -> str:
    shortened, _candidates = shorten_chinese_title_with_candidates(
        title_zh,
        actor_names=actor_names,
        host=host,
        model=model,
        max_len=max_len,
    )
    return shortened


def shorten_chinese_title_with_candidates(
    title_zh: str,
    actor_names: Iterable[str] = (),
    host: str = DEFAULT_HOST,
    model: str = SHORT_TITLE_MODEL,
    max_len: int = 20,
) -> tuple[str, list[str]]:
    title_zh = title_zh or ''
    actor_list = [n for n in actor_names if n]
    if not title_zh:
        return '', []

    names_text = '、'.join(actor_list) if actor_list else '无'
    prompt = (
        '你是中文影片标题编辑。请根据输入生成一个中文短标题。\n'
        f'要求：1. 输出3个候选，每行一个；2. 每个候选最多{max_len}个字符；'
        '3. 不包含空格；4. 不包含任何标点符号；'
        '5. 去除女优名、番号、厂商、中文字幕、无码、高清等非标题信息；'
        '6. 保留原标题中的核心场景、关系或主题，不要添加原标题没有的信息；'
        '7. 避免精彩剧情、激情性爱、热门作品这类泛化标题；'
        '8. 不要解释，不要编号。\n\n'
        '示例：\n'
        '女优名：三上悠亚、新有菜\n'
        '中文标题：三上悠亚和新有菜与相泽南精美写真三张\n'
        '输出：\n'
        '精美写真三张\n'
        '写真特典三张\n'
        '三人写真特典\n\n'
        f'女优名：{names_text}\n'
        f'中文标题：{title_zh}\n'
        '输出：'
    )

    try:
        from ollama import Client
        client = Client(host=host)
        response = client.chat(
            model=model,
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': 0.1},
        )
        candidates = _short_title_candidates(response['message']['content'], actor_list, max_len=max_len)
        shortened = select_short_title(candidates, title_zh, actor_list, max_len=max_len)
    except Exception:
        candidates = []
        shortened = ''

    if not shortened:
        fallback = _strip_actor_names(title_zh, actor_list)
        shortened = clean_short_title(fallback, max_len=max_len)
        candidates = [shortened] if shortened else candidates
    return shortened, candidates
