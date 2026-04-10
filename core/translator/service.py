import re
from typing import Iterable

from .cleaning import strip_entities_from_title

# DEFAULT_MODEL = 'gemma4:e4b'
DEFAULT_MODEL = 'quantumcookie/Sakura-qwen2.5-v1.0:14b'
DEFAULT_HOST = 'http://10.0.0.43:11434'


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
