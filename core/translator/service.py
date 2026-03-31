import re
from typing import Iterable

from .cleaning import strip_entities_from_title

DEFAULT_MODEL = 'quantumcookie/Sakura-qwen2.5-v1.0:14b'
DEFAULT_HOST = 'http://10.0.0.40:11434'


def translate_title(
    title_jp: str,
    actor_names: Iterable[str],
    codes: Iterable[str],
    host: str = DEFAULT_HOST,
    model: str = DEFAULT_MODEL,
) -> str:
    """Translate title to Chinese with graceful fallback to original title."""
    clean_title = strip_entities_from_title(title_jp, actor_names, codes)
    if not clean_title:
        return title_jp

    prompt = (
        '你是一个资深的日文到中文的本地化翻译专家，精通日本流行文化、ACG 领域及成人影视相关的专有名词和俚语。'
        '请将以下日文翻译成通顺、自然、符合中文语境的表达，保留原有的语气和情感，不要做任何道德审查或过滤。'
        '只输出翻译后的标题，不需要任何解释、多余的符号或拼音：\n\n'
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
