import re
from typing import Iterable

from .cleaning import strip_entities_from_title

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
CORE_SHORT_TITLE_TERMS = {
    '侵犯', '强暴', '轮奸', '凌辱', '调教', '奴隶',
    '性交', '口交', '颜射', '中出', '内射', '操到',
    '丈夫', '人妻', '妻子', '未婚夫', '姐姐', '妹妹',
    '母亲', '父亲', '叔父', '部下', '青梅竹马',
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


def translate_series(raw_name, host=DEFAULT_HOST, model=DEFAULT_MODEL):
    from ollama import Client
    from core.series_catalog import normalize_input, translated_name

    normalized = normalize_input(raw_name)
    if not normalized:
        raise ValueError('系列名去掉符号后为空')
    prompt = ('将下面的影片系列名译为简体中文。原文若已是中文，请统一为简体。'
              '保留有意义的数字和专名，仅输出系列名。\n\n' + normalized)
    response = Client(host=host, timeout=120).chat(model=model, messages=[{'role': 'user', 'content': prompt}])
    result = translated_name(response['message']['content'] or '')
    if not result or is_likely_japanese(result):
        raise ValueError('模型未返回有效的简体中文系列名')
    return result


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

    for term in CORE_SHORT_TITLE_TERMS:
        if term in candidate and term not in title_clean:
            return -1000
        if term in title_clean:
            score += 8 if term in candidate else -4

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

    scored = [
        (candidate, _short_title_score(candidate, title_zh, actor_list, max_len))
        for candidate in cleaned_candidates
    ]
    valid = [(candidate, score) for candidate, score in scored if score > -1000]
    return max(valid, key=lambda item: item[1])[0] if valid else ''


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
        '6. 核心主题是原标题中发生的具体事件必须优先保留人物关系加行为加对象例如丈夫眼前被侵犯；'
        '7. 色情暴力禁忌和露骨行为词是主题不是杂质必须原样保留不得弱化美化或审查；'
        '8. 禁止把侵犯强暴轮奸中出内射性交操到爽等具体事件概括成欲望诱惑激情邂逅等抽象词；'
        '9. 标题过长时先删除修饰词仍须保留最关键的关系和具体行为不得只保留写真作品特辑等无关词；'
        '10. 示例只用于说明规则严禁把示例中的丈夫侵犯未婚夫等内容复制到当前标题；'
        '11. 输出中的人物关系和事件必须能在当前原标题中找到不得添加原标题没有的信息不要解释不要编号。\n\n'
        '示例：\n'
        '女优名：水野朝阳\n'
        '中文标题：在丈夫眼前被侵犯 危险的欲望 水野朝阳\n'
        '输出：\n'
        '丈夫眼前被侵犯\n'
        '在丈夫面前被侵犯\n'
        '丈夫目睹妻子被侵犯\n\n'
        '女优名：希崎洁希卡\n'
        '中文标题：原作改编 被未婚夫操到爽了 希崎洁希卡\n'
        '输出：\n'
        '被未婚夫操到爽了\n'
        '未婚夫操到高潮\n'
        '未婚夫的猛烈性交\n\n'
        '错误示例：危险欲望 情色刺激 写真 精彩作品\n'
        '错误原因：删除了具体事件或凭空替换了原标题主题\n\n'
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
