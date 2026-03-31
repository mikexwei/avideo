import re
from typing import Iterable, Set


def _expand_name_variants(name: str) -> Set[str]:
    variants = {name}
    try:
        import zhconv
        variants.add(zhconv.convert(name, 'zh-cn'))
        variants.add(zhconv.convert(name, 'zh-tw'))
    except ImportError:
        pass
    return {v for v in variants if v}


def strip_entities_from_title(title_jp: str, actor_names: Iterable[str], codes: Iterable[str]) -> str:
    """Remove known codes and actor names before LLM translation."""
    clean_title = title_jp or ''

    for code in sorted({c for c in codes if c}, key=len, reverse=True):
        base_code = re.sub(r'-[cur]+$', '', code, flags=re.IGNORECASE)
        for token in {base_code, base_code.replace('-', '')}:
            if token:
                clean_title = re.sub(re.escape(token), '', clean_title, flags=re.IGNORECASE)

    clean_title = re.sub(r'【.*?】|\[.*?\]|（.*?）|\(.*?\)', '', clean_title)

    expanded_names = set()
    for raw_name in actor_names:
        if not raw_name:
            continue
        expanded_names.update(_expand_name_variants(raw_name.strip()))

    for actor in sorted(expanded_names, key=len, reverse=True):
        chars = list(re.sub(r'[^\w]|_', '', actor))
        if chars:
            pattern = r'\s*'.join(re.escape(c) for c in chars)
            clean_title = re.sub(pattern, ' ', clean_title, flags=re.IGNORECASE)

    clean_title = re.sub(r'[^\w\s]|_', ' ', clean_title)
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()
    return clean_title
