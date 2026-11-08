"""External dependency calls, executed in bounded Consumer threads."""


def execute(kind, payload):
    if kind == 'subtitle_cache':
        from core.subtitle_cache import extract
        return extract(payload['path'], payload['fingerprint'])
    if kind == 'series_translate':
        from core.translator.service import translate_series
        return translate_series(payload['raw_name'])
    if kind == 'series_report':
        from core.series_catalog import generate_report
        return generate_report(payload['members'], payload.get('draft_groups', ()), payload.get('round', 1))
    if kind == 'translate':
        from core.translator.service import translate_title
        return translate_title(payload['title'], payload.get('codes', []),
                               actor_name_map=payload.get('actor_name_map'), **payload.get('options', {}))
    if kind == 'short_title':
        from core.translator.service import shorten_chinese_title_with_candidates
        title, _ = shorten_chinese_title_with_candidates(payload['title'], actor_names=payload.get('actors', []))
        return title
    if kind == 'metadata_assets':
        from core.scraper.assets import download_cover
        data = payload['data']
        result = {}
        if data.get('cover_url'):
            result['cover_path'] = download_cover(data['cover_url'], payload['code'])
        return result
    if kind == 'avatar':
        from core.scraper.assets import download_avatar
        return download_avatar(payload['url'], payload['name'])
    raise ValueError('未知外部依赖 ' + kind)
