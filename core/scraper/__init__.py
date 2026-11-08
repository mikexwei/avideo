"""Lazy scraper exports keep browser dependencies out of Consumer imports."""
from importlib import import_module


__all__ = [
    'scrape_video_info',
    'scrape_ltx_links',
    'extract_ltx_links_from_reviews',
    'parse_detail_page',
    'scrape_actor_info',
    'simulate_human_behavior',
    'random_sleep',
    'bypass_javdb_security',
    'download_cover',
    'download_avatar',
    'load_javdb_cookies',
    'check_login_status',
    'launch_javdb_browser',
    'launch_javdb_context',
    'javdb_profile_lock',
    'FREEZE',
    'BASE_URL',
    'JAVDB_LOCALE',
]


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    if name in ('download_cover', 'download_avatar'):
        module = '.assets'
    elif name in ('simulate_human_behavior', 'random_sleep', 'bypass_javdb_security'):
        module = '.anti_bot'
    else:
        module = '.video_scraper'
    value = getattr(import_module(module, __name__), name)
    globals()[name] = value
    return value
