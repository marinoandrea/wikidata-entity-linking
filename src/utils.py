from typing import Optional

from decorator import decorate


def _cache(f, *args, **kwargs):
    key = (args, frozenset(kwargs.items())) if kwargs else args
    try:
        return f.cache[key]
    except KeyError:
        value = f(*args, **kwargs)
        f.cache[key] = value
        return value


def cached(f):
    f.cache = {}
    return decorate(f, _cache)


def get_trident_id_from_wd_uri(uri: str) -> Optional[str]:
    try:
        return uri.replace('>', '').split('/')[-1]
    except IndexError:
        return None
