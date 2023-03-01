import os


def is_os_evn(_: str) -> bool:
    _ = str.strip(_)
    return str.startswith(_, '${') and str.endswith(_, '}')
    pass


def get_env(_: str, _dv=None):
    __ = str.replace(_, '${', '')
    __ = str.replace(__, '}', '')
    __ = str.strip(__)
    return os.getenv(__, _dv)
    pass
