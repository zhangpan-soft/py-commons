import logging
import sys
import os
import threading

DEBUG = logging.DEBUG
INFO = logging.INFO
WARN = logging.WARNING
ERROR = logging.ERROR
CRIT = logging.CRITICAL

LOGGING_FORMAT = '%(asctime)s %(thread)d %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'

__LOCK__ = threading.Lock()

relations = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRIT': logging.CRITICAL
}

__LOGS__ = {}


def get_logger(log_name=None, level: int = INFO, base_dir='logs/'):
    # 如果log_name 为空,则设置为root
    if log_name is None:
        log_name = 'root'
        pass

    _level_name = os.getenv('logging.' + log_name + '.level', None)
    if _level_name:
        _level = relations.get(str.upper(_level_name), None)
        level = _level if _level else level
        pass

    # 如果log已存在,直接返回
    log = __LOGS__.get('root', None)
    if log:
        return log
    # 加锁,创建日志对象
    __LOCK__.acquire(blocking=True, timeout=10)

    try:
        # 创建日志对象
        log = logging.getLogger(log_name if not log_name else base_dir + log_name)
        # 设置日志级别
        log.setLevel(level)
        # 日志输出格式
        fmt = logging.Formatter(LOGGING_FORMAT)
        # 输出到控制台
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(fmt)
        log.addHandler(console_handler)

        # 输出到文件
        # filename = base_dir + log_name + '.log'
        # 日志文件按天进行保存，每天一个日志文件
        # file_handler = handlers.TimedRotatingFileHandler(filename=filename, when='D', backupCount=1, encoding='utf-8')
        # file_handler.setFormatter(fmt)
        # log.addHandler(file_handler)
        # # 按照大小自动分割日志文件，一旦达到指定的大小重新生成文件
        # file_handler2 = handlers.RotatingFileHandler(filename=filename, maxBytes=1 * 1024 * 1024 * 1024, backupCount=1,
        #                                              encoding='utf-8')
        # file_handler2.setFormatter(fmt)
        # log.addHandler(file_handler2)
        __LOGS__[log_name] = log
        pass
    finally:
        __LOCK__.release()
        pass
    return log
    pass
