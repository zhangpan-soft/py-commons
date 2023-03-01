import datetime
import threading


def log(msg: str, *args, need_tid=False, filename=None):
    if need_tid:
        __log_true__(msg, args, filename)
        pass
    else:
        __log_false__(msg, args, filename)
        pass
    pass


def __log_true__(msg: str, args, filename=None):
    ss = "%s %s : "
    s = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    a = (s, threading.currentThread().ident)
    a2 = (a + args) if args else a
    sss = ss + msg
    __print__(sss, a2, filename)
    pass


def __log_false__(msg: str, args, filename=None):
    __print__(msg, args, filename)
    pass


def __print__(msg: str, args, filename=None):
    if filename:
        f = open(file=filename, mode='w')
        print(msg % args, file=f)
        pass
    else:
        print(msg % args)
        pass
    pass
