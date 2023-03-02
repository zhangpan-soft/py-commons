import json
import time

import pymysql
from pymysql.cursors import Cursor
import threading
import re
import yaml
import os
from queue import LifoQueue
import util.os_env as os_env

from util.log import logger

log = logger.get_logger(log_name='util.db.connections')

__DEFAULT_USER__ = 'root'
__DEFAULT_PASSWORD__ = ''
__DEFAULT_DATABASE__ = 'test'
__DEFAULT_HOST__ = 'localhost'
__DEFAULT_PORT__ = 3306
__DEFAULT_CHARSET__ = 'utf8mb4'
__DEFAULT_MAX_ALLOWED_PACKET__ = 16 * 1024 * 1024
__DEFAULT_USE_UNICODE__ = True
__DEFAULT_CLIENT_FLAG__ = 0
__DEFAULT_CONNECT_TIMEOUT__ = 10
__DEFAULT_AUTOCOMMIT__ = True
__DEFAULT_LOCAL_INFILE__ = False
__DEFAULT_DEFER_CONNECT__ = False
__DEFAULT_BINARY_PREFIX__ = False
__DEFAULT_POOL_MAX_SIZE__ = 8
__DEFAULT_POOL_MIN_SIZE__ = 1
__DEFAULT_MAX_LIFETIME__ = 1800
__DEFAULT_VALIDATE_QUERY__ = 'SELECT 1'
__DEFAULT_VALIDATE_INTERVAL__ = 30
__DEFAULT_MAX_WAIT_TIME__ = 10
__DEFAULT_UNIX_SOCKET__ = None
__DEFAULT_SQL_MODE__ = None
__DEFAULT_READ_DEFAULT_FILE__ = None
__DEFAULT_CONV__ = None
__DEFAULT_INIT_COMMAND__ = None
__DEFAULT_READ_DEFAULT_GROUP__ = None
__DEFAULT_AUTH_PLUGIN_MAP__ = None
__DEFAULT_READ_TIMEOUT__ = None
__DEFAULT_WRITE_TIMEOUT__ = None
__DEFAULT_BIND_ADDRESS__ = None
__DEFAULT_PROGRAM_NAME__ = None
__DEFAULT_SERVER_PUBLIC_KEY__ = None
__DEFAULT_SSL__ = None
__DEFAULT_SSL_CA__ = None
__DEFAULT_SSL_CERT__ = None
__DEFAULT_SSL_DISABLED__ = None
__DEFAULT_SSL_KEY__ = None
__DEFAULT_SSL_VERIFY_CERT__ = None
__DEFAULT_SSL_VERIFY_IDENTITY__ = None
__DEFAULT_COMPRESS__ = None
__DEFAULT_NAMED_PIPE__ = None
__DEFAULT_PASSWD__ = None
__DEFAULT_DB__ = None


# 配置
class ConnectionProperties:
    def __init__(self,
                 user=__DEFAULT_USER__,  # The first four arguments is based on DB-API 2.0 recommendation.
                 password=__DEFAULT_PASSWORD__,
                 host=__DEFAULT_HOST__,
                 database=__DEFAULT_DATABASE__,
                 unix_socket=__DEFAULT_UNIX_SOCKET__,
                 port=__DEFAULT_PORT__,
                 charset=__DEFAULT_CHARSET__,
                 sql_mode=__DEFAULT_SQL_MODE__,
                 read_default_file=__DEFAULT_READ_DEFAULT_FILE__,
                 conv=__DEFAULT_CONV__,
                 use_unicode=__DEFAULT_USE_UNICODE__,
                 client_flag=__DEFAULT_CLIENT_FLAG__,
                 init_command=__DEFAULT_INIT_COMMAND__,
                 connect_timeout=__DEFAULT_CONNECT_TIMEOUT__,
                 read_default_group=__DEFAULT_READ_DEFAULT_GROUP__,
                 autocommit=__DEFAULT_AUTOCOMMIT__,
                 local_infile=__DEFAULT_LOCAL_INFILE__,
                 max_allowed_packet=__DEFAULT_MAX_ALLOWED_PACKET__,
                 defer_connect=__DEFAULT_DEFER_CONNECT__,
                 auth_plugin_map=__DEFAULT_AUTH_PLUGIN_MAP__,
                 read_timeout=__DEFAULT_READ_TIMEOUT__,
                 write_timeout=__DEFAULT_WRITE_TIMEOUT__,
                 bind_address=__DEFAULT_BIND_ADDRESS__,
                 binary_prefix=__DEFAULT_BINARY_PREFIX__,
                 program_name=__DEFAULT_PROGRAM_NAME__,
                 server_public_key=__DEFAULT_SERVER_PUBLIC_KEY__,
                 ssl=__DEFAULT_SSL__,
                 ssl_ca=__DEFAULT_SSL_CA__,
                 ssl_cert=__DEFAULT_SSL_CERT__,
                 ssl_disabled=__DEFAULT_SSL_DISABLED__,
                 ssl_key=__DEFAULT_SSL_KEY__,
                 ssl_verify_cert=__DEFAULT_SSL_VERIFY_CERT__,
                 ssl_verify_identity=__DEFAULT_SSL_VERIFY_IDENTITY__,
                 compress=__DEFAULT_COMPRESS__,  # not supported
                 named_pipe=__DEFAULT_NAMED_PIPE__,  # not supported
                 passwd=__DEFAULT_PASSWD__,  # deprecated
                 db=__DEFAULT_DB__,  # deprecated,,
                 pool_maxsize=__DEFAULT_POOL_MAX_SIZE__,
                 pool_minsize=__DEFAULT_POOL_MIN_SIZE__,
                 max_lifetime=__DEFAULT_MAX_LIFETIME__,
                 validate_query=__DEFAULT_VALIDATE_QUERY__,
                 validate_interval=__DEFAULT_VALIDATE_INTERVAL__,
                 max_wait_time=__DEFAULT_MAX_WAIT_TIME__):
        self.user = __DEFAULT_USER__ if user is None else user if not os_env.is_os_evn(user) else os_env.get_env(user,
                                                                                                                 __DEFAULT_USER__)
        self.password = __DEFAULT_PASSWORD__ if password is None else password if not os_env.is_os_evn(
            password) else os_env.get_env(password, __DEFAULT_PASSWORD__)
        self.host = __DEFAULT_HOST__ if host is None else host if not os_env.is_os_evn(host) else os_env.get_env(host,
                                                                                                                 __DEFAULT_HOST__)
        self.database = __DEFAULT_DATABASE__ if database is None else database if not os_env.is_os_evn(
            database) else os_env.get_env(database, __DEFAULT_DATABASE__)
        self.unix_socket = __DEFAULT_UNIX_SOCKET__ if unix_socket is None else unix_socket if not os_env.is_os_evn(
            unix_socket) else os_env.get_env(unix_socket, __DEFAULT_UNIX_SOCKET__)
        self.port = __DEFAULT_PORT__ if port is None else port if not os_env.is_os_evn(str(port)) else int(
            os_env.get_env(str(port), __DEFAULT_PORT__))
        self.charset = __DEFAULT_CHARSET__ if charset is None else charset if not os_env.is_os_evn(
            charset) else os_env.get_env(charset, __DEFAULT_CHARSET__)
        self.sql_mode = __DEFAULT_SQL_MODE__ if sql_mode is None else sql_mode if not os_env.is_os_evn(
            sql_mode) else os_env.get_env(sql_mode, __DEFAULT_SQL_MODE__)
        self.read_default_file = __DEFAULT_READ_DEFAULT_FILE__ if read_default_file is None else read_default_file if not os_env.is_os_evn(
            read_default_file) else os_env.get_env(read_default_file, __DEFAULT_READ_DEFAULT_FILE__)
        self.conv = __DEFAULT_CONV__ if conv is None else conv if not os_env.is_os_evn(conv) else os_env.get_env(conv,
                                                                                                                 __DEFAULT_CONV__)
        self.use_unicode = __DEFAULT_USE_UNICODE__ if use_unicode is None else use_unicode if not os_env.is_os_evn(
            str(use_unicode)) else bool(os_env.get_env(str(use_unicode), __DEFAULT_USE_UNICODE__))
        self.client_flag = __DEFAULT_CLIENT_FLAG__ if client_flag is None else client_flag if not os_env.is_os_evn(
            str(client_flag)) else int(os_env.get_env(str(client_flag), __DEFAULT_CLIENT_FLAG__))
        self.init_command = __DEFAULT_INIT_COMMAND__ if init_command is None else init_command if not os_env.is_os_evn(
            init_command) else os_env.get_env(init_command, __DEFAULT_INIT_COMMAND__)
        self.connect_timeout = __DEFAULT_CONNECT_TIMEOUT__ if connect_timeout is None else connect_timeout if not os_env.is_os_evn(
            str(connect_timeout)) else int(os_env.get_env(str(connect_timeout), __DEFAULT_CONNECT_TIMEOUT__))
        self.read_default_group = __DEFAULT_READ_DEFAULT_GROUP__ if read_default_group is None else read_default_group if not os_env.is_os_evn(
            read_default_group) else os_env.get_env(read_default_group, __DEFAULT_READ_DEFAULT_GROUP__)
        self.autocommit = __DEFAULT_AUTOCOMMIT__ if autocommit is None else autocommit if not os_env.is_os_evn(
            str(autocommit)) else bool(os_env.get_env(str(autocommit), __DEFAULT_AUTOCOMMIT__))
        self.local_infile = __DEFAULT_LOCAL_INFILE__ if local_infile is None else local_infile if not os_env.is_os_evn(
            str(local_infile)) else bool(os_env.get_env(str(local_infile), __DEFAULT_LOCAL_INFILE__))
        self.max_allowed_packet = __DEFAULT_MAX_ALLOWED_PACKET__ if max_allowed_packet is None else max_allowed_packet if not os_env.is_os_evn(
            str(max_allowed_packet)) else int(os_env.get_env(str(max_allowed_packet), __DEFAULT_MAX_ALLOWED_PACKET__))
        self.defer_connect = __DEFAULT_DEFER_CONNECT__ if defer_connect is None else defer_connect if not os_env.is_os_evn(
            str(defer_connect)) else bool(os_env.get_env(str(defer_connect), __DEFAULT_DEFER_CONNECT__))
        self.auth_plugin_map = __DEFAULT_AUTH_PLUGIN_MAP__ if auth_plugin_map is None else auth_plugin_map if not os_env.is_os_evn(
            auth_plugin_map) else os_env.get_env(auth_plugin_map, __DEFAULT_AUTH_PLUGIN_MAP__)
        self.read_timeout = __DEFAULT_READ_TIMEOUT__ if read_timeout is None else read_timeout if not os_env.is_os_evn(
            str(read_timeout)) else int(os_env.get_env(str(read_timeout), __DEFAULT_READ_TIMEOUT__))
        self.write_timeout = __DEFAULT_WRITE_TIMEOUT__ if write_timeout is None else write_timeout if not os_env.is_os_evn(
            str(write_timeout)) else int(os_env.get_env(str(write_timeout), __DEFAULT_WRITE_TIMEOUT__))
        self.bind_address = __DEFAULT_BIND_ADDRESS__ if bind_address is None else bind_address if not os_env.is_os_evn(
            bind_address) else os_env.get_env(bind_address, __DEFAULT_BIND_ADDRESS__)
        self.binary_prefix = __DEFAULT_BINARY_PREFIX__ if binary_prefix is None else binary_prefix if not os_env.is_os_evn(
            str(binary_prefix)) else bool(os_env.get_env(str(binary_prefix), __DEFAULT_BINARY_PREFIX__))
        self.program_name = __DEFAULT_PROGRAM_NAME__ if program_name is None else program_name if not os_env.is_os_evn(
            program_name) else os_env.get_env(program_name, __DEFAULT_PROGRAM_NAME__)
        self.server_public_key = __DEFAULT_SERVER_PUBLIC_KEY__ if server_public_key is None else server_public_key if not os_env.is_os_evn(
            server_public_key) else os_env.get_env(server_public_key, __DEFAULT_SERVER_PUBLIC_KEY__)
        self.ssl = __DEFAULT_SSL__ if ssl is None else ssl if not os_env.is_os_evn(ssl) else os_env.get_env(ssl,
                                                                                                            __DEFAULT_SSL__)
        self.ssl_ca = __DEFAULT_SSL_CA__ if ssl_ca is None else ssl_ca if not os_env.is_os_evn(
            ssl_ca) else os_env.get_env(ssl_ca, __DEFAULT_SSL_CA__)
        self.ssl_cert = __DEFAULT_SSL_CERT__ if ssl_cert is None else ssl_cert if not os_env.is_os_evn(
            ssl_cert) else os_env.get_env(ssl_cert, __DEFAULT_SSL_CERT__)
        self.ssl_disabled = __DEFAULT_SSL_DISABLED__ if ssl_disabled is None else ssl_disabled if not os_env.is_os_evn(
            ssl_disabled) else os_env.get_env(ssl_disabled, __DEFAULT_SSL_DISABLED__)
        self.ssl_key = __DEFAULT_SSL_KEY__ if ssl_key is None else ssl_key if not os_env.is_os_evn(
            ssl_key) else os_env.get_env(ssl_key, __DEFAULT_SSL_KEY__)
        self.ssl_verify_cert = __DEFAULT_SSL_VERIFY_CERT__ if ssl_verify_cert is None else ssl_verify_cert if not os_env.is_os_evn(
            ssl_verify_cert) else os_env.get_env(ssl_verify_cert, __DEFAULT_SSL_VERIFY_CERT__)
        self.ssl_verify_identity = __DEFAULT_SSL_VERIFY_IDENTITY__ if ssl_verify_identity is None else ssl_verify_identity if not os_env.is_os_evn(
            ssl_verify_identity) else os_env.get_env(ssl_verify_identity, __DEFAULT_SSL_VERIFY_IDENTITY__)
        self.compress = __DEFAULT_COMPRESS__ if compress is None else compress if not os_env.is_os_evn(
            compress) else os_env.get_env(compress, __DEFAULT_COMPRESS__)
        self.named_pipe = __DEFAULT_NAMED_PIPE__ if named_pipe is None else named_pipe if not os_env.is_os_evn(
            named_pipe) else os_env.get_env(named_pipe, __DEFAULT_NAMED_PIPE__)
        self.passwd = __DEFAULT_PASSWD__ if passwd is None else passwd if not os_env.is_os_evn(
            passwd) else os_env.get_env(passwd, __DEFAULT_PASSWD__)
        self.db = __DEFAULT_DB__ if db is None else db if not os_env.is_os_evn(db) else os_env.get_env(db,
                                                                                                       __DEFAULT_DB__)
        self.pool_maxsize = __DEFAULT_POOL_MAX_SIZE__ if pool_maxsize is None else pool_maxsize if not os_env.is_os_evn(
            str(pool_maxsize)) else int(os_env.get_env(str(pool_maxsize), __DEFAULT_POOL_MAX_SIZE__))
        self.pool_minsize = __DEFAULT_POOL_MIN_SIZE__ if pool_minsize is None else pool_minsize if not os_env.is_os_evn(
            str(pool_minsize)) else int(os_env.get_env(str(pool_minsize), __DEFAULT_POOL_MIN_SIZE__))
        self.max_lifetime = __DEFAULT_MAX_LIFETIME__ if max_lifetime is None else max_lifetime if not os_env.is_os_evn(
            str(max_lifetime)) else int(os_env.get_env(str(max_lifetime), __DEFAULT_MAX_LIFETIME__))
        self.validate_query = __DEFAULT_VALIDATE_QUERY__ if validate_query is None else validate_query if not os_env.is_os_evn(
            validate_query) else os_env.get_env(validate_query, __DEFAULT_VALIDATE_QUERY__)
        self.validate_interval = __DEFAULT_VALIDATE_INTERVAL__ if validate_interval is None else validate_interval if not os_env.is_os_evn(
            str(validate_interval)) else int(os_env.get_env(str(validate_interval), __DEFAULT_VALIDATE_INTERVAL__))
        self.max_wait_time = __DEFAULT_MAX_WAIT_TIME__ if max_wait_time is None else max_wait_time if not os_env.is_os_evn(
            str(max_wait_time)) else int(os_env.get_env(str(max_wait_time), __DEFAULT_MAX_WAIT_TIME__))
        pass

    pass


# 连接
class Connection(pymysql.Connection):

    # 构造
    def __init__(self, conn_name: str, properties: ConnectionProperties):
        super().__init__(
            user=properties.user,
            password=properties.password,
            host=properties.host,
            database=properties.database,
            unix_socket=properties.unix_socket,
            port=properties.port,
            charset=properties.charset,
            sql_mode=properties.sql_mode,
            read_default_file=properties.read_default_file,
            conv=properties.conv,
            use_unicode=properties.use_unicode,
            client_flag=properties.client_flag,
            cursorclass=Cursor,
            init_command=properties.init_command,
            connect_timeout=properties.connect_timeout,
            read_default_group=properties.read_default_group,
            autocommit=properties.autocommit,
            local_infile=properties.local_infile,
            max_allowed_packet=properties.max_allowed_packet,
            defer_connect=properties.defer_connect,
            auth_plugin_map=properties.auth_plugin_map,
            read_timeout=properties.read_timeout,
            write_timeout=properties.write_timeout,
            bind_address=properties.bind_address,
            binary_prefix=properties.binary_prefix,
            program_name=properties.program_name,
            server_public_key=properties.server_public_key,
            ssl=properties.ssl,
            ssl_ca=properties.ssl_ca,
            ssl_cert=properties.ssl_cert,
            ssl_disabled=properties.ssl_disabled,
            ssl_key=properties.ssl_key,
            ssl_verify_cert=properties.ssl_verify_cert,
            ssl_verify_identity=properties.ssl_verify_identity,
            compress=properties.compress,  # not supported
            named_pipe=properties.named_pipe,  # not supported
            passwd=properties.passwd,  # deprecated
            db=properties.db,  # deprecated
        )
        self.__properties__ = properties
        self.__conn_name__ = conn_name
        self.__valid__()
        pass

    # 删除
    def __del__(self):
        try:
            log.debug('Connection[__del__]')
            self.__close__()
            pass
        except BaseException as e:
            log.warn(e)
            pass
        pass

    # 真实关闭方法, 在对象销毁时, 自动调用
    def __close__(self):
        log.debug('Connection[__close__]')
        super().close()
        pass

    # 脚本退出时调用
    def __exit__(self, exc_type, exc_val, exc_tb):
        log.debug('Connection[__exit__]')
        self.__close__()
        pass

    # 连接校验
    def __valid__(self):
        self.query_one(self.__properties__.validate_query)
        pass

    # 重写关闭连接,这里不是关闭,而是释放连接到连接池
    def close(self) -> None:
        ConnectionPoolManager.get(pool_name=self.__conn_name__).release_conn()
        pass

    # 屏蔽游标方法
    def cursor(self, cursor: None = ...):
        raise Exception("unsupport")
        pass

    # 查询单条数据
    def query_one(self, sql: str, row_mapper=None, args: object = None):
        c = super().cursor()
        try:
            a = SqlParser.get_sql(sql, args)
            c.execute(a[0], a[1])
            data = c.fetchone()
            pass
        finally:
            c.close()
            pass
        if row_mapper:
            return row_mapper(data)
        return data
        pass

    # 查询多条数据
    def query_all(self, sql: str, row_mapper=None, args=None):
        c = super().cursor()
        try:
            a = SqlParser.get_sql(sql, args)
            c.execute(a[0], a[1])
            datas = c.fetchall()
            pass
        finally:
            c.close()
            pass
        if not row_mapper:
            return datas
        results = []
        for data in datas:
            results.append(row_mapper(data))
            pass
        return results

    # 更新
    def update(self, sql: str, args=None, autocommit=True) -> int:
        c = super().cursor()
        try:
            a = SqlParser.get_sql(sql, args)
            m = c.execute(a[0], a[1])
            c.close()
            if autocommit:
                self.commit()
                pass
            return m
        finally:
            c.close()
            pass
        pass

    # 一组更新语句,如果是需要一起提交,则可使用此语句, 或者批量更新都可用此语句
    def update_many(self,
                    sqls: list[str],
                    args: list,
                    autocommit=True,
                    assert_for_exception: bool = False,
                    rollback_for_exception: bool = False):
        c = super().cursor()
        results = list[int]()
        try:
            _index = -1
            for sql in sqls:
                try:
                    _index = _index + 1
                    a = SqlParser.get_sql(sql, args[_index] if args and len(args) > 0 else None)
                    m = c.execute(a[0], a[1])
                    results.append(m)
                    pass
                except Exception as e:
                    log.debug("本次执行出了异常,sql:%s,args:%s", sql, args[_index] if args and len(args) > 0 else None)
                    if assert_for_exception:
                        raise e
                    results.append(0)
                    pass
                pass
        except Exception as e:
            if rollback_for_exception:
                self.rollback()
                pass
            log.error("执行出了异常:%s", e)
            pass
        finally:
            if autocommit and not rollback_for_exception:
                self.commit()
                pass
            pass
            c.close()

            pass
        pass

    pass


# 连接池
class ConnectionPool:
    def __init__(self, pool_name: str, properties: ConnectionProperties):
        # 连接池名称
        self.pool_name = pool_name
        # 属性参数
        self.properties = properties
        # 可用连接队列
        self.__conns__ = LifoQueue[Connection](properties.pool_maxsize)
        # thread local
        self.__thread_local__ = threading.local()
        # 当前连接
        self.__thread_local__.current_conn = None
        # 当前连接锁
        self.__current_conn_lock__ = threading.Lock()
        # 连接数
        self.__conn_num__ = 0
        # 连接数锁
        self.__conn_num_lock__ = threading.Lock()
        # 正在销毁标志
        self.__destroying__ = False
        # 创建连接
        self.__create_connections__()
        pass

    # 当前连接数计算
    def __calc_conn_num__(self, increment: int = 1) -> bool:
        result = True
        self.__conn_num_lock__.acquire(blocking=True)
        self.__conn_num__ = self.__conn_num__ + increment
        if self.__conn_num__ > self.properties.pool_maxsize:
            result = False
            self.__conn_num__ = self.__conn_num__ - increment
            pass
        self.__conn_num_lock__.release()
        return result
        pass

    # 创建连接,线程安全
    def __create_connections__(self):
        # 先加数,加成功则创建连接,否则不创建
        if self.__calc_conn_num__(1):
            # 创建连接
            try:
                conn = Connection(self.pool_name, self.properties)
                self.__conns__.put(conn)
                pass
            # 创建连接异常,则回滚连接数
            except BaseException as e:
                self.__calc_conn_num__(-1)
                raise e
                pass
            pass
        pass

    # 获取连接,线程安全
    def get_conn(self) -> Connection:
        if self.__destroying__:
            raise Exception('连接池正在销毁')
        log.debug('get_conn,当前连接池大小:%s', self.__conns__.qsize())
        if self.__thread_local__.current_conn:
            log.debug('get_conn,当前连接池大小:%s', self.__conns__.qsize())
            return self.__thread_local__.current_conn
        self.__current_conn_lock__.acquire(blocking=True, timeout=self.properties.connect_timeout)
        try:
            if self.__thread_local__.current_conn:
                return self.__thread_local__.current_conn
            conn = self.__conns__.get(block=True, timeout=self.properties.connect_timeout)
            if not conn:
                raise Exception('无可用的连接')
            self.__thread_local__.current_conn = conn
            pass
        finally:
            self.__current_conn_lock__.release()
            pass
        log.debug('get_conn,当前连接池大小:%s', self.__conns__.qsize())
        return self.__thread_local__.current_conn
        pass

    # 释放连接
    def release_conn(self):
        conn = self.get_conn()
        self.__conns__.put(conn)
        self.__thread_local__.current_conn = None
        log.debug('release_conn,当前连接池大小:%s', self.__conns__.qsize())
        pass

    # 安全停止
    def __del__(self):
        self.__destroy__()
        pass

    def __destroy__(self):
        self.__destroying__ = True
        while self.__conns__.qsize() != self.__conn_num__:
            time.sleep(1)
            pass
        # for _ in range(self.__conns__.qsize()):
        #     self.__conns__.get().__del__()
        #     pass
        pass

    pass


# 连接管理器
class ConnectionPoolManager:
    __conn_pools__ = {}

    @staticmethod
    def get(pool_name: str) -> ConnectionPool:
        return ConnectionPoolManager.__conn_pools__[pool_name]
        pass

    @staticmethod
    def add(pool_name: str, pool: ConnectionPool) -> None:
        ConnectionPoolManager.__conn_pools__[pool_name] = pool
        pass

    @staticmethod
    def remove(pool_name: str):
        del ConnectionPoolManager.__conn_pools__[pool_name]
        pass

    @staticmethod
    def reset():
        ConnectionPoolManager.__conn_pools__ = {}
        pass

    pass


class ConnectionHelper:

    @staticmethod
    def query_one(sql: str, row_mapper=None, args=None):
        _ = SqlParser.parse(sql)
        pool = ConnectionPoolManager.get(_[0])
        conn = pool.get_conn()
        try:
            return conn.query_one(_[1], row_mapper, args)
        finally:
            conn.close()
        pass

    @staticmethod
    def query_all(sql: str, row_mapper=None, args=None):
        _ = SqlParser.parse(sql)
        pool = ConnectionPoolManager.get(_[0])
        conn = pool.get_conn()
        try:
            return conn.query_all(_[1], row_mapper, args)
        finally:
            conn.close()
        pass

    @staticmethod
    def update(sql: str, args=None) -> int:
        _ = SqlParser.parse(sql)
        pool = ConnectionPoolManager.get(_[0])
        conn = pool.get_conn()
        try:
            return conn.update(_[1], args)
        finally:
            conn.close()
        pass

    @staticmethod
    def get_conn(pool_name: str):
        return ConnectionPoolManager.get(pool_name.strip())
        pass

    pass


# 存储单个连接的map
class ConnectionMap:
    __conns__ = {}

    @staticmethod
    def add(conn: Connection) -> None:
        ConnectionMap.__conns__[conn.__conn_name__] = conn
        pass

    @staticmethod
    def get(conn_name: str) -> Connection:
        return ConnectionMap.__conns__[conn_name]
        pass

    @staticmethod
    def remove(conn_name: str) -> None:
        conn = ConnectionMap.get(conn_name)
        if conn:
            del ConnectionMap.__conns__[conn_name]
            pass
        pass

    pass


class SqlParser:

    @staticmethod
    def is_read(sql: str):
        return SqlParser.is_select(sql)

    @staticmethod
    def is_write(sql: str):
        return not SqlParser.is_read(sql)

    @staticmethod
    def is_update(sql: str):
        return sql and sql.lower().strip().startswith('update')
        pass

    @staticmethod
    def is_delete(sql: str):
        return sql and sql.lower().strip().startswith('delete')

    @staticmethod
    def is_insert(sql: str):
        return sql and sql.lower().strip().startswith('insert')

    @staticmethod
    def is_select(sql: str):
        return sql and sql.lower().strip().startswith('select')

    # 解析sql, 返回 [虚拟库名,真实sql,虚拟表名,真实表名]
    @staticmethod
    def parse(sql: str) -> tuple:
        _sql = sql.lower().strip()
        table_name = None
        _sql = _sql.replace('\n', " ")
        _sql = _sql.replace('\t', " ")
        _splits = re.split(r'\s+', _sql)
        _len = len(_splits)
        if SqlParser.is_select(sql) or SqlParser.is_delete(sql):
            for i in range(_len):
                if _splits[i] == 'from':
                    if i < (_len - 1):
                        table_name = _splits[i + 1]
                        break
                        pass
                    pass
                pass
            pass
        elif SqlParser.is_insert(sql):
            for i in range(_len):
                if _splits[i] == 'into':
                    if i < (_len - 1):
                        table_name = re.split(r'\(', _splits[i + 1])[0]
                        break
                    pass
                pass
            pass
        elif SqlParser.is_update(sql):
            for i in range(_len):
                if _splits[i] == 'update':
                    if i < (_len - 1):
                        table_name = _splits[i + 1]
                        break
                        pass
                    pass
                pass
            pass

        pool_name = Constants.DEFAULT_POOL_NAME
        a_table_name = table_name
        if table_name:
            table_name = table_name.strip()
            a_t = Constants.TABLE_CONFIG.get(table_name, None)
            if a_t:
                _splits = str.split(a_t, '.')
                if len(_splits) == 2:
                    pool_name = _splits[0].strip()
                    a_table_name = _splits[1].strip()
                    pass
                elif len(_splits) == 1:
                    pool_name = Constants.DEFAULT_POOL_NAME
                    a_table_name = _splits[0]
                    pass
                pass
            pass
        return pool_name, sql if not a_table_name else sql.replace(table_name, a_table_name), table_name, a_table_name,
        pass

    @staticmethod
    def get_sql(sql: str, args):
        log.debug("connections->sql:%s,args:%s", sql, args)
        if not args:
            return sql, None
        if isinstance(args, dict):
            keys = args.keys()
            for key in keys:
                sql = sql.replace(':' + key, '%(' + key + ')s')
                pass
            pass
        elif isinstance(args, list):
            sql = sql.replace('?', '%s')
            pass
        elif isinstance(args, tuple):
            sql = sql.replace('?', '%s')
            _args = []
            for arg in args:
                _args.append(arg)
                pass
            args = _args
            pass
        else:
            sql = sql.replace('?', '%s')
            pass
        return sql, args

    pass


class Constants:
    TABLE_CONFIG = {}

    DEFAULT_POOL_NAME = None

    @staticmethod
    def init_table_config(config: dict):
        Constants.TABLE_CONFIG = config
        pass

    @staticmethod
    def reset():
        Constants.TABLE_CONFIG = {}
        Constants.DEFAULT_POOL_NAME = None
        pass

    pass


__CONFIG_KEY__ = "db.util"
__CONFIG_CONNECTIONS_KEY__ = __CONFIG_KEY__ + ".connections"
__CONFIG_CONNECTIONS_POOLS_KEY__ = __CONFIG_CONNECTIONS_KEY__ + ".pools"
__CONFIG_CONNECTIONS_TABLES_KEY__ = __CONFIG_CONNECTIONS_KEY__ + ".tables"
__CONFIG_CONNECTIONS_DEFAULT_POOL_NAME_KEY__ = __CONFIG_CONNECTIONS_KEY__ + '.default_pool'

__CONFIG_FILES__ = [
    'application.yaml',
    'connections.yaml',
    'application.json',
    'connections.json'  # ,
    # 'application.properties',
    # 'connections.properties',
    # 'application.ini',
    # 'connections.ini'
]


class Starter:
    __STARTING__ = False
    __STARTING_LOCK__ = threading.Lock()

    @staticmethod
    def load_config() -> dict:
        config = {}
        root_path = os.path.abspath('')
        for _ in __CONFIG_FILES__:
            config_path = root_path + os.path.sep + 'config/' + _
            if os.path.exists(config_path):
                if Starter.is_yaml(config_path):
                    config = yaml.load(open(config_path, mode='r'), yaml.FullLoader)
                    break
                    pass
                if Starter.is_json(config_path):
                    config = json.load(open(config_path, mode='r'))
                    break
                    pass
                if Starter.is_properties(config_path):
                    pass

                pass
            pass
        return config
        pass

    @staticmethod
    def is_yaml(_p: str):
        return _p.endswith('yaml')
        pass

    @staticmethod
    def is_json(_p: str):
        return _p.endswith('json')
        pass

    @staticmethod
    def is_properties(_p: str):
        return _p.endswith('properties')
        pass

    @staticmethod
    def start(config=None):
        if Starter.__STARTING__:
            raise Exception('already staring...')
        Starter.__STARTING_LOCK__.acquire(blocking=True)
        try:
            if Starter.__STARTING__:
                raise Exception('already staring...')
            Starter.__STARTING__ = True
            pass
        finally:
            Starter.__STARTING_LOCK__.release()
            pass
        try:
            if not config:
                config = Starter.load_config()
                pass
            log.debug('config:%s', json.dumps(config))
            _splits = __CONFIG_CONNECTIONS_POOLS_KEY__.split(".")

            _ = Starter.__get_config__(root_config=config, key=__CONFIG_CONNECTIONS_POOLS_KEY__)

            _properties = {}

            _ks = _.keys()

            for _k in _ks:
                _properties[_k] = ConnectionProperties(
                    user=_.get(_k, {}).get('user', None),
                    password=_.get(_k, {}).get('password', None),
                    host=_.get(_k, {}).get('host', None),
                    database=_.get(_k, {}).get('database', None),
                    unix_socket=_.get(_k, {}).get('unix_socket', None),
                    port=_.get(_k, {}).get('port', None),
                    charset=_.get(_k, {}).get('charset', None),
                    sql_mode=_.get(_k, {}).get('sql_mode', None),
                    read_default_file=_.get(_k, {}).get('read_default_file', None),
                    conv=_.get(_k, {}).get('conv', None),
                    use_unicode=_.get(_k, {}).get('use_unicode', None),
                    client_flag=_.get(_k, {}).get('client_flag', None),
                    init_command=_.get(_k, {}).get('init_command', None),
                    connect_timeout=_.get(_k, {}).get('connect_timeout', None),
                    read_default_group=_.get(_k, {}).get('read_default_group', None),
                    autocommit=_.get(_k, {}).get('autocommit', None),
                    local_infile=_.get(_k, {}).get('local_infile', None),
                    max_allowed_packet=_.get(_k, {}).get('max_allowed_packet', None),
                    defer_connect=_.get(_k, {}).get('defer_connect', None),
                    auth_plugin_map=_.get(_k, {}).get('auth_plugin_map', None),
                    read_timeout=_.get(_k, {}).get('read_timeout', None),
                    write_timeout=_.get(_k, {}).get('write_timeout', None),
                    bind_address=_.get(_k, {}).get('bind_address', None),
                    binary_prefix=_.get(_k, {}).get('binary_prefix', None),
                    program_name=_.get(_k, {}).get('program_name', None),
                    server_public_key=_.get(_k, {}).get('server_public_key', None),
                    ssl=_.get(_k, {}).get('ssl', None),
                    ssl_ca=_.get(_k, {}).get('ssl_ca', None),
                    ssl_cert=_.get(_k, {}).get('ssl_cert', None),
                    ssl_disabled=_.get(_k, {}).get('ssl_disabled', None),
                    ssl_key=_.get(_k, {}).get('ssl_key', None),
                    ssl_verify_cert=_.get(_k, {}).get('ssl_verify_cert', None),
                    ssl_verify_identity=_.get(_k, {}).get('ssl_verify_identity', None),
                    compress=_.get(_k, {}).get('compress', None),  # not supported
                    named_pipe=_.get(_k, {}).get('named_pipe', None),  # not supported
                    passwd=_.get(_k, {}).get('passwd', None),  # deprecated
                    db=_.get(_k, {}).get('db', None),  # deprecated,,
                    pool_maxsize=_.get(_k, {}).get('pool_maxsize', None),
                    pool_minsize=_.get(_k, {}).get('pool_minsize', None),
                    max_lifetime=_.get(_k, {}).get('max_lifetime', None),
                    validate_query=_.get(_k, {}).get('validate_query', None),
                    validate_interval=_.get(_k, {}).get('validate_interval', None),
                    max_wait_time=_.get(_k, {}).get('max_wait_time', None)
                )
                pass

            for _pk in _properties.keys():
                ConnectionPoolManager.add(_pk, ConnectionPool(_pk, _properties[_pk]))
                pass

            _ = Starter.__get_config__(root_config=config, key=__CONFIG_CONNECTIONS_TABLES_KEY__)

            Constants.init_table_config(_)

            Constants.DEFAULT_POOL_NAME = Starter.__get_config__(root_config=config,
                                                                 key=__CONFIG_CONNECTIONS_DEFAULT_POOL_NAME_KEY__)
            pass
        except BaseException as e:
            Starter.__STARTING__ = False
            raise e
            pass

        pass

    @staticmethod
    def stop():
        # 重置Constants
        Constants.reset()
        # 重置ConnectionManager
        ConnectionPoolManager.reset()
        pass

    @staticmethod
    def __get_config__(root_config: dict, key: str):
        _ = dict(root_config)
        _splits = key.split(".")
        for _s in _splits:
            if not _:
                raise Exception("config error")
                pass
            _ = _.get(_s, None)
            pass
        return _
        pass

    pass
