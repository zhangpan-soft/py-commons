import json
import time

import pymysql
from pymysql.cursors import Cursor
import threading
import re
import yaml
import os
from queue import LifoQueue

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
        self.user = user if user is not None else __DEFAULT_USER__
        self.password = password if password is not None else __DEFAULT_PASSWORD__
        self.host = host if host is not None else __DEFAULT_HOST__
        self.database = database if database is not None else __DEFAULT_DATABASE__
        self.unix_socket = unix_socket if unix_socket is not None else __DEFAULT_UNIX_SOCKET__
        self.port = port if port is not None else __DEFAULT_PORT__
        self.charset = charset if charset is not None else __DEFAULT_CHARSET__
        self.sql_mode = sql_mode if sql_mode is not None else __DEFAULT_SQL_MODE__
        self.read_default_file = read_default_file if read_default_file is not None else __DEFAULT_READ_DEFAULT_FILE__
        self.conv = conv if conv is not None else __DEFAULT_CONV__
        self.use_unicode = use_unicode if use_unicode is not None else __DEFAULT_USE_UNICODE__
        self.client_flag = client_flag if client_flag is not None else __DEFAULT_CLIENT_FLAG__
        self.init_command = init_command if init_command is not None else __DEFAULT_INIT_COMMAND__
        self.connect_timeout = connect_timeout if connect_timeout is not None else __DEFAULT_CONNECT_TIMEOUT__
        self.read_default_group = read_default_group if read_default_group is not None else __DEFAULT_READ_DEFAULT_GROUP__
        self.autocommit = autocommit if autocommit is not None else __DEFAULT_AUTOCOMMIT__
        self.local_infile = local_infile if local_infile is not None else __DEFAULT_LOCAL_INFILE__
        self.max_allowed_packet = max_allowed_packet if max_allowed_packet is not None else __DEFAULT_MAX_ALLOWED_PACKET__
        self.defer_connect = defer_connect if defer_connect is not None else __DEFAULT_DEFER_CONNECT__
        self.auth_plugin_map = auth_plugin_map if auth_plugin_map is not None else __DEFAULT_AUTH_PLUGIN_MAP__
        self.read_timeout = read_timeout if read_timeout is not None else __DEFAULT_READ_TIMEOUT__
        self.write_timeout = write_timeout if write_timeout is not None else __DEFAULT_WRITE_TIMEOUT__
        self.bind_address = bind_address if bind_address is not None else __DEFAULT_BIND_ADDRESS__
        self.binary_prefix = binary_prefix if binary_prefix is not None else __DEFAULT_BINARY_PREFIX__
        self.program_name = program_name if program_name is not None else __DEFAULT_PROGRAM_NAME__
        self.server_public_key = server_public_key if server_public_key is not None else __DEFAULT_SERVER_PUBLIC_KEY__
        self.ssl = ssl if ssl is not None else __DEFAULT_SSL__
        self.ssl_ca = ssl_ca if ssl_ca is not None else __DEFAULT_SSL_CA__
        self.ssl_cert = ssl_cert if ssl_cert is not None else __DEFAULT_SSL_CERT__
        self.ssl_disabled = ssl_disabled if ssl_disabled is not None else __DEFAULT_SSL_DISABLED__
        self.ssl_key = ssl_key if ssl_key is not None else __DEFAULT_SSL_KEY__
        self.ssl_verify_cert = ssl_verify_cert if ssl_verify_cert is not None else __DEFAULT_SSL_VERIFY_CERT__
        self.ssl_verify_identity = ssl_verify_identity if ssl_verify_identity is not None else __DEFAULT_SSL_VERIFY_IDENTITY__
        self.compress = compress if compress is not None else __DEFAULT_COMPRESS__  # not supported
        self.named_pipe = named_pipe if named_pipe is not None else __DEFAULT_NAMED_PIPE__  # not supported
        self.passwd = passwd if passwd is not None else __DEFAULT_PASSWD__  # deprecated
        self.db = db if db is not None else __DEFAULT_DB__  # deprecated,
        self.pool_maxsize = pool_maxsize if pool_maxsize is not None else __DEFAULT_POOL_MAX_SIZE__
        self.pool_minsize = pool_minsize if pool_minsize is not None else __DEFAULT_POOL_MIN_SIZE__
        self.max_lifetime = max_lifetime if max_lifetime is not None else __DEFAULT_MAX_LIFETIME__
        self.validate_query = validate_query if validate_query is not None else __DEFAULT_VALIDATE_QUERY__
        self.validate_interval = validate_interval if validate_interval is not None else __DEFAULT_VALIDATE_INTERVAL__
        self.max_wait_time = max_wait_time if max_wait_time is not None else __DEFAULT_MAX_WAIT_TIME__
        pass

    pass


# 连接
class Connection(pymysql.Connection):

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

    def __del__(self):
        try:
            log.debug('Connection[__del__]')
            self.__close__()
            pass
        except:
            pass
        pass

    def __close__(self):
        log.debug('Connection[__close__]')
        super().close()
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.debug('Connection[__exit__]')
        self.__close__()
        pass

    def __valid__(self):
        self.query_one(self.__properties__.validate_query)
        pass

    def close(self) -> None:
        ConnectionPoolManager.get(pool_name=self.__conn_name__).release_conn()
        pass

    def cursor(self, cursor: None = ...):
        raise Exception("unsupport")
        pass

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
                pass
            pass
        pass

    # 获取连接,线程安全
    def get_conn(self) -> Connection:
        if self.__destroying__:
            raise Exception('连接池正在销毁')

        if self.__thread_local__.current_conn:
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
        return self.__thread_local__.current_conn
        pass

    # 释放连接
    def release_conn(self):
        conn = self.get_conn()
        self.__conns__.put(conn)
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
    def __get_conn_pool__(sql: str) -> ConnectionPool:
        pool_name = SqlParser.get_pool_name(sql)
        pool = ConnectionPoolManager.get(pool_name)
        return pool
        pass

    @staticmethod
    def query_one(sql: str, row_mapper=None, args=None):
        pool = ConnectionHelper.__get_conn_pool__(sql)
        conn = pool.get_conn()
        try:
            return conn.query_one(sql, row_mapper, args)
        finally:
            conn.close()
        pass

    @staticmethod
    def query_all(sql: str, row_mapper=None, args=None):
        pool = ConnectionHelper.__get_conn_pool__(sql)
        conn = pool.get_conn()
        try:
            return conn.query_all(sql, row_mapper, args)
        finally:
            conn.close()
        pass

    @staticmethod
    def update(sql: str, args=None):
        pool = ConnectionHelper.__get_conn_pool__(sql)
        conn = pool.get_conn()
        try:
            return conn.update(sql, args)
        finally:
            conn.close()
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
        return sql and str.startswith(str.lower(sql.strip(sql)), 'update')
        pass

    @staticmethod
    def is_delete(sql: str):
        return sql and str.startswith(str.lower(sql.strip(sql)), 'delete')

    @staticmethod
    def is_insert(sql: str):
        return sql and str.startswith(str.lower(sql.strip(sql)), 'insert')

    @staticmethod
    def is_select(sql: str):
        return sql and str.startswith(str.lower(sql.strip(sql)), 'select')

    @staticmethod
    def get_table_name(sql: str) -> str:
        _sql = str.lower(str.strip(sql))
        table_name = None
        _sql = str.replace(_sql, '\n', " ")
        _sql = str.replace(_sql, '\t', " ")
        _splits = re.split(r'\s*', _sql)
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
            if _len >= 3:
                _t = _splits[2]
                table_name = re.split(r'\(', _t)[0]
                pass
            pass
        elif SqlParser.is_update(sql):
            if _len >= 2:
                table_name = _splits[1]
                pass
            pass
        return table_name
        pass

    @staticmethod
    def get_pool_name(sql: str) -> str:
        table_name = SqlParser.get_table_name(sql)
        pool_name = None
        if table_name:
            v_table_name = Constants.TABLE_CONFIG[table_name]
            pool_name = str.split(v_table_name, '.')[0]
            pass
        return pool_name
        pass

    @staticmethod
    def get_sql(sql: str, args):
        log.debug("connections->sql:%s,args:%s", sql, args)
        if not args:
            return sql, None
        if isinstance(args, dict):
            keys = args.keys()
            for key in keys:
                sql = str.replace(sql, ':' + key, '%(' + key + ')s')
                pass
            pass
        elif isinstance(args, list):
            sql = str.replace(sql, '?', '%s')
            pass
        elif isinstance(args, tuple):
            sql = str.replace(sql, '?', '%s')
            _args = []
            for arg in args:
                _args.append(arg)
                pass
            args = _args
            pass
        else:
            sql = str.replace(sql, '?', '%s')
            pass
        return sql, args

    pass


class Constants:
    TABLE_CONFIG = {}

    @staticmethod
    def init_table_config(config: dict):
        Constants.TABLE_CONFIG = config
        pass

    @staticmethod
    def reset():
        Constants.TABLE_CONFIG = {}
        pass

    pass


__CONFIG_KEY__ = "db.util"
__CONFIG_CONNECTIONS_KEY__ = __CONFIG_KEY__ + ".connections"
__CONFIG_CONNECTIONS_POOLS_KEY__ = __CONFIG_CONNECTIONS_KEY__ + ".pools"
__CONFIG_CONNECTIONS_TABLES_KEY__ = __CONFIG_CONNECTIONS_KEY__ + ".tables"

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
        return str.endswith(_p, 'yaml')
        pass

    @staticmethod
    def is_json(_p: str):
        return str.endswith(_p, 'json')
        pass

    @staticmethod
    def is_properties(_p: str):
        return str.endswith(_p, 'properties')
        pass

    @staticmethod
    def start(config: None):
        if not config:
            config = Starter.load_config()
            pass
        log.debug('config:%s', json.dumps(config))
        _splits = str.split(__CONFIG_CONNECTIONS_POOLS_KEY__, ".")

        _ = Starter.__get_config_dict__(root_config=config, key=__CONFIG_CONNECTIONS_POOLS_KEY__)

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

        _ = Starter.__get_config_dict__(root_config=config, key=__CONFIG_CONNECTIONS_TABLES_KEY__)

        Constants.init_table_config(_)

        pass

    @staticmethod
    def stop():
        # 重置Constants
        Constants.reset()
        # 重置ConnectionManager
        ConnectionPoolManager.reset()
        pass

    @staticmethod
    def __get_config_dict__(root_config: dict, key: str):
        _ = dict(root_config)
        _splits = str.split(key, ".")
        for _s in _splits:
            if not _:
                raise Exception("config error")
                pass
            _ = _.get(_s, None)
            pass
        return _
        pass

    pass
