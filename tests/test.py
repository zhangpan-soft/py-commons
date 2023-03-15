import threading
import time

import util.db.connections as connections
import asyncio
from concurrent.futures import ThreadPoolExecutor


def start():
    connections.Starter.start({
        'db': {
            'util': {
                'connections': {
                    'pools': {
                        'a': {
                            'user': '${user}',
                            'password': '${password}',
                            'host': '${host}',
                            'port': '${port}',
                            'database': '${database}'
                        }
                    },
                    'default_pool': 'a',
                    'tables': {
                        # 'a': 'business_devices'
                    }
                }
            }
        }
    })
    pass


async def query_one():
    print(connections.ConnectionHelper.query_one("select * from business_devices limit 1"))
    pass


def test1():
    connections.Starter.start({
        'db': {
            'util': {
                'connections': {
                    'pools': {
                        'a': {
                            'user': '${user}',
                            'password': '${password}',
                            'host': '${host}',
                            'port': '${port}',
                            'database': '${database}'
                        }
                    },
                    'default_pool': 'a',
                    'tables': {
                        'a': 'business_devices'
                    }
                }
            }
        }
    })
    print(connections.ConnectionHelper.query_one('select * from a limit 1'))
    pass


def test2():
    connections.Starter.start({
        'db': {
            'util': {
                'connections': {
                    'pools': {
                        'a': {
                            'user': '${user}',
                            'password': '${password}',
                            'host': '${host}',
                            'port': '${port}',
                            'database': '${database}'
                        }
                    },
                    'default_pool': 'a',
                    'tables': {
                        'a': 'a.business_devices'
                    }
                }
            }
        }
    })
    print(connections.ConnectionHelper.query_one('select * from a limit 1'))
    pass


def test3():
    connections.Starter.start({
        'db': {
            'util': {
                'connections': {
                    'pools': {
                        'a': {
                            'user': '${user}',
                            'password': '${password}',
                            'host': '${host}',
                            'port': '${port}',
                            'database': '${database}'
                        }
                    },
                    'default_pool': 'a',
                    'tables': {
                        # 'a': 'business_devices'
                    }
                }
            }
        }
    })
    print(connections.ConnectionHelper.query_one('select * from business_devices limit 1'))
    pass


def test4():
    start()
    # task1 = asyncio.create_task(query_one())
    # task2 = asyncio.create_task(query_one())
    # task3 = asyncio.create_task(query_one())
    # task4 = asyncio.create_task(query_one())
    # task5 = asyncio.create_task(query_one())

    asyncio.run(query_one())
    asyncio.run(query_one())
    asyncio.run(query_one())
    asyncio.run(query_one())
    asyncio.run(query_one())
    pass


def test5():
    start()
    t1 = threading.Thread(target=connections.ConnectionHelper.query_one,
                          args=["select * from business_devices limit 1", None, None])
    t2 = threading.Thread(target=connections.ConnectionHelper.query_one,
                          args=["select * from business_devices limit 1", None, None])
    t3 = threading.Thread(target=connections.ConnectionHelper.query_one,
                          args=["select * from business_devices limit 1", None, None])
    t4 = threading.Thread(target=connections.ConnectionHelper.query_one,
                          args=["select * from business_devices limit 1", None, None])
    t5 = threading.Thread(target=connections.ConnectionHelper.query_one,
                          args=["select * from business_devices limit 1", None, None])
    t6 = threading.Thread(target=connections.ConnectionHelper.query_one,
                          args=["select * from business_devices limit 1", None, None])

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()

    time.sleep(10)
    pass


def test6():
    local = threading.local()

    print(local.__dict__)
    print(local.__dict__.get('a', None))
    pass


def test7():
    start()
    pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix='测试')
    features = []
    for i in range(100):
        feature1 = pool.submit(action)
        features.append(feature1)
        pass
    await_all_feature(features)
    pool.shutdown()
    time.sleep(100)
    stop()
    pass


def await_all_feature(features: list):
    while features and len(features) > 0:
        feature = features.pop()
        if feature.done():
            print(feature.result())
            pass
        else:
            features.append(feature)
        pass
    pass


def action():
    datas = connections.ConnectionHelper.query_one("select * from business_devices limit 1")
    print(datas)
    return datas
    pass


def test8():
    _l = ['a', 'b', 'c']
    print(_l.remove('a'))
    print(_l.remove('d'))
    pass


def test9():
    start()
    pass


def stop():
    connections.Starter.stop()
    pass


if __name__ == '__main__':
    # test1()
    # test2()
    # test4()
    # test5()
    # test6()
    test7()
    # test8()
    pass
