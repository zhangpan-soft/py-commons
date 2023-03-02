import util.db.connections as connections


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


if __name__ == '__main__':
    # test1()
    # test2()
    test3()
    pass
