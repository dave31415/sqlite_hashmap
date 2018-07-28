from time import time
import random

from sqlite_hashmap.hash_map import *


def test_key_value():
    db = get_db(file=None)

    table = 'hash_map'
    drop_table(db, table)
    create_lookup_table(db, table, key_type='TEXT', value_type='INTEGER')
    values_list = [('foo', 42), ('bar', 420), ('buzz', 99)]
    insert_many_hash_map(db, table, values_list)

    key = 'buzz'
    val = key_val_lookup(db, table, key)
    print(key, val)

    key = 'foo'
    val = key_val_lookup(db, table, key)
    print(key, val)

    key = 'blah'
    val = key_val_lookup(db, table, key)
    print(key, val)


# Test code below and timing


def key_value_stream():
    i = 0
    while True:
        if i % 1000000 == 0:
            print("%s million" % (i / 1000000))

        key = 'key' + str(i)
        val = 'val' + str(i)
        yield key, val
        i += 1


def test_load_big():
    n_million = 1
    n_read = 10000
    n = round(n_million * 1000000)

    print('Loading DB with %s million rows' % n_million)

    db = get_db()
    with db:
        # start of transaction
        hash_map = SQLiteHashMap(db, 'test_table')

        kv_stream = key_value_stream()
        key_vals = (next(kv_stream) for _ in range(n))

        start = time()

        hash_map.insert_many(key_vals)

        print('Done loading DB')
        runtime = time() - start
        rate = n / runtime
        rate = "{:,}".format(round(rate))
        print('Time loading DB: %s seconds, rate: %s inserts/sec' % (runtime, rate))

        key = 'key9'
        print(key, hash_map[key])

        key = 'key99'
        print(key, hash_map[key])

        key = 'key%s' % (n - 1)
        val = hash_map[key]
        print(key, val)
        assert val == 'val%s' % (n - 1)
        # end of transaction

    if n_read == 0:
        return

    random.seed(42)
    start = time()

    for _ in range(n_read):
        r = random.randint(0, n)
        key = 'key%s' % r
        value = key_val_lookup(db, 'test_table', key)
        assert value == 'val%s' % r

    runtime = time() - start
    rate = n_read / runtime
    rate = "{:,}".format(round(rate))
    runtime = "%0.3f" % runtime
    print('Time reading DB one by one: %s seconds, rate: %s lookups/sec' % (runtime, rate))


