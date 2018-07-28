import sqlite3
from functools import lru_cache


def get_db(file=None):
    if file:
        db = sqlite3.connect(file).cursor()
    else:
        # in memory only
        db = sqlite3.connect(":memory:")

    return db


def drop_table(db, table):
    sql = "DROP TABLE IF EXISTS %s" % table
    db.execute(sql)


def create_lookup_table(db, table, key_type='TEXT', value_type='INTEGER'):
    sql = "CREATE TABLE {table} (KEY {key_type} PRIMARY KEY, VALUE {value_type} )"

    sql = sql.format(table=table,
                     key_type=key_type,
                     value_type=value_type)
    db.execute(sql)


def insert_many_hash_map(db, table, values_list):
    db.executemany("INSERT INTO {table} VALUES (?,?)".format(table=table),
                   values_list)


def key_val_lookup(db, table, key):
    sql = "SELECT VALUE FROM {table} WHERE KEY = '{key}'".format(table=table,
                                                                 key=key)
    result = db.execute(sql).fetchone()
    if result is None:
        return None
    assert len(result) < 2
    return result[0]


class SQLiteHashMap:
    def __init__(self, db, name, key_type='TEXT', value_type='INTEGER'):
        self.name = name
        self.db = db
        self.key_type = key_type
        self.value_type = value_type
        drop_table(db, self.name)
        create_lookup_table(db, name, key_type=key_type, value_type=value_type)

    def insert_many(self, key_value_list):
        insert_many_hash_map(self.db, self.name, key_value_list)

    def __setitem__(self, key, value):
        key_value_list = [(key, value)]
        self.insert_many(key_value_list)

    # Number of key,val pairs that will be cached in
    # Python memory. Some power of 2.
    lru_cache_size_def = 262144

    @lru_cache(maxsize=lru_cache_size_def)
    def __getitem__(self, item):
        return key_val_lookup(self.db, self.name, item)
