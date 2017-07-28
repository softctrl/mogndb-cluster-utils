# -*- coding: utf-8 -*-

"""
scmgr.py

I am developing this program to help-me at my work.
I hope that this project may help you too.
But, i will not be responsible for any trouble that you may have.
You are by your own.

If you do not have knowledge so is better you do not use this.

Copyright (c) 2017 Carlos Timoshenko Rodrigues Lopes carlostimoshenkorodrigueslopes@gmail.com
http://www.0x09.com.br

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


"""

import sys
import json
from pymongo import MongoClient, ASCENDING, DESCENDING


# Just create a conection to a mongodb.
def connect_mongodb():

    config = load_admin_database_config()
    if not config:
        raise Exception('It was not possible to identify the admin connection configuration file.')

    usr = config['user']
    pwd = config['password']
    srv = config['server']
    prt = config['port']

    uri = 'mongodb://' + usr + ':' + pwd + '@' + srv + ':' + str(prt) + '/admin?authMechanism=SCRAM-SHA-1'
    return MongoClient(uri)


def load_admin_database_config():
    with open('mongodb.json') as data_file:
        return json.load(data_file)


# Load the database file configuration.
def load_database_config(database_config):
    with open(database_config) as data_file:
        return json.load(data_file)


# Print mongodb commands to configure a database
def print_mongodb_commands(db_config):
    if not db_config:
        return

    print('\n## Script to configure the %s database' % db_config['name'])

    print('use ' + db_config['name'])
    print('db.createUser({user: "' + db_config["user"] + '", pwd: "' + db_config["password"] +
          '", roles: [ { role: "readWrite", db: "' + db_config["name"] + '" } ]});')
    print('db.auth("' + db_config["user"] + '","' + db_config["password"] + '")')
    print('sh.enableSharding("' + db_config["name"] + '")')
    print('#####################################################')
    if 'collections' in db_config:
        for coll in db_config['collections']:
            print('db.createCollection("' + coll['name'] + '")')
            if coll['shard']:
                print('sh.shardCollection("' + db_config['name'] + '.' + coll['name'] + '", { _id: "hashed" } )')
            if 'indexes' in coll:
                for idx in coll['indexes']:
                    str_index = 'db.' + coll['name'] + '.createIndex({' + idx['name'] + ':' +\
                                str(1 if idx['asc'] else -1) + '}'
                    if 'ttl' in idx:
                        str_index = str_index + ',{expireAfterSeconds:' + str(idx['ttl']) + '})'
                    else:
                        str_index = str_index + ')'
                    print(str_index)
    print('#####################################################\n')


def database_exists(conn, name):
    if name in conn.database_names():
        return True
    else:
        return False


# Create a database on the mongodb cluster.
def create_database(conn, name, sharded=False):
    db = conn[name]
    if sharded:
        res = conn.admin.command('enableSharding', name)
        print('Enabling Shard for database[%s] -> %s' % (name, res))
    return db, res


# Add a new user to a informed database object.
def add_database_user(db, username, password, **kwargs):
    db.add_user(username, password, **kwargs)


# Create a new Collection.
def create_collection(conn, db, coll_name, sharded=False):
    collection = db.create_collection(coll_name)
    if sharded:
        # Enable shard for this collection:
        dbname = db.name
        res = conn.admin.command('shardCollection', dbname + '.' + coll_name, key={'_id': 'hashed'})
        print('Enabling Shard for collection[%s] -> %s' % (dbname + '.' + coll_name, res))
    return collection


# Create a index into the informed collection.
def create_collection_index(collection, index_name, index_sort=ASCENDING, ttl=-1):
    # sort = ASCENDING if idx['asc'] else DESCENDING
    if ttl > 0:
        res = collection.create_index([(index_name, index_sort)], expireAfterSeconds=ttl)
    else:
        res = collection.create_index([(index_name, index_sort)])
    print('Create Index[%s] for Collection[%s] -> %s' % (index_name, collection.name, res))
    return res


def create_data():
    None


def drop_database():
    None


# This method performs all creations and configurations
# on the mongodb Shard for the informed database.
# Be careful.
def process_database(db_config):

    if not db_config:
        raise Exception('There is no valid database configuration.')

    db_name = db_config['name']

    print('#####################################################')
    print('## Start to process the "%s" database' % db_name)
    print('#####################################################')

    conn = connect_mongodb()

    # Get the database names and verify if there is no database with the same name:
    if database_exists(conn, db_name):
        print('The informed database already exists into the cluster.')
        conn.close()
        return

    shard = db_config['shard']

    db, _ = create_database(conn, db_name, sharded=shard)

    # Add the user:
    username = db_config["user"]
    password = db_config["password"]
    kwargs = {'roles': [{'role': 'readWrite', 'db': db_name}]}
    add_database_user(db, username, password, **kwargs)

    # Authenticate:
    if db.authenticate(username, password):

        # Process all collections:
        if 'collections' in db_config:
            for coll in db_config['collections']:
                print('Processing collection %s' % str(coll))
                coll_name = coll['name']
                collection = create_collection(conn, db, coll_name, sharded=(shard and coll['shard']))
                if 'indexes' in coll:
                    for idx in coll['indexes']:
                        print(idx)
                        sort = ASCENDING if idx['asc'] else DESCENDING
                        ttl = -1
                        if 'ttl' in idx:
                            ttl = idx['ttl']
                        create_collection_index(collection, idx['name'], index_sort=sort, ttl=ttl)

    conn.close()
    print('#####################################################')
    print('##              FINISHED WITH 0 ERROR              ##')
    print('#####################################################')


# Main method
def main():

    if len(sys.argv) < 2:
        print('Usage : python scmgr.py [database-config.json]')
        sys.exit()

    db_config = load_database_config(sys.argv[1])
    if not db_config:
        return

    # TODO: process parameters here

    process_database(db_config)

    print('\nScrip for this session:')

    print_mongodb_commands(db_config)


if __name__ == "__main__":
    main()
