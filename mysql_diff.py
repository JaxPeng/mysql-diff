# coding: UTF-8

import MySQLdb
import MySQLdb.cursors
from sshtunnel import SSHTunnelForwarder
from config import *


def db_compare(remote_conn, local_conn, db_name):
    diff_tables = tables_diff(remote_conn, local_conn, db_name)
    diff_fields = fields_diff(remote_conn, local_conn, db_name)
    return diff_tables, diff_fields

def tables_diff(remote_conn, local_conn, db_name):
    local_cursor = local_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    remote_cursor = remote_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

    sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s'" % db_name

    local_cursor.execute(sql)
    local_tables = list()
    for row in local_cursor.fetchall():
        local_tables.append(row['TABLE_NAME'])

    remote_cursor.execute(sql)
    remote_tables = list()
    for row in remote_cursor.fetchall():
        remote_tables.append(row['TABLE_NAME'])

    return list(set(local_tables) ^ set(remote_tables))


def fields_diff(remote_conn, local_conn, db_name):
    local_cursor = local_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    remote_cursor = remote_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

    sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s'" % db_name

    local_cursor.execute(sql)
    local_tables = list()
    for row in local_cursor.fetchall():
        local_tables.append(row['TABLE_NAME'])

    remote_cursor.execute(sql)
    remote_tables = list()
    for row in remote_cursor.fetchall():
        remote_tables.append(row['TABLE_NAME'])

    tables = list(set(local_tables).intersection(set(remote_tables)))
    diff_fields = list()

    i = 0
    for table_name in tables:
        i += 1
        print '%s/%s' % (i, len(tables))

        sql = "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_DEFAULT FROM information_schema.COLUMNS \
        WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s'" % (table_name, db_name)

        local_cursor.execute(sql)
        for row in local_cursor.fetchall():

            sql = "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_DEFAULT FROM information_schema.COLUMNS \
            WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s' AND COLUMN_NAME='%s' " % (table_name, db_name, row['COLUMN_NAME'])

            remote_cursor.execute(sql)
            remote_row = remote_cursor.fetchone()
            if not remote_row:
                diff_fields.append({
                    'table_name': table_name,
                    'column_name': row['COLUMN_NAME'],
                    'local_column_type':  row['COLUMN_TYPE'],
                    'remote_column_type': '',
                    'local_column_default':  '',
                    'remote_column_default': '',
                })
            else:
                if row['COLUMN_TYPE'] != remote_row['COLUMN_TYPE'] or\
                        row['COLUMN_DEFAULT'] != remote_row['COLUMN_DEFAULT']:
                    diff_fields.append({
                        'table_name': table_name,
                        'column_name': row['COLUMN_NAME'],
                        'local_column_type':  row['COLUMN_TYPE'],
                        'remote_column_type': remote_row['COLUMN_TYPE'],
                        'local_column_default':  row['COLUMN_DEFAULT'],
                        'remote_column_default': remote_row['COLUMN_DEFAULT'],
                    })

    return diff_fields


def db_diff():

    if not ssh_tunnel:
        remote_conn = MySQLdb.connect(
            host=remote_mysql_host,
            port=remote_mysql_port,
            user=remote_user,
            passwd=remote_passwd,
            db=remote_db
        )

        local_conn = MySQLdb.connect(
            host=local_mysql_host,
            port=local_mysql_host,
            user=local_user,
            passwd=local_passwd,
            db=local_db
        )

        return db_compare(remote_conn, local_conn, db_compare_name)
    else:
        with SSHTunnelForwarder(
                 (ssh_hostname, ssh_port),
                 ssh_password=ssh_password,
                 ssh_username=ssh_username,
                 remote_bind_address=(remote_mysql_host, remote_mysql_port)) as server:

            remote_conn = MySQLdb.connect(
                host="127.0.0.1",
                port=server.local_bind_port,
                user=remote_user,
                passwd=remote_passwd,
                db=remote_db
            )

            local_conn = MySQLdb.connect(
                host=local_mysql_host,
                port=local_mysql_port,
                user=local_user,
                passwd=local_passwd,
                db=local_db
            )

            return db_compare(remote_conn, local_conn, local_db)


def print_red(str):
    return '\033[1;31;40m %s \033[0m' %str

def print_green(str):
    return '\033[32m %s \033[0m' %str

if __name__ == '__main__':

    tables, fields = db_diff()

    print '+----------------+'
    print '| Missing tables |'
    print '+----------------+'
    for table in tables:
         print print_red(table)

    print '+-------------------+'
    print '| Different columns |'
    print '+-------------------+'
    for field in fields:
        print 'Table: %s' % print_green(field['table_name'])
        print 'Column: %s' % print_red(field['column_name'])
        print 'Local column type: %s' % print_red(field['local_column_type'])
        print 'Remote column type: %s' % print_red(field['remote_column_type'])
        print 'Local column default: %s' % print_red(field['local_column_default'])
        print 'Remote column default: %s' % print_red(field['remote_column_default'])
        print '+--------------------------------------+'
