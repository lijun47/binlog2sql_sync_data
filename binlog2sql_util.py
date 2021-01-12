#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
import getpass
from contextlib import contextmanager
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)


if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False


def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False


def create_unique_file(filename):
    version = 0
    result_file = filename
    # if we have to try more than 1000 times, something is seriously wrong
    while os.path.exists(result_file) and version < 1000:
        result_file = filename + '.' + str(version)
        version += 1
    if version >= 1000:
        raise OSError('cannot create unique file %s.[0-1000]' % filename)
    return result_file


@contextmanager
def temp_open(filename, mode):
    f = open(filename, mode)
    try:
        yield f
    finally:
        f.close()
        os.remove(filename)


def parse_args():
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str, nargs='*',
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--start-position', '--start-pos', dest='start_pos', type=int,
                          help='Start position of the --start-file', default=4)
    interval.add_argument('--stop-file', '--end-file', dest='end_file', type=str,
                          help="Stop binlog file to be parsed. default: '--start-file'", default='')
    interval.add_argument('--stop-position', '--end-pos', dest='end_pos', type=int,
                          help="Stop position. default: latest position of '--stop-file'", default=0)
    interval.add_argument('--start-datetime', dest='start_time', type=str,
                          help="Start time. format %%Y-%%m-%%d %%H:%%M:%%S", default='')
    interval.add_argument('--stop-datetime', dest='stop_time', type=str,
                          help="Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;", default='')
    parser.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                        help="Continuously parse binlog. default: stop at the latest event when you start.")
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE'],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE.')

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                        help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                        help='Flashback data to start_position of start_file', default=False)
    parser.add_argument('--back-interval', dest='back_interval', type=float, default=1.0,
                        help="Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep")
    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.start_file:
        raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent) or isinstance(event, UpdateRowsEvent) or isinstance(event, DeleteRowsEvent):
        return True
    else:
        return False


def event_type(event):
    t = None
    if isinstance(event, WriteRowsEvent):
        t = 'INSERT'
    elif isinstance(event, UpdateRowsEvent):
        t = 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        t = 'DELETE'
    return t

def concat_sql_from_binlog_event(cursor, binlog_event, row=None, e_start_pos=None, flashback=False, no_pk=False):
    if flashback and no_pk:
        raise ValueError('only one of flashback or no_pk can be True')
    if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
            or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
        raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = {}
    if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent):
        if binlog_event.table == 'users':
            ll = users_ll_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
            bl = users_bl_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
            
        elif binlog_event.table == 'user_infos':
            ll = user_infos_ll_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
            bl = user_infos_bl_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)

        elif binlog_event.table == 'user_company':
            bl = user_company_bl_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
            ll = None

        elif binlog_event.table == 'company_subject':
            ll = company_subject_sql_pattern(binlog_event,'api_lanlingcb_dev2',binlog_event.table, row=row, flashback=flashback, no_pk=no_pk)
            bl = company_subject_sql_pattern(binlog_event,'blzg',binlog_event.table, row=row, flashback=flashback, no_pk=no_pk)

        elif binlog_event.table == 'company_info':
            ll = None  
            bl = company_info_bl_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)     
       
        if ll == None:
            sql['bl'] = cursor.mogrify(bl['template'], bl['values'])
        elif bl == None:
            sql['ll'] = cursor.mogrify(ll['template'], ll['values'])
        else:
            sql['ll'] = cursor.mogrify(ll['template'], ll['values'])
            sql['bl'] = cursor.mogrify(bl['template'], bl['values'])

        #time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
        #sql += ' #start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time)
    elif flashback is False and isinstance(binlog_event, QueryEvent) and binlog_event.query != 'BEGIN' \
            and binlog_event.query != 'COMMIT':
        if binlog_event.schema:
            sql = 'USE {0};\n'.format(binlog_event.schema)
        sql += '{0};'.format(fix_object(binlog_event.query))

    return sql

def company_info_bl_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []
    dest_database = 'blzg'
    dest_table = 'company_info'
    dest_fields = ['com_sub_id','scale','nature','main_business','introduction','label','website','lng',\
        'lat','banner','area_code','area_name','address']

    tmp_row = {}
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});UPDATE `{4}`.`{5}` SET {6} = "{7}" WHERE {8} = {9};'.format(
            dest_database, dest_table,','.join(dest_fields),', '.join(['%s'] * len(dest_fields)),
            dest_database,'company_subject','company_logo',row['values']['logo'],'com_sub_id',row['values']['com_sub_id']
        )

        for i in dest_fields:
            tmp_row[i] = row['values'][i]

    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};UPDATE `{5}`.`{6}` SET {7} = "{8}" WHERE {9} = {10};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['com_sub_id'],
            dest_database,'company_subject','company_logo',row['after_values']['logo'],'com_sub_id',row['after_values']['com_sub_id']
        )

        for i in dest_fields[1:]:
            tmp_row[i] = row['after_values'][i]

    values = map(fix_object, tmp_row.values())
    return {'template': template, 'values': list(values)}

def company_subject_sql_pattern(binlog_event,dest_database,dest_table, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_fields = ['com_sub_id','company_name','credit_code','manage_location','legal_person','busi_license','status','reviewer_id',\
        'reviewer_name','create_time','update_time','remark','id_card_front','id_card_back','bankcard','issuing_bank','verify_account',\
        'payment_money','is_payment','pay_failure_reason','bnkflg','eaccty','bank_outlet']
    tmp_row = {}
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            dest_database, dest_table,','.join(dest_fields),
            ', '.join(['%s'] * len(dest_fields))
        )

        for i in dest_fields:
            tmp_row[i] = row['values'][i]

        values = map(fix_object, tmp_row.values())

    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['com_sub_id']
        )

        for i in dest_fields[1:]:
            tmp_row[i] = row['after_values'][i]

        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def company_subject_bl_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_database = 'blzg'
    dest_table = 'company_subject'
    dest_fields = ['com_sub_id','company_name','credit_code','manage_location','legal_person','busi_license','status','reviewer_id',\
        'reviewer_name','create_time','update_time','remark','id_card_front','id_card_back','bankcard','issuing_bank','verify_account',\
        'payment_money','is_payment','pay_failure_reason','bnkflg','eaccty','bank_outlet']
    tmp_row = {}
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            dest_database, dest_table,','.join(dest_fields),
            ', '.join(['%s'] * len(dest_fields))
        )

        for i in dest_fields:
            tmp_row[i] = row['values'][i]

        values = map(fix_object, tmp_row.values())

    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['com_sub_id']
        )

        for i in dest_fields[1:]:
            tmp_row[i] = row['after_values'][i]

        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def company_subject_ll_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_database = 'api_lanlingcb_dev2'
    dest_table = 'company_subject'
    dest_fields = ['com_sub_id','company_name','credit_code','manage_location','legal_person','busi_license','status','reviewer_id',\
        'reviewer_name','create_time','update_time','remark','id_card_front','id_card_back','bankcard','issuing_bank','verify_account',\
            'payment_money','is_payment','pay_failure_reason','bnkflg','eaccty','bank_outlet']
    tmp_row = {}
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            dest_database, dest_table,','.join(dest_fields),
            ', '.join(['%s'] * len(dest_fields))
        )

        for i in dest_fields:
            tmp_row[i] = row['values'][i]

        values = map(fix_object, tmp_row.values())

    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['com_sub_id']
        )

        for i in dest_fields[1:]:
            tmp_row[i] = row['after_values'][i]

        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def user_company_bl_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_database = 'blzg'
    dest_table = 'sys_user'
    dest_fields = ['user_id','com_sub_id']
    tmp_row = {}
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['values']['user_id']
        )
        tmp_row['com_sub_id'] = row['values']['com_sub_id']

        values = map(fix_object, tmp_row.values())

    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['user_id']
        )
        tmp_row['com_sub_id'] = row['after_values']['com_sub_id']

        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def users_bl_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_database = 'blzg'
    dest_table = 'sys_user'
    dest_fields = ['user_id','user_name','phonenumber','password','status','create_time']
    tmp_row = {}
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            dest_database, dest_table,','.join(dest_fields),
            ', '.join(['%s'] * len(dest_fields))
        )
        tmp_row['user_id'] = row['values']['id']
        tmp_row['user_name'] = row['values']['phone']
        tmp_row['phonenumber'] = row['values']['phone']
        tmp_row['password'] = row['values']['password']
        tmp_row['valid'] = row['values']['valid']
        tmp_row['create_time'] = row['values']['create_time']

        values = map(fix_object, tmp_row.values())

    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['id']
        )
        tmp_row['user_name'] = row['after_values']['phone']
        tmp_row['phonenumber'] = row['after_values']['phone']
        tmp_row['password'] = row['after_values']['salt']
        tmp_row['valid'] = row['after_values']['valid']
        tmp_row['create_time'] = row['after_values']['create_time']

        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def users_ll_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_database = 'api_lanlingcb_dev2'
    dest_table = 'worker'
    dest_fields = ['worker_id','financial_account_id','worker_password','worker_phone','valid','create_time','is_del','salt']
    tmp_row = {}
    if isinstance(binlog_event, WriteRowsEvent):
        template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            dest_database, dest_table,','.join(dest_fields),
            ', '.join(['%s'] * len(dest_fields))
        )

        tmp_row['worker_id'] = row['values']['id']
        tmp_row['financial_account_id'] = 'wok'+str(row['values']['id'])
        tmp_row['worker_password'] = row['values']['password']
        tmp_row['worker_phone'] = row['values']['phone']
        tmp_row['valid'] = row['values']['valid']
        tmp_row['create_time'] = row['values']['create_time']
        tmp_row['is_del'] = row['values']['is_delete']
        tmp_row['salt'] = row['values']['salt']

        values = map(fix_object, tmp_row.values())

    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[2:]]),dest_fields[0],row['before_values']['id']
        )
        tmp_row['worker_password'] = row['after_values']['password']
        tmp_row['worker_phone'] = row['after_values']['phone']
        tmp_row['valid'] = row['after_values']['valid']
        tmp_row['create_time'] = row['after_values']['create_time']
        tmp_row['is_del'] = row['after_values']['is_delete']
        tmp_row['salt'] = row['after_values']['salt']
        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def user_infos_bl_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_database = 'blzg'
    dest_table = 'sys_user'
    dest_fields = ['user_id','nickname','email','sex','avatar']
    tmp_row = {}

    if isinstance(binlog_event, WriteRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4} and user_id > 100 ;'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['values']['user_id']
        )
        tmp_row['nickname'] = row['values']['nickname']
        tmp_row['email'] = row['values']['email']
        tmp_row['sex'] = row['values']['sex']
        tmp_row['avatar'] = row['values']['photo']

        values = map(fix_object, tmp_row.values())
    
    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4} and user_id > 100 ;'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['user_id']
        )
        tmp_row['nickname'] = row['after_values']['nickname']
        tmp_row['email'] = row['after_values']['email']
        tmp_row['sex'] = row['after_values']['sex']
        tmp_row['avatar'] = row['after_values']['photo']

        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def user_infos_ll_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []

    dest_database = 'api_lanlingcb_dev2'
    dest_table = 'worker'
    dest_fields = ['worker_id','sn','worker_name','nickname','sex','worker_email','certified','photo_url','information','score']
    tmp_row = {}

    if isinstance(binlog_event, WriteRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['values']['user_id']
        )
        tmp_row['sn'] = row['values']['sn']
        tmp_row['worker_name'] = row['values']['real_name']
        tmp_row['nickname'] = row['values']['nickname']
        tmp_row['sex'] = row['values']['sex']
        tmp_row['worker_email'] = row['values']['email']
        tmp_row['certified'] = row['values']['certified']
        tmp_row['photo_url'] = row['values']['photo']
        tmp_row['information'] = row['values']['per_sign']
        tmp_row['score'] = row['values']['score']

        values = map(fix_object, tmp_row.values())
    
    elif isinstance(binlog_event, UpdateRowsEvent):
        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} = {4};'.format(
            dest_database, dest_table,
            ', '.join(['`%s`=%%s' % k for k in dest_fields[1:]]),dest_fields[0],row['before_values']['user_id']
        )
        tmp_row['sn'] = row['after_values']['sn']
        tmp_row['worker_name'] = row['after_values']['real_name']
        tmp_row['nickname'] = row['after_values']['nickname']
        tmp_row['sex'] = row['after_values']['sex']
        tmp_row['worker_email'] = row['after_values']['email']
        tmp_row['certified'] = row['after_values']['certified']
        tmp_row['photo_url'] = row['after_values']['photo']
        tmp_row['information'] = row['after_values']['per_sign']
        tmp_row['score'] = row['after_values']['score']
        values = map(fix_object, tmp_row.values())

    return {'template': template, 'values': list(values)}

def generate_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []
    if flashback is True:
        if isinstance(binlog_event, WriteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                ' AND '.join(map(compare_items, row['after_values'].items())))
            values = map(fix_object, list(row['before_values'].values())+list(row['after_values'].values()))
    else:
        if isinstance(binlog_event, WriteRowsEvent):
            if no_pk:
                # print binlog_event.__dict__
                # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlog_event.primary_key:
                    row['values'].pop(binlog_event.primary_key)
            print(row['values'])
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            print(row['before_values'])
            print(row['after_values'])
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            values = map(fix_object, list(row['after_values'].values())+list(row['before_values'].values()))

    return {'template': template, 'values': list(values)}

def reversed_lines(fin):
    """Generate the lines of file in reverse order."""
    part = ''
    for block in reversed_blocks(fin):
        if PY3PLUS:
            block = block.decode("utf-8")
        for c in reversed(block):
            if c == '\n' and part:
                yield part[::-1]
                part = ''
            part += c
    if part:
        yield part[::-1]

def reversed_blocks(fin, block_size=4096):
    """Generate blocks of file's contents in reverse order."""
    fin.seek(0, os.SEEK_END)
    here = fin.tell()
    while 0 < here:
        delta = min(block_size, here)
        here -= delta
        fin.seek(here, os.SEEK_SET)
        yield fin.read(delta)
