#!/usr/bin/env python
# -*- coding: utf-8 -*-

from binlog2sql import Binlog2sql
import linecache,datetime

def main():
    conn_setting = {
    "host": "192.168.1.231",
    "port": 3306,
    "user": "root",
    "passwd": "wpzh5HDaLBhYSaBp@",
    'charset': 'utf8'
    }

    dest_conn_setting = {
    "host": "192.168.1.207",
    "port": 3306,
    "user": "root",
    "passwd": "wpzh5HDaLBhYSaBp@",
    'charset': 'utf8'
    }

    #mydql_data_dir = "/var/lib/mysql/"
    #start_file = linecache.getlines(mydql_data_dir+"mysql-bin.index")[-1].split('/')[-1].strip()
    start_file = 'mysql-bin.000020'
    databases = ['user_service']
    tables = ['users','user_infos','user_company','company_subject','company_info']
    sql_type = ['INSERT', 'UPDATE']
    only_dml = True
    start_pos = 4
    end_file = ''
    end_pos = 0

    #设置同步过去10分钟的数据开始:
    #start_time = (datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    start_time = ''
    stop_time = ''
    stop_never = True
    flashback = False
    no_pk = False
    back_interval = 1.0
    binlog2sql = Binlog2sql(connection_settings=conn_setting, dest_connection_settings=dest_conn_setting, start_file=start_file, start_pos=start_pos,
                            end_file=end_file, end_pos=end_pos, start_time=start_time,
                            stop_time=stop_time, only_schemas=databases, only_tables=tables,
                            no_pk=no_pk, flashback=flashback, stop_never=stop_never,
                            back_interval=back_interval, only_dml=only_dml, sql_type=sql_type)
    binlog2sql.process_binlog()
if __name__ == "__main__":
    main()