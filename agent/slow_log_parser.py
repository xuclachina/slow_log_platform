#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   test2.py
@Time    :   2020/03/03 16:13:48
@Author  :   xuchenliang
@Desc    :   None
'''
import os
import requests
import configparser
import logging
import time
import json
import re
import pymysql
import hashlib

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s %(levelname)s %(message)s',
                    datefmt='%a,%d %b %Y %H:%M:%S',
                    filename='./tmp.log',
                    filemode='a')

logger = logging.getLogger(__name__)


def read_config(config_path):
    config = configparser.ConfigParser()
    configfile = os.path.join('.', 'conf/config.ini')
    config.read(configfile)
    slow_log = config['slowlog']['filename']
    max_size = config['slowlog']['max_size']
    dbid = config['instance']['dbid']
    url = config['server']['url']
    metadir = config['meta']['dir']
    return slow_log, max_size, dbid, url, metadir


def read_slow_log_to_list(file_name, last_pos, read_size):
    # 组合每一分列表[],[]...
    sqltxt = []
    # 每组分列表
    sql = []
    # 拼接多个SQL语句
    output = ''
    # 设置分组列表标识
    isflag = 1
    with open(file_name) as f:
        f.seek(last_pos)
        for line in f.readlines():
            line = line.strip()
            if line.startswith('#'):
                sql.append(line)
            elif line.startswith('SET'):
                sql.append(line)
            elif line.startswith('USE'):
                continue
            elif line.startswith('use'):
                continue
            else:
                if line.endswith(';'):
                    if len(output) == 0:
                        sql.append(line)
                        isflag = 0
                    else:
                        line = output + ' ' + line
                        sql.append(line)
                        output = ''
                        isflag = 0
                else:
                    output += str(' ') + line
            if isflag == 0:
                sqltxt.append(sql)
                isflag = 1
                sql = []

    return sqltxt


def handler_slowlog(file_name, last_pos, read_size, dbid, url):
    result = read_slow_log_to_list(file_name, last_pos, read_size)
    for res in result:
        print(res)
        slow_dict = dict()
        slow_dict['dbid'] = int(dbid)
        # user部分处理
        userhost = res[1]
        db_user = userhost.replace('# User@Host:', '').split('[')[0].strip()
        slow_dict['db_user'] = db_user
        app_ip = userhost.replace('# User@Host:', '').split()[
            2].replace('[', '').replace(']', '')
        slow_dict['app_ip'] = app_ip
        thread_id = userhost.replace('# User@Host:', '').split(':')[1].strip()
        slow_dict['thread_id'] = int(thread_id)
        # querytime部分处理
        querytime = res[2]
        exec_duration = querytime.replace('# ', '').split()[1]
        slow_dict['exec_duration'] = exec_duration
        rows_sent = querytime.replace('# ', '').split()[5]
        slow_dict['rows_sent'] = int(rows_sent)
        rows_examined = querytime.replace('# ', '').split()[7]
        slow_dict['rows_examined'] = int(rows_examined)
        # starttime部分处理
        start_time = res[3].replace(';', '').split('=')[1]
        slow_dict['start_time'] = int(start_time)
        # sql部分处理
        line = res[4]
        slow_dict['orig_sql'] = line
        line_d = re.sub(r'\d+', "?", line)
        line_s = re.sub(r'([\'\"]).+?([\'\"])', "?", line_d)
        sql_parttern = re.sub(r'\(\?.+?\)', "(?)", line_s)
        slow_dict['sql_pattern'] = sql_parttern
        # fingerprint处理
        m1 = hashlib.md5()
        m1.update(str(sql_parttern).encode('utf-8'))
        fingerprint = m1.hexdigest()
        slow_dict['fingerprint'] = fingerprint
        ret = send_slow(url, slow_dict)
        # ret = handle_timeout(send_slow, 3, url, slow_dict)
        if ret:
            if ret.status_code == 200:
                logger.info('发送成功')
            else:
                logger.error('发送失败')
        else:
            logger.warning('发送超时')


def send_slow(url, slow_dict):
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    res = requests.request("post", url, json=slow_dict, headers=headers)
    return res


def rotate_slowlog(file, cur_pos, max_size):
    # conn = pymysql.connect(host='', user='', password='',
    #                        database='', charset='utf8')
    # cursor = conn.cursor()
    # sql = 'flush slow log;'
    # cursor.execute(sql)
    pass


def update_pos(pos, metafile):
    with open(metafile, 'w') as f:
        f.write(str(pos))


def get_file_size(slow_log):
    file_size = os.path.getsize(slow_log)
    return int(file_size)


def get_last_pos(metadir):
    meta_file = os.path.join(metadir, 'meta/lastposition')
    if not os.path.exists(metadir):
        meta_dir = os.path.join(metadir, 'meta/')
        os.mkdir(meta_dir)
        return 0
    else:
        try:
            with open(meta_file, 'r') as f:
                last_pos = int(f.read().strip())
            return last_pos
        except:
            return 0


def handle_timeout(func, timeout, *args, **kwargs):
    interval = 1

    ret = None
    while timeout > 0:
        begin_time = time.time()
        ret = func(*args, **kwargs)
        if ret:
            break
        time.sleep(interval)
        timeout -= time.time() - begin_time
    return ret


def main():
    while True:
        slow_log, max_size, dbid, url, metadir = read_config('config.cnf')
        metafile = os.path.join(metadir, 'meta/lastposition')
        last_pos = get_last_pos(metadir)
        cur_pos = get_file_size(slow_log)
        if last_pos == cur_pos:
            logger.info('本次无需处理')
            time.sleep(10)
            continue
        logger.info('上次位点：{} 本次位点：{}'.format(last_pos, cur_pos))
        handler_slowlog(slow_log, last_pos, cur_pos - last_pos, dbid, url)
        if rotate_slowlog(slow_log, cur_pos, max_size):
            pass
        update_pos(cur_pos, metafile)
        time.sleep(3)


if __name__ == '__main__':
    main()
