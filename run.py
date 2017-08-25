#!/usr/bin/python3
# -*- coding: utf-8 -*-

import configparser
from logging.handlers import RotatingFileHandler
import logging.config
import argparse
import os
from sys import stdout
import json
from urllib.parse import quote_plus as urlquote
from datetime import datetime
import oceanbuoyapp
import re


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "无效日期: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def valid_hour(s):
    try:
        hour = int(s)
        assert(0<= hour < 24)
        return hour
    except:
        msg = "无效整点小时： '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def valid_minute(s):
    try:
        minute = int(s)
        assert(0<= minute < 60)
        return minute
    except:
        msg = "无效整点分钟： '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def valid_buoys(s):
    if s:
        return '"' + s + '"'
    else:
        return s

def cfgparser():
    logger = logging.getLogger()
    config = configparser.ConfigParser()
    config.read('parser.ini')
    options = {}
    logger.debug("解析配置文件...")
    try:
        sections = config.sections()
        if 'Database' in sections:
            if 'dburl' in config['Database'] and config['Database']['dburl']:
                options['dburl'] = config['Database']['dburl']
            else:
                args = [urlquote(config['Database'][key]) for key in ['db', 'driver', 'user', 'password', 'host', 'dbname', 'urlparams']]
                options['dburl'] =  '{:}+{:}://{:}:{:}@{:}/{:}?{:}'.format(*args)
        else:
            logger.error('请在配置文件中提供数据库连接URL地址!')
            exit(1)

        if 'Paths' in sections:
            default_paths = ['.', './data', './报告', './原始报文归档', './疑问报文']
            for i, key in enumerate(['home_dir', 'data_dir', 'yield_dir', 'migrate_dir', 'recycle_dir']):
                if key in config['Paths'] and config['Paths'][key]:
                    options[key] = config['Paths'][key]
                else:
                    options[key] = default_paths[i]

            if not os.path.exists(options['data_dir']):
                logger.error('%s 不存在，请填写有效的原始报文目录！', options['data_dir'])
                exit(1)
        else:
            logger.error('请在配置文件中提供路径配置参数!')
            exit(1)

        if 'Report' in sections:
            for key in ['start_hour', 'start_minute', 'start_date', 'end_date']:
                if key in config['Report'] and config['Report'][key]:
                    options[key] = config['Report'][key]
                else:
                    options[key] = None

        if 'start_hour' in options:
            try:
                options['start_hour'] = int(options['start_hour']) if 0 <= int(options['start_hour']) < 24 else 20
            except ValueError:
                logger.warning('请提供合理的start_hour，应为[0,23]之间的整数。将使用默认值:20')
                options['start_hour'] = 20
        else:
            options['start_hour'] = 20

        if 'start_minute' in options:
            try:
                options['start_minute'] = int(options['start_minute']) if 0 <= int(options['start_minute']) < 60 else 10
            except ValueError:
                logger.warning('请提供合理的start_minute，应为[0,59]之间的整数。将使用默认值:10')
                options['start_minute'] = 10
        else:
            options['start_minute'] = 10


        if 'Parser' in sections:
            for item in ['valid', 'qw', 'qy', 'hw', 'hy', 'fs', 'fx', 'jmzt',
                        'zbwd', 'dydy', 'zthgjd', 'ztfwjd', 'ztfyjd', 'fsfx']:
                key = item + '_buoys'
                if key in config['Parser'] and config['Parser'][key]:
                    options[key] = config['Parser'][key]
                    qzhs = re.split('\s*[,|\s]\s*', options[key])
                    options[key] = ','.join(['"' + x + '"' for x in qzhs])

            for item in ['override', 'syncflush', 'autoflush']:
                if item in config['Parser'] and config['Parser'][item]:
                    options[item] = config['Parser'].getboolean(item, fallback=False)

    except KeyError:
        logger.error("解析配置文件时出错！")
        exit(1)

    return options


def main(**kwargs):
    if os.path.exists('logging.json'):
        with open('logging.json', 'rt') as f:
            log_cfg = json.load(f)
        logging.config.dictConfig(log_cfg)
    else:
        logger = logging.getLogger()
        f_handler = RotatingFileHandler(os.path.join('log', 'parser.log'), maxBytes=2000, backupCount=5)
        f_handler.setLevel(logging.INFO)
        s_handler = logging.StreamHandler(stdout)
        s_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(formatter)
        s_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        logger.addHandler(s_handler)
        logger.setLevel(logging.INFO)

    options = cfgparser()
    if kwargs:
        options.update(kwargs)
    options['start_hour'] += 1
    options['start_minute'] += 1
    oceanbuoyapp.main(**options)

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-s', '--startdate', help="所要解析报文的起始日期 格式:YYYY-MM-DD", type=valid_date)
    args_parser.add_argument('-e', '--enddate', help="所要解析报文的截止日期 格式:YYYY-MM-DD", type=valid_date)
    args_parser.add_argument('-k', '--hour', help="划分每个观测日的整点时刻", type=valid_hour)
    args_parser.add_argument('-m', '--minute', help="每个整点小时的观测窗口内第一个报文接收到的分钟数", type=valid_minute)
    args_parser.add_argument('-b', '--buoys', help="有效浮标区站号", nargs='*', type=valid_buoys)
    args_parser.add_argument('-a', '--action', help="单独选择本次操作的内容：数据解析入库、生成报告", choices=['sync', 'report'])
    args_parser.add_argument('--override', help="写入数据库时如遇到具有相同键值的字段将用新字段覆盖", action="store_true")
    args_parser.add_argument('--syncflush', help="解析一个报文文件后立即将解析数据入库", action="store_true")
    subparsers = args_parser.add_subparsers(help='数据库操作', dest='initdb')
    subparsers.add_parser('initdb', help="数据库操作，例如建表，删除表数据等")
    args = args_parser.parse_args()
    options = {}
    options['args_dstart'] = args.startdate
    options['args_dend'] = args.enddate
    options['args_hour'] = args.hour + 1 if args.hour != None else None
    options['args_minute'] = args.minute + 1 if args.minute != None else None
    options['args_buoys'] = ''.join(args.buoys) if args.buoys else None
    options['args_action'] = args.action
    options['args_override'] = args.override
    options['args_syncflush'] = args.syncflush
    options['args_initdb'] = args.initdb
    main(**options)