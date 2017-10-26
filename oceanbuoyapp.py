#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import logging
import logging.config
import tblparsers
import syncdb
import graphreporter
import zparser
from datetime import datetime
from datetime import timedelta

__author__ = "Chayce Chen"

def verify_zfile(filepath):
    logger = logging.getLogger(__name__)
    if os.path.splitext(filepath)[1] != '.txt':
        return False
    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()
            if len(lines) < 3 or len(lines) > 4:
                return False
            cols = lines[0].strip().split()
            if len(cols) != 9:
                return False
            # 需要添加判断观测日期和时间的逻辑，发现异常报文，例如含有无效观测日期时间
            cols.extend(lines[1].strip().split())
            if len(cols) != 11:
                return False
            elif len(cols[-1].strip()) != 124:
                return False
            elif len(lines) == 4:
                cols.extend(lines[2].strip().split())
                if len(cols) != 12 and len(cols[-1]) != 164:
                    return False
            return True
    except IOError:
        logger.error("读取原始报文文件 [%s] 出错", filepath)
        return False


def parse_zfile(filepath, chunks, dburl=None, syncflush=False, **kwargs):
    logger = logging.getLogger(__name__)
    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()
            cols = lines[0].strip().split()
            cols.extend(lines[1].strip().split())
            if len(lines) == 4:
                cols.extend(lines[2].strip().split())
            jsrqsj = datetime.fromtimestamp(os.path.getctime(filepath)).replace(microsecond=0)
            plfbqzh = cols[0]
            gcrqsj = datetime.strptime(cols[9], "%Y%m%d%H%M%S")
            rawdatagram = ''.join(('0x', cols[-1]))
    except IOError:
        logger.error("读取原始报文文件 [%s] 出错", filepath)
        return False
    except ValueError:
        if cols:
            logger.error("观测日期时间字段格式错误无法解析,将跳过此报文！")
        return False

    chunk = chunks.setdefault(plfbqzh, {})
    tblnames = ['plfbjbcsb', 'plfbgcsjb', 'plfbqwsjb', 'plfbqysjb', 'plfbhwsjb', 'plfbhysjb',
                'plfbfssjb', 'plfbfxsjb', 'plfbztxxb', 'plfbzkxxb']
    tblrows_now = {}
    tblrows_pending = {}
    tbl_parserargs = {}
    for tbl in tblnames:
        if tbl == 'plfbzkxxb':
            tblrows_now[tbl] = chunk.setdefault(tbl, {})
        else:
            tblrows_now[tbl] = chunk.setdefault(tbl, [])
        tblrows_pending[tbl] = {}
        tbl_parserargs[tbl] = {}
        tbl_parserargs[tbl]['row'] = tblrows_pending[tbl]

    try:
        namefileds = os.path.basename(filepath).strip().split('_')
        startHour = kwargs.get('starthour', 20)
        file_gcrqsj = datetime.strptime(namefileds[4],
                                        "%Y%m%d%H%M%S") + timedelta(hours=8)

    except:
        logger.exception(filepath)
        file_gcrqsj = jsrqsj

    if file_gcrqsj.hour >= startHour:
        interval_begin = file_gcrqsj.replace(hour=startHour)
        interval_end = file_gcrqsj.replace(hour=startHour) + timedelta(days=1)
    else:
        interval_begin = file_gcrqsj.replace(hour=startHour) - timedelta(days=1)
        interval_end = file_gcrqsj.replace(hour=startHour)
    dstdirname = "_".join([x for x in (interval_begin.strftime("%Y-%m-%d-%H"),
                                       interval_end.strftime("%Y-%m-%d-%H"),
                                       '记录文件')])

    tbl_parserargs['plfbjbcsb']['cols'] = cols[:9]
    tbl_parserargs['plfbgcsjb']['rawdatagram'] = rawdatagram
    tbl_parserargs['plfbgcsjb']['extra'] = {}
    tbl_parserargs['plfbgcsjb']['extra']['PLFBQZH'] = plfbqzh
    tbl_parserargs['plfbgcsjb']['extra']['GCRQSJ'] = gcrqsj
    tbl_parserargs['plfbgcsjb']['extra']['JSRQSJ'] = jsrqsj
    tbl_parserargs['plfbgcsjb']['extra']['JSRQ'] = jsrqsj.strftime("%Y-%m-%d")
    tbl_parserargs['plfbgcsjb']['extra']['JSSJ'] = jsrqsj.strftime("%H:%M")
    tbl_parserargs['plfbgcsjb']['extra']['JLWJMC'] = os.path.join(dstdirname, os.path.basename(filepath))
    # compact flag indicates whether communication protocols were included in the datagram.
    tbl_parserargs['plfbgcsjb']['extra']['COMPACT'] = True if len(lines) == 3 else False
    tbl_parserargs['plfbgcsjb']['extra']['QW'] = tblrows_now['plfbqwsjb']
    tbl_parserargs['plfbgcsjb']['extra']['QY'] = tblrows_now['plfbqysjb']
    tbl_parserargs['plfbgcsjb']['extra']['HW'] = tblrows_now['plfbhwsjb']
    tbl_parserargs['plfbgcsjb']['extra']['HY'] = tblrows_now['plfbhysjb']
    tbl_parserargs['plfbgcsjb']['extra']['FS'] = tblrows_now['plfbfssjb']
    tbl_parserargs['plfbgcsjb']['extra']['FX'] = tblrows_now['plfbfxsjb']
    tbl_parserargs['plfbgcsjb']['extra']['JBCS'] = tblrows_now['plfbjbcsb']
    tbl_parserargs['plfbgcsjb']['extra']['ZTXX'] = tblrows_now['plfbztxxb']
    tbl_parserargs['plfbgcsjb']['extra']['ZKXX'] = tblrows_now['plfbzkxxb']

    try:
        for tbl in tblnames:
            t_parser = getattr(tblparsers, '_'.join([x for x in ('parse', tbl)]))
            t_parser(**tbl_parserargs[tbl])
            if tblrows_pending[tbl]:
                if tbl == 'nt_plfbgcsjb':
                    exist = False
                    for idx, row in enumerate(tblrows_now[tbl]):
                        if row['PLFBQZH'] == tblrows_pending[tbl]['PLFBQZH'] and \
                                        row['GCRQSJ'] == tblrows_pending[tbl]['GCRQSJ']:
                            if tblrows_pending[tbl]['JSRQSJ'] > row['JSRQSJ']:
                                tblrows_pending[tbl]['FILECOUNT'] += 1
                                tblrows_now[idx] = tblrows_pending[tbl]
                            else:
                                row['FILECOUNT'] += 1
                            exist = True
                            break
                    if not exist:
                        tblrows_now[tbl].append(tblrows_pending[tbl])
                else:
                    tblrows_now[tbl].append(tblrows_pending[tbl])
    except KeyError:
        logger.error("观测日期时间的分钟位非标准值 %s，应为(00, 10, 15, 30, 45, 50)", gcrqsj.minute)
        return False

    if syncflush and dburl:
        return sync_chunks({plfbqzh: chunk}, dburl, **kwargs)

    return True


def sync_chunks(chunks, dburl, **kwargs):
    logger = logging.getLogger(__name__)
    options = {}
    options['override'] = kwargs.get('override', False)
    options['autoflush'] = kwargs.get('autoflush', True)
    options['errors'] = 0
    if options['override']:
        logger.info("本次入库将使用覆盖模式...")
    for k, v in chunks.items():
        logger.info("正在写入区站号%s的数据", k)
        syncdb.pending_rowrecords(dburl, [max(v['plfbjbcsb'], key=lambda x: x['ZJGCRQSJ'])], syncdb.BuoyMetaInfo,
                                  **options)
        syncdb.pending_rowrecords(dburl, v['plfbgcsjb'], syncdb.ObservationTable, **options)
        syncdb.pending_rowrecords(dburl, v['plfbqwsjb'], syncdb.QiWen, **options)
        syncdb.pending_rowrecords(dburl, v['plfbqysjb'], syncdb.QiYa, **options)
        syncdb.pending_rowrecords(dburl, v['plfbhwsjb'], syncdb.HaiWen, **options)
        syncdb.pending_rowrecords(dburl, v['plfbhysjb'], syncdb.HaiYan, **options)
        syncdb.pending_rowrecords(dburl, v['plfbfssjb'], syncdb.FengSu, **options)
        syncdb.pending_rowrecords(dburl, v['plfbfxsjb'], syncdb.FengXiang, **options)
        syncdb.pending_rowrecords(dburl, v['plfbztxxb'], syncdb.BuoyStatusInfo, **options)
        syncdb.pending_rowrecords(dburl, v['plfbzkxxb'].values(), syncdb.ElementZK, **options)
    if options['errors'] > 0:
        logger.info("本次数据写入数据库完成! 发生%s个错误", str(options['errors']))
        return False
    else:
        logger.info("已成功同步至数据库")
        return True


def get_rowcountbygcrqsj(dstart, dend, dburl=None):
    count = syncdb.get_rowcount(dburl, dstart, dend, syncdb.ObservationTable)
    return count


def get_latestZFileJSRQSJ(dburl):
    return syncdb.get_maxofcolumn(dburl, syncdb.ObservationTable, 'JSRQSJ')

'''
def write_migration_log(parsing_res, migrating_res, dStart, dEnd, synceddt, archive_dir, dburl):
    migration_logger = logging.getLogger('migration')
    f_handler = logging.FileHandler(os.path.join('log', 'migration.log'))
    f_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(formatter)
    migration_logger.handlers = [f_handler]
    migration_logger.propagate = False
    migration_logger.setLevel(logging.INFO)

    logentry = OrderedDict([('最新入库报文的创建时间', get_latestZFileJSRQSJ(dburl).strftime("%Y-%m-%d %H:%M")),
                            ('本次入库的日期时间', synceddt.strftime("%Y-%m-%d %H:%M")),
                            ('本次入库报文创建时间区间', "{}--{}".format(dStart.strftime("%Y-%m-%d %H:%M"),
                                                             dEnd.strftime("%Y-%m-%d %H:%M"))),
                            ('解析文件总数[解析成功，验证出错， 解析失败]', parsing_res),
                            ('入库行数', get_rowcountbygcrqsj(dStart, dEnd, dburl)),
                            ('迁移文件总数[迁移成功， 疑问待定， 未迁移]', migrating_res),
                            ('是否一致', parsing_res == migrating_res)])
    migration_logger.info("%s", logentry)
    headerflg = True if not os.path.exists(os.path.join(archive_dir, 'migration.log')) else False
    with open(os.path.join(archive_dir, 'migration.log'), 'a+') as o:
        w = csv.DictWriter(o, logentry.keys())
        if headerflg:
            w.writeheader()
        w.writerow(logentry)
'''

def yield_report(dStart, dEnd, yield_path, **kwargs):
    logger = logging.getLogger(__name__)
    reports_dir = os.path.join(yield_path, '图表报告')
    if not os.path.exists(reports_dir):
        os.mkdir(reports_dir)
    try:
        graphreporter.plotfigs(reports_dir, dStart, dEnd, **kwargs)
    except:
        logger.exception("出现不可恢复的错误，生成报告失败")


def main(**kwargs):
    logger = logging.getLogger(__name__)
    dburl = kwargs.get('dburl')
    data_dir = kwargs.get('data_dir') or './data'
    yield_dir = kwargs.get('yield_dir') or './yield'
    migrate_dir = kwargs.get('migrate_dir') or './migrate'
    recycle_dir = kwargs.get('recyle_dir') or './recyle'
    startHour = kwargs.get('args_hour') or kwargs.get('start_hour') or 21
    startHour -= 1
    startMinute = kwargs.get('args_minute') or kwargs.get('start_minute') or 11
    startMinute -= 1
    # valid_buoys = kwargs.get('args_buoys') or kwargs.get('valid_buoys') or ''
    action = kwargs.get('args_action') or kwargs.get('action')
    override = kwargs.get('args_override', False) or kwargs.get('override', False)
    autoflush = kwargs.get('autoflush', True)
    syncflush = kwargs.get('args_syncflush', False) or kwargs.get('syncflush', False)
    initdb = kwargs.get('args_initdb')
    logger.debug("start_hour:%s, start_minute:%s", startHour, startMinute)
    if initdb == 'initdb':
        syncdb.db_menu(dburl)
        return

    try:
        dEnd = kwargs.get('args_dend') or datetime.strptime(kwargs.get('end_date'), '%Y-%m-%d')
    except:
        hour = datetime.now().hour
        dEnd = datetime.now().replace(hour=startHour, minute=0, second=0, microsecond=0)
        if hour > startHour:
            dEnd += timedelta(days=1)
    else:
        dEnd = dEnd.replace(hour=startHour)

    try:
        dStart = kwargs.get('args_dstart') or datetime.strptime(kwargs.get('start_date'), '%Y-%m-%d')
    except:
        dStart = dEnd - timedelta(days=1)
    else:
        dStart = dStart.replace(hour=startHour)

    logger.info("本次操作所涉及原始报文接收时间的区间为 %s 至 %s", dStart.strftime("%Y-%m-%d %H:%M"),
                dEnd.strftime("%Y-%m-%d %H:%M"))

    if dStart < dEnd:
        if action == 'report':
            logger.info("本次选择操作： 生成报告")
            if not os.path.exists(yield_dir):
                os.mkdir(yield_dir)
            yield_report(dStart, dEnd, yield_dir, **kwargs)
        else:
            logger.info("本次选择操作: 解析入库%s", "" if action == 'sync' else " 生成报告")
            zp = zparser.ZFilesParser(data_dir, dStart, dEnd, startMinute, syncflush)
            zp.parsing(parse_zfile, verify_zfile, override=override, autoflush=autoflush, starthour=startHour)
            if zp.save_to_db(dburl, sync_chunks, override=override, autoflush=autoflush):
                if not os.path.exists(migrate_dir):
                    os.mkdir(migrate_dir)
                latestctime = get_latestZFileJSRQSJ(dburl)
                count_syncedrows = get_rowcountbygcrqsj(dStart, dEnd, dburl)
                zp.migrate_zfiles(startHour, migrate_dir, recycle_dir, latestctime=latestctime, count=count_syncedrows)
                if action != 'sync':
                    if not os.path.exists(yield_dir):
                        os.mkdir(yield_dir)
                    zp.yield_report(yield_dir, yield_report, **kwargs)