#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import logging
import shutil
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict
import csv



class ZFilesParser():
    def __init__(self, rawfileDir, dStart, dEnd, start_minute=0, syncflush=False, dburl=None):
        self.rawfileDir = rawfileDir
        self.dStart = dStart
        self.dEnd = dEnd
        self.start_minute = start_minute
        self.chunks = {}
        self.logger = logging.getLogger(__name__)
        self.syncflush = syncflush
        self.dburl = dburl
        self.isSynced = False
        self.synceddt = None
        self.parsedZfileNums = 0
        self.parsingFailedFiles = []
        self.verifyingFailedFiles = []

    def parsing(self, parse_func, verify_func, **kwargs):
        try:
            assert verify_func and callable(verify_func)
            assert parse_func and callable(parse_func)
        except AssertionError:
            self.logger.info('验证函数出错')
            self.logger.exception("准备解析时出错！请指定正确的报文文件验证函数和解析函数，然后重新执行程序。")
            return
        if self.syncflush:
            self.synceddt = datetime.now()
        self.logger.info("开始解析原始报文文件...")
        ctStart = self.dStart.replace(minute=self.start_minute)
        ctEnd = self.dEnd.replace(minute=self.start_minute)
        for dirpath, dirnames, filenames in os.walk(self.rawfileDir):
            if filenames:
                # filenames.sort(key=lambda x: os.path.getctime(os.path.join(dirpath, x)))
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if verify_func(filepath):
                        ctime_of_zfile = datetime.fromtimestamp(os.path.getctime(filepath)).replace(microsecond=0)
                        if ctStart <= ctime_of_zfile < ctEnd:
                            self.logger.debug("开始解析原始报文 %s", filepath)
                            if parse_func(filepath, self.chunks, self.syncflush, self.dburl, **kwargs):
                                msg = "解析、入库" if self.syncflush else "解析"
                                self.logger.debug("[%s] 原始报文%s成功!", filepath, msg)
                                self.parsedZfileNums += 1
                            else:
                                msg = "解析、入库" if self.syncflush else "解析"
                                self.logger.warning("[%s] 原始报文%s失败!", filepath, msg)
                                self.parsingFailedFiles.append(filepath)
                    else:
                        self.logger.warning("文件 [%s] 验证出错！运行程序将忽略该文件继续解析...", filepath)
                        self.verifyingFailedFiles.append(filepath)
        if self.syncflush:
            self.isSynced = True


    def save_to_db(self, dburl, sync_chunks_fun, **kwargs):
        if self.chunks and not self.isSynced:
            if sync_chunks_fun and callable(sync_chunks_fun):
                self.synceddt = datetime.now()
                self.logger.info("将解析后的数据写至数据库...")
                if sync_chunks_fun(self.chunks, dburl, **kwargs):
                    self.isSynced = True
                    self.logger.info("本次解析数据已同步到数据库")
                    return True
                else:
                    self.logger.info("本次解析未同步成功")
                    return False
            else:
                self.logger.error("报文信息准备写入数据库时出错！请指定正确的入库处理函数！")
                return False
        else:
            self.logger.warning("当前没有需要入库的信息！")
            return False

    def save_to_csvfile(self, yield_dir, yield_fun):
        if self.chunks:
            if yield_fun and callable(yield_fun):
                self.logger.info("将解析后的数据输出到CSV文件")
                yield_fun(self.chunks, yield_dir)
                self.logger.info("CSV文件保存完成")
                return True
            else:
                self.logger.error("报文信息准备写入文件时出错！请指定正确的写入处理函数！")
                return False
        else:
            self.logger.warning("当前没有需要序列化到文件的信息！")
            return False

    def yield_report(self, yield_path, yield_fun, **kwargs):
        if self.chunks and self.isSynced:
            if yield_fun and callable(yield_fun):
                self.logger.info("生成报告...")
                yield_fun(self.dStart, self.dEnd, yield_path, **kwargs)
                return True
            else:
                self.logger.error("生成报告时出错！请指定正确的报告生成函数！")
                return False
        else:
            self.logger.warning("当前没有信息，无需生成报告！")
            return False

    def migrate_zfiles(self, startHour, archive_dir, recyle_dir, **kwargs):
        if self.chunks and self.isSynced:
            migrate_fun = kwargs.get('migrate_fun')
            latestctime = kwargs.get('latestctime', self.dEnd-timedelta(seconds=1)) + timedelta(seconds=1)
            if migrate_fun and callable(migrate_fun):
                return migrate_fun(self.dStart, self.dEnd, self.start_minute, startHour, self.parsingFailedFiles,
                                   self.verifyingFailedFiles, archive_dir, recyle_dir)
            else:
                self.logger.info("准备迁移本次解析完成的报文文件")
                count = {'migrated': 0, 'dropped': 0, 'remained': len(self.parsingFailedFiles)}
                if not os.path.exists(recyle_dir):
                    os.mkdir(recyle_dir)
                self.logger.debug("准备将验证失败的报文文件迁移至疑问目录")
                for zfile in self.verifyingFailedFiles:
                    try:
                        shutil.move(zfile, os.path.join(recyle_dir, os.path.basename(zfile)))
                    except:
                        self.logger.warning("移动文件 [%s] 至回收目录时失败!", zfile)
                    else:
                        count['dropped'] += 1
                self.logger.debug("准备将解析成功的报文文件迁移至归档目录保存")
                ctStart = self.dStart.replace(minute=self.start_minute)
                ctEnd = self.dEnd.replace(minute=self.start_minute)
                for dirpath, dirnames, filenames in os.walk(self.rawfileDir):
                    if filenames:
                        for filename in (x for x in filenames if os.path.join(dirpath, x) not in self.parsingFailedFiles):
                            ctime_of_zfile = datetime.fromtimestamp(os.path.getctime(
                                os.path.join(dirpath, filename))).replace(microsecond=0)
                            if ctStart <= ctime_of_zfile < latestctime:
                                try:
                                    namefileds = filename.strip().split('_')
                                    file_gcrqsj = datetime.strptime(namefileds[4],
                                                                         "%Y%m%d%H%M%S") + timedelta(hours=8)

                                except:
                                    self.logger.exception(filepath)
                                    file_gcrqsj = ctime_of_zfile
                                if file_gcrqsj.hour >= startHour:
                                    interval_begin = file_gcrqsj.replace(hour=startHour)
                                    interval_end = file_gcrqsj.replace(hour=startHour) + timedelta(days=1)
                                else:
                                    interval_begin = file_gcrqsj.replace(hour=startHour) - timedelta(days=1)
                                    interval_end = file_gcrqsj.replace(hour=startHour)
                                dstdirname = "_".join([x for x in (interval_begin.strftime("%Y-%m-%d-%H"),
                                                                   interval_end.strftime("%Y-%m-%d-%H"),
                                                                   '记录文件')])
                                dstdirpath = os.path.join(archive_dir, dstdirname)
                                filepath = os.path.join(dirpath, filename)
                                if not os.path.exists(dstdirpath):
                                    os.mkdir(os.path.join(dstdirpath))
                                try:
                                    shutil.move(filepath, os.path.join(dstdirpath, filename))
                                except:
                                    self.logger.warning("移动文件 [%s] 至文档目录失败！", filepath)
                                else:
                                    count['migrated'] += 1

                if count['migrated'] != self.parsedZfileNums:
                    self.logger.warning("解析成功的原始报文文件[%s]和应迁移的原始报文文件数量[%s]不匹配！"
                                        "请查看相应的日志信息和报文文件！",
                                        self.parsedZfileNums, count['migrated'])
                self.logger.debug("准备生成迁移日志信息")
                migration_logger = logging.getLogger('migration')
                parsing_res = [self.parsedZfileNums, len(self.verifyingFailedFiles), len(self.parsingFailedFiles)]
                migrating_res = [count['migrated'], count['dropped'], count['remained']]
                logentry = OrderedDict([('最新入库报文的创建时间', latestctime.strftime("%Y-%m-%d %H:%M")),
                                        ('本次入库的日期时间', self.synceddt.strftime("%Y-%m-%d %H:%M")),
                                        ('本次入库报文观测时间区间', "{}--{}".format(self.dStart.strftime("%Y-%m-%d %H:%M"),
                                                                         self.dEnd.strftime("%Y-%m-%d %H:%M"))),
                                        ('本次入库报文创建时间区间', "{}--{}".format(ctStart.strftime("%Y-%m-%d %H:%M"),
                                                                         ctEnd.strftime("%Y-%m-%d %H:%M"))),
                                        ('解析文件总数[解析成功，验证出错， 解析失败]', parsing_res),
                                        ('入库行数', kwargs.get('count')),
                                        ('迁移文件总数[迁移成功， 疑问待定， 未迁移]', migrating_res),
                                        ('是否一致', parsing_res == migrating_res)])
                migration_logger.info("%s", logentry)
                if parsing_res != migrating_res:
                    self.logger.warning("解析报文文件和迁移报文文件数量不符！\n %s", logentry)
                headerflg = True if not os.path.exists(os.path.join(archive_dir, 'migration.log')) else False
                with open(os.path.join(archive_dir, 'migration.log'), 'a+') as o:
                    w = csv.DictWriter(o, logentry.keys())
                    if headerflg:
                        w.writeheader()
                    w.writerow(logentry)

                self.logger.debug("迁移日志信息写入完毕")
                self.logger.info("本次解析报文文件迁移完成")
                return True
        else:
            self.logger.info("当前没有文件需要迁移！")
            return False