#!/usr/bin/python3
# -*- coding:utf-8 -*-

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import SMALLINT
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.dialects.mysql import DOUBLE
import configparser
import logging
from urllib.parse import quote_plus as urlquote



Base = declarative_base()


class BuoyMetaInfo(Base):
    __tablename__ = 'nt_plfbjbcsb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    ZJGCRQSJ = Column(DateTime)
    XLH = Column(Unicode(256))
    BH = Column(Unicode(256))
    SBXH = Column(Unicode(256))
    TXLX = Column(Unicode(256))
    SFSJ = Column(DateTime)
    SFHY = Column(Unicode(256))
    JD = Column(DOUBLE)
    WD = Column(DOUBLE)
    GCCHBGD = Column(DOUBLE)
    QYCGQHBGD = Column(DOUBLE)
    GCFS = Column(Unicode(64))
    ZLKZBS = Column(Unicode(16))
    WJGZBS = Column(Unicode(16))
    SFCLQW = Column(Unicode(16))
    SFCLQY = Column(Unicode(16))
    SFCLFSFX = Column(Unicode(16))
    SFCLHW = Column(Unicode(16))
    SFCLYD = Column(Unicode(16))
    JLZHGXSJ = Column(DateTime)
    __table_args__ = (UniqueConstraint(PLFBQZH, name="stationnum_unique"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        o = session.query(cls).filter(cls.PLFBQZH == fieldnames['PLFBQZH'],
                                      cls.ZJGCRQSJ >= fieldnames['ZJGCRQSJ']).first()
        if o is None:
            try:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.ZJGCRQSJ < fieldnames['ZJGCRQSJ']).delete()
            except:
                pass
            o = cls(**fieldnames)
            session.add(o)


class BuoyStatusInfo(Base):
    __tablename__ = 'nt_plfbztxxb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime)
    JMZT = Column(SMALLINT(unsigned=True))
    ZBWD = Column(DOUBLE)
    DYDY = Column(DOUBLE)
    ZTHGJD = Column(DOUBLE)
    ZTFYJD = Column(DOUBLE)
    ZTFWJD = Column(DOUBLE)
    CXCQW = Column(TINYINT(unsigned=True))
    KMGGLW = Column(TINYINT(unsigned=True))
    ZTSJJSW = Column(TINYINT(unsigned=True))
    ZTSJYXW = Column(TINYINT(unsigned=True))
    GPSSJJSW = Column(TINYINT(unsigned=True))
    GPSJSSJYXW = Column(TINYINT(unsigned=True))
    GPSDWSJYXW = Column(TINYINT(unsigned=True))
    YXJSWXS = Column(TINYINT(unsigned=True))
    JD = Column(DOUBLE)
    WD = Column(DOUBLE)
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_sampling_process"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.GCRQSJ==fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o


class ObservationTable(Base):
    __tablename__ = 'nt_plfbgcsjb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    # ID = Column(Unicode(32), primary_key=True)
    # ID = Column(BigInteger, Sequence('datatab_id_seq'), primary_key=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    JSRQSJ = Column(DateTime)
    JSRQ = Column(Unicode(32))
    JSSJ = Column(Unicode(16))
    BDTXZLBS = Column(Unicode(128))
    SJBCD = Column(SMALLINT(unsigned=True))
    # SJBCD = Column(Numeric)
    SXFZHJDZ = Column(Unicode(64))
    XXLB = Column(Unicode(8))
    FXFZDDZ = Column(Unicode(64))
    FXSJ = Column(Unicode(16))
    FXSJS = Column(TINYINT)
    FXSJF = Column(TINYINT)
    DWCDW = Column(INTEGER(unsigned=True))
    # DWCDW = Column(Numeric)

    FBLX = Column(Unicode(32))
    SJPZ = Column(Unicode(32))
    ZTPZ = Column(Unicode(32))
    GCRQN = Column(SMALLINT)
    GCRQY = Column(TINYINT)
    GCRQR = Column(TINYINT)
    GCSJS = Column(TINYINT)
    GCSJF = Column(TINYINT)
    JD = Column(DOUBLE)
    JDBQ = Column(Unicode(16))
    WD = Column(DOUBLE)
    WDBQ = Column(Unicode(16))

    QW00 = Column(DOUBLE)
    QW00ZK = Column(TINYINT)
    QW10 = Column(DOUBLE)
    QW10ZK = Column(TINYINT)
    QW20 = Column(DOUBLE)
    QW20ZK = Column(TINYINT)
    QW30 = Column(DOUBLE)
    QW30ZK = Column(TINYINT)
    QW40 = Column(DOUBLE)
    QW40ZK = Column(TINYINT)
    QW50 = Column(DOUBLE)
    QW50ZK = Column(TINYINT)

    QY00 = Column(DOUBLE)
    QY00ZK = Column(TINYINT)
    QY00QS = Column(DOUBLE)
    QY30 = Column(DOUBLE)
    QY30ZK = Column(TINYINT)
    QY30QS = Column(DOUBLE)

    HW00 = Column(DOUBLE)
    HW00ZK = Column(TINYINT)
    HW15 = Column(DOUBLE)
    HW15ZK = Column(TINYINT)
    HW30 = Column(DOUBLE)
    HW30ZK = Column(TINYINT)
    HW45 = Column(DOUBLE)
    HW45ZK = Column(TINYINT)

    HY00 = Column(DOUBLE)
    HY00ZK = Column(TINYINT)
    HY15 = Column(DOUBLE)
    HY15ZK = Column(TINYINT)
    HY30 = Column(DOUBLE)
    HY30ZK = Column(TINYINT)
    HY45 = Column(DOUBLE)
    HY45ZK = Column(TINYINT)

    FS00 = Column(DOUBLE)
    FS00ZK = Column(TINYINT)
    FS10 = Column(DOUBLE)
    FS10ZK = Column(TINYINT)
    FS20 = Column(DOUBLE)
    FS20ZK = Column(TINYINT)
    FS30 = Column(DOUBLE)
    FS30ZK = Column(TINYINT)
    FS40 = Column(DOUBLE)
    FS40ZK = Column(TINYINT)
    FS50 = Column(DOUBLE)
    FS50ZK = Column(TINYINT)

    FX00 = Column(DOUBLE)
    FX00ZK = Column(TINYINT)
    FX10 = Column(DOUBLE)
    FX10ZK = Column(TINYINT)
    FX20 = Column(DOUBLE)
    FX20ZK = Column(TINYINT)
    FX30 = Column(DOUBLE)
    FX30ZK = Column(TINYINT)
    FX40 = Column(DOUBLE)
    FX40ZK = Column(TINYINT)
    FX50 = Column(DOUBLE)
    FX50ZK = Column(TINYINT)

    JMZT = Column(SMALLINT(unsigned=True))
    ZBWD = Column(DOUBLE)
    DYDY = Column(DOUBLE)
    YXZT = Column(Unicode(16))
    DWHZTZT = Column(Unicode(128))
    ZTHGJD = Column(DOUBLE)
    ZTFYJD = Column(DOUBLE)
    ZTFWJD = Column(DOUBLE)
    BDTXZT = Column(Unicode(32))
    BWJYH = Column(Unicode(32))
    CRCBZ = Column(Unicode(32))
    TXBSJYH = Column(Unicode(32))
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_sampling_process"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.GCRQSJ==fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o
        return o

    @classmethod
    def get_rowcountbygcrqsj(cls, session, dstart, dend):
        o = session.query(cls).filter(cls.GCRQSJ >= dstart, cls.GCRQSJ < dend).count()
        return o


class QiWen(Base):
    __tablename__ = 'nt_plfbqwsjb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    QW = Column(DOUBLE)
    ZK = Column(TINYINT)
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_qw_sampling"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.GCRQSJ==fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o
        return o


class QiYa(Base):
    __tablename__ = 'nt_plfbqysjb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    QY = Column(DOUBLE)
    ZK = Column(TINYINT)
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_qy_sampling"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.GCRQSJ==fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o
        return o


class HaiWen(Base):
    __tablename__ = 'nt_plfbhwsjb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    HW = Column(DOUBLE)
    ZK = Column(TINYINT)
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_hw_sampling"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH == fieldnames['PLFBQZH'],
                                          cls.GCRQSJ == fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o
        return o


class HaiYan(Base):
    __tablename__ = 'nt_plfbhysjb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    HY = Column(DOUBLE)
    ZK = Column(TINYINT)
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_hy_sampling"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.GCRQSJ==fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o
        return o


class FengSu(Base):
    __tablename__ = 'nt_plfbfssjb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    FS = Column(DOUBLE)
    ZK = Column(TINYINT)
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_fs_sampling"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.GCRQSJ==fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o
        return o


class FengXiang(Base):
    __tablename__ = 'nt_plfbfxsjb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    FX = Column(DOUBLE)
    ZK = Column(TINYINT)
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, name="station_fx_sampling"),)

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        cache = session._unique_cache = getattr(session, '_unique_cache', {})
        key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        o = cache.get(key)
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH==fieldnames['PLFBQZH'], cls.GCRQSJ==fieldnames['GCRQSJ']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter_by(PLFBQZH=fieldnames['PLFBQZH'], GCRQSJ=fieldnames['GCRQSJ']).first()
                if o is None:
                    o = cls(**fieldnames)
                    session.add(o)
            cache[key] = o
        return o


class ElementZK(Base):
    __tablename__ = 'nt_plfbzkxxb'
    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    PLFBQZH = Column(Unicode(64), nullable=False)
    GCRQSJ = Column(DateTime, nullable=False)
    YSLX = Column(Unicode(8), nullable=False)
    ZK = Column(TINYINT)
    VALUE = Column(DOUBLE)
    JLWJMC = Column(Unicode(1024))
    __table_args__ = (UniqueConstraint(PLFBQZH, GCRQSJ, YSLX, name="station_qw_sampling"),)
    __element_models__ = {
        'QW': QiWen,
        'QY': QiYa,
        'HW': HaiWen,
        'HY': HaiYan,
        'FS': FengSu,
        'FX': FengXiang
    }

    @classmethod
    def get_unique(cls, session, override=False, **fieldnames):
        # cache = session._unique_cache = getattr(session, '_unique_cache', {})
        # key = (cls, fieldnames['PLFBQZH'], fieldnames['GCRQSJ'])
        # o = cache.get(key)
        model = cls.__element_models__.get(fieldnames['YSLX'])
        o = session.query(model).filter(model.PLFBQZH == fieldnames['PLFBQZH'],
                                        model.GCRQSJ == fieldnames['GCRQSJ']).first()
        if o is None:
            if override:
                session.query(cls).filter(cls.PLFBQZH == fieldnames['PLFBQZH'],
                                          cls.GCRQSJ == fieldnames['GCRQSJ'],
                                          cls.YSLX == fieldnames['YSLX']).delete()
                o = cls(**fieldnames)
                session.add(o)
            else:
                o = session.query(cls).filter(cls.PLFBQZH == fieldnames['PLFBQZH'],
                                              cls.GCRQSJ == fieldnames['GCRQSJ'],
                                              cls.YSLX == fieldnames['YSLX'],
                                              cls.ZK <= fieldnames['ZK']).first()
                if o is None:
                    session.query(cls).filter(cls.PLFBQZH == fieldnames['PLFBQZH'],
                                              cls.GCRQSJ == fieldnames['GCRQSJ'],
                                              cls.YSLX == fieldnames['YSLX'],
                                              cls.ZK > fieldnames['ZK']).delete()
                    o = cls(**fieldnames)
                    session.add(o)
        return o


'''
class ChuanboXinxi(Base):
    __tablename__ = 'nt_cbjbxxb'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    imo = Column(Unicode(64), nullable=False)
    mmsi = Column(Unicode(64), nullable=False)
    callsign = Column(Unicode(64), nullable=False)
    shipname = Column(Unicode(32))
    nationality = Column(Unicode(32))
    chinesename = Column(Unicode(16))
    type = Column(Unicode(16))
    avgspeed = Column(DOUBLE)
    maxspeed = Column(DOUBLE)
    length = Column(DOUBLE)
    width = Column(DOUBLE)
    draught = Column(DOUBLE)
    yard = Column(Unicode(32))
    buildingsite = Column(Unicode(64))
    buildingdate = Column(DateTime)
    launchdate = Column(DateTime)
    cabinnum = Column(TINYINT)
    deckcount = Column(TINYINT)
    mouldeddepth = Column(DOUBLE)
    mouldedbreadth = Column(DOUBLE)
    displacement = Column(DOUBLE)
    __table_args__ = (UniqueConstraint(imo, name="imo_unique"),)


class HangxianXinxi(Base):
    __tablename__ = 'nt_hxjbxxb'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    routeid = Column(Unicode(32), nullable=False)
    routename = Column(Unicode(32))
    traveltime = Column(DOUBLE)
    type = Column(Unicode(16), nullable=False)
    departure = Column(Unicode(32))
    destination = Column(Unicode(32))
    start_lng = Column(DOUBLE, nullable=False)
    start_lat = Column(DOUBLE, nullable=False)
    end_lng = Column(DOUBLE, nullable=False)
    end_lat = Column(DOUBLE, nullable=False)
    __table_args__ = (UniqueConstraint(routeid, name="routeid_unique"),)


class HangxianJihuaLuxian(Base):
    __tablename__ = 'nt_hxjhlxb'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    routeid = Column(Unicode(32), nullable=False)
    traveltime = Column(DOUBLE, nullable=False)
    lng = Column(DOUBLE, nullable=False)
    lat = Column(DOUBLE, nullable=False)
    __table_args__ = (UniqueConstraint(routeid, traveltime, name="routeid_unique"),)


class HangxianBanci(Base):
    __tablename__ = 'nt_hxbcb'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    routeid = Column(Unicode(32), nullable=False)
    depaturetime = Column(DateTime, nullable=False)
    IMO = Column(Unicode(64))
    __table_args__ = (UniqueConstraint(routeid, depaturetime, name="banci_unique"),)
'''

plfbgcsjb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '接收日期时间', '接收日期', '接收时间', '北斗通讯指令标识',
                      '数据报长度(字节)', '收信方指挥机地址', '信息类别', '发信方终端地址', '发信时间', '发信时间时', '发信时间分',
                      '电文位长度(比特)', '浮标类型', '数据配置', '状态配置', '观测日期年', '观测日期月', '观测日期日', '观测时间时',
                      '观测时间分', '经度(deg)', '经度半球', '纬度(deg)', '纬度半球', '气温00(℃)', '气温00质控', '气温10(℃)',
                      '气温10质控', '气温20(℃)', '气温20质控', '气温30(℃)', '气温30质控', '气温40(℃)', '气温40质控',
                      '气温50(℃)', '气温50质控', '气压00(hPa)', '气压00质控', '气压00趋势', '气压30(hPa)', '气压30质控',
                      '气压30趋势', '海温00(℃)', '海温00质控', '海温15(℃)', '海温15质控', '海温30(℃)', '海温30质控',
                      '海温45(℃)', '海温45质控', '海盐00(mS/cm)', '海盐00质控', '海盐15(mS/cm)', '海盐15质控', '海盐30(mS/cm)',
                      '海盐30质控', '海盐45(mS/cm)', '海盐45质控', '风速00(m/s)', '风速00质控', '风速10(m/s)', '风速10质控',
                      '风速20(m/s)', '风速20质控', '风速30(m/s)', '风速30质控', '风速40(m/s)', '风速40质控', '风速50(m/s)',
                      '风速50质控', '风向00(deg)', '风向00质控', '风向10(deg)', '风向10质控', '风向20(deg)', '风向20质控',
                      '风向30(deg)', '风向30质控', '风向40(deg)', '风向40质控', '风向50(deg)', '风向50质控', '浸没状态(次数)',
                      '主板温度(℃)', '电源电压(V)', '运行状态', '定位和姿势状态', '姿态横滚角度', '姿态俯仰角度', '姿态方位角度',
                      '北斗通讯状态', '报文校验和', 'CRC标志', '通讯标识校验和', '记录文件名称']
plfbjbxxb_comments = ['系统编号', '漂流浮标区站号', '最近观测时间', '序列号', '编号', '设备型号', '通讯类型', '释放时间', '释放海域',
                      '最近观测点经度', '最近观测点纬度', '观测场海拔高度', '气压传感器海拔高度', '观测方式', '质量控制标识',
                      '文件更正标识', '是否测量气温', '是否测量气压', '是否测量风向风速', '是否测量海温', '是否测量盐度',
                      '记录最后更新时间']
plfbztxxb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '浸没状态(次数)', '主板温度(℃)',
                      '电源电压(V)', '姿态横滚角度', '姿态俯仰角度', '姿态方位角度', '程序重启位',
                      '看门狗管理位', '姿态数据接收位', '姿态数据有效位', 'GPS数据接收位', 'GPS校时数据有效位',
                      'GPS定位数据有效位', '有效接收卫星数', '经度', '纬度']
plfbqwsjb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '气温(℃)', '质控', '记录文件名称']
plfbqysjb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '气压(hPa)', '质控', '记录文件名称']
plfbhwsjb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '海温(℃)', '质控', '记录文件名称']
plfbhysjb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '海盐(mS/cm)', '质控', '记录文件名称']
plfbfssjb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '风速(m/s)', '质控', '记录文件名称']
plfbfxsjb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '风向(deg)', '质控', '记录文件名称']
plfbzkxxb_comments = ['系统编号', '漂流浮标区站号', '观测日期时间', '要素类型', '质控', '要素值', '记录文件名称']
'''
cbjbxxb_comments = ['系统编号', '船舶代码', '海上移动通信业务标识', '呼号', '船舶名称', '国籍', '中文船名', '船舶类型',
                    '平均船速', '最大船速', '船长', '船宽', '吃水', '造船厂', '造船地点', '造船日期', '下水日期', '船舱数',
                    '甲板数', '型深', '型宽', '排水量']
hxjbxxb_comments = ['系统编号', '航线编号', '航线名称', '全程用时', '类型标识', '出发地', '目的地', '起点经度', '起点纬度',
                    '终点经度', '终点纬度']
hxjhlxb_comments = ['系统编号', '航线编号', '航行时间', '到达地经度', '到达地纬度']
hxbcb_comments = ['系统编号', '航线编号', '出发时间', '船舶代码']
'''


class SyncDB:
    def __init__(self, basecls, dburl):
        self.basecls = basecls
        self.dburl = dburl
        self.engine = create_engine(dburl)
        self.session = sessionmaker(bind=self.engine)

    def getengine(self):
        return self.engine

    def create_table(self):
        self.basecls.metadata.create_all(self.engine)

    def drop_table(self):
        self.basecls.metadata.drop_all(self.engine)

    def add_comments(self, ddls):
        with self.engine.connect() as conn:
            for ddl in ddls:
                print(ddl)
                conn.execute(ddl)

    def truncate_table(self, ddls):
        with self.engine.connect() as conn:
            for ddl in ddls:
                print(ddl)
                conn.execute(ddl)

    def get_maxofcolumn(self, modelcls, column):
        tmpsession = self.session()
        o = tmpsession.query(func.max(getattr(modelcls, column)).label("max"),)
        return o.one().max

    def get_rowcount(self, dstart, dend, modelcls=ObservationTable):
        tmpsession = self.session()
        if hasattr(modelcls, 'get_rowcountbygcrqsj'):
            count = modelcls.get_rowcountbygcrqsj(tmpsession, dstart, dend)
            return count
        else:
            return None


def pending_rowrecords(dburl, rows, modelcls, **kwargs):
    logger = logging.getLogger(__name__)
    try:
        syncinst = SyncDB(Base, dburl)
        session = syncinst.session(autoflush=kwargs.get('autoflush', True))
        for rowrecord in rows:
            modelcls.get_unique(session, kwargs.get('override', False), **rowrecord)
        session.commit()
    except:
        kwargs['errors'] += 1
        if session:
            logger.exception("本次数据写入数据库时出错，稍后请重试！")
            session.rollback()
            logger.warning("事务已回滚!")
        else:
            logger.error("创建数据库连接时失败，请检查数据库连接参数并检查当前数据库运行状态！")


def initdbtable(dburl):
    syncinst = SyncDB(Base, dburl)
    syncinst.drop_table()
    syncinst.create_table()
    ddls = _addComments('nt_plfbgcsjb', plfbgcsjb_comments, syncinst.getengine())
    ddls.extend(_addComments('nt_plfbjbcsb', plfbjbxxb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbqwsjb', plfbqwsjb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbqysjb', plfbqysjb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbhwsjb', plfbhwsjb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbhysjb', plfbhysjb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbfssjb', plfbfssjb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbfxsjb', plfbfxsjb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbztxxb', plfbztxxb_comments, syncinst.getengine()))
    ddls.extend(_addComments('nt_plfbzkxxb', plfbzkxxb_comments, syncinst.getengine()))
    # ddls.extend(_addComments('nt_cbjbxxb', cbjbxxb_comments, syncinst.getengine()))
    # ddls.extend(_addComments('nt_hxjbxxb', hxjbxxb_comments, syncinst.getengine()))
    # ddls.extend(_addComments('nt_hxjhlxb', hxjhlxb_comments, syncinst.getengine()))
    # ddls.extend(_addComments('nt_hxbcb', hxbcb_comments, syncinst.getengine()))
    syncinst.add_comments(ddls)


def truncatetable(dburl):
    syncinst = SyncDB(Base, dburl)
    ddls = __truncateTable('nt_plfbgcsjb', syncinst.getengine())
    ddls.extend(__truncateTable('nt_plfbjbcsb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbqwsjb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbqysjb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbhwsjb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbhysjb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbfssjb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbfxsjb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbztxxb', syncinst.getengine()))
    ddls.extend(__truncateTable('nt_plfbzkxxb', syncinst.getengine()))
    # ddls.extend(__truncateTable('nt_cbjbxxb', syncinst.getengine()))
    # ddls.extend(__truncateTable('nt_hxjbxxb', syncinst.getengine()))
    # ddls.extend(__truncateTable('nt_hxjhlxb', syncinst.getengine()))
    # ddls.extend(__truncateTable('nt_hxbcb', syncinst.getengine()))
    syncinst.truncate_table(ddls)


def insert_row(dburl):
    syncinst = SyncDB(Base, dburl)
    ddls = _builRows('numbers')
    syncinst.add_comments(ddls)


def get_maxofcolumn(dburl, modelcls, columnname, base=Base):
    syncinst = SyncDB(base, dburl)
    latestct = syncinst.get_maxofcolumn(modelcls, columnname)
    return latestct


def get_rowcount(dburl, dstart, dend, modelcls, base=Base):
    syncinst = SyncDB(base, dburl)
    count = syncinst.get_rowcount(dstart, dend, modelcls)
    return count


def getDBCon():
    # print("Reading database configurations...")
    logger = logging.getLogger('_syncdb__getDBCon[fun]')
    config = configparser.ConfigParser()
    config.read('parser.ini')
    dburl = ''
    try:
        sections = config.sections()
        if 'Database' in sections:
            if 'dburl' in config['Database'] and config['Database']['dburl']:
                dburl = config['Database']['dburl']
            else:
                args = [urlquote(config['Database'][key]) for key in ['db', 'driver', 'user', 'password', 'host', 'dbname', 'urlparams']]
                # args[2] = urlquote(args[2])
                # args[3] = urlquote(args[3])
                dburl = '{:}+{:}://{:}:{:}@{:}/{:}?{:}'.format(*args)
        else:
            dburl = ''
    except KeyError:
        logger.exception("Errors found in the parser.ini. Please check it.")
        exit(1)
    # dburl = "mysql+mysqlconnector://buoy2017:Nuist123456@115.159.148.71/cma01?charset=utf8"
    syncinst = SyncDB(Base, dburl)
    return syncinst.getengine()


def _addComments(tablename, cols_comments, engine):
    add_comments = []
    md = MetaData()
    table = Table(tablename, md, autoload=True, autoload_with=engine)
    columns = table.c
    comments_iter = iter(cols_comments)
    for column in columns:
        try:
            ddl = "alter table {:} modify {:} {:} {:} {:} comment '{:}';".format(tablename, column.name, column.type,
                                                                                'not null' if not column.nullable else '',
                                                                                'auto_increment' if column.name == 'ID' else '',
                                                                                next(comments_iter))
            add_comment = DDL(ddl)
            add_comments.append(add_comment)
        except StopIteration:
            break
    return add_comments


def _builRows(tablename):
    ddls = []
    i = 0
    while i < 0x7fff:
        ddl = "insert into {:} (n) values ({:})".format(tablename, i)
        i += 1
        ddls.append(ddl)
    return ddls


def __truncateTable(tablename, engine):
    ddls = []
    md = MetaData()
    table = Table(tablename, md, autoload=True, autoload_with=engine)
    ddl = "TRUNCATE {:};".format(tablename)
    ddls.append(DDL(ddl))
    return ddls


def to_fix_fs(dburl):
    syncinst = SyncDB(Base, dburl)
    session = syncinst.session()
    i = 0
    for row in session.query(ObservationTable).all():
        for col in ['FS00', 'FS10', 'FS20', 'FS30', 'FS40', 'FS50']:
            oldvalue = float(getattr(row, col))
            newvalue = oldvalue * 0.1 if oldvalue < 256 else 25.5 + (oldvalue - 255) * 0.2
            setattr(row, col, round(newvalue, 2))
    for row in session.query(FengSu).all():
        oldvalue = float(getattr(row, 'FS'))
        newvalue = oldvalue * 0.1 if oldvalue < 256 else 25.5 + (oldvalue - 255) * 0.2
        setattr(row, 'FS', round(newvalue, 2))
    session.commit()


def db_menu(dburl):
    logger = logging.getLogger(__name__)
    logger.debug(dburl)
    promotion = input("将重建相关数据库表，现有表数据将丢失!!!\n 输入 'Y' 重新创建表结构。\n"
                      "输入 'T' 删除现有表格的数据但保留表结构。 \n"
                      "输入 'N' 退出。\n 请选择: ")
    if promotion.upper() == 'Y' or promotion.upper() == 'YES':
        logger.info("创建相关表...\n")
        initdbtable(dburl)
        logger.info("操作完成!\n")
    elif promotion.upper() == 'T':
        logger.info("删除表数据...\n")
        truncatetable(dburl)
        logger.info("操作完成!")
    else:
        logger.info("退出操作")
        exit(0)
