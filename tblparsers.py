# -*- encoding:utf-8 -*-
from bitstring import BitStream
import datetime
import re


def parse_plfbjbcsb(cols, row):
    for indx, key in enumerate(['PLFBQZH', 'ZJGCRQSJ', 'WD', 'JD', 'GCCHBGD', 'QYCGQHBGD', 'GCFS', 'ZLKZBS', 'WJGZBS']):
        if '/' not in cols[indx]:
            if indx == 1:
                row[key] = datetime.datetime.strptime(cols[indx], "%Y%m%d%H%M%S") + datetime.timedelta(hours=8)
            elif indx == 2:
                row[key] = round(int(cols[indx][:2]) + int(cols[indx][2:4])/60.0 + int(cols[indx][4:])/3600.0, 4)
            elif indx == 3:
                row[key] = round(int(cols[indx][:3]) + int(cols[indx][3:5])/60.0 + int(cols[indx][5:])/3600.0, 4)
            elif indx == 4:
                row[key] = round(int(cols[indx])/10.0, 1)
            elif indx == 5:
                row[key] = round(int(cols[indx])/10.0, 1)
            elif indx == 6:
                row[key] = "器测项目为人工观测" if cols[indx] == '1' else "器测项目为自动站观测"
            else:
                row[key] = cols[indx]


def parse_plfbgcsjb(rawdatagram, row, extra):
    bstream = BitStream(rawdatagram)
    datagramfmt = 'bytes:5, uint:16, uint:24, hex:8, uint:24, uint:8, uint:8, uint:16, hex:8, bin:8, bin:8,uint:7, ' \
                  'uint:4, uint:5, uint:5, uint:6, uint:20, uint:1, uint:19, uint:1, uint:8, uint:2, uint:8, uint:2, ' \
                  'uint:8, uint:2, uint:8, uint:2, uint:8, uint:2, uint:8, uint:2, uint:11, uint:2, uint:9, uint:11, ' \
                  'uint:2, uint:9, uint:9, uint:2, uint:9, uint:2, uint:9, uint:2, uint:9, uint:2, uint:12, uint:2, ' \
                  'uint:12, uint:2, uint:12, uint:2, uint:12, uint:2, uint:9, uint:2, uint:9, uint:2, uint:9, uint:2, ' \
                  'uint:9, uint:2, uint:9, uint:2, uint:9, uint:2, uint:7, uint:2, uint:7, uint:2, uint:7, uint:2, ' \
                  'uint:7, uint:2, uint:7, uint:2, uint:7, uint:2, uint:6, uint:7, uint:6, bin:8, bin:8, uint:9, ' \
                  'uint:8, uint:9, bin:12, uint:7, hex:8, hex:8'

    keywords = ['BDTXZLBS', 'SJBCD', 'SXFZHJDZ', 'XXLB', 'FXFZDDZ', 'FXSJS', 'FXSJF', 'DWCDW', 'FBLX', 'SJPZ', 'ZTPZ',
                'GCRQN', 'GCRQY', 'GCRQR', 'GCSJS', 'GCSJF', 'JD', 'JDBQ', 'WD', 'WDBQ', 'QW00', 'QW00ZK', 'QW10', 'QW10ZK',
                'QW20', 'QW20ZK', 'QW30', 'QW30ZK', 'QW40', 'QW40ZK', 'QW50', 'QW50ZK', 'QY00', 'QY00ZK', 'QY00QS', 'QY30',
                'QY30ZK', 'QY30QS', 'HW00', 'HW00ZK', 'HW15', 'HW15ZK', 'HW30', 'HW30ZK', 'HW45', 'HW45ZK', 'HY00', 'HY00ZK',
                'HY15', 'HY15ZK', 'HY30', 'HY30ZK', 'HY45', 'HY45ZK', 'FS00', 'FS00ZK', 'FS10', 'FS10ZK', 'FS20', 'FS20ZK',
                'FS30', 'FS30ZK', 'FS40', 'FS40ZK', 'FS50', 'FS50ZK', 'FX00', 'FX00ZK', 'FX10', 'FX10ZK', 'FX20', 'FX20ZK',
                'FX30', 'FX30ZK', 'FX40', 'FX40ZK', 'FX50', 'FX50ZK', 'JMZT', 'ZBWD', 'DYDY', 'YXZT', 'DWHZTZT', 'ZTHGJD',
                'ZTFYJD', 'ZTFWJD', 'BDTXZT', 'BWJYH', 'CRCBZ', 'TXBSJYH']

    values = bstream.unpack(', '.join(re.split('\s*,\s*', datagramfmt)[8:-2]) if extra['COMPACT'] else datagramfmt)
    for indx, key in enumerate(keywords[8:-2] if extra['COMPACT'] else keywords):
        row[key] = values[indx]

    row['PLFBQZH'] = extra['PLFBQZH']
    row['GCRQSJ'] = extra['GCRQSJ']
    row['JSRQSJ'] = extra['JSRQSJ']
    row['JSSJ'] = extra['JSSJ']
    row['JSRQ'] = extra['JSRQ']
    row['JLWJMC'] = extra['JLWJMC']

    row['FBLX'] = ''.join(['0x', row['FBLX'].upper()])
    row['JD'] = round(row['JD'] * 0.0002, 4)
    row['JDBQ'] = 'E' if row['JDBQ'] == 0 else 'W'
    row['WD'] = round(row['WD']*0.0002, 4)
    row['WDBQ'] = 'N' if row['WDBQ'] == 0 else 'S'
    row['QW00'] = round(row['QW00'] * 0.25 - 20.0, 2)
    row['QW10'] = round(row['QW10'] * 0.25 - 20.0, 2)
    row['QW20'] = round(row['QW20'] * 0.25 - 20.0, 2)
    row['QW30'] = round(row['QW30'] * 0.25 - 20.0, 2)
    row['QW40'] = round(row['QW40'] * 0.25 - 20.0, 2)
    row['QW50'] = round(row['QW50'] * 0.25 - 20.0, 2)

    row['QY00'] = round(row['QY00'] * 0.2 + 900.0, 2)
    row['QY00QS'] = round(row['QY00QS'] * 0.1 - 25.5, 2)
    row['QY30'] = round(row['QY30'] * 0.2 + 900.0, 2)
    row['QY30QS'] = round(row['QY30QS'] * 0.1 - 25.5, 2)

    row['HW00'] = round(row['HW00'] * 0.08 - 5.0, 2)
    row['HW15'] = round(row['HW15'] * 0.08 - 5.0, 2)
    row['HW30'] = round(row['HW30'] * 0.08 - 5.0, 2)
    row['HW45'] = round(row['HW45'] * 0.08 - 5.0, 2)

    row['HY00'] = round(row['HY00'] * 0.02, 2)
    row['HY15'] = round(row['HY15'] * 0.02, 2)
    row['HY30'] = round(row['HY30'] * 0.02, 2)
    row['HY45'] = round(row['HY45'] * 0.02, 2)

    row['FS00'] = round(row['FS00'] * 0.1, 2) if row['FS00'] < 256 else round((row['FS00'] - 255) * 0.2 + 25.5, 2)
    row['FS10'] = round(row['FS10'] * 0.1, 2) if row['FS10'] < 256 else round((row['FS10'] - 255) * 0.2 + 25.5, 2)
    row['FS20'] = round(row['FS20'] * 0.1, 2) if row['FS20'] < 256 else round((row['FS20'] - 255) * 0.2 + 25.5, 2)
    row['FS30'] = round(row['FS30'] * 0.1, 2) if row['FS30'] < 256 else round((row['FS30'] - 255) * 0.2 + 25.5, 2)
    row['FS40'] = round(row['FS40'] * 0.1, 2) if row['FS40'] < 256 else round((row['FS40'] - 255) * 0.2 + 25.5, 2)
    row['FS50'] = round(row['FS50'] * 0.1, 2) if row['FS50'] < 256 else round((row['FS50'] - 255) * 0.2 + 25.5, 2)


    row['FX00'] = round(row['FX00'] * 2.8125, 2)
    row['FX10'] = round(row['FX10'] * 2.8125, 2)
    row['FX20'] = round(row['FX20'] * 2.8125, 2)
    row['FX30'] = round(row['FX30'] * 2.8125, 2)
    row['FX40'] = round(row['FX40'] * 2.8125, 2)
    row['FX50'] = round(row['FX50'] * 2.8125, 2)

    row['ZBWD'] = row['ZBWD'] - 20
    row['DYDY'] = row['DYDY'] * 0.2 + 5
    row['DWHZTZT'] = row['DWHZTZT']
    row['ZTHGJD'] = row['ZTHGJD'] - 180
    row['ZTFYJD'] = row['ZTFYJD'] - 90
    row['BWJYH'] = hex(row['BWJYH']).upper().replace('X', 'x')

    if not extra['COMPACT']:
        row['BDTXZLBS'] = row['BDTXZLBS'].decode('utf-8')
        row['XXLB'] = ''.join(['0x', row['XXLB'].upper()])
        row['FXSJ'] = datetime.time(row['FXSJS'], row['FXSJF']).strftime('%H:%M')
        row['CRCBZ'] = ''.join(['0x', row['CRCBZ'].upper()])
        row['TXBSJYH'] = ''.join(['0x', row['TXBSJYH'].upper()])

    intervals = {'QW':10, 'QY':30, 'HW':15, 'HY':15, 'FS':10, 'FX':10}
    elements = ['QW', 'QY', 'HW', 'HY', 'FS', 'FX']
    basemin = extra['GCRQSJ'].minute if extra['GCRQSJ'].minute > 0 else 60
    for element in elements:
        step = intervals[element]
        min_iter = 0
        while min_iter < basemin:
            try:
                if not row[''.join([x for x in (element, ('0' + str(min_iter))[-2:], 'ZK')])]:
                    row_element = {}
                    row_element['PLFBQZH'] = extra['PLFBQZH']
                    row_element['GCRQSJ'] = extra['GCRQSJ'].replace(minute=min_iter)
                    row_element[element] = row[''.join([x for x in [element, ('0' + str(min_iter))[-2:]]])]
                    row_element['JLWJMC'] = extra['JLWJMC']
                    extra[element].append(row_element)
            except KeyError:
                continue
            finally:
                min_iter += step

    # 解析YXZT, DWHZTZT两个字段，并把值写入nt_plfbztxxb
    row_element = {}
    row_element['PLFBQZH'] = extra['PLFBQZH']
    row_element['GCRQSJ'] = extra['GCRQSJ']
    row_element['JMZT'] = row['JMZT']
    row_element['ZBWD'] = round(row['ZBWD'], 2)
    row_element['DYDY'] = round(row['DYDY'], 2)
    row_element['ZTHGJD'] = row['ZTHGJD']
    row_element['ZTFYJD'] = row['ZTFYJD']
    row_element['ZTFWJD'] = row['ZTFWJD']
    row_element['CXCQW'] = int(row['YXZT'][-1], 2)
    row_element['KMGGLW'] = int(row['YXZT'][-2], 2)
    row_element['ZTSJJSW'] = int(row['DWHZTZT'][0], 2)
    row_element['ZTSJYXW'] = int(row['DWHZTZT'][1], 2)
    row_element['GPSSJJSW'] = int(row['DWHZTZT'][2], 2)
    row_element['GPSJSSJYXW'] = int(row['DWHZTZT'][3], 2)
    row_element['GPSDWSJYXW'] = int(row['DWHZTZT'][4], 2)
    row_element['YXJSWXS'] = int(row['DWHZTZT'][-3:], 2)
    row_element['JD'] = row['JD'] if row['JDBQ'] == 'E' else -row['JD']
    row_element['WD'] = row['WD'] if row['WDBQ'] == 'N' else -row['WD']
    extra['ZTXX'].append(row_element)

    # 用nt_plfbgcsjb中的经度、纬度字段值重写nt_plfbjbcsb中的经纬度字段
    try:
        extra['JBCS'][-1]['JD'] = row['JD'] if row['JDBQ'] == 'E' else -row['JD']
        extra['JBCS'][-1]['WD'] = row['WD'] if row['WDBQ'] == 'N' else -row['WD']
    except KeyError:
        pass

def parse_plfbqwsjb(row):
    pass

def parse_plfbqysjb(row):
    pass

def parse_plfbhwsjb(row):
    pass

def parse_plfbhysjb(row):
    pass

def parse_plfbfssjb(row):
    pass

def parse_plfbfxsjb(row):
    pass

def parse_plfbztxxb(row):
    pass

if __name__ == '__main__':
    parse_plfbgcsjb(0, 0, 0)