#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import syncdb
import datetime
import os
import gc
from windrose import plot_windrose
import logging
from sys import stdout
import matplotlib.dates as mdates
from docxtpl import DocxTemplate
from docxtpl import InlineImage
from docx.shared import Mm
import folium
import folium.plugins as plugins

def plotpie_by_buoy(dstart, dend, yieldpath, engine, **kwargs):
    logger = logging.getLogger(__name__)
    errors = 0
    plt.style.use('seaborn-white')
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = 'Hiragino Sans GB'
    plt.rcParams['axes.unicode_minus'] = False
    try:
        valid_buoys = kwargs.get('args_buoys') or kwargs.get('valid_buoys') or ''
        sql_snippet = 'and plfbqzh in (' + valid_buoys + ')' if valid_buoys else ''
        # valid_buoys = 'and plfbqzh in (' + kwargs.get('valid_buoys') + ')' if kwargs.get('valid_buoys') else ''
        sql_pies = 'select plfbqzh,gcrqsj from nt_plfbgcsjb where gcrqsj >= "{:}" and gcrqsj < "{:}" ' \
                   'and time_format(gcrqsj, "%i") = time_format("00", "%i") ' \
                   ' {:} order by gcrqsj;'.format(dstart.strftime("%Y-%m-%d %H:%M"), dend.strftime("%Y-%m-%d %H:%M"),
                                                  sql_snippet)
        logger.debug(sql_pies)
        df = pd.read_sql_query(sql_pies, engine)
    except:
        errors += 1
        logger.exception("查询浮标观测数据表时出错，饼图绘制失败")
        return errors
    try:
        if df.empty:
            logger.debug('空数据集')
            return errors
        dfpivot = df.pivot(columns='plfbqzh', values='plfbqzh', index='gcrqsj')
        dfcount = dfpivot.isnull().sum()
        rowcount = dfpivot.shape[0]
        rows = []
        pies_path = os.path.join(yieldpath, 'pies')
        # html_path = os.path.join(yieldpath, 'html')
        if not os.path.exists(pies_path):
            os.mkdir(pies_path)
        # if not os.path.exists(html_path):
        #     os.mkdir(html_path)
        # fig, ax = plt.subplots()
    except:
        errors += 1
        logger.exception("处理饼图数据时出错，饼图绘制失败")
        return errors

    for column in dfpivot.columns:
        try:
            divisor = rowcount - dfcount[column]
            dividend = 24
            labels = ["缺报率", "到报率"]
            rows.append({'buoy': column, 'dropcount': dividend - divisor, 'rcvcount': divisor})
            sizes = [dividend - divisor, divisor]
            colors = ['lightcoral', 'yellowgreen']
            explode = [0.1, 0]
            patches, texts, juck = plt.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%', colors=colors,
                                     shadow=True, startangle=140)
            # plt.legend(patches, labels, loc="best")
            plt.legend(patches, [x + str(y) + '条' for x, y in zip(['缺报: ', '到报: '], sizes)], loc="upper right",
                       bbox_to_anchor=(0.3, 0.18))
            plt.axis('equal')
            plt.title('漂流浮标{:} {:} 至 {:} '.format(column, dstart.strftime("%-m月%-d日%H时"),
                                                            dend.strftime("%-m月%-d日%H时"),),
                      fontsize=20, fontweight='bold')
            # plt.tight_layout()
            figname = '_'.join([x for x in (column, dstart.strftime("%-m月%-d日%H时"),
                                            dend.strftime("%-m月%-d日%H时"),)])
            plt.savefig(os.path.join(pies_path, figname), dpi=300)
            plt.cla()
            plt.clf()
            plt.close()
        except:
            errors += 1
            logger.exception("绘制饼图 %s 时出错", figname)

    gc.collect()
    return errors



def plotspline_by_item(item, dstart, dend, yieldpath, engine, **kwargs):
    logger = logging.getLogger(__name__)
    errors = 0
    plt.style.use('seaborn-white')
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = 'Hiragino Sans GB'
    plt.rcParams['axes.unicode_minus'] = False
    params = {'legend.fontsize': 'x-large',
              'axes.labelsize': 'x-large',
              'xtick.labelsize': 'x-large',
              'ytick.labelsize': 'x-large'}
    plt.rcParams.update(params)
    itemname = item['itemname']
    tblname = item['tblname']
    freq = item['freq']
    try:
        item_buoys = kwargs.get(itemname + '_buoys') or ''
        valid_buoys = item_buoys if item_buoys else (kwargs.get('args_buoys') or kwargs.get('valid_buoys') or '')
        sql_snippet = 'and plfbqzh in (' + valid_buoys + ')' if valid_buoys else ''
        sql_splines = 'select plfbqzh,gcrqsj,{:} from {:} where gcrqsj >= "{:}" and gcrqsj < "{:}" {:}' \
               'order by gcrqsj;'.format(itemname, tblname, dstart.strftime("%Y-%m-%d %H:%M"),
                                         dend.strftime("%Y-%m-%d %H:%M"), sql_snippet)
        # print(sqls)
        # df = pd.read_sql_query('select plfbqzh,gcrqsj,qw from nt_plfbqwsjb where  '
        #                       'gcrqsj >= "2017-05-30 20:00:00" order by gcrqsj;', engine)

        df = pd.read_sql_query(sql_splines, engine)
    except:
        errors += 1
        logger.exception("从%s中读取%s值时出错，绘制折线图失败", tblname, itemname)
        return errors

    if df.empty:
        logger.debug('空数据集!')
        return errors

    try:
        dfpivot = df.pivot(index='gcrqsj', columns='plfbqzh', values=itemname)
        # print(dfpivot[dfpivot.columns[0:1]])
        # dfint = dfpivot.resample('T').interpolate('cubic')
        # print(dfpivot.index)
        xticks = pd.date_range(start=dstart, end=dend, freq=freq)
        dfpivot = dfpivot.reindex(xticks)
    except:
        errors += 1
        logger.exception("处理%s要素数据时出错,绘制折线图失败", itemname)
        return errors

    splines_path = os.path.join(yieldpath, 'splines')
    rosecharts_path = os.path.join(yieldpath, 'rosecharts')
    # html_path = os.path.join(yieldpath, 'html')
    if not os.path.exists(splines_path):
        os.mkdir(splines_path)
    if not os.path.exists(rosecharts_path):
        os.mkdir(rosecharts_path)
    # if not os.path.exists(html_path):
    #     os.mkdir(html_path)
    try:
        fig, ax = plt.subplots()
        dfpivot.index.name = '观测日期和时间'
        xticksperhour = pd.date_range(start=dstart, end=dend, freq='H')
        styles = ['v-', 'D-', 'x-', 'p-', '*-', '|-', 's-', 'c-', 'd-', ',-', 'H+']
        dfpivot.plot(grid='on', ax=ax, figsize=(16, 7), rot=0, style=styles, xticks=xticksperhour.to_pydatetime(),
                     markersize=3, markerfacecolor='green')
        ax.legend([x for x in dfpivot.columns], loc='center left', bbox_to_anchor=(1, 0.5), frameon=True)
        ax.xaxis.grid(True, which='minor')
        ax.xaxis.grid(True, which='major')
        ax.yaxis.grid(True, which='minor')
        if kwargs.get('major_xlabels'):
            ax.set_xticklabels(kwargs.get('major_xlabels'), ha='center')

        plt.ylabel(item['ylabel'])
        plt.title('{:} 至 {:} {:}监测'.format(dstart.strftime("%-m月%-d日%H时"), dend.strftime("%-m月%-d日%H时"),
                                           item['title']), fontsize=20, fontweight='bold')
        figname = '_'.join([x for x in (item['title'], dstart.strftime("%m-%d-%H_"),
                                        dend.strftime("%m-%d-%H"))])
        plt.savefig(os.path.join(splines_path, figname), dpi=300)
    except KeyError:
        errors += 1
        logger.warning("绘制 %s 时出错！跳过该文件继续执行...", '_'.join([x for x in (itemname, dstart.strftime("%m-%d-%H_"),
                                                                       dend.strftime("%m-%d-%H"))]))

    fig.clf()
    plt.close(fig)
    gc.collect()

    if item['itemname'] == 'fs':
        for column in dfpivot.columns:
            try:
                sql_wrm = 'select s.gcrqsj,s.fs,d.fx from nt_plfbfssjb s, nt_plfbfxsjb d where s.plfbqzh ="{:}" and ' \
                          's.gcrqsj >= "{:}" and s.gcrqsj < "{:}" and ' \
                          's.plfbqzh = d.plfbqzh and s.gcrqsj = d.gcrqsj'.format(column,
                                                                                 dstart.strftime("%Y-%m-%d %H:%M"),
                                                                                 dend.strftime("%Y-%m-%d %H:%M"))
                wind_df_by_buoy = pd.read_sql_query(sql_wrm, engine)
                wind_df_by_buoy.index = wind_df_by_buoy.gcrqsj
                if not wind_df_by_buoy[wind_df_by_buoy.fs > 0].empty:
                    ax = plot_windrose(wind_df_by_buoy, kind='bar', var_name='fs', direction_name='fx',
                                       bins=6, clean_flag=False)
                    ax.legend(loc='best', frameon=True, prop={'size':9})
                    for text in ax.get_legend().get_texts():
                        text.set_text(text.get_text()[:-1] + ']')
                    figname = ''.join([x for x in (column, dstart.strftime(" %m-%d %H"), '时 至 ',
                                                dend.strftime(" %m-%d %H"),
                                                '时 风玫瑰图')])
                    plt.title(figname, fontsize=20, fontweight='bold')
                    plt.savefig(os.path.join(rosecharts_path, figname), dpi=300)
                    plt.close()
            except:
                errors += 1
                logger.warning("绘制 %s 时出错！跳过该文件继续执行...", ''.join([x for x in (column,
                                                                              dstart.strftime("%m-%d %H:%M"),
                                                                              '时 至 ',
                                                                              dend.strftime("%m-%d %H:%M"),
                                                                              '时 风玫瑰图')]))
    plt.close('all')
    gc.collect()
    return errors


def plotfigs(yieldpath, startdate, enddate, **kwargs):
    logger = logging.getLogger(__name__)
    date_iter = startdate
    while date_iter < enddate:
        logger.debug("{:} --- {:}".format(date_iter.strftime("%Y-%m-%d %H:%M"),
                                          (date_iter + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M")))
        plotfigs_daily(date_iter, date_iter + datetime.timedelta(days=1), yieldpath, **kwargs)
        date_iter += datetime.timedelta(days=1)



def plotfigs_daily(dstart, dend, yieldpath, **kwargs):
    logger = logging.getLogger(__name__)
    errors = 0
    dirname = "_".join([x for x in (dstart.strftime("%Y年%-m月%-d日%H时"), dend.strftime("%Y年%-m月%-d日%H时"))])
    if not os.path.exists(os.path.join(yieldpath, dirname)):
        os.mkdir(os.path.join(yieldpath, dirname))
    else:
        logger.warning("发现已生成的报告文件夹，将覆盖原报告文件")
        # return
    major_xlabels = gen_xlabels(dstart, dend)
    engine = syncdb.getDBCon()
    if not (kwargs.get('args_buoys') or kwargs.get('valid_buoys')):
        try:
            qzh_df = pd.read_sql_query('select plfbqzh from nt_plfbjbcsb;', engine)
            qzh_df['plfbqzh'] = qzh_df.apply(lambda row: '"' + row['plfbqzh'] + '"', axis=1)
            kwargs['all_buoys'] = ','.join(qzh_df['plfbqzh'])
        except:
            logger.warning('获取所有区站号失败')

    errors += plot_wind_barbs(dstart, dend, os.path.join(yieldpath, dirname), engine, major_xlabels=major_xlabels, **kwargs)
    logger.debug("准备绘制到报率饼图")
    errors += plotpie_by_buoy(dstart, dend, os.path.join(yieldpath, dirname), engine, **kwargs)

    items = []
    items.append({'itemname': 'qw', 'tblname': 'nt_plfbqwsjb', 'legend': '气温 单位(℃)',
                  'ylabel': '观测气温值 单位(℃)', 'title': '气温', 'freq': '10T'})
    items.append({'itemname': 'qy', 'tblname': 'nt_plfbqysjb', 'legend': '气压 单位(hPa)',
                  'ylabel': '观测气压值 单位(hPa)', 'title': '气压', 'freq': '30T'})
    items.append({'itemname': 'hw', 'tblname': 'nt_plfbhwsjb', 'legend': '海温 单位(℃)',
                  'ylabel': '观测海温值 单位(℃)', 'title': '海温', 'freq': '15T'})
    items.append({'itemname': 'hy', 'tblname': 'nt_plfbhysjb', 'legend': '海盐 单位(mS/cm)',
                  'ylabel': '观测海盐值 单位(mS/cm)', 'title': '海盐', 'freq': '15T'})
    items.append({'itemname': 'fs', 'tblname': 'nt_plfbfssjb', 'legend': '风速 单位(m/s)',
                  'ylabel': '观测风速值 单位(m/s)', 'title': '风速', 'freq': '10T'})
    items.append({'itemname': 'fx', 'tblname': 'nt_plfbfxsjb', 'legend': '风向 单位(deg)',
                  'ylabel': '观测风向值 单位(deg)', 'title': '风向', 'freq': '10T'})
    items.append({'itemname': 'jmzt', 'tblname': 'nt_plfbgcsjb', 'legend': '浸没状态(次数)',
                  'ylabel': '浸没次数', 'title': '浸没状态', 'freq': 'H'})
    items.append({'itemname': 'zbwd', 'tblname': 'nt_plfbgcsjb', 'legend': '主板温度(℃)',
                  'ylabel': '主板温度', 'title': '主板温度', 'freq': 'H'})
    items.append({'itemname': 'dydy', 'tblname': 'nt_plfbgcsjb', 'legend': '电源电压(V)',
                  'ylabel': '电源电压(V)', 'title': '电源电压', 'freq': 'H'})
    items.append({'itemname': 'zthgjd', 'tblname': 'nt_plfbgcsjb', 'legend': '姿态横滚角度(deg)',
                  'ylabel': '姿态横滚角度(deg)', 'title': '姿态横滚角度', 'freq': 'H'})
    items.append({'itemname': 'ztfyjd', 'tblname': 'nt_plfbgcsjb', 'legend': '姿态俯仰角度(deg)',
                  'ylabel': '姿态俯仰角度(deg)', 'title': '姿态俯仰角度', 'freq': 'H'})
    items.append({'itemname': 'ztfwjd', 'tblname': 'nt_plfbgcsjb', 'legend': '姿态方位角度(deg)',
                  'ylabel': '姿态方位角度(deg)', 'title': '姿态方位角度', 'freq': 'H'})

    for item in items:
        logger.debug("准备绘制%s折线图", item['title'])
        errors += plotspline_by_item(item, dstart, dend, os.path.join(yieldpath, dirname), engine,
                                     major_xlabels=major_xlabels, **kwargs)

    errors += exporttbls_by_item({'itemname': 'yxzt,dwhztzt', 'tblname': 'nt_plfbgcsjb'}, dstart, dend,
                                 os.path.join(yieldpath, dirname), engine, **kwargs)
    export_docx(os.path.join(yieldpath, dirname), dstart, dend, **kwargs)
    export_track_html(dstart, dend, os.path.join(yieldpath, dirname), engine, **kwargs)
    if errors > 0:
        logger.warning("%s 报告生成完毕， 但发生 %s 个错误", dirname, str(errors))
    else:
        logger.info("%s报告生成完毕", dirname)


def export_docx(report_path, dstart, dend, **kwargs):
    tpl_path = kwargs.get('template_dir') or './template'
    tpl = DocxTemplate(os.path.join(tpl_path, 'report_tpl.docx'))
    context = {
        'datetime_interval' : " 至 ".join([x for x in (dstart.strftime("%Y年%-m月%-d日%H时"),
                                                      dend.strftime("%Y年%-m月%-d日%H时"))]),
        'buoys' : kwargs.get('args_buoys') or kwargs.get('valid_buoys') or kwargs.get('all_buoys', '')
    }
    subdirs = ['pies', 'barbs', 'splines', '']
    for subdir in ['pies', 'barbs', 'splines', 'rosecharts']:
        if os.path.exists(os.path.join(report_path, subdir)):
            subpath = os.path.join(report_path, subdir)
            for filename in os.listdir(subpath):
                filepath = os.path.join(subpath, filename)
                add_inline_image(tpl, context, subdir, filepath)
    tpl.render(context)
    report_filename = "_".join([x for x in (dstart.strftime("%Y年%-m月%-d日%H时"), dend.strftime("%Y年%-m月%-d日%H时"),
                                            '.docx')])

    tpl.save(os.path.join(report_path, report_filename))

def add_inline_image(tpl, context, subdir, filepath):
    if subdir == 'splines':
        if '气温' in filepath:
            image_list = context.setdefault('splines_qw', [])
        elif '气压' in filepath:
            image_list = context.setdefault('splines_qy', [])
        elif '海温' in filepath:
            image_list = context.setdefault('splines_hw', [])
        elif '海盐' in filepath:
            image_list = context.setdefault('splines_hy', [])
        elif '风速' in filepath:
            image_list = context.setdefault('splines_fs', [])
        elif '风向' in filepath:
            image_list = context.setdefault('splines_fx', [])
        elif '浸没' in filepath:
            image_list = context.setdefault('splines_jmzt', [])
        elif '主板温度' in filepath:
            image_list = context.setdefault('splines_zbwd', [])
        elif '电源电压' in filepath:
            image_list = context.setdefault('splines_dydy', [])
        elif '横滚' in filepath:
            image_list = context.setdefault('splines_hgjd', [])
        elif '俯仰' in filepath:
            image_list = context.setdefault('splines_fyjd', [])
        elif '方位' in filepath:
            image_list = context.setdefault('splines_fwjd', [])
        else:
            pass
        image_list.append(InlineImage(tpl, filepath, width=Mm(146.5), height=Mm(64.1)))
    elif subdir == 'pies':
        context.setdefault('pies', []).append(InlineImage(tpl, filepath, width=Mm(146.5), height=Mm(109.9)))
    elif subdir == 'barbs':
        context.setdefault('windbarbs', []).append(InlineImage(tpl, filepath, width=Mm(146.5), height=Mm(64.1)))
    elif subdir == 'rosecharts':
        context.setdefault('windrose', []).append(InlineImage(tpl, filepath, width=Mm(146.5), height=Mm(146.5)))
    else:
        pass


def export_track_html(dstart, dend, report_path, engine, **kwargs):
    logger = logging.getLogger(__name__)
    errors = 0
    try:
        valid_buoys = 'and plfbzh in (' + kwargs.get('valid_buoys') + ')' if kwargs.get('valid_buoys') else ''
        sql_for_tracking = 'select plfbqzh,gcrqsj,jd,jdbq,wd,wdbq from nt_plfbgcsjb where gcrqsj >= "{:}" and gcrqsj < "{:}" ' \
                           'and time_format(gcrqsj, "%i") = time_format("00", "%i") {:}'\
            .format(dstart.strftime("%Y-%m-%d %H:%M"), dend.strftime("%Y-%m-%d %H:%M"), valid_buoys)
        df = pd.read_sql_query(sql_for_tracking, engine)
    except:
        errors += 1
        logger.exception("查询浮标位置数据时出错")
        return errors

    try:
        if df.empty:
            logger.debug("空数据集")
            return errors
        df['jd'] = df.apply(lambda row: row['jd'] if row['jdbq'] == 'E' else -row['jd'], axis=1)
        df['wd'] = df.apply(lambda row: row['wd'] if row['wdbq'] == 'N' else -row['wd'], axis=1)
        df['latlng'] = list(zip(df.wd, df.jd))
        dfpivot = df.pivot(index='gcrqsj', columns='plfbqzh', values='latlng')
        xticks = pd.date_range(start=dstart, end=dend - datetime.timedelta(hours=1), freq='H')
        dfpivot = dfpivot.reindex(xticks)
    except:
        errors += 1
        logger.exception("处理浮标轨迹信息时出错")
        return errors

    map = folium.Map(location=[19.36, 121.56], zoom_start=4,
                     min_zoom=4, max_zoom=19,
                     tiles='http://t{s}.tianditu.cn/DataServer?T=vec_w&X={x}&Y={y}&L={z}',
                     subdomains=["0", "1", "2","3"],
                     attr='tian ditu')
    map.add_tile_layer(tiles='http://t{s}.tianditu.cn/DataServer?T=cva_w&X={x}&Y={y}&L={z}',
                       subdomains=["0", "1", "2", "3"],
                       attr='tian ditu label')
    # folium.Marker([21.2, 119], popup='<b>经度:</b>119<br/><b>纬度:</b>21.2').add_to(map)
    for qzh in dfpivot.columns:
        buoy_track = folium.PolyLine([item for item in dfpivot[qzh] if item and item[1] != 0],
                        popup=qzh,
                        weight=3,
                        color='purple',
                        dash_array='4,4')
        map.add_child(buoy_track)
        attr_dir = {'fill': 'purple', 'font-size': '24'}
        attr_txt = {'font-size': '12', 'orientation': 'flip'}
        # plugins.PolyLineTextPath(buoy_track, "> ", repeat=True, offset=9, attributes=attr_dir).add_to(map)
        plugins.PolyLineTextPath(buoy_track, qzh, offset=-5, attributes=attr_txt).add_to(map)
        for index, pos in dfpivot[qzh].iteritems():
            if pos and pos[1] != 0:
                folium.CircleMarker(location=pos, radius=5, color='tomato', fill=True,
                                    fill_color='tomato', fill_opacity=0.8,
                                    popup='<b>区站号:</b>{}<br/><b>到达时刻:</b>{}<br/><b>经度:</b>{}<br/><b>纬度:</b>{}'
                                    .format(qzh, index, pos[0], pos[1])).add_to(map)
    map.fit_bounds(buoy_track.get_bounds())
    html_filename = "_".join([x for x in (dstart.strftime("%Y年%-m月%-d日%H时"), dend.strftime("%Y年%-m月%-d日%H时"),
                                            '.html')])
    map.save(os.path.join(report_path, html_filename))
    return errors



def exporttbls_by_item(items, dstart, dend, yieldpath, engine, **kwargs):
    # logger = logging.getLogger('grapherporter__exporttbls_by_item[f]')
    # logger.setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    errors = 0
    itemname = items['itemname']
    tblname = items['tblname']
    try:
        valid_buoys = 'and plfbqzh in (' + kwargs.get('valid_buoys') + ')' if kwargs.get('valid_buoys') else ''
        sql_for_statuscode = 'select plfbqzh,gcrqsj,{:} from {:} where gcrqsj >= "{:}" and gcrqsj < "{:}" and ' \
                             'time_format(gcrqsj, "%i") = time_format("00", "%i") ' \
                             ' {:} order by plfbqzh;'.format(itemname, tblname, dstart.strftime("%Y-%m-%d %H:%M"),
                                                             dend.strftime("%Y-%m-%d %H:%M"), valid_buoys)
        df = pd.read_sql_query(sql_for_statuscode, engine)
    except:
        errors += 1
        logger.exception("查询浮标状态数据时出错")
        return errors

    try:
        if df.empty:
            return errors
        df.rename(columns={'plfbqzh': '漂流浮标区站号', 'gcrqsj': '观测日期时间'}, inplace=True)
        df['程序重启位'] = df.apply(lambda row: '程序重启' if row['yxzt'][-1] == '1' else '正常连续运行', axis=1)
        df['看门狗管理位'] = df.apply(lambda row: '管理正常' if row['yxzt'][-2] == '1' else '管理异常', axis=1)
        del df['yxzt']
        df['姿态数据接收位'] = df.apply(lambda row: '接收有效' if row['dwhztzt'][0] == '1' else '未接收到数据', axis=1)
        df['姿态数据有效位'] = df.apply(lambda row: '姿态有效' if row['dwhztzt'][1] == '1' else '姿态异常', axis=1)
        df['GPS数据接收位'] = df.apply(lambda row: '接收有效' if row['dwhztzt'][2] == '1' else '接收失败', axis=1)
        df['GPS校时数据有效位'] = df.apply(lambda row: '校时有效' if row['dwhztzt'][3] == '1' else '校时异常', axis=1)
        df['GPS定位数据有效位'] = df.apply(lambda row: '定位有效' if row['dwhztzt'][4] == '1' else '定位无效', axis=1)
        df['有效接收卫星数'] = df.apply(lambda row: int(row['dwhztzt'][-3:], 2), axis=1)
        del df['dwhztzt']
    except:
        errors += 1
        logger.exception("处理浮标状态数据时出错")
        return errors

    try:
        logger.debug("准备输出漂流浮标状态信息表...")
        xlsxpath = os.path.join(yieldpath, 'xlsx')
        if not os.path.exists(xlsxpath):
            os.mkdir(xlsxpath)

        xlsxname = '_'.join([x for x in ('状态参数表', dstart.strftime("%m-%d-%H_"), dend.strftime("%m-%d-%H"))])
        writer = pd.ExcelWriter(os.path.join(xlsxpath, xlsxname + '.xlsx'), engine='xlsxwriter')
        df.to_excel(writer, 'Sheet1')
        writer.save()
    except:
        errors += 1
        logger.exception("将浮标状态信息保存至xlsx文档时出错")
    return errors


def gen_xlabels(dStart, dEnd):
    dt_iter = dStart
    onehour = datetime.timedelta(hours=1)
    major_labels = []
    while dt_iter <= dEnd:
        if dt_iter.hour == 0:
            major_labels.append(dt_iter.strftime('%H:%M\n%-m月%-d日'))
        elif dt_iter.hour % 3 == 0:
            major_labels.append(dt_iter.strftime('%H:%M'))
        else:
            major_labels.append('')
        dt_iter += onehour
    return major_labels


def plot_wind_barbs(dstart, dend, yieldpath, engine, **kwargs):
    logger = logging.getLogger(__name__)
    errors = 0
    plt.style.use('seaborn-white')
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = 'Hiragino Sans GB'
    plt.rcParams['axes.unicode_minus'] = False
    params = {'legend.fontsize': 'x-large',
              'axes.labelsize': 'x-large',
              'xtick.labelsize': 'x-large',
              'ytick.labelsize': 'x-large'}
    plt.rcParams.update(params)
    itemname = 'fsfx'
    tblname = 'nt_plfb[fs|fx]sjb'
    freq = '10T'
    item_buoys = kwargs.get(itemname + '_buoys') or ''
    valid_buoys = item_buoys if item_buoys else (kwargs.get('args_buoys') or kwargs.get('valid_buoys')
                                                 or kwargs.get('all_buoys', ''))
    logger.debug(kwargs.get(itemname + '_buoys'))
    logger.debug(valid_buoys)
    for qzh in valid_buoys.split(','):
        if not qzh:
            continue
        try:
            sql_barbs = 'select a.gcrqsj, a.fs, b.fx from nt_plfbfssjb a LEFT JOIN nt_plfbfxsjb b ON ' \
                        'a.plfbqzh = b.plfbqzh and a.gcrqsj = b.gcrqsj where a.gcrqsj >= "{:}" and a.gcrqsj <= "{:}" ' \
                        'and a.plfbqzh = {:} order by a.gcrqsj;'.format(dstart.strftime("%Y-%m-%d %H:%M"),
                                                                        dend.strftime("%Y-%m-%d %H:%M"),
                                                                        qzh)
            logger.debug(sql_barbs)
            df = pd.read_sql_query(sql_barbs, engine, index_col='gcrqsj')
        except:
            errors += 1
            logger.exception("从%s中读取%s值时出错，绘制风羽图失败", tblname, itemname)
            return errors

        if df.empty or df[df.fs > 0].empty:
            logger.debug('空数据集!')
            continue

        try:
            # dfpivot = df.pivot(index='gcrqsj', columns='plfbqzh', values=itemname)
            # print(dfpivot[dfpivot.columns[0:1]])
            # dfint = dfpivot.resample('T').interpolate('cubic')
            # print(dfpivot.index)
            # df['fx'] = df['fx'].apply(lambda x: math.radians(x))
            df['fx_r'] = df.apply(lambda row: math.radians(row['fx']), axis=1)
            df['U'] = df.apply(lambda row: -row['fs'] * math.sin(row['fx_r']), axis=1)
            df['V'] = df.apply(lambda row: -row['fs'] * math.cos(row['fx_r']), axis=1)
            xticks = pd.date_range(start=dstart, end=dend, freq=freq)
            df = df.reindex(xticks)
        except:
            errors += 1
            logger.exception("处理%s要素数据时出错,绘制风羽图失败", itemname)
            return errors

        barbs_path = os.path.join(yieldpath, 'barbs')
        if not os.path.exists(barbs_path):
            os.mkdir(barbs_path)
        try:
            fig, ax = plt.subplots(1, 1, figsize=(16, 7))
            # df.index.name = '观测日期和时间'
            # ax.plot(df.index.values.astype('d'), df.fs, color='k')
            # logger.info(np.sqrt(df.U.values*df.U.values*144 + df.V.values*df.V.values*144))
            idx = mdates.date2num(df.index.to_pydatetime())
            ax.plot(idx, df.fs, 'v-', color='g', markersize=1)
            ax.barbs(idx, df.fs, df.U.values, df.V.values, barb_increments=dict(half=2, full=4),
                     length=8, pivot='middle', barbcolor=['b'])
            ax.xaxis.set_minor_locator(mdates.HourLocator(interval=3))
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
            ax.xaxis.set_minor_formatter(mdates.DateFormatter('%H:%M'))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('\n%-m月%-d日'))
            ax.xaxis.grid(True, which='minor')
            ax.xaxis.grid(True, which='major')
            ax.yaxis.grid(True, which='major')
            plt.ylabel('观测风速值 单位(m/s)')
            plt.xlabel('观测日期和时间')

            plt.title('{:} 至 {:} {:}站 风羽图'.format(dstart.strftime("%-m月%-d日%H时"), dend.strftime("%-m月%-d日%H时"),
                                                  qzh[1:-1]), fontsize=20, fontweight='bold')
            figname = '_'.join([x for x in (qzh[1:-1], dstart.strftime("%m-%d %H_"),
                                            dend.strftime("%m-%d %H"))])
            plt.savefig(os.path.join(barbs_path, figname), dpi=300)
        except KeyError:
            errors += 1
            logger.warning("绘制 %s 时出错！跳过该文件继续执行...", '_'.join([x for x in (itemname, dstart.strftime("%m-%d-%H-%M_"),
                                                                           dend.strftime("%m-%d-%H-%M"))]))
        fig.clf()
        plt.close(fig)
        gc.collect()
    plt.close('all')
    return errors


if __name__ == '__main__':
    dstart = datetime.datetime(2017, 9, 28, 20)
    dend = datetime.datetime(2017, 9, 29, 20)
    engine = syncdb.getDBCon()
    export_track_html(dstart, dend, '.', engine)
    exit(0)
    dirname = "_".join([x for x in (dstart.strftime("%Y年%-m月%-d日%H时"), dend.strftime("%Y年%-m月%-d日%H时"))])
    print(dirname)
    yieldpath = './yield/图表报告'
    if not os.path.exists(os.path.join(yieldpath, dirname)):
        os.mkdir(os.path.join(yieldpath, dirname))
    print(plot_wind_barbs(dstart, dend, os.path.join(yieldpath, dirname), engine, valid_buoys = '"S1042"'))