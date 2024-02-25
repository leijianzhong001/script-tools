# -*- coding:utf-8 -*-
import os
import traceback

import openpyxl
import requests
from concurrent.futures import ThreadPoolExecutor
import re
from openpyxl.workbook import Workbook
import pymysql.cursors

"""
导出所有的订阅链路到excel中
"""

COOKIE_STR = 'experimentation_subject_id=IjFjYjNlNTYzLWRiZTAtNDM3ZC05MzFlLTk4OGY3MjZkYmI1MSI%3D--642f87fac21ad96137b0ca2b46f781df1d1781bf; tradeMA=68; idsLoginUserIdLastTime=20013921; isso_ld=true; isso_us=20013921; scm-prd=siC07E171F39F366D7AD3261D61FEAE5EA; custno=20013921; _snma=1%7C170702760811448759%7C1707027608114%7C1708581444417%7C1708584359850%7C7%7C4; _snmp=17085843597737784; _snmb=170858435985351870%7C1708584359863%7C1708584359854%7C1; _snvd=1708581338362qBSqLmb+EpD; secureToken=CA7E0CBB18245FB714061EFBE851DDCA; JSESSIONID=m4vvv83m8tdc2o94driysm46; authId=si930B0630C695308B274EEBD35CFB33CF'
URL_GET_TASK_INFO = 'http://rdrsmgr2.cnsuning.com/mgr/taskInfo/getTaskInfoByKind'
URL_GET_TPS_INFO = 'http://rdrsmgr2.cnsuning.com/mgr/taskInfo/getTpsInfo'
URL_GET_CONFIG_INFO = 'http://rdrsmgr2.cnsuning.com/mgr/taskInfo/getConfigInfo'
URL_GET_IP_INFO = 'http://itsm.cnsuning.com/traffic-web-in/api/getServerByIpInfo.htm'
URL_EXECUTE_SQL = 'http://rdrsmgr2.cnsuning.com/mgr/serverProxy/getSqlExecute'
PAGE_SIZE = 100
EXCEL_FILE = 'D:\\file\\202402\\订阅链路导出.xlsx'
vip_info = {}


def get_vip_from_task_name(task_name):
    """
    从taskName中获取vip
    :return:
    """
    # 找到上面字符串中包含的ip
    return re.findall(r"\d+.\d+.\d+.\d+", task_name)[0]


def execute_sql(sql_statement):
    """
    保存到数据库
    :return:
    """
    if sql_statement is None:
        return None

    # 连接到MySQL数据库
    conn = pymysql.connect(
        host="10.237.217.107",
        user="fabu",
        password="2d8VfEk6jFD6",
        database="snrs_sit"
    )

    # 创建游标对象
    cursor = conn.cursor()

    # 执行SQL查询
    cursor.execute(sql_statement)

    if 'select' in sql_statement:
        # 获取所有记录列表
        results = cursor.fetchall()
        return results
    if 'insert' in sql_statement or 'update' in sql_statement or 'delete' in sql_statement:
        conn.commit()
        # 返回影响的行数
        return cursor.rowcount

    # 关闭游标和数据库连接
    cursor.close()
    conn.close()


def init_vip_info():
    global vip_info
    rows = execute_sql('select * from vip_from_itsm;')
    # 字典推导为一个key为vip， 值为dict的字典
    for row in rows:
        vip = row[0]
        if vip_info.get(vip) is None:
            vip_info[vip] = []
        db_list = vip_info[vip]
        row_dict = {'vip': row[0], 'ip': row[1], 'role': row[2], 'sys_en': row[3], 'sys_cn': row[4], 'bk_ip': row[5], 'err_msg': row[6]}
        db_list.append(row_dict)
    print('vip信息初始化完成！')


def set_consistent(link_info):
    """
    从itsm查备份网络ip对应的系统名称，并比较是否和当前系统一致
    :param link_info:
    :return:
    """
    rdrs_bk_ip = link_info.get('bkIp').strip()
    rdrs_sys_en = link_info.get('systemName').strip()
    task_name = link_info.get('taskName').strip()
    vip = get_vip_from_task_name(task_name).strip()

    if vip not in vip_info:
        print(f'{task_name} vip信息不存在！\n', end='')
        link_info['consistent'] = False
        link_info['consistent_reason'] = 'vip信息不存在'
        return

    db_list = vip_info.get(vip)
    sys_en_list = [db.get('sys_en') for db in db_list]
    link_info['itsmSystem'] = ','.join(f'{a}' for a in sys_en_list)
    # 将bk_ip_list中的所有元素拼接成一个字符串
    bk_ip_list = [db.get('bk_ip') for db in db_list]
    link_info['itsmBkIp'] = ','.join(f'{a}' for a in bk_ip_list)
    link_info['consistent'] = True
    link_info['consistent_reason'] = ''
    for db in db_list:
        sys_en = db.get('sys_en')
        if sys_en is None:
            link_info['consistent'] = False
            link_info['consistent_reason'] = db.get('err_msg')
            return

        if rdrs_sys_en != sys_en:
            link_info['consistent'] = False
            link_info['consistent_reason'] = 'vip已不归属于当前系统'
            return

    db_ip = [db.get('ip') for db in db_list]
    if rdrs_bk_ip not in bk_ip_list and rdrs_bk_ip not in db_ip and rdrs_bk_ip != vip:
        link_info['consistent'] = False
        link_info['consistent_reason'] = '备份网络ip已变更'


def get_config_info_0(link_info):
    """
    获取链路的配置信息
    :param link_info: 链路信息
    :return:
    """
    try:
        task_name = link_info.get('taskName')
        param = {'taskName': task_name}
        response = requests.get(URL_GET_CONFIG_INFO, cookies=cookies, headers=headers, params=param)
        if response.status_code != 200:
            link_info['bkIp'] = 'NA'
            return
        response_dict = response.json()
        endpoint_code = response_dict.get('code')
        if str(endpoint_code) != '200':
            link_info['bkIp'] = 'NA'
            return

        database_hostname = response_dict.get('result').get('database.hostname')
        link_info['bkIp'] = database_hostname
        set_consistent(link_info)
    except Exception as e:
        link_info['consistent'] = False
        link_info['consistent_reason'] = f'发生异常{e}'
        traceback.print_exc()


def set_config_info(link_list):
    """
    获取链路对应的tps信息
    :param link_list: 链路列表
    :return:
    """
    executor = ThreadPoolExecutor(max_workers=1)
    # map方法会将link_list中的每个元素传递给get_tps_info_0函数
    executor.map(get_config_info_0, link_list)
    # 这里等待所有任务执行完毕再返回这个函数
    executor.shutdown()


def cookie_to_dict():
    """
    将字符串形式的cookie转为字典格式
    :return:
    """
    # 列表推导
    cookie_entries = [(entry.strip().split('=')[0], entry.strip().split('=')[1]) for entry in COOKIE_STR.split(';')]
    # 字典推导
    cookie_dict = {k: v for k, v in cookie_entries}
    return cookie_dict


cookies = cookie_to_dict()
headers = {'Content-Type': 'application/json'}


def get_page_link_data(page, size, kind):
    """
    获取分页数据
    :param kind:
    :param page: 页号
    :param size: 每页数据条数
    :return: 该页数据
    """
    link_page_data_param = {'taskNo': '', 'kind': kind, 'taskName': '', 'status': '', 'system': '', 'page': page,
                            'size': size}
    # 在Cookie Version 0中规定空格、方括号、圆括号、等于号、逗号、双引号、斜杠、问号、@，冒号，分号等特殊符号都不能作为Cookie的内容。
    response = requests.get(URL_GET_TASK_INFO, cookies=cookies, headers=headers, params=link_page_data_param)
    if response.status_code != 200:
        return None
    response_dict = response.json()
    endpoint_code = response_dict.get('code')
    if str(endpoint_code) != '200':
        return None
    result = response_dict.get('result')
    data = result.get('data')
    if data is None or len(data) == 0:
        return None
    total = result.get('total')
    size = result.get('size')
    return data, total, size


def get_inc_link_list():
    """
    获取订阅链路列表
    :return:
    """
    link_list = []
    # 先探测一下数据总条数
    page_data = get_page_link_data(1, 10, '-INC-')
    if page_data is None:
        return link_list
    total = page_data[1]
    print()
    print('链路类型: {}'.format('订阅链路'))
    print('链路总量: {}'.format(total))
    print('每页数量: {}'.format(PAGE_SIZE))
    print('最大页数: {}'.format(int(total / PAGE_SIZE) + 2))
    print('开始获取页数据...')
    # 下面的代码和  for (int i = 1; i <= total / PAGE_SIZE + 1; i++)  是等价的
    for page_no in range(1, int(total / PAGE_SIZE) + 2):
        page_data = get_page_link_data(page_no, PAGE_SIZE, '-INC-')
        if page_data is None:
            continue
        page_link_list = page_data[0]
        # 设置链路的配置信息
        set_config_info(page_link_list)
        # 获取链路的任务号，系统总监
        link_list.extend(page_link_list)
        print('当前页号: {} 数据获取完成'.format(page_no))
    print('订阅链路所有页数据获取完成！')
    return link_list


def get_sink_link_list():
    """
    获取同步链路列表
    :return:
    """
    link_list = []
    # 先探测一下数据总条数
    page_data = get_page_link_data(1, 10, '-SINK-')
    if page_data is None:
        return link_list
    total = page_data[1]
    print()
    print('链路类型: {}'.format('同步链路'))
    print('链路总量: {}'.format(total))
    print('每页数量: {}'.format(PAGE_SIZE))
    print('最大页数: {}'.format(int(total / PAGE_SIZE) + 2))
    print('开始获取页数据...')
    # 下面的代码和  for (int i = 1; i <= total / PAGE_SIZE + 1; i++)  是等价的
    for page_no in range(1, int(total / PAGE_SIZE) + 2):
        page_data = get_page_link_data(page_no, PAGE_SIZE, '-SINK-')
        if page_data is None:
            continue
        page_link_list = page_data[0]
        # 设置链路的配置信息
        set_config_info(page_link_list)
        link_list.extend(page_link_list)
        print('当前页号: {} 数据获取完成'.format(page_no))
    print('同步链路所有页数据获取完成！')
    return link_list


def write_sink_link_list(wb, max_no, sink_link_list):
    """
    :param wb: 工作簿
    :param max_no: sheet页最大序号，用于创建新的sheet页
    :param sink_link_list: 同步链路列表
    """
    sink_sheet_name = "{}-{}".format('同步链路', max_no)
    ws = wb.create_sheet(sink_sheet_name)
    print('创建sheet页: {}'.format(sink_sheet_name))
    header = ['taskId', 'taskName', 'taskStatus', 'systemName', 'clusterName', 'workIp', 'startTime', 'maxBinlogFile',
              'nowBinlogFile', 'minBinlogFile', 'nowPosition', 'trace', 'delay', 'delayOfnum', 'haveData', 'tps', 'vip',
              'realSystemName', 'consistent']
    ws.append(header)
    for link_dict in sink_link_list:
        row = [link_dict.get(key) for key in header]
        ws.append(row)
    wb.save(EXCEL_FILE)
    print('写入 {} 条同步链路数据到excel成功！'.format(len(sink_link_list)))


def write_link_list(inc_link_list):
    """
    写入link_list到excel中
    :param inc_link_list: 订阅链路列表
    :return: 工作表
    """
    wb = Workbook()
    if os.path.exists(EXCEL_FILE):
        # 存在则使用已存在的文件
        wb = openpyxl.load_workbook(EXCEL_FILE)
    sheet_name = '订阅链路'
    if sheet_name in wb.sheetnames:
        # 存在则删除
        wb.remove(wb[sheet_name])
    ws = wb.create_sheet(sheet_name)
    print('创建sheet页: {}'.format(sheet_name))

    header = ['taskId', 'taskName', 'taskStatus', 'systemName', 'clusterName', 'workIp', 'startTime', 'maxBinlogFile',
              'nowBinlogFile', 'minBinlogFile', 'nowPosition', 'trace', 'delay', 'delayOfnum', 'haveData', 'tps',
              'bkIp', 'itsmSystem', 'itsmBkIp', 'consistent', 'consistent_reason']
    ws.append(header)
    for link_dict in inc_link_list:
        row = [link_dict.get(key) for key in header]
        ws.append(row)
    wb.save(EXCEL_FILE)
    print('写入 {} 条订阅链路数据到excel成功！'.format(len(inc_link_list)))


def main():
    # 初始化vip信息
    init_vip_info()
    # 获取全量的链路列表
    inc_link_list = get_inc_link_list()
    # 写入链路列表到excel中
    write_link_list(inc_link_list)


if __name__ == '__main__':
    main()
