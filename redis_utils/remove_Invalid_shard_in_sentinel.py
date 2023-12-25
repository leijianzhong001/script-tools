# -*- coding:utf-8 -*-
import json
from redis import Redis
import requests
import time

sentinel_client_67 = Redis(host='10.237.188.67', port=26379, decode_responses=True, socket_timeout=1)
sentinel_client_68 = Redis(host='10.237.188.68', port=26379, decode_responses=True, socket_timeout=1)
sentinel_client_69 = Redis(host='10.237.188.69', port=26379, decode_responses=True, socket_timeout=1)


def shard_meta_check(sentinel_client, shard_name_list):
    url = 'http://snrsadmin.cnsuning.com/snrs/sql/safe'
    headers = {'Content-Type': 'application/json'}

    # 最终的检查报告
    exists_shard = []
    # 查询批次数量
    batch_num = 50
    # 查询语句
    sql_statement_t = 'SELECT GROUP_CONCAT(shardName) FROM all_shard a WHERE a.`shardName` IN({})'
    for i in range(0, len(shard_name_list), batch_num):
        batch_shards = shard_name_list[i:i + batch_num]
        sql_param = str(batch_shards).replace("[", '').replace(']', '')
        batch_sql_statement = sql_statement_t.format(sql_param)
        post_data = {
            "secretKey": "KiEyYYMdIhseLsMXbAhCmFHMjLwQqXrXRSrQfhsmZiHCKluxwZpPYuMHtJQjTZpp",
            "data": batch_sql_statement
        }
        json_data = json.dumps(post_data)
        response = requests.post(url, data=json_data, headers=headers)
        if response.status_code != 200:
            # 查询失败的一律认为存在
            exists_shard = exists_shard + batch_shards

        json_response = json.loads(response.text)
        csv_data = json_response['data'][1][0]
        exists_shard = exists_shard + csv_data.split(',')

    # 取差集
    unlawful_shard_list = list(set(shard_name_list) - set(exists_shard))
    unlawful_shard_list.sort()
    for unlawful_shard in unlawful_shard_list:
        rep = sentinel_client.sentinel_remove(unlawful_shard.strip())
        print('shard_meta_check', unlawful_shard, rep)


def shard_ip_check(sentinel_client, master_dict_list):
    url = 'http://itsm.cnsuning.com/traffic-web-in/api/getServerByIpInfo.htm'
    headers = {'Content-Type': 'application/json'}
    post_data = {
        "systemEnName": "SNRS",
        "key": "a4d9bef0270946c7b2b53999eb1488fa",
        "authType": 0
    }
    print('master_dict_list len: ', len(master_dict_list))
    for master_dict in master_dict_list:
        post_data['timeStamp'] = int(round(time.time() * 1000))
        post_data['ip'] = master_dict.get('address').split(":")[0]
        json_data = json.dumps(post_data)
        response = requests.post(url, data=json_data, headers=headers)
        if response.status_code != 200:
            continue
        json_response = json.loads(response.text)
        if json_response['code'] != 200:
            continue
        server_info_arr = json_response['datas']
        # if len(server_info_arr) != 0:
        #     continue
        print(master_dict.get('name'), server_info_arr)


def main():
    shard_name_list = []
    master_dict_list = []
    sentinel_client = sentinel_client_69
    rep = sentinel_client.info(section='sentinel')
    for idx, master_dict in rep.items():
        # 不是sentinel中master字典的内容跳过
        if not idx.startswith('master') or not isinstance(master_dict, dict):
            continue
        status = master_dict.get('status')
        if str(status).lower() == 'ok':
            continue
        shard_name_list.append(master_dict.get('name'))
        master_dict_list.append(master_dict)
    # shard_meta_check(sentinel_client, shard_name_list)
    shard_ip_check(sentinel_client, master_dict_list)


if __name__ == '__main__':
    main()
